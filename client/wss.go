package client

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"
	log "github.com/sirupsen/logrus"

	"frp/config"
	"frp/tunnel"
)

const clientMaxConcurrentStreams = 512

// WSSClient maintains one WSS/yamux session for every configured service.
type WSSClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	localServers map[string]string
	streamSlots  chan struct{}

	sessionMu sync.Mutex
	session   *yamux.Session

	wg       sync.WaitGroup
	streamWG sync.WaitGroup
	stopOnce sync.Once
}

// NewWSS creates the multiplexed WebSocket client selected by transport=wss_mux.
func NewWSS() *WSSClient {
	ctx, cancel := context.WithCancel(context.Background())
	localServers := make(map[string]string, len(config.C.Client.LocalServers))
	for _, local := range config.C.Client.LocalServers {
		localServers[local.ProxyServerName] = local.LocalAddr
	}
	return &WSSClient{
		ctx:          ctx,
		cancel:       cancel,
		localServers: localServers,
		streamSlots:  make(chan struct{}, clientMaxConcurrentStreams),
	}
}

// Start launches the reconnect loop without requiring the VPS to be online.
func (c *WSSClient) Start() error {
	c.wg.Add(1)
	go c.reconnectLoop()
	return nil
}

// Stop terminates the shared tunnel and waits for active streams.
func (c *WSSClient) Stop() {
	c.stopOnce.Do(func() {
		c.cancel()
		if session := c.swapSession(nil); session != nil {
			_ = session.Close()
		}
		c.wg.Wait()
		c.streamWG.Wait()
	})
}

func (c *WSSClient) reconnectLoop() {
	defer c.wg.Done()
	backoff := time.Second
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		connectedAt := time.Now()
		err := c.runSession()
		if c.ctx.Err() != nil {
			return
		}
		log.Warnf("WSS tunnel disconnected: %s", err)
		if time.Since(connectedAt) >= time.Minute {
			backoff = time.Second
		} else if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
		delay := jitter(backoff)
		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (c *WSSClient) runSession() error {
	clientConfig := config.C.Client
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, clientConfig.RemoteAddr)
		},
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: clientConfig.ControlServerName,
		},
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   false,
	}
	defer transport.CloseIdleConnections()

	endpoint := url.URL{
		Scheme: clientConfig.WebSocketScheme,
		Host:   clientConfig.ControlServerName,
		Path:   clientConfig.WebSocketPath,
	}
	header := make(http.Header)
	header.Set("Authorization", "Bearer "+config.C.Token)
	dialCtx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	wsConn, _, err := websocket.Dial(dialCtx, endpoint.String(), &websocket.DialOptions{
		HTTPClient:      &http.Client{Transport: transport},
		HTTPHeader:      header,
		Subprotocols:    []string{tunnel.Subprotocol},
		CompressionMode: websocket.CompressionDisabled,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("dial %s via %s: %w", endpoint.String(), clientConfig.RemoteAddr, err)
	}
	if wsConn.Subprotocol() != tunnel.Subprotocol {
		_ = wsConn.Close(websocket.StatusPolicyViolation, "subprotocol required")
		return fmt.Errorf("server did not negotiate %s", tunnel.Subprotocol)
	}

	netConn := websocket.NetConn(c.ctx, wsConn, websocket.MessageBinary)
	session, err := yamux.Client(netConn, tunnel.YamuxConfig())
	if err != nil {
		_ = netConn.Close()
		return fmt.Errorf("create yamux client: %w", err)
	}
	if previous := c.swapSession(session); previous != nil {
		_ = previous.Close()
	}
	defer func() {
		c.clearSession(session)
		_ = session.Close()
		_ = netConn.Close()
	}()
	log.Infof("WSS tunnel connected to %s", clientConfig.RemoteAddr)

	for {
		stream, err := session.AcceptStreamWithContext(c.ctx)
		if err != nil {
			return err
		}
		select {
		case c.streamSlots <- struct{}{}:
			c.streamWG.Add(1)
			go c.handleStream(stream)
		default:
			log.Warnf("reject tunnel stream: stream limit reached")
			_ = stream.Close()
		}
	}
}

func (c *WSSClient) handleStream(stream net.Conn) {
	defer c.streamWG.Done()
	defer func() { <-c.streamSlots }()
	defer stream.Close()

	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	service, err := tunnel.ReadService(stream)
	if err != nil {
		log.Warnf("read tunnel stream route: %s", err)
		return
	}
	localAddr, ok := c.localServers[service]
	if !ok {
		log.Warnf("reject unknown tunnel service %s", service)
		return
	}
	localConn, err := (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext(c.ctx, "tcp", localAddr)
	if err != nil {
		log.Warnf("connect service %s at %s: %s", service, localAddr, err)
		return
	}
	defer localConn.Close()
	_ = stream.SetDeadline(time.Time{})
	tunnel.Bridge(stream, localConn)
}

func (c *WSSClient) swapSession(session *yamux.Session) *yamux.Session {
	c.sessionMu.Lock()
	previous := c.session
	c.session = session
	c.sessionMu.Unlock()
	return previous
}

func (c *WSSClient) clearSession(session *yamux.Session) {
	c.sessionMu.Lock()
	if c.session == session {
		c.session = nil
	}
	c.sessionMu.Unlock()
}

func jitter(max time.Duration) time.Duration {
	if max <= time.Second {
		return max
	}
	half := max / 2
	random, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(half)))
	if err != nil {
		return max
	}
	return half + time.Duration(random.Int64())
}
