package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"
	log "github.com/sirupsen/logrus"

	"frp/config"
	"frp/tunnel"
)

const maxConcurrentStreams = 512

type wssForwardListener struct {
	service  string
	listener net.Listener
}

// WSServer exposes loopback listeners and forwards them over one WSS session.
type WSServer struct {
	ctx    context.Context
	cancel context.CancelFunc

	httpServer   *http.Server
	httpListener net.Listener
	forwards     []wssForwardListener

	sessionMu sync.RWMutex
	session   *yamux.Session

	streamSlots chan struct{}
	wg          sync.WaitGroup
	stopOnce    sync.Once
}

// NewWSS creates the multiplexed WebSocket server selected by transport=wss_mux.
func NewWSS() *WSServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WSServer{
		ctx:         ctx,
		cancel:      cancel,
		streamSlots: make(chan struct{}, maxConcurrentStreams),
	}
}

// Start binds the WebSocket endpoint and one loopback listener per service.
func (s *WSServer) Start() error {
	serverConfig := config.C.Server
	for _, forward := range serverConfig.ForwardServers {
		listener, err := net.Listen("tcp", forward.ListenAddr)
		if err != nil {
			s.closeForwardListeners()
			return fmt.Errorf("listen for service %s on %s: %w", forward.ProxyServerName, forward.ListenAddr, err)
		}
		s.forwards = append(s.forwards, wssForwardListener{
			service:  forward.ProxyServerName,
			listener: listener,
		})
	}

	httpListener, err := net.Listen("tcp", serverConfig.ListenAddr)
	if err != nil {
		s.closeForwardListeners()
		return fmt.Errorf("listen for WebSocket tunnel on %s: %w", serverConfig.ListenAddr, err)
	}
	s.httpListener = httpListener

	mux := http.NewServeMux()
	mux.HandleFunc(serverConfig.WebSocketPath, s.handleWebSocket)
	mux.HandleFunc("/healthz", s.handleHealth)
	s.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.Serve(s.httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("WebSocket server stopped unexpectedly: %s", err)
			s.cancel()
		}
	}()

	for _, forward := range s.forwards {
		forward := forward
		s.wg.Add(1)
		go s.acceptForwardConnections(forward)
	}

	log.Infof("WSS mux server listening on %s with %d services", serverConfig.ListenAddr, len(s.forwards))
	return nil
}

// Stop closes the tunnel and all public loopback listeners.
func (s *WSServer) Stop() {
	s.stopOnce.Do(func() {
		s.cancel()
		if session := s.currentSession(); session != nil {
			_ = session.Close()
		}
		if s.httpServer != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.httpServer.Shutdown(shutdownCtx)
			cancel()
		}
		s.closeForwardListeners()
		s.wg.Wait()
	})
}

func (s *WSServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != config.C.Server.WebSocketPath {
		http.NotFound(w, r)
		return
	}
	requestHost := r.Host
	if host, _, err := net.SplitHostPort(r.Host); err == nil {
		requestHost = host
	}
	if !strings.EqualFold(requestHost, config.C.Server.ControlServerName) {
		http.NotFound(w, r)
		return
	}
	expected := "Bearer " + config.C.Token
	provided := r.Header.Get("Authorization")
	if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		http.NotFound(w, r)
		return
	}

	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{tunnel.Subprotocol},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		log.Warnf("accept WebSocket tunnel: %s", err)
		return
	}
	if wsConn.Subprotocol() != tunnel.Subprotocol {
		_ = wsConn.Close(websocket.StatusPolicyViolation, "subprotocol required")
		return
	}

	connCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	netConn := websocket.NetConn(connCtx, wsConn, websocket.MessageBinary)
	session, err := yamux.Server(netConn, tunnel.YamuxConfig())
	if err != nil {
		_ = netConn.Close()
		log.Warnf("create yamux server: %s", err)
		return
	}

	previous := s.replaceSession(session)
	if previous != nil {
		_ = previous.Close()
	}
	log.Infof("WSS tunnel connected")
	<-session.CloseChan()
	s.clearSession(session)
	_ = netConn.Close()
	log.Infof("WSS tunnel disconnected")
}

func (s *WSServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if session := s.currentSession(); session != nil && !session.IsClosed() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	http.Error(w, "tunnel unavailable", http.StatusServiceUnavailable)
}

func (s *WSServer) acceptForwardConnections(forward wssForwardListener) {
	defer s.wg.Done()
	for {
		conn, err := forward.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Warnf("accept service %s: %s", forward.service, err)
				continue
			}
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}
		select {
		case s.streamSlots <- struct{}{}:
			s.wg.Add(1)
			go s.forwardConnection(forward.service, conn)
		default:
			log.Warnf("reject service %s connection: stream limit reached", forward.service)
			_ = conn.Close()
		}
	}
}

func (s *WSServer) forwardConnection(service string, publicConn net.Conn) {
	defer s.wg.Done()
	defer func() { <-s.streamSlots }()
	defer publicConn.Close()

	session := s.currentSession()
	if session == nil || session.IsClosed() {
		return
	}
	stream, err := session.OpenStream()
	if err != nil {
		log.Warnf("open stream for %s: %s", service, err)
		return
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	if err := tunnel.WriteService(stream, service); err != nil {
		log.Warnf("write stream route for %s: %s", service, err)
		return
	}
	_ = stream.SetDeadline(time.Time{})
	tunnel.Bridge(publicConn, stream)
}

func (s *WSServer) replaceSession(session *yamux.Session) *yamux.Session {
	s.sessionMu.Lock()
	previous := s.session
	s.session = session
	s.sessionMu.Unlock()
	return previous
}

func (s *WSServer) clearSession(session *yamux.Session) {
	s.sessionMu.Lock()
	if s.session == session {
		s.session = nil
	}
	s.sessionMu.Unlock()
}

func (s *WSServer) currentSession() *yamux.Session {
	s.sessionMu.RLock()
	session := s.session
	s.sessionMu.RUnlock()
	return session
}

func (s *WSServer) closeForwardListeners() {
	for _, forward := range s.forwards {
		_ = forward.listener.Close()
	}
}
