package server

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"io"
	mrand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"frp/config"
	"frp/utils"
)

var (
	httpReqData = []byte("GET / HTTP/1.1\r\nAccept: */*\n\r\n")
)

// Server define server
type Server struct {
	forwardServerMap sync.Map
	stopChan         chan struct{}
	listener         net.Listener
	wg               sync.WaitGroup
}

// New create the Server
func New() *Server {
	s := new(Server)
	s.stopChan = make(chan struct{}, 1)
	return s
}

// Start start the server
func (s *Server) Start() error {
	serverConfig := &config.C.Server
	serverCert, err := tls.LoadX509KeyPair(serverConfig.CertPath, serverConfig.KeyPath)
	if err != nil {
		return errors.Wrap(err, "load certificate")
	}

	certs := []tls.Certificate{}
	for _, fs := range serverConfig.ForwardServers {
		cert, err := tls.LoadX509KeyPair(fs.CertPath, fs.KeyPath)
		if err != nil {
			return errors.Wrap(err, "load certificate")
		}
		certs = append(certs, cert)

		f := newForwardServer(&s.wg, fs.ProxyServerName)
		s.forwardServerMap.Store(fs.ProxyServerName, f)
	}
	certs = append(certs, serverCert)
	tlsConfig := tls.Config{
		Certificates:       certs,
		Rand:               rand.Reader,
		GetConfigForClient: utils.SetTCPKeepAlive,
	}
	s.listener, err = tls.Listen("tcp", config.C.Server.ListenAddr, &tlsConfig)
	if err != nil {
		return errors.Wrap(err, "listen failed")
	}

	s.forwardServerMap.Range(func(key, value interface{}) bool {
		fs := value.(*ForwardServer)
		fs.start()
		return true
	})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.stopChan:
				log.Infof("receive signal to exit")
				return
			default:
			}
			con, err := s.listener.Accept()

			if err != nil {
				log.Warnf("accept failed: %s", err.Error())
				continue
			}
			go s.handleConn(con)
		}
	}()

	return nil
}

// Stop stop the server
func (s *Server) Stop() {
	log.Infof("stop the server...")
	s.stopChan <- struct{}{}
	_ = s.listener.Close()
	s.forwardServerMap.Range(func(key, value interface{}) bool {
		fs := value.(*ForwardServer)
		if fs != nil {
			fs.stop()
		}
		return true
	})
	s.wg.Wait()
	log.Infof("stop server completely")
}

func (s *Server) handleConn(conn net.Conn) {
	tlsConn := conn.(*tls.Conn)
	if err := tlsConn.Handshake(); err != nil {
		log.Warnf("handshake failed: %s", err.Error())
		_ = tlsConn.Close()
		return
	}

	serverName := tlsConn.ConnectionState().ServerName
	if serverName == config.C.Server.ControlServerName {
		s.parseConn(tlsConn)
	} else {
		fs, ok := s.forwardServerMap.Load(serverName)
		if !ok || fs == nil {
			if !ok {
				log.Warnf("server %s not support", serverName)
			} else {
				log.Warnf("server %s not ready", serverName)
			}
			_ = tlsConn.Close()
		} else {
			f := fs.(*ForwardServer)
			if f.proxyServerConn == nil {
				_ = tlsConn.Close()
				log.Warnf("server %s client not ready", serverName)
				return
			}
			f.connsChan <- conn
			log.Debugf("new connection %s", tlsConn.RemoteAddr().String())
		}
	}
}

func (s *Server) parseConn(conn *tls.Conn) {
	isCloseConn := true
	defer func() {
		if isCloseConn {
			_ = conn.Close()
		}
	}()

	request := newRequestData(conn, 5*time.Second)
	err := request.parseRequest()
	switch {
	case err != nil && !errors.Is(err, config.MagicNumberNotEqual):
		log.Warnf("read client request failed: %s", err.Error())
	case request.magicNumber != config.MagicNumber:
		log.Warnf("not invalid connection: %s", conn.RemoteAddr().String())
		pendingData, _ := utils.EncodeDatas([]interface{}{request.magicNumber})
		ForwardHTTPLoop(pendingData, config.C.Server.LocalHTTPAddr, conn)
	case request.token != config.C.Token:
		log.Warnf("not invalid connection whit invalid token %s request: %s", request.token,
			conn.RemoteAddr().String())
		ForwardHTTPLoop(httpReqData, config.C.Server.LocalHTTPAddr, conn)
	default:
		forwardServer, ok := s.forwardServerMap.Load(request.serverName)
		if !ok || forwardServer == nil {
			log.Warnf("server %s not found", request.serverName)
		} else {
			fs := forwardServer.(*ForwardServer)
			if err := utils.WriteConn(conn, []byte{0x77}); err != nil {
				log.Warnf("write ack for %s failed", fs.proxyServerName)
			} else {
				isCloseConn = false
				if fs.proxyServerConn != nil {
					_ = fs.proxyServerConn.Close()
				}
				fs.proxyServerConn = conn
				// reset deadline
				_ = fs.proxyServerConn.SetDeadline(time.Time{})
				log.Infof("server %s ready", fs.proxyServerName)
			}
		}
	}
}

// ForwardServer define forward server
type ForwardServer struct {
	wg       *sync.WaitGroup
	stopChan chan struct{}

	proxyServerName string
	proxyServerConn net.Conn
	connsChan       chan net.Conn

	sessionWG   sync.WaitGroup
	sessionsID  uint64
	sessionsMut sync.Mutex
	sessionsMap map[uint64]*Session
}

func newForwardServer(wg *sync.WaitGroup, proxyServerName string) *ForwardServer {
	fs := &ForwardServer{
		wg:              wg,
		proxyServerName: proxyServerName,
	}

	fs.stopChan = make(chan struct{}, 1)
	fs.connsChan = make(chan net.Conn, 1)
	fs.sessionsMap = map[uint64]*Session{}
	fs.sessionsID = mrand.Uint64()

	return fs
}

func (fs *ForwardServer) removeSession(id uint64) {
	fs.sessionsMut.Lock()
	defer fs.sessionsMut.Unlock()
	session, ok := fs.sessionsMap[id]
	if !ok {
		return
	}
	session.close()
	delete(fs.sessionsMap, id)
}

func (fs *ForwardServer) storeSession(id uint64, session *Session) {
	fs.sessionsMut.Lock()
	defer fs.sessionsMut.Unlock()
	fs.sessionsMap[id] = session
}

func (fs *ForwardServer) loadSession(id uint64) (session *Session, ok bool) {
	fs.sessionsMut.Lock()
	defer fs.sessionsMut.Unlock()
	session, ok = fs.sessionsMap[id]
	return session, ok
}

func (fs *ForwardServer) stopAllSessions() {
	fs.sessionsMut.Lock()
	for _, s := range fs.sessionsMap {
		s.close()
	}
	fs.sessionsMut.Unlock()
	fs.sessionWG.Wait()
}

func (fs *ForwardServer) stop() {
	fs.stopChan <- struct{}{}
}

func (fs *ForwardServer) start() {
	fs.wg.Add(1)
	go func() {
		defer fs.wg.Done()
		for {
			select {
			case <-fs.stopChan:
				log.Infof("%s: receive signal to stop", fs.proxyServerName)
				fs.stopAllSessions()
				return
			case conn := <-fs.connsChan:
				go fs.handleSession(conn)
			}
		}
	}()
}

/*
	      8           8
	 ________________________
	|            |           |
	|magic number|session id |
	|____________|___________|
*/
type sessionReqData struct {
	conn    net.Conn
	timeout time.Duration

	magicNumber uint64
	sessionID   uint64
	err         error
}

func newSessionReqData(conn net.Conn, timeout time.Duration) *sessionReqData {
	r := &sessionReqData{
		conn:    conn,
		timeout: timeout,
	}
	return r
}

func (r *sessionReqData) read(data interface{}) {
	if r.err != nil {
		return
	}
	if err := r.conn.SetReadDeadline(time.Now().Add(r.timeout)); err != nil {
		r.err = errors.Wrap(err, "set read deadline")
		return
	}
	r.err = binary.Read(r.conn, binary.LittleEndian, data)
}

func (r *sessionReqData) readMagicNumber() *sessionReqData {
	r.read(&r.magicNumber)
	return r
}

func (r *sessionReqData) readSessionID() *sessionReqData {
	r.read(&r.sessionID)
	return r
}

func (fs *ForwardServer) handleSession(conn net.Conn) {
	isCloseConn := true
	defer func() {
		if isCloseConn {
			_ = conn.Close()
		}
	}()

	reqData := newSessionReqData(conn, 5*time.Second)
	reqData.readMagicNumber()
	switch {
	case reqData.err != nil:
		log.Warnf("read client request failed: %s", reqData.err.Error())
	case reqData.magicNumber == config.MagicNumber:
		reqData.readSessionID()
		if reqData.err != nil {
			log.Warnf("read client request failed: %s", reqData.err.Error())
		} else {
			session, ok := fs.loadSession(reqData.sessionID)
			if !ok {
				log.Warnf("session %d not found", reqData.sessionID)
			} else {
				isCloseConn = false
				session.serverConn = conn
				fs.sessionWG.Add(1)
				_ = session.serverConn.SetDeadline(time.Time{})
				session.forwardLoop()
			}
		}
	default:
		pendingData, _ := utils.EncodeDatas([]interface{}{reqData.magicNumber})
		session := &Session{
			sessionID:   atomic.AddUint64(&fs.sessionsID, 1),
			fs:          fs,
			clientConn:  conn,
			waitCh:      make(chan struct{}),
			pendingData: pendingData,
		}
		fs.storeSession(session.sessionID, session)
		/*
			      8           8           4
			 ________________________________________
			|            |           |         |     |
			|magic number|session id |token_len|token|
			|____________|___________|_________|_____|
		*/
		var datas = []interface{}{
			config.MagicNumber,
			session.sessionID,
			uint32(len(config.C.Token)),
			[]byte(config.C.Token),
		}
		bytes, err := utils.EncodeDatas(datas)
		if err != nil {
			log.Warnf("EncodeDatas failed: %s", err.Error())
			fs.removeSession(session.sessionID)
		} else {
			err = utils.WriteConn(fs.proxyServerConn, bytes)
			if err != nil {
				log.Warnf("write session info failed: %s", err.Error())
				fs.removeSession(session.sessionID)
			} else {
				_ = session.clientConn.SetDeadline(time.Time{})
				ticker := time.NewTicker(10 * time.Second)
				select {
				case <-ticker.C:
					log.Warnf("wait for server %s timeout", fs.proxyServerName)
					fs.removeSession(session.sessionID)
					ForwardHTTPLoop(session.pendingData, config.C.Server.LocalHTTPAddr, conn)
				case <-session.waitCh:
					log.Infof("new session for %s handshake done", fs.proxyServerName)
					isCloseConn = false
				}
			}
		}
	}
}

// Session define session
type Session struct {
	sessionID   uint64
	fs          *ForwardServer
	clientConn  net.Conn
	serverConn  net.Conn
	waitCh      chan struct{}
	pendingData []byte
}

func (s *Session) forwardLoop() {
	defer close(s.waitCh)
	defer func() { _ = s.clientConn.Close() }()
	defer func() { _ = s.serverConn.Close() }()
	defer s.fs.sessionWG.Done()
	defer s.fs.removeSession(s.sessionID)

	s.waitCh <- struct{}{}

	n1 := len(s.pendingData)
	n2 := 0
	for n2 < n1 {
		n, err := s.serverConn.Write(s.pendingData[n2:])
		if err != nil {
			log.Warnf("failed to forward data: %s", err.Error())
			return
		}

		n2 += n
	}

	done := make(chan struct{})
	go func() {
		_, err := io.Copy(s.serverConn, s.clientConn)
		if err != nil {
			log.Warnf("failed to forward data: %s", err.Error())
		}
		done <- struct{}{}
	}()

	go func() {
		_, err := io.Copy(s.clientConn, s.serverConn)
		if err != nil {
			log.Warnf("failed to forward data: %s", err.Error())
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}

func (s *Session) close() {
	if s == nil {
		return
	}

	if s.clientConn != nil {
		_ = s.clientConn.Close()
	}

	if s.serverConn != nil {
		_ = s.serverConn.Close()
	}
}

/*
	      8           4            4          variable
	 ________________________________________________________
	|            |         |               |     |           |
	|magic number|token_len|server_name_len|token|server name|
	|____________|_________|_______________|_____|___________|
*/
type requestData struct {
	conn    *tls.Conn
	timeout time.Duration

	tokenLen      uint32
	serverNameLen uint32
	magicNumber   uint64
	token         string
	serverName    string
	err           error
}

func newRequestData(conn *tls.Conn, timeout time.Duration) *requestData {
	r := &requestData{
		conn:    conn,
		timeout: timeout,
	}
	return r
}

func (r *requestData) read(data interface{}) {
	if r.err != nil {
		return
	}
	if err := r.conn.SetReadDeadline(time.Now().Add(r.timeout)); err != nil {
		r.err = errors.Wrap(err, "set read deadline")
		return
	}
	r.err = binary.Read(r.conn, binary.LittleEndian, data)
}

func (r *requestData) readTokenLen() *requestData {
	r.read(&r.tokenLen)
	return r
}

func (r *requestData) readServerNameLen() *requestData {
	r.read(&r.serverNameLen)
	return r
}

func (r *requestData) readMagicNumber() *requestData {
	r.read(&r.magicNumber)
	if r.err == nil && r.magicNumber != config.MagicNumber {
		r.err = config.MagicNumberNotEqual
	}
	return r
}

func (r *requestData) readToken() *requestData {
	token := make([]byte, r.tokenLen)
	r.read(&token)
	if r.err == nil {
		r.token = string(token)
	}
	return r
}

func (r *requestData) readServerName() *requestData {
	serverName := make([]byte, r.serverNameLen)
	r.read(&serverName)
	if r.err == nil {
		r.serverName = string(serverName)
	}
	return r
}

func (r *requestData) parseRequest() error {
	r.readMagicNumber().readTokenLen().readServerNameLen()
	requireLen := r.tokenLen + r.serverNameLen + 16
	if requireLen > 512 {
		return errors.Errorf("not invalid connection whit too long request %d : %s",
			requireLen, r.conn.RemoteAddr().String())
	}
	r.readToken().readServerName()

	return r.err
}

// ForwardHTTPLoop forward http
func ForwardHTTPLoop(pendingData []byte, addr string, inConn net.Conn) {
	defer func() { _ = inConn.Close() }()
	outConn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Warnf("failed to connect to local http server: %s", err.Error())
		return
	}

	defer func() { _ = outConn.Close() }()

	n1 := len(pendingData)
	n2 := 0
	for n2 < n1 {
		n, err := outConn.Write(pendingData[n2:])
		if err != nil {
			log.Warnf("failed to write to local http server: %s", err.Error())
			return
		}

		n2 += n
	}

	done := make(chan struct{})
	go func() {
		_, err := io.Copy(outConn, inConn)
		if err != nil {
			log.Warnf("failed to forward local http server: %s", err.Error())
		}
		done <- struct{}{}
	}()

	go func() {
		_, err := io.Copy(inConn, outConn)
		if err != nil {
			log.Warnf("failed to forward local http server: %s", err.Error())
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}
