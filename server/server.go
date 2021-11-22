package server

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"io"
	mrand "math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"frp/config"
	"frp/utils"
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
	s.stopChan = make(chan struct{})
	return s
}

// Start start the server
func (s *Server) Start() (err error) {
	serverConfig := &config.C.Server
	serverCert, err := tls.LoadX509KeyPair(serverConfig.CertPath, serverConfig.KeyPath)
	if err != nil {
		return errors.Wrap(err, "load certificate failed")
	}

	certs := []tls.Certificate{}
	for _, fs := range serverConfig.ForwardServers {
		cert, err := tls.LoadX509KeyPair(fs.CertPath, fs.KeyPath)
		if err != nil {
			return errors.Wrap(err, "load certificate failed")
		}
		certs = append(certs, cert)

		s.forwardServerMap.Store(fs.ServerName, nil)
	}
	certs = append(certs, serverCert)
	tlsConfig := tls.Config{Certificates: certs}
	tlsConfig.Rand = rand.Reader
	s.listener, err = tls.Listen("tcp", config.C.Server.ListenAddr, &tlsConfig)
	if err != nil {
		return errors.Wrap(err, "listen failed")
	}

	s.wg.Add(1)
	go func() {
		for {
			select {
			case <-s.stopChan:
				log.Infof("server exit")
				return
			default:
			}
			con, err := s.listener.Accept()
			if err != nil {
				log.Warnf("accept failed: %s", err.Error())
				continue
			}
			go s.startHandleConn(con)
		}
	}()

	return err
}

// Stop stop the server
func (s *Server) Stop() {
	log.Infof("stop the server...")
	s.stopChan <- struct{}{}
	s.listener.Close()
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

func (s *Server) startHandleConn(conn net.Conn) {
	tlsConn := conn.(*tls.Conn)
	if err := tlsConn.Handshake(); err != nil {
		log.Warnf("handshake failed: %s", err.Error())
		tlsConn.Close()
		return
	}

	serverName := tlsConn.ConnectionState().ServerName
	if serverName == config.C.Server.Host {
		s.parseConn(tlsConn)
	} else {
		fs, ok := s.forwardServerMap.Load(serverName)
		if !ok || fs == nil {
			if !ok {
				log.Warnf("server %s not support", serverName)
			} else {
				log.Warnf("server %s not ready", serverName)
			}
			tlsConn.Close()
		} else {
			f := fs.(*ForwardServer)
			f.connsChan <- tlsConn
			log.Debugf("new connection %s", tlsConn.RemoteAddr().String())
		}
	}
}

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
		r.err = errors.Wrap(err, "set read deadline failed")
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
	return r
}

func (r *requestData) readToken() *requestData {
	token := make([]byte, r.tokenLen)
	r.read(&token)
	if r.err != nil {
		r.token = string(token)
	}
	return r
}

func (r *requestData) readServerName() *requestData {
	serverName := make([]byte, r.serverNameLen)
	r.read(&serverName)
	if r.err != nil {
		r.serverName = string(serverName)
	}
	return r
}

func (r *requestData) parseRequest() error {
	r.readTokenLen().readServerNameLen()
	requireLen := r.tokenLen + r.serverNameLen + 16
	if requireLen > 512 {
		return errors.Errorf("not invalid connection whit too long request %d : %s",
			requireLen, r.conn.RemoteAddr().String())
	}
	r.readToken().readServerName()

	return r.err
}

func (s *Server) parseConn(conn *tls.Conn) {
	/*
		          8           4            4          variable
			 ________________________________________________________
			|            |         |               |     |           |
			|magic number|token_len|server_name_len|token|server name|
			|____________|_________|_______________|_____|___________|
	*/
	request := newRequestData(conn, 5*time.Second)
	request.readMagicNumber()
	if request.magicNumber != config.MagicNumber {
		log.Warnf("not invalid connection: %s", conn.RemoteAddr().String())
		buf, _ := utils.EncodeDatas([]interface{}{request.magicNumber})
		ForwardHTTPLoop(buf, config.C.Server.LocalHTTPAddr, conn)
	} else {
		err := request.parseRequest()
		switch {
		case err != nil:
			log.Warnf("read client request failed: %s", err.Error())
			conn.Close()
		case request.token != config.C.Server.Token:
			log.Warnf("not invalid connection whit invalid token %s request: %s", request.token,
				conn.RemoteAddr().String())
			ForwardHTTPLoop(nil, config.C.Server.LocalHTTPAddr, conn)
		default:
			forwardServer, ok := s.forwardServerMap.Load(request.serverName)
			if !ok {
				log.Warnf("server %s not found", request.serverName)
				conn.Close()
			}
			if forwardServer == nil {
				fs := &ForwardServer{
					wg:         &s.wg,
					stopChan:   make(chan struct{}),
					serverName: request.serverName,
					clientConn: conn,
					connsChan:  make(chan *tls.Conn, 256),
					sessionsID: mrand.Uint64(),
				}
				s.forwardServerMap.Store(request.serverName, fs)
				fs.wg.Add(1)
				fs.start()
			} else {
				log.Warnf("repeat forwardserver %s connection", request.serverName)
				conn.Close()
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
	defer s.clientConn.Close()
	defer s.serverConn.Close()

	s.waitCh <- struct{}{}

	n1 := len(s.pendingData)
	n2 := 0
	for n2 < n1 {
		n, err := s.serverConn.Write(s.pendingData[n2:])
		if err != nil {
			return
		}

		n2 += n
	}

	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(s.serverConn, s.clientConn)
		done <- struct{}{}
	}()

	go func() {
		_, _ = io.Copy(s.clientConn, s.serverConn)
		done <- struct{}{}
	}()

	<-done
	<-done

	s.fs.removeSession(s.sessionID)
	s.fs.sessionWG.Done()
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

// ForwardServer define forward server
type ForwardServer struct {
	wg       *sync.WaitGroup
	stopChan chan struct{}

	serverName string
	clientConn net.Conn
	connsChan  chan *tls.Conn

	sessionWG   sync.WaitGroup
	sessionsID  uint64
	sessionsMut sync.Mutex
	sessionsMap map[uint64]*Session
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
	defer fs.sessionsMut.Unlock()

	for _, s := range fs.sessionsMap {
		s.close()
	}

	fs.sessionWG.Wait()
}

func (fs *ForwardServer) stop() {
	fs.stopChan <- struct{}{}
}

func (fs *ForwardServer) start() {
	defer fs.wg.Done()
	for {
		select {
		case <-fs.stopChan:
			log.Infof("receive signal to stop")
			fs.stopAllSessions()
			return
		case conn := <-fs.connsChan:
			go fs.handleSession(conn)
		}
	}
}

func (fs *ForwardServer) handleSession(conn *tls.Conn) {
	/*
		          8           8
			 ________________________
			|            |           |
			|magic number|session id |
			|____________|___________|
	*/

	var readBuf [16]byte
	readLen := 0

	n, err := readConn(conn, readBuf[0:8], 8, 5*time.Second)
	if err != nil {
		log.Warnf("read connection failed: %s", err.Error())
		conn.Close()
		return
	}
	readLen += n

	isClient := false
	var session *Session

	number := binary.LittleEndian.Uint64(readBuf[0:8])
	if number != config.MagicNumber {
		isClient = true
	} else {
		n, err = readConn(conn, readBuf[8:16], 8, 5*time.Second)
		if err != nil {
			log.Warnf("read connection failed: %s", err.Error())
			conn.Close()
			return
		}
		readLen += n
		sessionID := binary.LittleEndian.Uint64(readBuf[8:16])
		ok := false
		session, ok = fs.loadSession(sessionID)
		if !ok {
			isClient = true
		}
	}

	if !isClient {
		session.serverConn = conn
		fs.sessionWG.Add(1)
		session.forwardLoop()
		return
	}

	session = &Session{
		sessionID:   atomic.AddUint64(&fs.sessionsID, 1),
		fs:          fs,
		clientConn:  conn,
		waitCh:      make(chan struct{}),
		pendingData: readBuf[0:readLen],
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
		len(config.C.Server.Token),
		config.C.Server.Token,
	}
	bytes, err := utils.EncodeDatas(datas)
	if err != nil {
		fs.removeSession(session.sessionID)
		return
	}
	err = writeConn(session.clientConn, bytes)
	if err != nil {
		log.Warnf("write session info failed: %s", err.Error())
		fs.removeSession(session.sessionID)
		return
	}

	ticker := time.NewTicker(10 * time.Second)
	select {
	case <-ticker.C:
		log.Warnf("wait for server %s timeout", fs.serverName)
		fs.removeSession(session.sessionID)
		ForwardHTTPLoop(session.pendingData, config.C.Server.LocalHTTPAddr, conn)
	case <-session.waitCh:
		log.Info("new session for %s handshake done", fs.serverName)
	}
}

// ForwardHTTPLoop forward http
func ForwardHTTPLoop(readBuf []byte, addr string, inConn net.Conn) {
	defer inConn.Close()
	outConn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Warnf("failed to connect to local http server: %s", err.Error())
		return
	}

	defer outConn.Close()

	n1 := len(readBuf)
	n2 := 0
	for n2 < n1 {
		n, err := outConn.Write(readBuf[n2:])
		if err != nil {
			return
		}

		n2 += n
	}

	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(outConn, inConn)
		done <- struct{}{}
	}()

	go func() {
		_, _ = io.Copy(inConn, outConn)
		done <- struct{}{}
	}()

	<-done
	<-done
}

func writeConn(conn net.Conn, buf []byte) error {
	bufLen := len(buf)
	n := 0
	for bufLen < n {
		m, err := conn.Write(buf[n:bufLen])
		if err != nil {
			return err
		}
		n += m
	}
	return nil
}

func readConn(conn net.Conn, buf []byte, maxLen int, timeout time.Duration) (int, error) {
	if timeout < 1*time.Second {
		timeout = 1 * time.Second
	}

	deadline := time.Now().Add(timeout)
	readLen := 0
	for readLen < maxLen {
		if err := conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			return 0, errors.Wrap(err, "set read deadline failed")
		}

		n, err := conn.Read(buf[readLen:])
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			return 0, errors.Wrap(err, "read connection failed")
		}

		readLen += n
		if deadline.Before(time.Now()) {
			return readLen, nil
		}
	}

	return readLen, nil
}
