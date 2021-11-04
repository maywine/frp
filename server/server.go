package server

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"frp/config"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Server define server
type Server struct {
	forwardServer sync.Map
	stopChan      chan struct{}
	listener      net.Listener
	wg            sync.WaitGroup
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
		s.forwardServer.Store(fs.ServerName, nil)
	}

	certs = append(certs, serverCert)
	tlsConfig := tls.Config{Certificates: certs}
	tlsConfig.Rand = rand.Reader
	s.listener, err = tls.Listen("tcp", config.C.Server.ListenAddr, &tlsConfig)
	if err != nil {
		return errors.Wrap(err, "listen faileds")
	}
	go func() {
		defer s.listener.Close()
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
	s.forwardServer.Range(func(key, value interface{}) bool {
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
		return
	}

	serverName := tlsConn.ConnectionState().ServerName
	if serverName == config.C.Server.Host {
		s.handleClientConn(tlsConn)
	} else {
		forwardServer, ok := s.forwardServer.Load(serverName)
		if !ok || forwardServer == nil {
			if !ok {
				log.Warnf("server %s not support", serverName)
			} else {
				log.Warnf("server %s not ready", serverName)
			}
			tlsConn.Close()
		}

		server := forwardServer.(*ForwardServer)
		select {
		case <-server.quitChan:
			log.Warnf("forward server %s exit", serverName)
		case server.connsChan <- tlsConn:
			log.Debugf("new connection %s", tlsConn.RemoteAddr().String())
		}
	}
}

func (s *Server) handleClientConn(conn *tls.Conn) {
	/*
		          8           4            4          variable
			 ________________________________________________________
			|            |         |               |     |           |
			|magic number|token_len|server_name_len|token|server name|
			|____________|_________|_______________|_____|___________|
	*/

	var buf [512]byte
	readLen := 0

	n, err := readConn(conn, buf[0:8], 8, 5*time.Second)
	if err != nil {
		log.Warnf("read client request failed: %s", err.Error())
		conn.Close()
		return
	}
	readLen += n

	number := binary.LittleEndian.Uint64(buf[0:8])
	if number != config.MagicNumber {
		log.Warnf("not invalid connection: %s", conn.RemoteAddr().String())
		ForwardHTTPLoop(buf[0:readLen], config.C.Server.LocalHTTPAddr, conn)
		return
	}

	n, err = readConn(conn, buf[8:], 8, 5*time.Second)
	if err != nil {
		log.Warnf("read client request failed: %s", err.Error())
		conn.Close()
		return
	}
	readLen += n

	tokenLen := binary.LittleEndian.Uint32(buf[8:12])
	serverNameLen := binary.LittleEndian.Uint32(buf[12:16])
	requireLen := tokenLen + serverNameLen + 16
	if requireLen > 512 {
		log.Warnf("not invalid connection whit too long request %d : %s", requireLen, conn.RemoteAddr().String())
		ForwardHTTPLoop(buf[0:readLen], config.C.Server.LocalHTTPAddr, conn)
		return
	}

	// read token and server name
	n, err = readConn(conn, buf[readLen:], int(requireLen), 5*time.Second)
	if err != nil {
		log.Warnf("read client request failed: %s", err.Error())
		conn.Close()
		return
	}
	readLen += n

	parseToken := string(buf[16:tokenLen])
	if parseToken != config.C.Server.Token {
		log.Warnf("not invalid connection whit invalid token %s request: %s", parseToken, conn.RemoteAddr().String())
		ForwardHTTPLoop(buf[0:readLen], config.C.Server.LocalHTTPAddr, conn)
		return
	}

	parseserverName := string(buf[16+tokenLen:])
	forwardServer, ok := s.forwardServer.Load(parseserverName)
	if !ok {
		log.Warnf("server %s not found", parseserverName)
		conn.Close()
		return
	}

	if forwardServer == nil {
		fs := &ForwardServer{
			wg:         &s.wg,
			stopChan:   make(chan struct{}),
			quitChan:   make(chan struct{}),
			serverName: parseserverName,
			clientConn: conn,
			connsChan:  make(chan *tls.Conn, 256)}
		s.forwardServer.Store(parseserverName, fs)
		fs.start()
	} else {
		log.Warnf("repeat forwardserver %s connection", parseserverName)
		conn.Close()
		return
	}
}

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
	wg         *sync.WaitGroup
	stopChan   chan struct{}
	quitChan   chan struct{}
	serverName string
	clientConn net.Conn
	connsChan  chan *tls.Conn
	sessionsID uint64
	sessions   sync.Map
}

func (s *ForwardServer) removeSession(id uint64) {
	val, ok := s.sessions.Load(id)
	if !ok {
		return
	}
	session := val.(*Session)
	session.close()
	s.sessions.Delete(id)
}

func (fs *ForwardServer) stop() {
	fs.stopChan <- struct{}{}
}

func (fs *ForwardServer) start() {
	fs.wg.Add(1)

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

	isClinet := false
	var session *Session

	number := binary.LittleEndian.Uint64(readBuf[0:8])
	if number != config.MagicNumber {
		isClinet = true
	} else {
		n, err = readConn(conn, readBuf[8:16], 8, 5*time.Second)
		if err != nil {
			log.Warnf("read connection failed: %s", err.Error())
			conn.Close()
			return
		}
		readLen += n
		sessionID := binary.LittleEndian.Uint64(readBuf[8:16])
		s, ok := fs.sessions.Load(sessionID)
		if !ok {
			isClinet = true
		} else {
			session = s.(*Session)
		}
	}

	if !isClinet {
		session.serverConn = conn
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
	fs.sessions.Store(session.sessionID, session)

	/*
		          8           8           4
			 ________________________________________
			|            |           |         |     |
			|magic number|session id |token_len|token|
			|____________|___________|_________|_____|
	*/

	var data = []interface{}{
		config.MagicNumber,
		session.sessionID,
		len(config.C.Server.Token),
		config.C.Server.Token,
	}
	buf := new(bytes.Buffer)
	for _, v := range data {
		err := binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			fs.removeSession(session.sessionID)
			return
		}
	}

	err = writeConn(session.clientConn, buf.Bytes())
	if err != nil {
		log.Warnf("write session info failed: %s", err.Error())
		fs.removeSession(session.sessionID)
		return
	}

	ticker := time.NewTicker(10 * time.Second)
	select {
	case <-ticker.C:
		log.Warnf("wait for server %s timeout", fs.serverName)
		fs.sessions.Delete(session.sessionID)
		ForwardHTTPLoop(session.pendingData, config.C.Server.LocalHTTPAddr, conn)
	case <-session.waitCh:
		log.Info("new session for %s handshake done", fs.serverName)
	}
}

func (fs *ForwardServer) stopAllSessions() {

}

func ForwardHTTPLoop(readbuf []byte, addr string, inConn net.Conn) {
	defer inConn.Close()
	outConn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Warnf("failed to connect to local http server: %s", err.Error())
		return
	}

	defer outConn.Close()

	n1 := len(readbuf)
	n2 := 0
	for n2 < n1 {
		n, err := outConn.Write(readbuf[n2:])
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
