package server

import (
	crand "crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"frp/config"
	"io"
	mrand "math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Server define server
type Server struct {
	fserver  sync.Map
	stopChan chan struct{}
	listener net.Listener
	wg       sync.WaitGroup
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
		s.fserver.Store(fs.ServerName, nil)
	}

	certs = append(certs, serverCert)
	tlsConfig := tls.Config{Certificates: certs}
	tlsConfig.Rand = crand.Reader
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
	s.fserver.Range(func(key, value interface{}) bool {
		fs := value.(*ForwardServer)
		if fs != nil {
			fs.stop()
		}
		return true
	})

	s.wg.Wait()
	log.Infof("stop server completely")
}

// Stop stop the server
func (s *Server) startHandleConn(conn net.Conn) {
	tlsConn := conn.(*tls.Conn)
	tlsConn.Handshake()

	serverName := tlsConn.ConnectionState().ServerName
	if serverName == config.C.Server.Host {
		s.handleClientConn(tlsConn)
	} else {
		fserver, ok := s.fserver.Load(serverName)
		if !ok || fserver == nil {
			if !ok {
				log.Warnf("server %s not support", serverName)
			} else {
				log.Warnf("server %s not ready", serverName)
			}
			tlsConn.Close()
		}

		server := fserver.(*ForwardServer)
		select {
		case <-server.quitChan:
			log.Warnf("forward server %s exit", serverName)
		case server.connsChan <- tlsConn:
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
		log.Warnf("read client request failed: ", err.Error())
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
		log.Warnf("read client request failed: ", err.Error())
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
		log.Warnf("read client request failed: ", err.Error())
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
	fserver, ok := s.fserver.Load(parseserverName)
	if !ok {
		log.Warnf("server %s not found", parseserverName)
		conn.Close()
		return
	}

	if fserver == nil {
		fs := &ForwardServer{wg: &s.wg, clientConn: conn, connsChan: make(chan *tls.Conn, 256)}
		s.fserver.Store(parseserverName, fs)
		fs.start()
	} else {
		log.Warnf("repeat forwardserver %s connection", parseserverName)
		conn.Close()
		return
	}
}

type Session struct {
	clientConn net.Conn
	serverConn net.Conn
}

// ForwardServer define forward server
type ForwardServer struct {
	wg         *sync.WaitGroup
	stopChan   chan struct{}
	quitChan   chan struct{}
	clientConn net.Conn
	connsChan  chan *tls.Conn
	sessions   sync.Map
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

	// var buf [16]byte
	// readLen := 0
	// for readLen < 16 {
	// 	n, err := conn.Read(buf)
	// 	if err != nil {

	// 	}
	// }
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
		io.Copy(outConn, inConn)
		done <- struct{}{}
	}()

	go func() {
		io.Copy(inConn, outConn)
		done <- struct{}{}
	}()

	<-done
	<-done
}

func randID() uint64 {
	return mrand.Uint64()
}

func readConn(conn net.Conn, buf []byte, maxLen int, timeOut time.Duration) (int, error) {
	if timeOut < 1*time.Second {
		timeOut = 1 * time.Second
	}

	beg := time.Now()
	readLen := 0
	for readLen < maxLen {
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, err := conn.Read(buf[readLen:])
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			return 0, errors.Wrap(err, "read connection failed")
		}

		readLen += n
		if beg.Add(timeOut).Before(time.Now()) {
			return readLen, nil
		}
	}

	return readLen, nil
}
