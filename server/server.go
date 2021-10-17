package server

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"frp/config"
	"io"
	"net"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	magicNumber uint64 = 235300467370941978
)

// StopChan the stop chan
type StopChan chan struct{}

// Server define server
type Server struct {
	fserver  sync.Map
	stopChan StopChan
	listener net.Listener
	wg       sync.WaitGroup
}

// New create the Server
func New() *Server {
	s := new(Server)
	s.stopChan = make(StopChan)
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
			go s.handleConn(con)
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
func (s *Server) handleConn(conn net.Conn) {
	tlsConn := conn.(*tls.Conn)
	tlsConn.Handshake()
	if tlsConn.ConnectionState().ServerName == config.C.Server.Host {
		/*
			          8           4            4          variable
				 ________________________________________________________
				|            |         |               |     |           |
				|magic number|token_len|server_name_len|token|server name|
				|____________|_________|_______________|_____|___________|
		*/
		var buf [512]byte
		readLen := 0
		for readLen < 16 {
			n, err := tlsConn.Read(buf[readLen:])
			if err != nil {
				log.Warnf("read client request failed: ", err.Error())
				return
			}
			readLen += n
		}

		number := binary.LittleEndian.Uint64(buf[0:8])
		if number != magicNumber {
			log.Warnf("not invalid connection: %s", tlsConn.RemoteAddr().String())
			goto forward
		}

		tokenLen := binary.LittleEndian.Uint32(buf[8:12])
		serverNameLen := binary.LittleEndian.Uint32(buf[12:16])
		requireLen := tokenLen + serverNameLen + 16
		if requireLen > 512 {
			log.Warnf("not invalid connection whit too long request %d : %s", requireLen, tlsConn.RemoteAddr().String())
			goto forward
		}
		for uint32(readLen) < requireLen {

		}
	}

forward:
	forwardHTTPLoop(config.C.Server.LocalHTTPAddr, conn)
}

// ForwardServer define forward server
type ForwardServer struct {
	wg       *sync.WaitGroup
	stopChan StopChan
}

func (fs *ForwardServer) stop() {
	fs.stopChan <- struct{}{}
	fs.wg.Done()
}

func forwardHTTPLoop(addr string, inConn net.Conn) {
	outConn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Warnf("failed to connect to local http server: %s", err.Error())
		inConn.Close()
		return
	}

	defer inConn.Close()
	defer outConn.Close()

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
