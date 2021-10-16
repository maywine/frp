package server

import (
	"crypto/rand"
	"crypto/tls"
	"frp/config"
	"net"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// StopChan the stop chan
type StopChan chan struct{}

// Server define server
type Server struct {
	serverCert tls.Certificate
	stopChan   StopChan
	listener   net.Listener
	wg         sync.WaitGroup
}

// New create the Server
func New() *Server {
	s := new(Server)
	s.stopChan = make(StopChan)
	return s
}

// Init init the Server
func (s *Server) Init() (err error) {
	serverConfig := &config.C.Server
	s.serverCert, err = tls.LoadX509KeyPair(serverConfig.CertPath, serverConfig.KeyPath)
	if err != nil {
		return errors.Wrap(err, "load certificate failed")
	}

	return err
}

// Start start the server
func (s *Server) Start() (err error) {
	tlsConfig := tls.Config{Certificates: []tls.Certificate{s.serverCert}}
	tlsConfig.Rand = rand.Reader
	s.listener, err = tls.Listen("tcp", config.C.Server.ListenAddr, &tlsConfig)
	if err != nil {
		return errors.Wrap(err, "listen faileds")
	}
	go func() {
		for {
			select {
			case <-s.stopChan:
				log.Infof("server exit")
				return
			default:
			}
			_, err := s.listener.Accept()
			if err != nil {
				log.Warnf("accept failed: %s", err.Error())
				continue
			}
			// todo con
		}
	}()
	return err
}

// Stop stop the server
func (s *Server) Stop() {
	s.stopChan <- struct{}{}
}
