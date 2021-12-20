package client

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"frp/config"
	"frp/utils"
)

// Client define server
type Client struct {
	localServerMap sync.Map
	wg             sync.WaitGroup
}

// New create the Client
func New() *Client {
	c := new(Client)
	return c
}

// Start start the client
func (c *Client) Start() error {
	clientConfig := &config.C.Client
	for _, client := range clientConfig.LocalServers {
		localServer := newLocalServer(&c.wg, client.LocalAddr, clientConfig.RemoteAddr,
			clientConfig.ControlServerName, client.ProxyServerName)
		c.localServerMap.Store(client.ProxyServerName, localServer)
		if err := localServer.start(); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

// Stop stop the client
func (s *Client) Stop() {
	log.Infof("stop the client...")
	s.localServerMap.Range(func(key, value interface{}) bool {
		ls := value.(*LocalServer)
		if ls != nil {
			ls.stop()
		}
		return true
	})
	s.wg.Wait()
	log.Infof("stop server completely")
}

// LocalServer define forward server
type LocalServer struct {
	wg       *sync.WaitGroup
	stopChan chan struct{}

	localAddr         string
	remoteAddr        string
	controlServerName string
	proxyServerName   string

	serverConn net.Conn
	connsChan  chan net.Conn

	sessionWG   sync.WaitGroup
	sessionsMut sync.Mutex
	sessionsMap map[uint64]*Session
}

func newLocalServer(wg *sync.WaitGroup, localAddr, remoteAddr, controlServerName, proxyServerName string) *LocalServer {
	s := &LocalServer{
		wg:                wg,
		stopChan:          make(chan struct{}, 1),
		localAddr:         localAddr,
		remoteAddr:        remoteAddr,
		controlServerName: controlServerName,
		proxyServerName:   proxyServerName,
		connsChan:         make(chan net.Conn, 1),
		sessionsMap:       map[uint64]*Session{},
	}
	return s
}

func (ls *LocalServer) storeSession(s *Session) {
	ls.sessionsMut.Lock()
	defer ls.sessionsMut.Unlock()
	ls.sessionsMap[s.sessionID] = s
}

func (ls *LocalServer) removeSession(id uint64) {
	ls.sessionsMut.Lock()
	defer ls.sessionsMut.Unlock()
	session, ok := ls.sessionsMap[id]
	if !ok {
		return
	}
	session.close()
	delete(ls.sessionsMap, id)
}

func (ls *LocalServer) start() error {
	var isStop uint32

	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
		<-ls.stopChan
		atomic.StoreUint32(&isStop, 1)
		log.Infof("%s: receive signal to exit", ls.proxyServerName)
		if ls.serverConn != nil {
			_ = ls.serverConn.Close()
		}
		ls.stopAllSessions()
	}()

	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
	Loop:
		if atomic.LoadUint32(&isStop) == 1 {
			return
		}
		if ls.serverConn == nil {
			err := ls.connectToControlServer()
			if err != nil {
				log.Warnf("connect to remote failed: %s", err.Error())
				time.Sleep(5 * time.Second)
				goto Loop
			}
		}
		for {
			id, err := ls.readSession(ls.serverConn)
			if err != nil {
				log.Warnf("unexpect error: %s, re-connect to remote", err.Error())
				_ = ls.serverConn.Close()
				ls.serverConn = nil
				goto Loop
			}
			if id == 0 {
				log.Warnf("ignore invalid zero session id")
			} else {
				session := &Session{
					sessionID: id,
					ls:        ls,
				}
				if err := ls.initConnection(session); err != nil {
					log.Warnf("init connection failed, error: %s", err.Error())
				}
				ls.storeSession(session)
				session.start()
			}
		}
	}()

	return nil
}

func (ls *LocalServer) readSession(conn net.Conn) (id uint64, err error) {
	var magicNumber uint64
	if err := binary.Read(conn, binary.LittleEndian, &magicNumber); err != nil {
		return 0, err
	}
	if magicNumber != config.MagicNumber {
		return 0, errors.Errorf("magic number not equal, expect %d, actual %d", config.MagicNumber, magicNumber)
	}
	if err := binary.Read(conn, binary.LittleEndian, &id); err != nil {
		return 0, err
	}
	var tokenLen uint32
	if err := binary.Read(conn, binary.LittleEndian, &tokenLen); err != nil {
		return 0, err
	}
	token := make([]byte, tokenLen)
	if err := binary.Read(conn, binary.LittleEndian, &token); err != nil {
		return 0, err
	}
	if string(token) != config.C.Token {
		return 0, errors.Errorf("token not equal")
	}

	return id, nil
}

func (ls *LocalServer) stop() {
	ls.stopChan <- struct{}{}
}

func (ls *LocalServer) stopAllSessions() {
	ls.sessionsMut.Lock()
	for _, s := range ls.sessionsMap {
		s.close()
	}
	ls.sessionsMut.Unlock()
	ls.sessionWG.Wait()
}

func (ls *LocalServer) connectToControlServer() error {
	tlsConfig := tls.Config{
		Rand:               rand.Reader,
		ServerName:         ls.controlServerName,
		GetConfigForClient: utils.SetTCPKeepAlive,
	}

	serverConn, err := tls.Dial("tcp", ls.remoteAddr, &tlsConfig)
	if err != nil {
		return errors.Wrapf(err, "connect to control server")
	}
	err = serverConn.Handshake()
	if err != nil {
		return errors.Wrapf(err, "handshake")
	}

	ls.serverConn = serverConn
	isErr := true
	defer func() {
		if isErr {
			_ = ls.serverConn.Close()
			ls.serverConn = nil
		}
	}()

	datas := []interface{}{
		config.MagicNumber,
		uint32(len(config.C.Token)),
		uint32(len(ls.proxyServerName)),
		[]byte(config.C.Token),
		[]byte(ls.proxyServerName),
	}
	buf, err := utils.EncodeDatas(datas)
	if err != nil {
		return errors.WithMessage(err, "encode data")
	}

	// set deadline
	err = ls.serverConn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return errors.WithMessage(err, "set deadline")
	}
	err = utils.WriteConn(ls.serverConn, buf)
	if err != nil {
		return errors.Wrapf(err, "write request data")
	}
	// reset deadline
	err = ls.serverConn.SetDeadline(time.Time{})
	if err != nil {
		return errors.WithStack(err)
	}
	readAck := make([]byte, 1)
	n, err := ls.serverConn.Read(readAck)
	if err != nil {
		return errors.Wrapf(err, "read ack %s", ls.proxyServerName)
	}
	if n != 1 || readAck[0] != 0x77 {
		return errors.Errorf("check ack failed with len %d, ack %v", n, readAck)
	}

	isErr = false
	return nil
}

func (ls *LocalServer) initConnection(s *Session) error {
	tlsConfig := tls.Config{
		Rand:       rand.Reader,
		ServerName: ls.proxyServerName,
	}

	serverConn, err := tls.Dial("tcp", ls.remoteAddr, &tlsConfig)
	if err != nil {
		return errors.Wrapf(err, "connect to proxy server %s", ls.proxyServerName)
	}

	isErr := true
	defer func() {
		if isErr {
			s.close()
		}
	}()

	datas := []interface{}{
		config.MagicNumber,
		s.sessionID,
	}
	buf, err := utils.EncodeDatas(datas)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := utils.WriteConn(serverConn, buf); err != nil {
		return errors.Wrapf(err, "write session info")
	}
	s.serverConn = serverConn

	clientConn, err := net.Dial("tcp", ls.localAddr)
	if err != nil {
		return errors.Wrapf(err, "connect to local server %s", ls.localAddr)
	}
	s.clientConn = clientConn

	isErr = false
	return nil
}

// Session define session
type Session struct {
	sessionID  uint64
	ls         *LocalServer
	serverConn net.Conn
	clientConn net.Conn
}

func (s *Session) start() {
	s.ls.sessionWG.Add(1)
	go func() {
		s.forwardLoop()
	}()
}

func (s *Session) close() {
	if s == nil {
		return
	}
	if s.serverConn != nil {
		_ = s.serverConn.Close()
	}
	if s.clientConn != nil {
		_ = s.clientConn.Close()
	}
}

func (s *Session) forwardLoop() {
	defer func() { _ = s.clientConn.Close() }()
	defer func() { _ = s.serverConn.Close() }()
	defer s.ls.sessionWG.Done()
	defer s.ls.removeSession(s.sessionID)

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
}
