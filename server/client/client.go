package client

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"frp/config"
	"frp/utils"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Client define server
type Client struct {
	localServerMap sync.Map
	stopChan       chan struct{}
	wg             sync.WaitGroup
}

// New create the Client
func New() *Client {
	c := new(Client)
	c.stopChan = make(chan struct{})
	return c
}

// Start start the client
func (c *Client) Start() (err error) {
	clientConfig := &config.C.Client
	for _, client := range clientConfig.LocalServers {
		localServer := newLocalServer(&c.wg, client.ServerName)
		c.localServerMap.Store(client.ServerName, localServer)
	}

	return nil
}

// Session define session
type Session struct {
	sessionID uint64
}

// LocalServer define forward server
type LocalServer struct {
	wg       *sync.WaitGroup
	stopChan chan struct{}

	remoteAddr string
	remoteHost string
	serverName string
	serverConn net.Conn
	connsChan  chan net.Conn

	sessionWG   sync.WaitGroup
	sessionsMut sync.Mutex
	sessionsMap map[uint64]*Session
}

func newLocalServer(wg *sync.WaitGroup, serverName string) *LocalServer {
	s := &LocalServer{
		wg:          wg,
		stopChan:    make(chan struct{}),
		serverName:  serverName,
		connsChan:   make(chan net.Conn, 1),
		sessionsMap: map[uint64]*Session{},
	}
	return s
}

func (ls *LocalServer) start() error {
	if err := ls.connectToRemote(); err != nil {
		return errors.WithStack(err)
	}

	readSession := func(conn net.Conn) (id uint64, err error) {
		var magicNumber uint64
		if err := binary.Read(conn, binary.LittleEndian, &magicNumber); err != nil {
			return 0, err
		}
		if err := binary.Read(conn, binary.LittleEndian, &id); err != nil {
			return 0, err
		}

		return id, nil
	}
	go func() {
	Loop:
		for ls.serverConn == nil {
			err := ls.connectToRemote()
			if err != nil {
				log.Warnf("connect to remote failed: %s", err.Error())
				time.Sleep(5 * time.Second)
			}
		}
		for {
			id, err := readSession(ls.serverConn)
			if err != nil {
				log.Warnf("read session failed: %s, re-connect to remote", err.Error())
				goto Loop
			}
		}
	}()

	return nil
}

func (ls *LocalServer) connectToRemote() error {
	tlsConfig := tls.Config{
		Rand:       rand.Reader,
		ServerName: ls.remoteHost,
	}

	var err error
	ls.serverConn, err = tls.Dial("tcp", ls.remoteAddr, &tlsConfig)
	if err != nil {
		return errors.Wrapf(err, "make connection for %s", ls.serverName)
	}

	isErr := true
	defer func() {
		if isErr {
			_ = ls.serverConn.Close()
			ls.serverConn = nil
		}
	}()

	datas := []interface{}{
		config.MagicNumber,
		len(config.C.Token),
		len(ls.serverName),
		config.C.Token,
		ls.serverName,
	}
	buf, err := utils.EncodeDatas(datas)
	if err != nil {
		return errors.WithStack(err)
	}

	// set deadline
	err = ls.serverConn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return errors.WithStack(err)
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
		return errors.Wrapf(err, "read ack %s failed", ls.serverName)
	}
	if n != 1 || readAck[0] != 0x77 {
		return errors.Errorf("check ack failed with len %d, ack %v", n, readAck)
	}

	isErr = false
	return nil
}
