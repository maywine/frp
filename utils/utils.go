package utils

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"net"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// EncodeDatas encode data
func EncodeDatas(datas []interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, v := range datas {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// WriteConn write buf to conn
func WriteConn(conn net.Conn, buf []byte) error {
	bufLen := len(buf)
	n := 0
	for n < bufLen {
		m, err := conn.Write(buf[n:bufLen])
		if err != nil {
			return err
		}
		n += m
	}
	return nil
}

func setKeepaliveParameters(conn *net.TCPConn) {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		log.Warn("on getting raw connection object for keepalive parameter setting", err.Error())
	}

	_ = rawConn.Control(func(fdPtr uintptr) {
		fd := int(fdPtr)
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, 3); err != nil {
			log.Warn("on setting keepalive probe count, error: %s", err.Error())
		}
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, 3); err != nil {
			log.Warn("on setting keepalive retry interval, error: %s", err.Error())
		}
	})
}

// SetTCPKeepAlive set tcp connection keep Alive
func SetTCPKeepAlive(clientHello *tls.ClientHelloInfo) (*tls.Config, error) {
	if tcpConn, ok := clientHello.Conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(true); err != nil {
			log.Warn("could not enable keep alive, error: %s", err.Error())
		}
		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			log.Warn("could not set keep alive period, error: %s", err.Error())
		}
		setKeepaliveParameters(tcpConn)
	} else {
		log.Warn("TLS over non-TCP connection")
	}

	return nil, nil
}
