package tunnel

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

const (
	protocolMagic  = "FRP2"
	maxServiceName = 255
	// Subprotocol prevents a generic WebSocket endpoint from being accepted.
	Subprotocol = "frp.mux.v1"
)

// WriteService bounds the route header so a peer cannot force large allocations.
func WriteService(conn net.Conn, service string) error {
	if len(service) == 0 || len(service) > maxServiceName {
		return fmt.Errorf("invalid service name length %d", len(service))
	}
	header := make([]byte, len(protocolMagic)+2+len(service))
	copy(header, protocolMagic)
	binary.BigEndian.PutUint16(header[len(protocolMagic):], uint16(len(service)))
	copy(header[len(protocolMagic)+2:], service)
	for len(header) > 0 {
		written, err := conn.Write(header)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrUnexpectedEOF
		}
		header = header[written:]
	}
	return nil
}

// ReadService validates the route before any private service is contacted.
func ReadService(conn net.Conn) (string, error) {
	header := make([]byte, len(protocolMagic)+2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
	if string(header[:len(protocolMagic)]) != protocolMagic {
		return "", fmt.Errorf("invalid stream protocol magic")
	}
	nameLen := int(binary.BigEndian.Uint16(header[len(protocolMagic):]))
	if nameLen == 0 || nameLen > maxServiceName {
		return "", fmt.Errorf("invalid service name length %d", nameLen)
	}
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, name); err != nil {
		return "", err
	}
	return string(name), nil
}

// YamuxConfig bounds stream setup and close operations on the shared tunnel.
func YamuxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.AcceptBacklog = 128
	cfg.KeepAliveInterval = 15 * time.Second
	cfg.ConnectionWriteTimeout = 10 * time.Second
	cfg.StreamOpenTimeout = 10 * time.Second
	cfg.StreamCloseTimeout = 30 * time.Second
	cfg.LogOutput = io.Discard
	return cfg
}

// Bridge copies a full-duplex stream and propagates half-close when supported.
func Bridge(left, right net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go copyAndCloseWrite(&wg, left, right)
	go copyAndCloseWrite(&wg, right, left)
	wg.Wait()
}

func copyAndCloseWrite(wg *sync.WaitGroup, dst, src net.Conn) {
	defer wg.Done()
	_, _ = io.Copy(dst, src)
	if halfCloser, ok := dst.(interface{ CloseWrite() error }); ok {
		_ = halfCloser.CloseWrite()
		return
	}
	_ = dst.Close()
}
