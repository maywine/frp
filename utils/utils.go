package utils

import (
	"bytes"
	"encoding/binary"
	"net"
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
	for bufLen < n {
		m, err := conn.Write(buf[n:bufLen])
		if err != nil {
			return err
		}
		n += m
	}
	return nil
}
