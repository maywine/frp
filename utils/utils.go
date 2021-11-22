package utils

import (
	"bytes"
	"encoding/binary"
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
