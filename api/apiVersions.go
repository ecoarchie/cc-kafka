package api

import (
	"bytes"
	"encoding/binary"
)

type ApiVersionsKey struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  []byte
}

func (key *ApiVersionsKey) Encode() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, key.Key)
	binary.Write(buff, binary.BigEndian, key.MinVersion)
	binary.Write(buff, binary.BigEndian, key.MaxVersion)

	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(key.TagBuffer))))
	return buff.Bytes()
}
