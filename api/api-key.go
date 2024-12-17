package api

import (
	"bytes"
	"encoding/binary"
)

type ApiKeyResponse struct {
	CorrelationID  int32
	ErrorCode      ErrorCode
	ApiKeys        []ApiKey
	ThrottleTimeMs int32
	TagBuffer      []byte
}

type ApiKey interface {
	Encode() []byte
}

type ErrorCode int16

var (
	ErrNoError               ErrorCode = 0
	ErrUnsupportedApiVersion ErrorCode = 35
)

func NewApiKeyResponse(requestKey int16, version int16, correlationID int32, apiKeys []ApiKey) *ApiKeyResponse {
	ar := new(ApiKeyResponse)
	ar.CorrelationID = correlationID
	if version > 4 || version < 0 {
		ar.ErrorCode = ErrUnsupportedApiVersion
		return ar
	}
	ar.ErrorCode = ErrNoError
	ar.ApiKeys = append(ar.ApiKeys, apiKeys...)

	return ar
}

func (akr *ApiKeyResponse) EncodeResponse() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, akr.CorrelationID)
	binary.Write(buff, binary.BigEndian, akr.ErrorCode)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(akr.ApiKeys)+1)))
	for _, key := range akr.ApiKeys {
		// binary.Write(buff, binary.BigEndian, key.Key)
		// binary.Write(buff, binary.BigEndian, key.MinVersion)
		// binary.Write(buff, binary.BigEndian, key.MaxVersion)

		// buff.Write(binary.AppendUvarint([]byte{}, uint64(len(key.TagBuffer))))
		buff.Write(key.Encode())
	}
	binary.Write(buff, binary.BigEndian, akr.ThrottleTimeMs)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(akr.TagBuffer))))

	resBuff := []byte{}
	resBuff = binary.BigEndian.AppendUint32(resBuff, uint32(len(buff.Bytes())))
	resBuff = append(resBuff, buff.Bytes()...)
	return resBuff
}
