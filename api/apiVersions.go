package api

import (
	"bytes"
	"encoding/binary"
)

type ApiVersionsResponse struct {
	Header         *ResponseHeaderV0
	ErrorCode      ErrorCode
	ApiKeys        []ApiKey
	ThrottleTimeMs int32
	TagBuffer      []byte
}

func NewApiKeyVersionsResponse(header *RequestHeaderV2, body []byte) ApiKeyResp {
	keys := []ApiKey{}
	apiKey := NewApiKey(18, 0, 4, []byte{})
	describeTopicPartitionsKey := NewApiKey(75, 0, 0, []byte{})
	fetchKey := NewApiKey(1, 16, 16, []byte{})
	keys = append(keys, *apiKey, *describeTopicPartitionsKey, *fetchKey)
	ar := new(ApiVersionsResponse)
	ar.Header = &ResponseHeaderV0{}
	ar.Header.CorrelationID = header.CorrelationID
	if header.RequestApiVersion > 4 || header.RequestApiVersion < 0 {
		ar.ErrorCode = ErrUnsupportedApiVersion
		return ar
	}
	ar.ErrorCode = ErrNoError
	ar.ApiKeys = append(ar.ApiKeys, keys...)

	return ar
}

func (akr *ApiVersionsResponse) EncodeResponse() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, akr.Header.CorrelationID)
	binary.Write(buff, binary.BigEndian, akr.ErrorCode)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(akr.ApiKeys)+1)))
	for _, key := range akr.ApiKeys {
		buff.Write(key.Encode())
	}
	binary.Write(buff, binary.BigEndian, akr.ThrottleTimeMs)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(akr.TagBuffer))))

	resBuff := []byte{}
	resBuff = binary.BigEndian.AppendUint32(resBuff, uint32(len(buff.Bytes())))
	resBuff = append(resBuff, buff.Bytes()...)
	return resBuff
}
