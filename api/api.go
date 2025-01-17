package api

import (
	"bytes"
	"encoding/binary"
)

type ApiKey struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  []byte
}

func NewApiKey(key, minVersion, maxVersion int16, tagBuffer []byte) *ApiKey {
	return &ApiKey{
		Key:        key,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
		TagBuffer:  tagBuffer,
	}
}

func (key *ApiKey) Encode() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, key.Key)
	binary.Write(buff, binary.BigEndian, key.MinVersion)
	binary.Write(buff, binary.BigEndian, key.MaxVersion)

	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(key.TagBuffer))))
	return buff.Bytes()
}

type NullableString string

func (ns NullableString) FromBytes(b []byte) NullableString {
	return NullableString(string(b))
}

type TagBuffer []byte

type ErrorCode int16

var (
	ErrNoError               ErrorCode = 0
	ErrUnknownTopic          ErrorCode = 3
	ErrUnsupportedApiVersion ErrorCode = 35
	ErrUnknownTopicId        ErrorCode = 100
)

type ApiKeyResp interface {
	EncodeResponse() []byte
}

type ResponseFunc func(*RequestHeaderV2, []byte) ApiKeyResp

var ApiKeysMap map[int16]ResponseFunc = map[int16]ResponseFunc{
	1:  NewFetchResponse,
	18: NewApiKeyVersionsResponse,
	75: NewDescribeTopicPartitionsResponse,
}
