package api

import (
	"bytes"
	"encoding/binary"
)

type Request struct {
	Header *RequestHeaderV2
	Body   []byte
}

type RequestHeaderV2 struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationID     int32
	ClientID          NullableString
	TagBuffer         TagBuffer
}

type ResponseHeaderV0 struct {
	CorrelationID int32
}

type ResponseHeaderV1 struct {
	CorrelationID int32
	TagBuffer
}

func DecodeRequestForSpecifiedVersion(b []byte) (*Request, error) {
	buff := bytes.NewBuffer(b)
	var requestApiKey int16
	err := binary.Read(buff, binary.BigEndian, &requestApiKey)
	if err != nil {
		return nil, err
	}

	var requestApiVersion int16
	err = binary.Read(buff, binary.BigEndian, &requestApiVersion)
	if err != nil {
		return nil, err
	}

	var correlationID int32
	err = binary.Read(buff, binary.BigEndian, &correlationID)
	if err != nil {
		return nil, err
	}

	var clientID NullableString

	var idSize int16
	err = binary.Read(buff, binary.BigEndian, &idSize)
	if err != nil {
		return nil, err
	}

	if idSize == -1 {
		clientID = ""
	} else {
		idBuff := make([]byte, idSize)
		buff.Read(idBuff)
		clientID = NullableString(string(idBuff))
	}
	var tagBuffer TagBuffer
	tagBuffLen, err := binary.ReadUvarint(buff)
	if err != nil {
		return nil, err
	}
	if tagBuffLen == 0 {
		tagBuffer = []byte{}
	}

	header := &RequestHeaderV2{
		RequestApiKey:     requestApiKey,
		RequestApiVersion: requestApiVersion,
		CorrelationID:     correlationID,
		ClientID:          clientID,
		TagBuffer:         tagBuffer,
	}

	return &Request{
		Header: header,
		Body:   buff.Bytes(),
	}, nil
}
