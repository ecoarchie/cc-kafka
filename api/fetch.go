package api

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type FetchRequest struct {
	Header              *RequestHeaderV2
	MaxWaitMs           [4]byte
	MinBytes            [4]byte
	MaxBytes            [4]byte
	IsolationLevel      byte
	SessionId           [4]byte
	SessionEpoch        [4]byte
	Topics              []*FetchRequestTopic
	ForgottenTopicsData []*ForgottenTopic
	RackId              string
	TagBuffer           []byte
}

func (f *FetchRequest) DecodeRequest(body []byte) error {
	buff := bytes.NewBuffer(body)
	binary.Read(buff, binary.BigEndian, f.MaxWaitMs)
	binary.Read(buff, binary.BigEndian, f.MinBytes)
	binary.Read(buff, binary.BigEndian, f.MaxBytes)
	binary.Read(buff, binary.BigEndian, f.IsolationLevel)
	binary.Read(buff, binary.BigEndian, f.SessionId)
	binary.Read(buff, binary.BigEndian, f.SessionEpoch)
	topicArrLen, err := binary.ReadUvarint(buff)
	if err != nil {
		return errors.Wrap(err, "error parsing fetch topic request length")
	}
	for i := 0; i < int(topicArrLen)-1; i++ {
		t := new(FetchRequestTopic)
		binary.Read(buff, binary.BigEndian, t)
		f.Topics = append(f.Topics, t)
	}
	fogrottenTopicArrLen, err := binary.ReadUvarint(buff)
	if err != nil {
		return errors.Wrap(err, "error parsing fetch forgotten topic request length")
	}
	for i := 0; i < int(fogrottenTopicArrLen)-1; i++ {
		t := new(ForgottenTopic)
		binary.Read(buff, binary.BigEndian, t)
		f.ForgottenTopicsData = append(f.ForgottenTopicsData, t)
	}
	binary.Read(buff, binary.BigEndian, &f.RackId)
	buff.ReadByte()
	f.TagBuffer = []byte{}
	return nil
}

type FetchRequestTopic struct {
	TopicId    uuid.UUID
	Partitions []*FetchRequestPartition
	TagBuffer  []byte
}

type ForgottenTopic struct {
	TopicId    uuid.UUID
	Partitions [][4]byte
	TagBuffer  []byte
}

type FetchRequestPartition struct {
	Partition          [4]byte
	CurrentLeaderEpoch [4]byte
	FetchOffset        [8]byte
	LastFetchedEpoch   [4]byte
	LogStartOffset     [8]byte
	PartitionMacBytes  [4]byte
	TagBuffer          []byte
}

type FetchResponse struct {
	Header         *ResponseHeaderV1
	ThrottleTimeMs int32
	ErrorCode      ErrorCode
	SessionId      int32
	Responses      []*FetchRespVal
	TagBuffer      []byte
}

type FetchRespVal struct {
	TopicId    uuid.UUID
	Partitions []*FetchResponsePartition
	TagBuffer  []byte
}

func (f *FetchRespVal) Encode() []byte {
	return nil
}

type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            ErrorCode
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []*AbortedTransaction
	PreferredReadReplica int32
	Records              []byte
	TagBuffer            []byte
}

type AbortedTransaction struct {
	ProducerId  int64
	FirstOffset int64
	TagBuffer   []byte
}

func NewFetchResponse(header *RequestHeaderV2, body []byte) ApiKeyResp {
	req := new(FetchRequest)
	req.Header = header
	err := req.DecodeRequest(body)
	if err != nil {
		fmt.Println("Here in decode request error")
		fmt.Println(err)
	}
	res := new(FetchResponse)
	res.Header = &ResponseHeaderV1{
		CorrelationID: req.Header.CorrelationID,
		TagBuffer:     []byte{},
	}
	res.ThrottleTimeMs = 0
	res.ErrorCode = ErrNoError
	res.SessionId = 0
	if len(req.Topics) == 0 {
		res.Responses = []*FetchRespVal{}
	}
	res.TagBuffer = []byte{}
	return res
}

func (f *FetchResponse) EncodeResponse() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, f.Header.CorrelationID)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(f.Header.TagBuffer))))
	binary.Write(buff, binary.BigEndian, f.ThrottleTimeMs)
	binary.Write(buff, binary.BigEndian, f.ErrorCode)
	binary.Write(buff, binary.BigEndian, f.SessionId)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(f.Responses)+1)))
	for _, r := range f.Responses {
		buff.Write(r.Encode())
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(0)))
	resBuff := []byte{}
	resBuff = binary.BigEndian.AppendUint32(resBuff, uint32(buff.Len()))
	resBuff = append(resBuff, buff.Bytes()...)
	return resBuff
}
