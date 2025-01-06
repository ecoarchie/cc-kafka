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
	binary.Read(buff, binary.BigEndian, &f.MaxWaitMs)
	binary.Read(buff, binary.BigEndian, &f.MinBytes)
	binary.Read(buff, binary.BigEndian, &f.MaxBytes)
	binary.Read(buff, binary.BigEndian, &f.IsolationLevel)
	binary.Read(buff, binary.BigEndian, &f.SessionId)
	binary.Read(buff, binary.BigEndian, &f.SessionEpoch)
	topicArrLen, err := binary.ReadUvarint(buff)
	if err != nil {
		return errors.Wrap(err, "error parsing fetch topic request length")
	}
	fmt.Printf("topic arr length: %v\n", topicArrLen)
	for i := 0; i < int(topicArrLen)-1; i++ {
		t := new(FetchRequestTopic)
		binary.Read(buff, binary.BigEndian, &t.TopicId)
		partitionLen, err := binary.ReadUvarint(buff)
		fmt.Printf("partition length: %v\n", partitionLen)
		if err != nil {
			return errors.Wrap(err, "error parsing fetch partition array request length")
		}

		for i := 0; i < int(partitionLen)-1; i++ {
			partition := new(FetchRequestPartition)
			binary.Read(buff, binary.BigEndian, &partition.Partition)
			binary.Read(buff, binary.BigEndian, &partition.CurrentLeaderEpoch)
			binary.Read(buff, binary.BigEndian, &partition.FetchOffset)
			binary.Read(buff, binary.BigEndian, &partition.LastFetchedEpoch)
			binary.Read(buff, binary.BigEndian, &partition.LogStartOffset)
			binary.Read(buff, binary.BigEndian, &partition.PartitionMacBytes)
			partition.TagBuffer = []byte{}
			t.Partitions = append(t.Partitions, partition)
		}
		t.TagBuffer = []byte{}
		f.Topics = append(f.Topics, t)
	}
	forgottenTopicArrLen, err := binary.ReadUvarint(buff)
	fmt.Printf("forgotten topics length: %v\n", forgottenTopicArrLen)
	if err != nil {
		return errors.Wrap(err, "error parsing fetch forgotten topic request length")
	}
	for i := 0; i < int(forgottenTopicArrLen)-1; i++ {
		t := new(ForgottenTopic)
		binary.Read(buff, binary.BigEndian, t)
		f.ForgottenTopicsData = append(f.ForgottenTopicsData, t)
	}
	rackIdLen, err := binary.ReadUvarint(buff)
	if err != nil {
		return errors.Wrap(err, "error parsing fetch racklen request length")
	}
	fmt.Printf("rackId length: %v\n", rackIdLen)
	if rackIdLen-1 == 0 {
		f.RackId = ""
	}
	_, err = buff.ReadByte()
	if err != nil {
		fmt.Printf("error parsing last byte for tagged field len: %v\n", err)
	}
	f.TagBuffer = []byte{}
	fmt.Printf("After encoding request HEAder: %+v\n", f.Header)
	fmt.Printf("After encoding request Body: %+v\n", f)
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
	Partition          int32
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
	buff := new(bytes.Buffer)
	buff.Write(f.TopicId[:])
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(f.Partitions)+1)))
	for _, r := range f.Partitions {
		buff.Write(r.Encode())
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(f.TagBuffer))))
	return buff.Bytes()
}

type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            ErrorCode
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []*AbortedTransaction
	PreferredReadReplica int32
	Records              [][]byte
	TagBuffer            []byte
}

func (fp *FetchResponsePartition) Encode() []byte {
	// TODO to implement
	buff := new(bytes.Buffer)

	binary.Write(buff, binary.BigEndian, fp.PartitionIndex)
	binary.Write(buff, binary.BigEndian, fp.ErrorCode)
	binary.Write(buff, binary.BigEndian, fp.HighWatermark)
	binary.Write(buff, binary.BigEndian, fp.LastStableOffset)
	binary.Write(buff, binary.BigEndian, fp.LogStartOffset)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(fp.AbortedTransactions)+1)))
	for _, a := range fp.AbortedTransactions {
		buff.Write(a.Encode())
	}
	binary.Write(buff, binary.BigEndian, fp.PreferredReadReplica)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(fp.Records)+1)))
	for _, r := range fp.Records {
		buff.Write(r)
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(fp.TagBuffer))))

	return buff.Bytes()
}

type AbortedTransaction struct {
	ProducerId  int64
	FirstOffset int64
	TagBuffer   []byte
}

func (a *AbortedTransaction) Encode() []byte {
	buff := new(bytes.Buffer)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(a.TagBuffer))))
	return buff.Bytes()
}

func NewFetchResponse(header *RequestHeaderV2, body []byte) ApiKeyResp {
	req := new(FetchRequest)
	req.Header = header
	err := req.DecodeRequest(body)
	// fmt.Printf("fetch request: %+v\n", req.Topics[0].Partitions[0])
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
	res.Responses = []*FetchRespVal{}
	if len(req.Topics) > 0 {
		fmt.Println("DO we even get here")
		for _, t := range req.Topics {
			frv := new(FetchRespVal)

			frv.TopicId = t.TopicId
			// TODO get partitions from file. for now just return error partition
			p := new(FetchResponsePartition)
			isValidTopicId, batchRecs := FindBatchesForTopicRequest(t)
			if !isValidTopicId {
				p.ErrorCode = ErrUnknownTopicId
			} else {
				p.Records = batchRecs
			}
			p.PartitionIndex = 0
			p.TagBuffer = []byte{}
			frv.Partitions = append(frv.Partitions, p)

			frv.TagBuffer = []byte{}
			res.Responses = append(res.Responses, frv)
		}
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
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(f.TagBuffer))))
	resBuff := []byte{}
	resBuff = binary.BigEndian.AppendUint32(resBuff, uint32(buff.Len()))
	resBuff = append(resBuff, buff.Bytes()...)
	return resBuff
}
