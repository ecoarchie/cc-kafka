package api

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type DescribeTopicPartitionsRequest struct {
	Header                 *RequestHeaderV2
	Topics                 []*RequestTopic
	ResponsePartitionLimit int32
	Cursor                 Cursor
	TagBuffer              TagBuffer
}

func (r *DescribeTopicPartitionsRequest) DecodeRequest(body []byte) error {
	buff := bytes.NewBuffer(body)
	topicArrLen, err := binary.ReadUvarint(buff)
	if err != nil {
		return errors.Wrap(err, "error reading topic array length")
	}
	topicArrLen -= 1
	if topicArrLen == 0 {
		r.Topics = []*RequestTopic{}
	} else {
		r.Topics = make([]*RequestTopic, topicArrLen)
		for i := 0; i < int(topicArrLen); i++ {
			var t RequestTopic
			nameLen, err := binary.ReadUvarint(buff)
			if err != nil {
				return errors.Wrap(err, "error reading name of the topic")
			}
			name := make([]byte, nameLen-1)
			err = binary.Read(buff, binary.BigEndian, name)
			if err != nil {
				return errors.Wrap(err, "error reading name from buff")
			}
			t.Name = string(name)

			buff.ReadByte() // read length of tagged fields array. is 0
			t.TagBuffer = []byte{}
			r.Topics[i] = &t
		}
	}

	err = binary.Read(buff, binary.BigEndian, &r.ResponsePartitionLimit)
	if err != nil {
		return errors.Wrap(err, "error reading response partition limit")
	}
	r.Cursor = NullCursor()
	r.TagBuffer = []byte{}
	return nil
}

type DescribeTopicPartitionsResponse struct {
	Header       *ResponseHeaderV1
	ThrottleTime int32
	Topics       []*ResponseTopic
	NextCursor   Cursor
	TagBuffer    TagBuffer
}

type ResponseTopic struct {
	ErrorCode                 ErrorCode
	Name                      string
	TopicId                   uuid.UUID
	IsInternal                bool
	Patritions                []*Partition
	TopicAuthorizedOperations int32
	TagBuffer                 TagBuffer
}

func (rt *ResponseTopic) Encode() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, rt.ErrorCode)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len([]byte(rt.Name))+1)))
	buff.WriteString(rt.Name)
	id, err := rt.TopicId.MarshalBinary()
	if err != nil {
		return nil
	}

	binary.Write(buff, binary.BigEndian, id)
	if rt.IsInternal {
		binary.Write(buff, binary.BigEndian, byte(1))
	} else {
		binary.Write(buff, binary.BigEndian, byte(0))
	}

	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(rt.Patritions)+1)))
	for _, p := range rt.Patritions {
		buff.Write(p.Encode())
	}
	binary.Write(buff, binary.BigEndian, rt.TopicAuthorizedOperations)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(rt.TagBuffer))))
	return buff.Bytes()
}

type Partition struct {
	ErrorCode              ErrorCode
	PartitionIndex         [4]byte
	LeaderId               [4]byte
	LeaderEpoch            [4]byte
	ReplicaNodes           [][4]byte
	IsrNodes               [][4]byte
	EligibleLeaderReplicas [][4]byte
	LastKnownIlr           [][4]byte
	OfflineReplicas        [][4]byte
	TagBuffer              []byte
}

func (p Partition) Encode() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, p.ErrorCode)
	buff.Write(p.PartitionIndex[:])
	buff.Write(p.LeaderId[:])
	buff.Write(p.LeaderEpoch[:])
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(p.ReplicaNodes)+1)))
	for _, n := range p.ReplicaNodes {
		buff.Write(n[:])
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(p.IsrNodes)+1)))
	for _, n := range p.IsrNodes {
		buff.Write(n[:])
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(p.EligibleLeaderReplicas)+1)))
	for _, n := range p.EligibleLeaderReplicas {
		buff.Write(n[:])
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(p.LastKnownIlr)+1)))
	for _, n := range p.LastKnownIlr {
		buff.Write(n[:])
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(p.OfflineReplicas)+1)))
	for _, n := range p.OfflineReplicas {
		buff.Write(n[:])
	}
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(p.TagBuffer))))
	return buff.Bytes()
}

type RequestTopic struct {
	Name      string
	TagBuffer TagBuffer
}

type Cursor []byte

func NullCursor() Cursor {
	return []byte{0xff}
}

func NewDescribeTopicPartitionsResponse(header *RequestHeaderV2, body []byte) ApiKeyResp {
	req := new(DescribeTopicPartitionsRequest)
	req.Header = header
	err := req.DecodeRequest(body)
	if err != nil {
		fmt.Println("Here in decode request error")
		fmt.Println(err)
	}
	res := new(DescribeTopicPartitionsResponse)
	res.Header = &ResponseHeaderV1{
		CorrelationID: req.Header.CorrelationID,
		TagBuffer:     []byte{},
	}
	res.ThrottleTime = 0
	reqTopic := req.Topics[0]

	topic := new(ResponseTopic)
	topicId, partitions := FindPartitionsForTopic(reqTopic.Name)
	if topicId == uuid.Nil {
		topic.ErrorCode = ErrUnknownTopic
		topic.Patritions = []*Partition{}
	} else {
		topic.Patritions = partitions
	}

	topic.Name = reqTopic.Name
	topic.TopicId = topicId
	topic.IsInternal = false
	topic.TopicAuthorizedOperations = 3576
	topic.TagBuffer = []byte{}
	res.Topics = append(res.Topics, topic)
	return res
}

func (d *DescribeTopicPartitionsResponse) EncodeResponse() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, d.Header.CorrelationID)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(d.Header.TagBuffer))))
	binary.Write(buff, binary.BigEndian, d.ThrottleTime)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(d.Topics)+1)))
	for _, t := range d.Topics {
		buff.Write(t.Encode())
	}
	buff.Write(NullCursor())
	buff.Write(binary.AppendUvarint([]byte{}, uint64(0)))
	resBuff := []byte{}
	resBuff = binary.BigEndian.AppendUint32(resBuff, uint32(buff.Len()))
	resBuff = append(resBuff, buff.Bytes()...)
	return resBuff
}
