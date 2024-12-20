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
		return errors.Wrap(err, "error readint topic array length")
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

			t.TagBuffer = []byte{}
			r.Topics[i] = &t
		}
	}
	// r.Topics = topics

	err = binary.Read(buff, binary.BigEndian, r.ResponsePartitionLimit)
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
	binary.Write(buff, binary.LittleEndian, rt.IsInternal)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(rt.Patritions)+1)))
	binary.Write(buff, binary.BigEndian, rt.TopicAuthorizedOperations)
	buff.Write(binary.AppendUvarint([]byte{}, uint64(len(rt.TagBuffer))))
	return buff.Bytes()
}

type Partition struct {
	ErrorCode              ErrorCode
	PartitionIndex         int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           int32
	IsrNodes               int32
	EligibleLeaderReplicas int32
	LastKnownIlr           int32
	OfflineReplicas        int32
}

type RequestTopic struct {
	Name      string
	TagBuffer TagBuffer
}

type Cursor []byte

func NullCursor() Cursor {
	var c []byte
	return append(c, 0xff)
}

func NewDescribeTopicPartitionsResponse(header *RequestHeaderV2, body []byte) ApiKeyResp {
	req := new(DescribeTopicPartitionsRequest)
	req.Header = header
	var err error
	err = req.DecodeRequest(body)
	if err != nil {
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
	topic.ErrorCode = ErrUnknownTopic
	topic.Name = reqTopic.Name
	topic.TopicId, err = uuid.Parse("00000000-0000-0000-0000-000000000000")
	if err != nil {
		fmt.Println("errorparsing uuid ")
		return nil
	}
	topic.IsInternal = false
	topic.Patritions = []*Partition{}
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
