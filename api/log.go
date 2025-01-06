package api

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
)

type RecordBatchBytes struct{}

type RecordBatch struct {
	Header       *BatchHeader
	RecordsBytes [][]byte
}

func (rb RecordBatch) ToBytes() []byte {
	w := new(bytes.Buffer)

	binary.Write(w, binary.BigEndian, rb.Header)
	w.Write(binary.AppendUvarint([]byte{}, uint64(len(rb.RecordsBytes)+1)))

	for _, r := range rb.RecordsBytes {
		w.Write(binary.AppendVarint([]byte{}, int64(len(r))))
		w.Write(r)
	}
	return w.Bytes()
}

type BatchHeader struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	MagicByte            int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsLength        int32
}

type Record struct {
	Length            int64
	Attributes        int8
	TimestampDelta    int64
	OffsetDelta       int64
	KeyLength         int64
	Key               []byte
	ValueLength       int64
	ValueHeader       ValueHeader
	Value             Value
	HeadersArrayCount uint64
}

type ValueHeader struct {
	FrameVersion int8
	Type         int8
	Version      int8
}

type Value interface {
	GetName() string
}

// type 12
type FeatureLevelValue struct {
	Name              []byte
	FeatureLevel      [2]byte
	TaggedFieldsCount uint64
}

func (f FeatureLevelValue) GetName() string {
	return string(f.Name)
}

func NewFeatureValue(b *bufio.Reader) *FeatureLevelValue {
	len, _ := binary.ReadUvarint(b)
	t := new(FeatureLevelValue)
	t.Name = make([]byte, len-1)
	b.Read(t.Name)
	b.Read(t.FeatureLevel[:])
	t.TaggedFieldsCount, _ = binary.ReadUvarint(b)

	return t
}

// type 2
type TopicValue struct {
	Name              []byte
	Uuid              [16]byte
	TaggedFieldsCount uint64
}

func (t TopicValue) GetName() string {
	return string(t.Name)
}

func (t TopicValue) GetUuid() uuid.UUID {
	return t.Uuid
}

func NewTopicValue(b *bufio.Reader) *TopicValue {
	len, _ := binary.ReadUvarint(b)
	t := new(TopicValue)
	t.Name = make([]byte, len-1)
	b.Read(t.Name)
	err := binary.Read(b, binary.BigEndian, t.Uuid[:])
	if err != nil {
		fmt.Println("Error decoding uuid as bigendian", err)
	}
	if err != nil {
		fmt.Println("Errpr parsing uuid", err)
	}
	t.TaggedFieldsCount, _ = binary.ReadUvarint(b)
	return t
}

// type 3
type PartitionValue struct {
	PartitionId       [4]byte
	TopicUuid         [16]byte
	ReplicaArray      [][4]byte
	ISR               [][4]byte
	RemovingReplicas  [][4]byte
	AddingReplicas    [][4]byte
	LeaderId          [4]byte
	LeaderEpoch       [4]byte
	PartitionEpoch    [4]byte
	Directories       [][16]byte
	TaggedFieldsCount uint64
}

func (p PartitionValue) GetName() string {
	return ""
}

func (p *PartitionValue) GetTopicId() uuid.UUID {
	return p.TopicUuid
}

func NewPartitionValue(b *bufio.Reader) *PartitionValue {
	p := new(PartitionValue)
	b.Read(p.PartitionId[:])
	_, err := b.Read(p.TopicUuid[:])
	if err != nil {
		fmt.Println("error reading topic id", err)
	}
	replCount, _ := binary.ReadUvarint(b)
	for i := 0; i < int(replCount)-1; i++ {
		p.ReplicaArray = append(p.ReplicaArray, [4]byte{})
		_, err := b.Read(p.ReplicaArray[i][:])
		if err != nil {
			fmt.Println("Error reading replica array")
			panic(err)
		}
	}
	isrCount, _ := binary.ReadUvarint(b)
	for i := 0; i < int(isrCount)-1; i++ {
		p.ISR = append(p.ISR, [4]byte{})
		b.Read(p.ISR[i][:])
	}
	removingReplicasCount, _ := binary.ReadUvarint(b)
	for i := 0; i < int(removingReplicasCount)-1; i++ {
		p.RemovingReplicas = append(p.RemovingReplicas, [4]byte{})
		b.Read(p.RemovingReplicas[i][:])
	}
	addingReplicasCount, _ := binary.ReadUvarint(b)
	for i := 0; i < int(addingReplicasCount)-1; i++ {
		p.AddingReplicas = append(p.AddingReplicas, [4]byte{})
		b.Read(p.AddingReplicas[i][:])
	}
	_, err = b.Read(p.LeaderId[:])
	if err != nil {
		fmt.Printf("error readinf leader id: %v", err)
	}
	b.Read(p.LeaderEpoch[:])
	b.Read(p.PartitionEpoch[:])
	directoriesCount, _ := binary.ReadUvarint(b)
	for i := 0; i < int(directoriesCount)-1; i++ {
		p.Directories = append(p.Directories, [16]byte{})
		b.Read(p.Directories[i][:])
	}
	p.TaggedFieldsCount, _ = binary.ReadUvarint(b)
	return p
}

func FindPartitionsForTopic(name string) (uuid.UUID, []*Partition) {
	logName := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	fd, err := os.Open(logName)
	if err != nil {
		panic("unable to read log file")
	}
	defer fd.Close()

	var topicId uuid.UUID
	batches := getRecordBatches(fd)
	var partitions []*Partition
	for _, batch := range batches {
		for _, rec := range batch.RecordsBytes {
			record := parseRecordBytes(rec)
			if record.ValueHeader.Type == 2 {
				value := record.Value.(*TopicValue)
				if value.GetName() == name {
					topicId = value.GetUuid()
				}
			}
			if record.ValueHeader.Type == 3 {
				value := record.Value.(*PartitionValue)
				if value.GetTopicId() == topicId {
					p := new(Partition)
					p.ErrorCode = 0
					p.PartitionIndex = value.PartitionId
					p.LeaderId = value.LeaderId
					p.LeaderEpoch = value.LeaderEpoch
					p.ReplicaNodes = value.ReplicaArray
					p.IsrNodes = value.ISR
					p.TagBuffer = []byte{}
					partitions = append(partitions, p)

				}
			}
		}
	}
	return topicId, partitions
}

func FindBatchesForTopicRequest(t *FetchRequestTopic) (bool, [][]byte) {
	logName := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	partitionDirName := "/tmp/kraft-combined-logs/"
	fd, err := os.Open(logName)
	if err != nil {
		panic("unable to read log file")
	}
	defer fd.Close()

	isTopicExist := false
	batches := getRecordBatches(fd)
	partitionDirNames := []string{}
	var batchesToReturn [][]byte
	for _, batch := range batches {
		for _, rec := range batch.RecordsBytes {
			record := parseRecordBytes(rec)
			if record.ValueHeader.Type == 2 {
				value := record.Value.(*TopicValue)
				if value.GetUuid() == t.TopicId {
					isTopicExist = true
					topicName := value.GetName()
					for _, p := range t.Partitions {
						partitionDirNames = append(partitionDirNames, partitionDirName+topicName+"-"+string(p.Partition)+"/")
					}
				}
			}
			// if record.ValueHeader.Type == 3 {
			// 	value := record.Value.(*PartitionValue)
			// 	if value.GetTopicId() == t.TopicId {
			// 		fmt.Printf("batch exists %v\n", batch)
			// 		fmt.Printf("partition: %+v\n", value)
			// 		batchesToReturn = append(batchesToReturn, batch)
			// 		break
			// 	}
			// }
		}
	}
	for _, f := range partitionDirNames {
		bb, err := os.ReadFile(f + "00000000000000000000.log")
		if err != nil {
			fmt.Printf("error reading partition log file: %v\n", err)
			return isTopicExist, batchesToReturn
		}
		batchesToReturn = append(batchesToReturn, bb)
	}
	return isTopicExist, batchesToReturn
}

func getRecordBatches(fd *os.File) []*RecordBatch {
	rd := bufio.NewReader(fd)
	batches := make([]*RecordBatch, 0)
	for {
		rb := new(RecordBatch)
		rb.Header = new(BatchHeader)
		err := binary.Read(rd, binary.BigEndian, rb.Header)
		if err != nil && err == io.EOF {
			break
		}
		recBuff := make([]byte, rb.Header.BatchLength-49)
		rd.Read(recBuff)
		b := bytes.NewBuffer(recBuff)
		rb.RecordsBytes = make([][]byte, rb.Header.RecordsLength)
		for i := 0; i < int(rb.Header.RecordsLength); i++ {
			rLen, _ := binary.ReadVarint(b)
			rec := make([]byte, rLen)

			binary.Read(b, binary.BigEndian, rec)
			rb.RecordsBytes[i] = rec
		}
		batches = append(batches, rb)
	}
	return batches
}

func parseRecordBytes(rec []byte) *Record {
	buf := bufio.NewReader(bytes.NewReader(rec))
	r := new(Record)
	binary.Read(buf, binary.BigEndian, &r.Attributes)
	r.TimestampDelta, _ = binary.ReadVarint(buf)
	r.OffsetDelta, _ = binary.ReadVarint(buf)
	r.KeyLength, _ = binary.ReadVarint(buf)
	r.ValueLength, _ = binary.ReadVarint(buf)
	binary.Read(buf, binary.BigEndian, &r.ValueHeader)
	switch r.ValueHeader.Type {
	case 2:
		r.Value = NewTopicValue(buf)
	case 3:
		r.Value = NewPartitionValue(buf)
	case 12:
		r.Value = NewFeatureValue(buf)
	}

	r.HeadersArrayCount, _ = binary.ReadUvarint(buf)
	return r
}
