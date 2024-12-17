package api

type DescribeTopicPartitions struct{}

func (d *DescribeTopicPartitions) Encode() []byte {
	return nil
}
