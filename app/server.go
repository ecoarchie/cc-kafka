package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/api"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)

	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buff := bufio.NewReader(conn)
		var messageSize int32
		binary.Read(buff, binary.BigEndian, &messageSize)
		fmt.Printf("message size is %d\n", messageSize)

		var requestApiKey int16
		binary.Read(buff, binary.BigEndian, &requestApiKey)
		fmt.Printf("requestApiKey is %d\n", requestApiKey)

		var requestApiVersion int16
		binary.Read(buff, binary.BigEndian, &requestApiVersion)
		fmt.Printf("requestApiVersion is %d\n", requestApiVersion)

		var correlationID int32
		binary.Read(buff, binary.BigEndian, &correlationID)
		fmt.Printf("correlationID is %d\n", correlationID)

		keys := []api.ApiKey{}
		apiKey := api.ApiKey{
			Key:        requestApiKey,
			MinVersion: 0,
			MaxVersion: 4,
			TagBuffer:  []byte{},
		}
		keys = append(keys, apiKey)
		response := api.NewApiKeyResponse(requestApiKey, requestApiVersion, correlationID, keys)

		conn.Write(response.Encode())

	}
}

type Header struct {
	requestApiKey     int16
	requestApiVersion int16
	correlationID     int32
}

func (h *Header) Len() int32 {
	return int32(4)
}

type Message struct {
	sizeBytes int32
	header    Header
	body      []byte
}

func NewMessage(requestApiKey int16, requestApiVersion int16, correlationID int32, body []byte) *Message {
	h := Header{
		requestApiKey:     requestApiKey,
		requestApiVersion: requestApiVersion,
		correlationID:     correlationID,
	}

	return &Message{
		sizeBytes: h.Len() + int32(len(body)),
		header:    h,
		body:      body,
	}
}

func (m *Message) Marshal() []byte {
	var reply []byte
	reply = binary.BigEndian.AppendUint32(reply, uint32(m.sizeBytes)) // hardcode 0 size reply
	reply = binary.BigEndian.AppendUint32(reply, uint32(m.header.correlationID))
	reply = append(reply, m.body...)

	return reply
}

func encodeUnsignedVarInt(value uint32) []byte {
	var buf bytes.Buffer
	for {
		// Extract the lowest 7 bits
		encodedByte := byte(value & 0x7F)
		value >>= 7

		// If more bytes are coming, set the MSB to 1
		if value != 0 {
			encodedByte |= 0x80
		}

		buf.WriteByte(encodedByte)

		// Break if this is the last byte (MSB = 0)
		if value == 0 {
			break
		}
	}
	return buf.Bytes()
}
