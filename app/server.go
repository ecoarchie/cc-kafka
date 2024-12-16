package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
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

	// reply
	// var body []byte
	body := new(bytes.Buffer)
	if requestApiVersion != 4 {
		// body = binary.BigEndian.AppendUint16(body, uint16(35))
		binary.Write(body, binary.BigEndian, int16(35))
	} else {
		fmt.Println("error code is zero")
		// body = binary.BigEndian.AppendUint16(body, uint16(0))
		binary.Write(body, binary.BigEndian, int16(0))
	}

	if requestApiKey == 18 {
		fmt.Println("request api key is 18")
		numOfKeys := 2
		body.Write(encodeUnsignedVarInt(uint32(numOfKeys)))
		// body = binary.BigEndian.AppendUint32(body, uint32(numOfKeys))
		apiKey := requestApiKey
		binary.Write(body, binary.BigEndian, apiKey)
		// body = binary.BigEndian.AppendUint16(body, uint16(apiKey))
		minVersion := int16(1)
		binary.Write(body, binary.BigEndian, minVersion)
		// body = binary.BigEndian.AppendUint16(body, uint16(minVersion))
		maxVersion := int16(4)
		binary.Write(body, binary.BigEndian, maxVersion)
		// body = binary.BigEndian.AppendUint16(body, uint16(maxVersion))

		tagFieldBodyLength := 0
		body.Write(encodeUnsignedVarInt(uint32(tagFieldBodyLength)))
		// body = append(body, encodeUnsignedVarInt(uint32(tagFieldBodyLength))...)

		var throttleTime int32 = 0
		binary.Write(body, binary.BigEndian, throttleTime)
		// body = binary.BigEndian.AppendUint32(body, uint32(throttleTime))
		tagFieldLength := 0
		body.Write(encodeUnsignedVarInt(uint32(tagFieldLength)))
		// body = append(body, encodeUnsignedVarInt(uint32(tagFieldLength))...)

	}
	// mes := NewMessage(requestApiKey, requestApiVersion, correlationID, body)
	mes := NewMessage(requestApiKey, requestApiVersion, correlationID, body.Bytes())
	fmt.Printf("message: %+v\n", mes)
	reply := mes.Marshal()
	fmt.Printf("marshaled message: %+v\n", reply)
	// var reply []byte
	// reply = binary.BigEndian.AppendUint32(reply, uint32(0)) // hardcode 0 size reply
	// reply = binary.BigEndian.AppendUint32(reply, uint32(correlationID))

	// if requestApiVersion != 4 {
	// 	reply = binary.BigEndian.AppendUint16(reply, uint16(35))
	// }

	conn.Write(reply)
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
	// reply = binary.BigEndian.AppendUint16(reply, uint16(m.header.requestApiKey))
	// reply = binary.BigEndian.AppendUint16(reply, uint16(m.header.requestApiVersion))
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
