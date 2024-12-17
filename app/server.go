package main

import (
	"bufio"
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
		apiKey := &api.ApiVersionsKey{
			Key:        requestApiKey,
			MinVersion: 0,
			MaxVersion: 4,
			TagBuffer:  []byte{},
		}
		describeTopicPartitionsKey := &api.ApiVersionsKey{
			Key:        75,
			MinVersion: 0,
			MaxVersion: 0,
			TagBuffer:  []byte{},
		}
		keys = append(keys, apiKey, describeTopicPartitionsKey)
		response := api.NewApiKeyResponse(requestApiKey, requestApiVersion, correlationID, keys)

		conn.Write(response.EncodeResponse())
	}
}
