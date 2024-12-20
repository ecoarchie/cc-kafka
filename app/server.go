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

		body := make([]byte, messageSize)
		buff.Read(body)

		request, err := api.DecodeRequestForSpecifiedVersion(body)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Header: %+v\n", request)

		// response := api.NewApiKeyVersionsResponse(requestHeader)
		respFunc := api.ApiKeysMap[request.Header.RequestApiKey]
		response := respFunc(request.Header, request.Body)
		fmt.Printf("GET response %+v\n", response)
		conn.Write(response.EncodeResponse())
	}
}

// func DecodeRequest(buff *bytes.Buffer) {
// 	var messageSize int32
// 	binary.Read(buff, binary.BigEndian, &messageSize)
// 	fmt.Printf("message size is %d\n", messageSize)

// 	// api.DecodeRequestForSpecifiedVersion(buff)
// }
