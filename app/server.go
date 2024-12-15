package main

import (
	"bufio"
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

	buff.Discard(2) // discard 2 bytes for request_api_key

	var requestApiVersion int16
	binary.Read(buff, binary.BigEndian, &requestApiVersion)
	fmt.Printf("requestApiVersion is %d\n", requestApiVersion)

	var correlationID int32
	binary.Read(buff, binary.BigEndian, &correlationID)
	fmt.Printf("correlationID is %d\n", correlationID)

	// reply
	var reply []byte
	reply = binary.BigEndian.AppendUint32(reply, uint32(0)) // hardcode 0 size reply
	reply = binary.BigEndian.AppendUint32(reply, uint32(correlationID))

	if requestApiVersion != 4 {
		reply = binary.BigEndian.AppendUint16(reply, uint16(35))
	}

	// mesSizeRepl := make([]byte, 4)
	// binary.BigEndian.PutUint32(mesSizeRepl, uint32(0))

	// correlationReply := make([]byte, 4)
	// binary.BigEndian.PutUint32(correlationReply, uint32(correlationID))
	// conn.Write(append(mesSizeRepl, correlationReply...))
	conn.Write(reply)
}
