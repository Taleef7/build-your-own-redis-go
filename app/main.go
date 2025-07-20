package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleClient(conn net.Conn) {
	defer conn.Close()

	// Handle multiple commands from the same connection
	reader := bufio.NewReader(conn)
	for {
		// Read the number of arguments (starts with *)
		line, err := reader.ReadString('\n')
		if err != nil {
			// Connection closed or error occurred
			break
		}
		
		// Check if it's a RESP array (starts with *)
		if !strings.HasPrefix(line, "*") {
			continue
		}
		
		// Parse the number of arguments
		argCount, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
		if err != nil {
			continue
		}
		
		// Read all arguments
		for i := 0; i < argCount; i++ {
			// Read the argument length (starts with $)
			argLenLine, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			
			if !strings.HasPrefix(argLenLine, "$") {
				continue
			}
			
			// Parse the argument length
			argLen, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(argLenLine, "$")))
			if err != nil {
				continue
			}
			
			// Read the argument value
			arg := make([]byte, argLen)
			_, err = reader.Read(arg)
			if err != nil {
				break
			}
			
			// Read the \r\n after the argument
			_, err = reader.ReadString('\n')
			if err != nil {
				break
			}
		}
		
		// Respond with PONG for each complete command
		conn.Write([]byte("+PONG\r\n"))
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	// Accept multiple connections concurrently
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		
		// Handle each client in a separate goroutine
		go handleClient(conn)
	}
}
