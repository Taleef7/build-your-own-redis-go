package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// StorageEntry represents a key-value pair with optional expiry
type StorageEntry struct {
	value  string
	expiry *time.Time // nil means no expiry
}

// Global storage for key-value pairs with mutex for thread safety
var (
	storage      = make(map[string]StorageEntry)
	listStorage  = make(map[string][]string) // New map for lists
	storageMutex sync.RWMutex
)

func parseRESPArray(reader *bufio.Reader) ([]string, error) {
	// Read the number of arguments (starts with *)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// Check if it's a RESP array (starts with *)
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array, got: %s", line)
	}

	// Parse the number of arguments
	argCount, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}

	args := make([]string, argCount)

	// Read all arguments
	for i := 0; i < argCount; i++ {
		// Read the argument length (starts with $)
		argLenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		if !strings.HasPrefix(argLenLine, "$") {
			return nil, fmt.Errorf("expected bulk string, got: %s", argLenLine)
		}

		// Parse the argument length
		argLen, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(argLenLine, "$")))
		if err != nil {
			return nil, err
		}

		// Read the argument value
		arg := make([]byte, argLen)
		_, err = reader.Read(arg)
		if err != nil {
			return nil, err
		}

		// Read the \r\n after the argument
		_, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		args[i] = string(arg)
	}

	return args, nil
}

func isExpired(entry StorageEntry) bool {
	if entry.expiry == nil {
		return false
	}
	return time.Now().After(*entry.expiry)
}

func handleCommand(args []string) string {
	if len(args) == 0 {
		return "-ERR no command\r\n"
	}

	command := strings.ToLower(args[0])

	switch command {
	case "ping":
		return "+PONG\r\n"
	case "echo":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for ECHO command\r\n"
		}
		// Return the argument as a RESP bulk string
		message := args[1]
		return fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)
	case "set":
		if len(args) < 3 {
			return "-ERR wrong number of arguments for SET command\r\n"
		}
		key := args[1]
		value := args[2]

		// Parse optional expiry arguments
		var expiry *time.Time
		for i := 3; i < len(args); i += 2 {
			if i+1 >= len(args) {
				return "-ERR wrong number of arguments for SET command\r\n"
			}

			option := strings.ToLower(args[i])
			if option == "px" {
				// Parse expiry in milliseconds
				expiryMs, err := strconv.Atoi(args[i+1])
				if err != nil {
					return "-ERR value is not an integer or out of range\r\n"
				}
				expiryTime := time.Now().Add(time.Duration(expiryMs) * time.Millisecond)
				expiry = &expiryTime
			}
		}

		// Store the key-value pair with expiry
		storageMutex.Lock()
		storage[key] = StorageEntry{value: value, expiry: expiry}
		storageMutex.Unlock()

		return "+OK\r\n"
	case "get":
		if len(args) < 2 {
			return "-ERR wrong number of arguments for GET command\r\n"
		}
		key := args[1]

		// Retrieve the value
		storageMutex.RLock()
		entry, exists := storage[key]
		storageMutex.RUnlock()

		if !exists {
			// Return null bulk string for non-existent keys
			return "$-1\r\n"
		}

		// Check if the key has expired
		if isExpired(entry) {
			// Remove expired key
			storageMutex.Lock()
			delete(storage, key)
			storageMutex.Unlock()
			return "$-1\r\n"
		}

		// Return the value as a RESP bulk string
		return fmt.Sprintf("$%d\r\n%s\r\n", len(entry.value), entry.value)
	case "rpush":
		if len(args) < 3 {
			return "-ERR wrong number of arguments for RPUSH command\r\n"
		}
		key := args[1]
		value := args[2]
		storageMutex.Lock()
		list, exists := listStorage[key]
		if !exists {
			listStorage[key] = []string{value}
			storageMutex.Unlock()
			return ":1\r\n"
		}
		// Append to existing list
		list = append(list, value)
		listStorage[key] = list
		length := len(list)
		storageMutex.Unlock()
		return fmt.Sprintf(":%d\r\n", length)
	default:
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	// Handle multiple commands from the same connection
	reader := bufio.NewReader(conn)
	for {
		// Parse the RESP array (command and arguments)
		args, err := parseRESPArray(reader)
		if err != nil {
			// Connection closed or error occurred
			break
		}

		// Handle the command and get response
		response := handleCommand(args)

		// Send the response
		conn.Write([]byte(response))
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
