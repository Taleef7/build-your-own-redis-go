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

// Stream support
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

var streamStorage = make(map[string][]StreamEntry)

// For BLPOP: map from list key to a slice of waiting channels
var blpopWaiters = make(map[string][]chan [2]string)
var blpopMutex sync.Mutex

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
	case "blpop":
		if len(args) != 3 {
			return "-ERR wrong number of arguments for BLPOP command\r\n"
		}
		key := args[1]
		timeoutStr := args[2]
		timeoutSec, err := strconv.ParseFloat(timeoutStr, 64)
		if err != nil || timeoutSec < 0 {
			return "-ERR invalid timeout\r\n"
		}
		storageMutex.Lock()
		list := listStorage[key]
		if len(list) > 0 {
			removed := list[0]
			list = list[1:]
			listStorage[key] = list
			storageMutex.Unlock()
			resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(removed), removed)
			return resp
		}
		storageMutex.Unlock()
		ch := make(chan [2]string, 1)
		blpopMutex.Lock()
		blpopWaiters[key] = append(blpopWaiters[key], ch)
		blpopMutex.Unlock()
		if timeoutSec == 0 {
			result := <-ch // Wait indefinitely
			resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(result[0]), result[0], len(result[1]), result[1])
			return resp
		}
		timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
		select {
		case result := <-ch:
			resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(result[0]), result[0], len(result[1]), result[1])
			timer.Stop()
			return resp
		case <-timer.C:
			// Remove this waiter from the queue
			blpopMutex.Lock()
			waiters := blpopWaiters[key]
			for i, waiter := range waiters {
				if waiter == ch {
					blpopWaiters[key] = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
			blpopMutex.Unlock()
			return "$-1\r\n"
		}
	case "rpush":
		if len(args) < 3 {
			return "-ERR wrong number of arguments for RPUSH command\r\n"
		}
		key := args[1]
		elements := args[2:]
		storageMutex.Lock()
		list, exists := listStorage[key]
		if !exists {
			list = []string{}
		}
		// Before appending, check for BLPOP waiters
		blpopMutex.Lock()
		waiters := blpopWaiters[key]
		if len(waiters) > 0 {
			ch := waiters[0]
			blpopWaiters[key] = waiters[1:]
			blpopMutex.Unlock()
			ch <- [2]string{key, elements[0]}
			if len(elements) > 1 {
				// Only the first element goes to the waiter, the rest go to the list
				list = append(list, elements[1:]...)
				listStorage[key] = list
			}
			length := len(list) + 1 // +1 for the element sent to the waiter
			storageMutex.Unlock()
			return fmt.Sprintf(":%d\r\n", length)
		}
		blpopMutex.Unlock()
		list = append(list, elements...)
		listStorage[key] = list
		length := len(list)
		storageMutex.Unlock()
		return fmt.Sprintf(":%d\r\n", length)
	case "lpush":
		if len(args) < 3 {
			return "-ERR wrong number of arguments for LPUSH command\r\n"
		}
		key := args[1]
		elements := args[2:]
		storageMutex.Lock()
		list := listStorage[key]
		// Before prepending, check for BLPOP waiters
		blpopMutex.Lock()
		waiters := blpopWaiters[key]
		if len(waiters) > 0 {
			ch := waiters[0]
			blpopWaiters[key] = waiters[1:]
			blpopMutex.Unlock()
			ch <- [2]string{key, elements[0]}
			if len(elements) > 1 {
				// Only the first element goes to the waiter, the rest go to the list
				list = append(elements[1:], list...)
				listStorage[key] = list
			}
			length := len(list) + 1 // +1 for the element sent to the waiter
			storageMutex.Unlock()
			return fmt.Sprintf(":%d\r\n", length)
		}
		blpopMutex.Unlock()
		// Prepend elements in order (leftmost argument becomes new head)
		for i := 0; i < len(elements); i++ {
			list = append([]string{elements[i]}, list...)
		}
		listStorage[key] = list
		length := len(list)
		storageMutex.Unlock()
		return fmt.Sprintf(":%d\r\n", length)
	case "lrange":
		if len(args) != 4 {
			return "-ERR wrong number of arguments for LRANGE command\r\n"
		}
		key := args[1]
		startRaw, err1 := strconv.Atoi(args[2])
		stopRaw, err2 := strconv.Atoi(args[3])
		storageMutex.RLock()
		list := listStorage[key]
		storageMutex.RUnlock()
		listLen := len(list)
		if err1 != nil || err2 != nil || listLen == 0 {
			return "*0\r\n"
		}
		// Convert negative indexes
		start := startRaw
		stop := stopRaw
		if start < 0 {
			start = listLen + start
			if start < 0 {
				start = 0
			}
		}
		if stop < 0 {
			stop = listLen + stop
			if stop < 0 {
				stop = 0
			}
		}
		if start > stop || start >= listLen {
			return "*0\r\n"
		}
		if stop >= listLen {
			stop = listLen - 1
		}
		result := list[start : stop+1]
		resp := fmt.Sprintf("*%d\r\n", len(result))
		for _, elem := range result {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
		}
		return resp
	case "llen":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for LLEN command\r\n"
		}
		key := args[1]
		storageMutex.RLock()
		list := listStorage[key]
		storageMutex.RUnlock()
		return fmt.Sprintf(":%d\r\n", len(list))
	case "lpop":
		if len(args) != 2 && len(args) != 3 {
			return "-ERR wrong number of arguments for LPOP command\r\n"
		}
		key := args[1]
		count := 1
		if len(args) == 3 {
			var err error
			count, err = strconv.Atoi(args[2])
			if err != nil || count < 1 {
				return "-ERR count must be a positive integer\r\n"
			}
		}
		storageMutex.Lock()
		list := listStorage[key]
		if len(list) == 0 {
			storageMutex.Unlock()
			if len(args) == 3 {
				return "*0\r\n"
			}
			return "$-1\r\n"
		}
		if len(args) == 2 {
			removed := list[0]
			list = list[1:]
			listStorage[key] = list
			storageMutex.Unlock()
			return fmt.Sprintf("$%d\r\n%s\r\n", len(removed), removed)
		}
		// Multi-element LPOP
		if count > len(list) {
			count = len(list)
		}
		removed := list[:count]
		list = list[count:]
		listStorage[key] = list
		storageMutex.Unlock()
		resp := fmt.Sprintf("*%d\r\n", len(removed))
		for _, elem := range removed {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
		}
		return resp
	case "xadd":
		if len(args) < 5 || (len(args)-3)%2 != 0 {
			return "-ERR wrong number of arguments for XADD command\r\n"
		}
		streamKey := args[1]
		entryID := args[2]
		// Validate ID format and ordering
		idParts := strings.Split(entryID, "-")
		if len(idParts) != 2 {
			return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
		}
		idTime, err1 := strconv.ParseInt(idParts[0], 10, 64)
		idSeq, err2 := strconv.ParseInt(idParts[1], 10, 64)
		if err1 != nil || err2 != nil || idTime < 0 || idSeq < 0 {
			return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
		}
		if idTime == 0 && idSeq == 0 {
			return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
		}
		storageMutex.Lock()
		entries := streamStorage[streamKey]
		if len(entries) > 0 {
			lastID := entries[len(entries)-1].ID
			lastParts := strings.Split(lastID, "-")
			lastTime, _ := strconv.ParseInt(lastParts[0], 10, 64)
			lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)
			if idTime < lastTime || (idTime == lastTime && idSeq <= lastSeq) {
				storageMutex.Unlock()
				return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
			}
		}
		fields := make(map[string]string)
		for i := 3; i < len(args); i += 2 {
			fields[args[i]] = args[i+1]
		}
		entry := StreamEntry{ID: entryID, Fields: fields}
		streamStorage[streamKey] = append(streamStorage[streamKey], entry)
		storageMutex.Unlock()
		return fmt.Sprintf("$%d\r\n%s\r\n", len(entryID), entryID)
	case "type":
		if len(args) != 2 {
			return "-ERR wrong number of arguments for TYPE command\r\n"
		}
		key := args[1]
		storageMutex.RLock()
		_, exists := storage[key]
		_, streamExists := streamStorage[key]
		storageMutex.RUnlock()
		if exists {
			return "+string\r\n"
		}
		if streamExists {
			return "+stream\r\n"
		}
		return "+none\r\n"
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
