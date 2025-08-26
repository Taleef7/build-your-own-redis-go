package main

import (
	"bufio"
	"flag"
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

// replicaMode is true when server started with --replicaof
var replicaMode bool

// Stream support
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

var streamStorage = make(map[string][]StreamEntry)

// For BLPOP: map from list key to a slice of waiting channels
var blpopWaiters = make(map[string][]chan [2]string)
var blpopMutex sync.Mutex

// Replication metadata (initialized at startup)
var masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var masterReplOffset int64 = 0

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
		if entryID == "*" {
			idTime := time.Now().UnixNano() / int64(time.Millisecond)
			storageMutex.Lock()
			entries := streamStorage[streamKey]
			maxSeq := int64(-1)
			for _, e := range entries {
				parts := strings.Split(e.ID, "-")
				if len(parts) != 2 {
					continue
				}
				t, _ := strconv.ParseInt(parts[0], 10, 64)
				s, _ := strconv.ParseInt(parts[1], 10, 64)
				if t == idTime && s > maxSeq {
					maxSeq = s
				}
			}
			var idSeq int64
			if maxSeq < 0 {
				idSeq = 0
			} else {
				idSeq = maxSeq + 1
			}
			entryID = fmt.Sprintf("%d-%d", idTime, idSeq)
			// Validate ordering as before
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
		}
		// Handle explicit and partial auto-generated IDs
		idParts := strings.Split(entryID, "-")
		var idTime int64
		var idSeq int64
		var err1, err2 error
		if len(idParts) == 2 && idParts[1] == "*" {
			idTime, err1 = strconv.ParseInt(idParts[0], 10, 64)
			if err1 != nil || idTime < 0 {
				return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
			}
			// Auto-generate sequence number
			storageMutex.Lock()
			entries := streamStorage[streamKey]
			maxSeq := int64(-1)
			for _, e := range entries {
				parts := strings.Split(e.ID, "-")
				if len(parts) != 2 {
					continue
				}
				t, _ := strconv.ParseInt(parts[0], 10, 64)
				s, _ := strconv.ParseInt(parts[1], 10, 64)
				if t == idTime && s > maxSeq {
					maxSeq = s
				}
			}
			if idTime == 0 && maxSeq < 0 {
				idSeq = 1
			} else if maxSeq < 0 {
				idSeq = 0
			} else {
				idSeq = maxSeq + 1
			}
			entryID = fmt.Sprintf("%d-%d", idTime, idSeq)
			// Validate ordering as before
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
		}
		// Validate ID format and ordering
		if len(idParts) != 2 {
			return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
		}
		idTime, err1 = strconv.ParseInt(idParts[0], 10, 64)
		idSeq, err2 = strconv.ParseInt(idParts[1], 10, 64)
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

	case "info":
		// Only the replication section is required for these stages
		if len(args) >= 2 && strings.ToLower(args[1]) == "replication" {
			role := "master"
			if replicaMode {
				role = "slave"
			}
			// Build multiline body: each line ends with \r\n; last line should not count its trailing CRLF in length, but must be sent
			body := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", role, masterReplID, masterReplOffset)
			return fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)
		}
		// Unused sections -> empty bulk string
		return "$0\r\n"
	case "xrange":
		if len(args) != 4 {
			return "-ERR wrong number of arguments for XRANGE command\r\n"
		}
		streamKey := args[1]
		startID := args[2]
		endID := args[3]
		storageMutex.RLock()
		entries := streamStorage[streamKey]
		storageMutex.RUnlock()
		// Parse start and end IDs
		parseID := func(id string, isStart bool) (int64, int64) {
			parts := strings.Split(id, "-")
			if len(parts) == 1 {
				t, _ := strconv.ParseInt(parts[0], 10, 64)
				if isStart {
					return t, 0
				} else {
					return t, 1<<63 - 1 // max int64
				}
			}
			t, _ := strconv.ParseInt(parts[0], 10, 64)
			s, _ := strconv.ParseInt(parts[1], 10, 64)
			return t, s
		}
		startT, startS := parseID(startID, true)
		endT, endS := parseID(endID, false)
		var resp string
		var resultEntries []string
		for _, entry := range entries {
			parts := strings.Split(entry.ID, "-")
			if len(parts) != 2 {
				continue
			}
			et, _ := strconv.ParseInt(parts[0], 10, 64)
			es, _ := strconv.ParseInt(parts[1], 10, 64)
			if (et > startT || (et == startT && es >= startS)) && (et < endT || (et == endT && es <= endS)) {
				// Format entry as RESP array
				entryResp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.ID), entry.ID)
				// Fields as RESP array
				fields := make([]string, 0, len(entry.Fields)*2)
				for k, v := range entry.Fields {
					fields = append(fields, k, v)
				}
				fieldsResp := fmt.Sprintf("*%d\r\n", len(fields))
				for _, fv := range fields {
					fieldsResp += fmt.Sprintf("$%d\r\n%s\r\n", len(fv), fv)
				}
				entryResp += fieldsResp
				resultEntries = append(resultEntries, entryResp)
			}
		}
		resp = fmt.Sprintf("*%d\r\n", len(resultEntries))
		for _, e := range resultEntries {
			resp += e
		}
		return resp
	case "xread":
		// Support: XREAD [BLOCK ms] streams stream1 stream2 ... id1 id2 ...
		blockTimeout := int64(0)
		blockIdx := -1
		streamsIdx := -1
		for i := 1; i < len(args); i++ {
			if strings.ToLower(args[i]) == "block" && i+1 < len(args) {
				blockIdx = i
				blockTimeout, _ = strconv.ParseInt(args[i+1], 10, 64)
				continue
			}
			if strings.ToLower(args[i]) == "streams" {
				streamsIdx = i
				break
			}
		}
		if streamsIdx == -1 || streamsIdx+1 >= len(args) {
			return "-ERR wrong number of arguments for XREAD command\r\n"
		}
		streams := []string{}
		ids := []string{}
		for i := streamsIdx + 1; i < len(args); i++ {
			if i-streamsIdx-1 < (len(args)-streamsIdx-1)/2 {
				streams = append(streams, args[i])
			} else {
				ids = append(ids, args[i])
			}
		}
		if len(streams) != len(ids) {
			return "-ERR number of streams and IDs must match\r\n"
		}
		// Try to get entries immediately
		found := false
		var resp string
		allEntryArrs := make([][]string, len(streams))
		for i, streamKey := range streams {
			lastID := ids[i]
			storageMutex.RLock()
			entries := streamStorage[streamKey]
			storageMutex.RUnlock()
			// If lastID is $, treat as current max ID in the stream (or 0-0 if empty)
			if lastID == "$" {
				if len(entries) == 0 {
					lastID = "0-0"
				} else {
					lastID = entries[len(entries)-1].ID
				}
			}
			lastParts := strings.Split(lastID, "-")
			var lastT, lastS int64
			if len(lastParts) == 2 {
				lastT, _ = strconv.ParseInt(lastParts[0], 10, 64)
				lastS, _ = strconv.ParseInt(lastParts[1], 10, 64)
			} else {
				lastT, _ = strconv.ParseInt(lastID, 10, 64)
				lastS = 0
			}
			var entryArrs []string
			for _, entry := range entries {
				parts := strings.Split(entry.ID, "-")
				if len(parts) != 2 {
					continue
				}
				et, _ := strconv.ParseInt(parts[0], 10, 64)
				es, _ := strconv.ParseInt(parts[1], 10, 64)
				if et > lastT || (et == lastT && es > lastS) {
					entryResp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.ID), entry.ID)
					fields := make([]string, 0, len(entry.Fields)*2)
					for k, v := range entry.Fields {
						fields = append(fields, k, v)
					}
					fieldsResp := fmt.Sprintf("*%d\r\n", len(fields))
					for _, fv := range fields {
						fieldsResp += fmt.Sprintf("$%d\r\n%s\r\n", len(fv), fv)
					}
					entryResp += fieldsResp
					entryArrs = append(entryArrs, entryResp)
				}
			}
			if len(entryArrs) > 0 {
				found = true
			}
			allEntryArrs[i] = entryArrs
		}
		if found || blockIdx == -1 {
			// Return immediately if found or not blocking
			resp = fmt.Sprintf("*%d\r\n", len(streams))
			for i, streamKey := range streams {
				entryArrs := allEntryArrs[i]
				resp += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n*%d\r\n", len(streamKey), streamKey, len(entryArrs))
				for _, e := range entryArrs {
					resp += e
				}
			}
			return resp
		}
		// Blocking: set up a channel and wait for new entries or timeout
		ch := make(chan string, 1)
		for i, streamKey := range streams {
			go func(idx int, key, lastID string) {
				// Wait for a new entry to be added to this stream
				for {
					storageMutex.RLock()
					entries := streamStorage[key]
					storageMutex.RUnlock()
					// If lastID is $, treat as current max ID in the stream (or 0-0 if empty)
					if lastID == "$" {
						if len(entries) == 0 {
							lastID = "0-0"
						} else {
							lastID = entries[len(entries)-1].ID
						}
					}
					lastParts := strings.Split(lastID, "-")
					var lastT, lastS int64
					if len(lastParts) == 2 {
						lastT, _ = strconv.ParseInt(lastParts[0], 10, 64)
						lastS, _ = strconv.ParseInt(lastParts[1], 10, 64)
					} else {
						lastT, _ = strconv.ParseInt(lastID, 10, 64)
						lastS = 0
					}
					for _, entry := range entries {
						parts := strings.Split(entry.ID, "-")
						if len(parts) != 2 {
							continue
						}
						et, _ := strconv.ParseInt(parts[0], 10, 64)
						es, _ := strconv.ParseInt(parts[1], 10, 64)
						if et > lastT || (et == lastT && es > lastS) {
							// Format and send the response for this stream only
							entryResp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.ID), entry.ID)
							fields := make([]string, 0, len(entry.Fields)*2)
							for k, v := range entry.Fields {
								fields = append(fields, k, v)
							}
							fieldsResp := fmt.Sprintf("*%d\r\n", len(fields))
							for _, fv := range fields {
								fieldsResp += fmt.Sprintf("$%d\r\n%s\r\n", len(fv), fv)
							}
							entryResp += fieldsResp
							resp := fmt.Sprintf("*1\r\n*2\r\n$%d\r\n%s\r\n*1\r\n%s", len(key), key, entryResp)
							ch <- resp
							return
						}
					}
					time.Sleep(10 * time.Millisecond)
				}
			}(i, streamKey, ids[i])
		}
		if blockTimeout == 0 {
			// Wait forever
			return <-ch
		}
		timer := time.NewTimer(time.Duration(blockTimeout) * time.Millisecond)
		select {
		case r := <-ch:
			timer.Stop()
			return r
		case <-timer.C:
			return "$-1\r\n"
		}

	case "incr":
		// INCR key
		if len(args) != 2 {
			return "-ERR wrong number of arguments for INCR command\r\n"
		}
		key := args[1]
		// Read-modify-write with mutex
		storageMutex.Lock()
		entry, exists := storage[key]
		if !exists {
			// Key doesn't exist: create it with value "1" (stage 2 requirement)
			entry = StorageEntry{value: "1", expiry: nil}
			storage[key] = entry
			storageMutex.Unlock()
			return ":1\r\n"
		}
		// Try to parse current value as integer
		current, err := strconv.ParseInt(entry.value, 10, 64)
		if err != nil {
			storageMutex.Unlock()
			return "-ERR value is not an integer or out of range\r\n"
		}
		current++
		entry.value = strconv.FormatInt(current, 10)
		storage[key] = entry
		storageMutex.Unlock()
		return fmt.Sprintf(":%d\r\n", current)

	case "multi":
		// Start a transaction; for this stage just acknowledge with +OK
		return "+OK\r\n"

	case "exec":
		// If MULTI hasn't been called, return the exact error bytes required by tests
		return "-ERR EXEC without MULTI\r\n"

	default:
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
		// INFO command: support `INFO replication` returning role
		// (handled above)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	// Handle multiple commands from the same connection
	reader := bufio.NewReader(conn)
	inMulti := false
	var queuedCommands [][]string

	for {
		// Parse the RESP array (command and arguments)
		args, err := parseRESPArray(reader)
		if err != nil {
			// Connection closed or error occurred
			break
		}

		if len(args) == 0 {
			conn.Write([]byte("-ERR no command\r\n"))
			continue
		}

		cmd := strings.ToLower(args[0])

		// Start transaction
		if cmd == "multi" {
			inMulti = true
			queuedCommands = nil
			conn.Write([]byte("+OK\r\n"))
			continue
		}

		// DISCARD: abort transaction
		if cmd == "discard" {
			if !inMulti {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
				continue
			}
			// Abort transaction
			inMulti = false
			queuedCommands = nil
			conn.Write([]byte("+OK\r\n"))
			continue
		}

		// Execute transaction
		if cmd == "exec" {
			if !inMulti {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			if len(queuedCommands) == 0 {
				// Empty transaction
				inMulti = false
				conn.Write([]byte("*0\r\n"))
				continue
			}

			// Execute queued commands and collect their raw RESP replies
			responses := make([]string, 0, len(queuedCommands))
			for _, q := range queuedCommands {
				responses = append(responses, handleCommand(q))
			}

			// Reset transaction state
			inMulti = false
			queuedCommands = nil

			// Build RESP array where each element is the raw reply for a queued command
			resp := fmt.Sprintf("*%d\r\n", len(responses))
			for _, r := range responses {
				resp += r
			}
			conn.Write([]byte(resp))
			continue
		}

		if inMulti {
			// Queue the command and respond with QUEUED
			queuedCommands = append(queuedCommands, args)
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		// Not in MULTI: handle command immediately
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
	// Allow overriding the listen port via --port (default 6379)
	port := flag.Int("port", 6379, "port to listen on")
	replicaOf := flag.String("replicaof", "", "<host> <port> of master to replicate")
	flag.Parse()
	if *replicaOf != "" {
		replicaMode = true
	}
	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", *port)
		os.Exit(1)
	}
	defer l.Close()

	// If running as a replica, initiate handshake with master by sending PING
	if replicaMode {
		parts := strings.Fields(*replicaOf)
		if len(parts) == 2 {
			masterAddr := fmt.Sprintf("%s:%s", parts[0], parts[1])
			go func(listenPort int) {
				conn, err := net.Dial("tcp", masterAddr)
				if err != nil {
					// Silent fail to avoid noisy logs in tests
					return
				}
				// 1) Send RESP array for PING
				_, _ = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
				// Read master's response line (e.g., +PONG)
				rd := bufio.NewReader(conn)
				_, _ = rd.ReadString('\n')

				// 2a) REPLCONF listening-port <PORT>
				pstr := strconv.Itoa(listenPort)
				cmd1 := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(pstr), pstr)
				_, _ = conn.Write([]byte(cmd1))
				// Read +OK
				_, _ = rd.ReadString('\n')

				// 2b) REPLCONF capa psync2
				cmd2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
				_, _ = conn.Write([]byte(cmd2))
				// Read +OK
				_, _ = rd.ReadString('\n')

				// 2c) REPLCONF capa eof (some masters expect this capability too)
				cmd3 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n"
				_, _ = conn.Write([]byte(cmd3))
				// Read +OK
				_, _ = rd.ReadString('\n')

				// 3) PSYNC ? -1
				psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
				_, _ = conn.Write([]byte(psync))
				// Keep the connection open for future stages
			}(*port)
		}
	}

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
