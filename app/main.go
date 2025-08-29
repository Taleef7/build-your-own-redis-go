package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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

// Pub/Sub: global subscribers registry (channel -> set of connections)
var pubsubSubscribers = make(map[string]map[net.Conn]bool)
var pubsubMutex sync.Mutex

// Sorted sets
type ZMember struct {
	member string
	score  float64
}

// Keep both: a map for quick member lookup and a sorted slice by (score, member)
type ZSet struct {
	dict   map[string]float64   // member -> score
	sorted []ZMember            // maintained sorted ascending by score, then member
}

var zsetStorage = make(map[string]*ZSet)

// Replication metadata (initialized at startup)
var masterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var masterReplOffset int64 = 0

// Empty RDB file (valid, empty DB) as base64; decoded once
var emptyRDBData []byte
var emptyRDBOnce sync.Once

// RDB configuration parameters
var rdbDir string = ""
var rdbFilename string = ""

func getEmptyRDB() []byte {
	emptyRDBOnce.Do(func() {
		// This is a minimal valid empty RDB (from challenge hints)
		const b64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
		data, err := base64.StdEncoding.DecodeString(b64)
		if err == nil {
			emptyRDBData = data
		} else {
			emptyRDBData = []byte{} // fallback, should not happen
		}
	})
	return emptyRDBData
}

// Track replica connections for command propagation
var replicaConns []net.Conn
var replicaConnsMutex sync.Mutex

// Track last acknowledged offsets from replicas (by connection)
var replicaAckOffsets = make(map[net.Conn]int64)

// (rdbDir and rdbFilename declared above under RDB configuration parameters)

// Minimal RDB loader: parse only enough to read string keys from REDIS0011 files
func loadRDB(dir, filename string) {
	if dir == "" || filename == "" {
		return
	}
	path := filepath.Join(dir, filename)
	f, err := os.Open(path)
	if err != nil {
		// File may not exist; that's okay
		return
	}
	defer f.Close()
	rd := bufio.NewReader(f)

	// Header: 5 bytes "REDIS" + 4 bytes version
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(rd, hdr); err != nil || string(hdr) != "REDIS" {
		return
	}
	ver := make([]byte, 4)
	if _, err := io.ReadFull(rd, ver); err != nil {
		return
	}
	// Loop through sections
	var pendingExpiry *time.Time
	for {
		b, err := rd.ReadByte()
		if err != nil {
			return
		}
		switch b {
		case 0xFA: // AUX: two strings
			_ = rdbReadString(rd)
			_ = rdbReadString(rd)
		case 0xFE: // SELECTDB
			_ = rdbReadLength(rd)
		case 0xFB: // RESIZEDB
			_ = rdbReadLength(rd)
			_ = rdbReadLength(rd)
		case 0xFD: // EXPIRETIME (seconds, little-endian)
			buf := make([]byte, 4)
			if _, err := io.ReadFull(rd, buf); err != nil {
				return
			}
			sec := int64(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24)
			t := time.Unix(sec, 0)
			pendingExpiry = &t
		case 0xFC: // EXPIRETIME_MS (milliseconds, little-endian)
			buf := make([]byte, 8)
			if _, err := io.ReadFull(rd, buf); err != nil {
				return
			}
			ms := int64(uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 | uint64(buf[3])<<24 |
				uint64(buf[4])<<32 | uint64(buf[5])<<40 | uint64(buf[6])<<48 | uint64(buf[7])<<56)
			t := time.Unix(0, ms*int64(time.Millisecond))
			pendingExpiry = &t
		case 0xFF: // EOF
			// read checksum (8 bytes) if present
			_, _ = io.CopyN(io.Discard, rd, 8)
			return
		default:
			// Treat as value type; 0 = string key/value
			if b == 0x00 {
				key := rdbReadString(rd)
				val := rdbReadString(rd)
				if key != nil && val != nil {
					var expCopy *time.Time
					if pendingExpiry != nil {
						te := *pendingExpiry
						expCopy = &te
					}
					storageMutex.Lock()
					storage[string(key)] = StorageEntry{value: string(val), expiry: expCopy}
					storageMutex.Unlock()
					pendingExpiry = nil
				}
			} else {
				// Unsupported type: skip conservatively by abandoning parse
				return
			}
		}
	}
}

// rdbReadLength parses the RDB length-encoding and returns the length as int
func rdbReadLength(r *bufio.Reader) int {
	b, err := r.ReadByte()
	if err != nil {
		return 0
	}
	enc := (b & 0xC0) >> 6
	if enc == 0 { // 6-bit
		return int(b & 0x3F)
	}
	if enc == 1 { // 14-bit
		b2, _ := r.ReadByte()
		return int(((int(b) & 0x3F) << 8) | int(b2))
	}
	if enc == 2 { // 32-bit big-endian
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0
		}
		return int(uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3]))
	}
	// enc == 3: encoded string value indicator; return this marker to caller by negative length
	return -int(b & 0x3F)
}

// rdbReadString reads a string-encoded value (including encoded-int forms) and returns bytes
func rdbReadString(r *bufio.Reader) []byte {
	// Peek first length byte
	b, err := r.ReadByte()
	if err != nil {
		return nil
	}
	enc := (b & 0xC0) >> 6
	if enc == 0 { // 6-bit length
		l := int(b & 0x3F)
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil
		}
		return buf
	}
	if enc == 1 { // 14-bit
		b2, _ := r.ReadByte()
		l := int(((int(b) & 0x3F) << 8) | int(b2))
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil
		}
		return buf
	}
	if enc == 2 { // 32-bit big-endian length
		buf4 := make([]byte, 4)
		if _, err := io.ReadFull(r, buf4); err != nil {
			return nil
		}
		l := int(uint32(buf4[0])<<24 | uint32(buf4[1])<<16 | uint32(buf4[2])<<8 | uint32(buf4[3]))
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil
		}
		return buf
	}
	// Encoded value (ints or LZF). We support int8/int16/int32 forms.
	subtype := b & 0x3F
	switch subtype {
	case 0: // 8-bit int
		v, _ := r.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(v)), 10))
	case 1: // 16-bit int, little-endian
		buf := make([]byte, 2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil
		}
		n := int16(uint16(buf[0]) | uint16(buf[1])<<8)
		return []byte(strconv.FormatInt(int64(n), 10))
	case 2: // 32-bit int, little-endian
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil
		}
		n := int32(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24)
		return []byte(strconv.FormatInt(int64(n), 10))
	default:
		// LZF or unsupported
		return nil
	}
}

// Helper to encode a RESP array
func encodeRESPArray(args []string) []byte {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, a := range args {
		b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(a), a))
	}
	return []byte(b.String())
}

// Determine if a command should be propagated to replicas
func isWriteCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch strings.ToLower(args[0]) {
	case "set", "del", "incr", "decr", "lpush", "rpush", "lpop", "xadd", "zadd":
		return true
	default:
		return false
	}
}

// Send a command (as RESP array) to all replica connections
func propagateToReplicas(args []string) {
	// Copy current targets under lock
	replicaConnsMutex.Lock()
	targets := make([]net.Conn, len(replicaConns))
	copy(targets, replicaConns)
	replicaConnsMutex.Unlock()

	payload := encodeRESPArray(args)
	var dead []net.Conn
	for _, c := range targets {
		if _, err := c.Write(payload); err != nil {
			dead = append(dead, c)
		}
	}
	// Increase the global replication stream offset once per command
	masterReplOffset += int64(len(payload))
	if len(dead) > 0 {
		// Prune dead connections
		replicaConnsMutex.Lock()
		filtered := replicaConns[:0]
		for _, c := range replicaConns {
			remove := false
			for _, d := range dead {
				if c == d {
					remove = true
					break
				}
			}
			if !remove {
				filtered = append(filtered, c)
			}
		}
		replicaConns = filtered
		// Remove their ack offsets as well
		for _, d := range dead {
			delete(replicaAckOffsets, d)
		}
		replicaConnsMutex.Unlock()
	}
}

// Ask replicas to send their current processed offsets
func requestReplicaAcks() {
	replicaConnsMutex.Lock()
	targets := make([]net.Conn, len(replicaConns))
	copy(targets, replicaConns)
	replicaConnsMutex.Unlock()
	getack := encodeRESPArray([]string{"REPLCONF", "GETACK", "*"})
	for _, c := range targets {
		_, _ = c.Write(getack)
	}
}

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
			return "*-1\r\n"
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
	case "zadd":
		// Stage ct1: create a new sorted set with a single member
		// Syntax: ZADD key score member
		if len(args) != 4 {
			return "-ERR wrong number of arguments for ZADD command\r\n"
		}
		key := args[1]
		scoreStr := args[2]
		member := args[3]
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return "-ERR value is not a valid float\r\n"
		}
		storageMutex.Lock()
		defer storageMutex.Unlock()
		// Only handle creating a new set with one member for this stage
		zs := zsetStorage[key]
		if zs == nil {
			zs = &ZSet{dict: make(map[string]float64)}
			zsetStorage[key] = zs
		}
		// Count only if member is new
		added := 0
		if _, exists := zs.dict[member]; !exists {
			added = 1
			zs.dict[member] = score
			// insert maintaining order (linear insert for now, n is tiny for this stage)
			inserted := false
			for i, zm := range zs.sorted {
				if score < zm.score || (score == zm.score && member < zm.member) {
					zs.sorted = append(zs.sorted[:i], append([]ZMember{{member: member, score: score}}, zs.sorted[i:]...)...)
					inserted = true
					break
				}
			}
			if !inserted {
				zs.sorted = append(zs.sorted, ZMember{member: member, score: score})
			}
		} else {
			// For this stage, if member exists, we still return 0 and leave ordering/score as-is or update score without counting
			if _, exists := zs.dict[member]; !exists {
				added = 1
				zs.dict[member] = score
				// simple append; order maintenance minimal for stage scope
				inserted := false
				for i, zm := range zs.sorted {
					if score < zm.score || (score == zm.score && member < zm.member) {
						zs.sorted = append(zs.sorted[:i], append([]ZMember{{member: member, score: score}}, zs.sorted[i:]...)...)
						inserted = true
						break
					}
				}
				if !inserted {
					zs.sorted = append(zs.sorted, ZMember{member: member, score: score})
				}
			} else {
				// Update score but don't count as added (future stages may refine logic)
				old := zs.dict[member]
				if old != score {
					zs.dict[member] = score
					// Rebuild sorted slice minimally
					for i, zm := range zs.sorted {
						if zm.member == member {
							zs.sorted = append(zs.sorted[:i], zs.sorted[i+1:]...)
							break
						}
					}
					// re-insert
					inserted := false
					for i, zm := range zs.sorted {
						if score < zm.score || (score == zm.score && member < zm.member) {
							zs.sorted = append(zs.sorted[:i], append([]ZMember{{member: member, score: score}}, zs.sorted[i:]...)...)
							inserted = true
							break
						}
					}
					if !inserted {
						zs.sorted = append(zs.sorted, ZMember{member: member, score: score})
					}
				}
			}
		}
		return fmt.Sprintf(":%d\r\n", added)
	case "zrank":
		// Syntax: ZRANK key member
		if len(args) != 3 {
			return "-ERR wrong number of arguments for ZRANK command\r\n"
		}
		key := args[1]
		member := args[2]
		storageMutex.RLock()
		zs := zsetStorage[key]
		if zs == nil {
			storageMutex.RUnlock()
			return "$-1\r\n"
		}
		if _, ok := zs.dict[member]; !ok {
			storageMutex.RUnlock()
			return "$-1\r\n"
		}
		// Find index in sorted order
		rank := -1
		for i, zm := range zs.sorted {
			if zm.member == member {
				rank = i
				break
			}
		}
		storageMutex.RUnlock()
		if rank < 0 {
			return "$-1\r\n"
		}
		return fmt.Sprintf(":%d\r\n", rank)
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
		_, zsetExists := zsetStorage[key]
		storageMutex.RUnlock()
		if exists {
			return "+string\r\n"
		}
		if streamExists {
			return "+stream\r\n"
		}
		if zsetExists {
			return "+zset\r\n"
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

	// subscribe handled in handleClient to maintain per-connection state

	case "keys":
		// Support only KEYS *
		if len(args) != 2 || args[1] != "*" {
			return "-ERR only KEYS * is supported\r\n"
		}
		// Collect keys from string storage only (adequate for this stage)
		storageMutex.RLock()
		keys := make([]string, 0, len(storage))
		for k := range storage {
			keys = append(keys, k)
		}
		storageMutex.RUnlock()
		resp := fmt.Sprintf("*%d\r\n", len(keys))
		for _, k := range keys {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
		}
		return resp

	case "config":
		// Support: CONFIG GET <param>
		if len(args) >= 3 && strings.ToLower(args[1]) == "get" {
			param := strings.ToLower(args[2])
			switch param {
			case "dir":
				name := "dir"
				val := rdbDir
				return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(name), name, len(val), val)
			case "dbfilename":
				name := "dbfilename"
				val := rdbFilename
				return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(name), name, len(val), val)
			default:
				return "*0\r\n"
			}
		}
		return "-ERR wrong number of arguments for CONFIG GET\r\n"

	case "replconf":
		// For this stage, ignore arguments and acknowledge with +OK
		return "+OK\r\n"

	case "psync":
		// Replica initial sync request -> reply with FULLRESYNC <REPL_ID> 0
		return fmt.Sprintf("+FULLRESYNC %s %d\r\n", masterReplID, masterReplOffset)
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
			// On timeout, Redis returns a Null Array for XREAD
			return "*-1\r\n"
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

	case "wait":
		// Return the number of connected replicas as RESP integer
		replicaConnsMutex.Lock()
		cnt := len(replicaConns)
		replicaConnsMutex.Unlock()
		return fmt.Sprintf(":%d\r\n", cnt)

	default:
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
		// INFO command: support `INFO replication` returning role
		// (handled above)
	}
}

func handleClient(conn net.Conn) {
	defer func() {
		// Cleanup this connection from global pubsub registry on close
		pubsubMutex.Lock()
		for ch, set := range pubsubSubscribers {
			if set[conn] {
				delete(set, conn)
				if len(set) == 0 {
					delete(pubsubSubscribers, ch)
				}
			}
		}
		pubsubMutex.Unlock()
		conn.Close()
	}()

	// Handle multiple commands from the same connection
	reader := bufio.NewReader(conn)
	inMulti := false
	var queuedCommands [][]string
	// Track the last produced replication stream offset for this client's writes
	var lastWriteOffset int64 = -1
	// Pub/Sub: per-connection subscribed channels
	subscribed := make(map[string]bool)

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

		// If this is a replica connection sending its ACK, record and continue
		if cmd == "replconf" && len(args) >= 3 && strings.ToLower(args[1]) == "ack" {
			// args[2] is offset
			if off, err := strconv.ParseInt(args[2], 10, 64); err == nil {
				replicaConnsMutex.Lock()
				replicaAckOffsets[conn] = off
				replicaConnsMutex.Unlock()
			}
			// No response required to ACK
			continue
		}

		// PSYNC handshake (master side): reply FULLRESYNC and then stream an empty RDB
		if cmd == "psync" {
			// Respond with FULLRESYNC <REPL_ID> 0\r\n
			fmt.Fprintf(conn, "+FULLRESYNC %s %d\r\n", masterReplID, masterReplOffset)
			// Then send RDB file: $<len>\r\n<binary>
			rdb := getEmptyRDB()
			fmt.Fprintf(conn, "$%d\r\n", len(rdb))
			conn.Write(rdb)
			// Register this connection as a replica target for propagation
			replicaConnsMutex.Lock()
			replicaConns = append(replicaConns, conn)
			replicaAckOffsets[conn] = 0
			replicaConnsMutex.Unlock()
			// After sending RDB, keep the connection open for future stages
			continue
		}

		// Subscribed-mode gate: if this client has any subscriptions, only allow a subset
		if len(subscribed) > 0 {
			allowed := map[string]bool{
				"subscribe":    true,
				"unsubscribe":  true,
				"psubscribe":   true,
				"punsubscribe": true,
				"ping":         true,
				"quit":         true,
				"reset":        true,
			}
			if !allowed[cmd] {
				conn.Write([]byte(fmt.Sprintf("-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", cmd)))
				continue
			}
			// In subscribed mode, PING returns an array ["pong", "<message or empty>"]
			if cmd == "ping" {
				msg := ""
				if len(args) >= 2 {
					msg = args[1]
				}
				var b strings.Builder
				b.WriteString("*2\r\n")
				b.WriteString("$4\r\npong\r\n")
				b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg))
				conn.Write([]byte(b.String()))
				continue
			}
		}

		// Handle SUBSCRIBE here to keep per-connection state
		if cmd == "subscribe" {
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for SUBSCRIBE command\r\n"))
				continue
			}
			for i := 1; i < len(args); i++ {
				ch := args[i]
				if !subscribed[ch] {
					subscribed[ch] = true
					// Register globally
					pubsubMutex.Lock()
					set := pubsubSubscribers[ch]
					if set == nil {
						set = make(map[net.Conn]bool)
						pubsubSubscribers[ch] = set
					}
					set[conn] = true
					pubsubMutex.Unlock()
				}
				count := len(subscribed)
				// Send one confirmation per channel
				var b strings.Builder
				b.WriteString("*3\r\n")
				b.WriteString("$9\r\nsubscribe\r\n")
				b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(ch), ch))
				b.WriteString(fmt.Sprintf(":%d\r\n", count))
				conn.Write([]byte(b.String()))
			}
			continue
		}

		// Handle UNSUBSCRIBE (one or more channels). If no channels provided, unsubscribe from all.
		if cmd == "unsubscribe" {
			// Build list of channels to unsubscribe
			var chans []string
			if len(args) >= 2 {
				chans = args[1:]
			} else {
				// Unsubscribe all current subscriptions
				for ch := range subscribed {
					chans = append(chans, ch)
				}
				// If none, still send a single reply indicating 0 remaining
				if len(chans) == 0 {
					var b strings.Builder
					b.WriteString("*3\r\n")
					b.WriteString("$11\r\nunsubscribe\r\n")
					b.WriteString("$0\r\n\r\n")
					b.WriteString(":0\r\n")
					conn.Write([]byte(b.String()))
					continue
				}
			}

			for _, ch := range chans {
				// Only delete if subscribed
				if subscribed[ch] {
					delete(subscribed, ch)
					// Remove from global registry
					pubsubMutex.Lock()
					if set, ok := pubsubSubscribers[ch]; ok {
						delete(set, conn)
						if len(set) == 0 {
							delete(pubsubSubscribers, ch)
						}
					}
					pubsubMutex.Unlock()
				}
				count := len(subscribed)
				var b strings.Builder
				b.WriteString("*3\r\n")
				b.WriteString("$11\r\nunsubscribe\r\n")
				b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(ch), ch))
				b.WriteString(fmt.Sprintf(":%d\r\n", count))
				conn.Write([]byte(b.String()))
			}
			continue
		}

		// PUBLISH command: deliver message to all subscribers and respond with count
		if cmd == "publish" {
			if len(args) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for PUBLISH command\r\n"))
				continue
			}
			channel := args[1]
			message := args[2]
			// Snapshot subscribers to avoid holding the lock during writes
			var targets []net.Conn
			pubsubMutex.Lock()
			if set, ok := pubsubSubscribers[channel]; ok {
				targets = make([]net.Conn, 0, len(set))
				for c := range set {
					targets = append(targets, c)
				}
			}
			pubsubMutex.Unlock()
			// Deliver message to each subscriber
			if len(targets) > 0 {
				payload := encodeRESPArray([]string{"message", channel, message})
				for _, c := range targets {
					// Best-effort delivery; ignore errors here
					_, _ = c.Write(payload)
				}
			}
			count := len(targets)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
			continue
		}

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

		// Special-case WAIT to coordinate with replica ACKs
		if cmd == "wait" {
			// WAIT numreplicas timeout(ms)
			// If no prior writes from this client, return current connected replicas immediately
			replicaConnsMutex.Lock()
			currentReplicas := len(replicaConns)
			replicaConnsMutex.Unlock()
			if lastWriteOffset < 0 {
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", currentReplicas)))
				continue
			}
			// Parse args
			required := 0
			timeoutMs := 0
			if len(args) >= 2 {
				required, _ = strconv.Atoi(args[1])
			}
			if len(args) >= 3 {
				timeoutMs, _ = strconv.Atoi(args[2])
			}
			// Helper to count replicas that have acked >= lastWriteOffset
			countAcked := func() int {
				replicaConnsMutex.Lock()
				defer replicaConnsMutex.Unlock()
				count := 0
				for _, c := range replicaConns {
					if off, ok := replicaAckOffsets[c]; ok && off >= lastWriteOffset {
						count++
					}
				}
				return count
			}
			// Immediate count
			acked := countAcked()
			if acked >= required {
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", acked)))
				continue
			}
			// Poll until timeout
			deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
			for {
				requestReplicaAcks()
				time.Sleep(20 * time.Millisecond)
				acked = countAcked()
				if acked >= required || time.Now().After(deadline) {
					conn.Write([]byte(fmt.Sprintf(":%d\r\n", acked)))
					break
				}
			}
			continue
		}

		// Not in MULTI: handle command immediately
		response := handleCommand(args)

		// Send the response
		conn.Write([]byte(response))

		// After successful write commands, propagate to replicas (masters only)
		if !replicaMode && isWriteCommand(args) {
			propagateToReplicas(args)
			// Record the offset after producing this write in the stream
			lastWriteOffset = masterReplOffset
		}
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
	// RDB persistence flags
	dirFlag := flag.String("dir", "", "directory where RDB file is stored")
	dbfileFlag := flag.String("dbfilename", "", "RDB filename")
	flag.Parse()
	if *replicaOf != "" {
		replicaMode = true
	}
	// Store RDB config (empty is allowed)
	rdbDir = *dirFlag
	rdbFilename = *dbfileFlag
	// Attempt to load existing RDB at startup (missing file is fine)
	loadRDB(rdbDir, rdbFilename)
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
			masterAddr := net.JoinHostPort(parts[0], parts[1])
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

				// 3) PSYNC ? -1
				psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
				_, _ = conn.Write([]byte(psync))
				// After PSYNC, master sends +FULLRESYNC ... and an RDB bulk payload, then streamed commands.
				// 1) Read FULLRESYNC line
				_, _ = rd.ReadString('\n')
				// 2) Read bulk length line like $<len>\r\n
				lenLine, err := rd.ReadString('\n')
				if err != nil || !strings.HasPrefix(lenLine, "$") {
					return
				}
				trim := strings.TrimSpace(strings.TrimPrefix(lenLine, "$"))
				rdbLen, err := strconv.Atoi(trim)
				if err != nil {
					return
				}
				// 3) Read exactly rdbLen bytes (no trailing CRLF per spec in challenge)
				if rdbLen > 0 {
					remaining := rdbLen
					buf := make([]byte, 4096)
					for remaining > 0 {
						toRead := remaining
						if toRead > len(buf) {
							toRead = len(buf)
						}
						n, err := io.ReadFull(rd, buf[:toRead])
						if err != nil {
							return
						}
						remaining -= n
					}
				}
				// 4) Now continuously read propagated commands (RESP arrays)
				// Maintain a running byte offset of processed commands
				var replOffset int64 = 0
				parseWithCount := func(r *bufio.Reader) ([]string, int, error) {
					consumed := 0
					line, err := r.ReadString('\n')
					if err != nil {
						return nil, consumed, err
					}
					consumed += len(line)
					if !strings.HasPrefix(line, "*") {
						return nil, consumed, fmt.Errorf("expected array, got: %s", line)
					}
					argCount, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
					if err != nil {
						return nil, consumed, err
					}
					out := make([]string, argCount)
					for i := 0; i < argCount; i++ {
						argLenLine, err := r.ReadString('\n')
						if err != nil {
							return nil, consumed, err
						}
						consumed += len(argLenLine)
						if !strings.HasPrefix(argLenLine, "$") {
							return nil, consumed, fmt.Errorf("expected bulk string, got: %s", argLenLine)
						}
						argLen, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(argLenLine, "$")))
						if err != nil {
							return nil, consumed, err
						}
						buf := make([]byte, argLen)
						n, err := io.ReadFull(r, buf)
						if err != nil {
							return nil, consumed, err
						}
						consumed += n
						tail, err := r.ReadString('\n')
						if err != nil {
							return nil, consumed, err
						}
						consumed += len(tail)
						out[i] = string(buf)
					}
					return out, consumed, nil
				}

				for {
					cmdArgs, consumed, err := parseWithCount(rd)
					if err != nil {
						return
					}
					if len(cmdArgs) >= 2 && strings.ToLower(cmdArgs[0]) == "replconf" && strings.ToLower(cmdArgs[1]) == "getack" {
						// Reply with current offset before including this GETACK's bytes
						ack := encodeRESPArray([]string{"REPLCONF", "ACK", strconv.FormatInt(replOffset, 10)})
						_, _ = conn.Write(ack)
						// Now count this GETACK command as processed
						replOffset += int64(consumed)
						continue
					}
					// Apply to local state without sending any response to master
					_ = handleCommand(cmdArgs)
					replOffset += int64(consumed)
				}
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
