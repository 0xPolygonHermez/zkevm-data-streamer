package datastreamer_test

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/stretchr/testify/require"
)

// AUX ------------------------------------------------------------------------
const hashLength = 32

type hash [hashLength]byte

func (h *hash) setBytes(b []byte) {
	if len(b) > len(h) {
		b = b[len(b)-hashLength:]
	}

	copy(h[hashLength-len(b):], b)
}

func bytesToHash(b []byte) hash {
	var h hash
	h.setBytes(b)
	return h
}

func has0xPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

func hex2Bytes(str string) []byte {
	h, _ := hex.DecodeString(str)
	return h
}

func fromHex(s string) []byte {
	if has0xPrefix(s) {
		s = s[2:]
	}
	if len(s)%2 == 1 {
		s = "0" + s
	}
	return hex2Bytes(s)
}

func hexToHash(s string) hash { return bytesToHash(fromHex(s)) }

// ----------------------------------------------------------------------------

type TestEntry struct {
	FieldA uint64 // 8 bytes
	FieldB hash   // 32 bytes
	FieldC []byte // n bytes
}

type TestBookmark struct {
	FieldA []byte
}
type TestHeader struct {
	PacketType   uint8
	HeadLength   uint32
	StreamType   uint64
	TotalLength  uint64
	TotalEntries uint64
}

func (t TestEntry) Encode() []byte {
	bytes := make([]byte, 0)
	bytes = binary.BigEndian.AppendUint64(bytes, t.FieldA)
	bytes = append(bytes, t.FieldB[:]...)
	bytes = append(bytes, t.FieldC...)
	return bytes
}

func (t TestEntry) Decode(bytes []byte) TestEntry {
	t.FieldA = binary.BigEndian.Uint64(bytes[:8])
	t.FieldB = bytesToHash(bytes[8:40])
	t.FieldC = bytes[40:]
	return t
}

func (t TestBookmark) Encode() []byte {
	return t.FieldA
}

var (
	config = datastreamer.Config{
		Port:     6900,
		Filename: "/tmp/datastreamer_test.bin",
		Log: log.Config{
			Environment: "development",
			Level:       "debug",
			Outputs:     []string{"stdout"},
		},
		WriteTimeout: 3 * time.Second,
	}
	leveldb      = config.Filename[0:strings.IndexRune(config.Filename, '.')] + ".db"
	streamServer *datastreamer.StreamServer
	streamType   = datastreamer.StreamType(1)
	entryType1   = datastreamer.EntryType(1)
	entryType2   = datastreamer.EntryType(2)

	testEntries = []TestEntry{
		{
			FieldA: 0,
			FieldB: hexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			FieldC: []byte("test entry 0"),
		},
		{
			FieldA: 1,
			FieldB: hexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			FieldC: []byte("test entry 1"),
		},
		{
			FieldA: 2,
			FieldB: hexToHash("0x2234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			FieldC: []byte("test entry 2"),
		},
		{
			FieldA: 3,
			FieldB: hexToHash("0x3234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			FieldC: []byte("test entry 3"),
		},
		{
			FieldA: 4,
			FieldB: hexToHash("0x3234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			FieldC: []byte("large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4" +
				"large test entry 4 large test entry 4 large test entry 4 large test entry 4"),
		},
	}

	badUpdateEntry = TestEntry{
		FieldA: 10,
		FieldB: hexToHash("0xa1cdef7890abcdef1234567890abcdef1234567890abcdef1234567890123456"),
		FieldC: []byte("test entry not updated"),
	}

	okUpdateEntry = TestEntry{
		FieldA: 11,
		FieldB: hexToHash("0xa2cdef7890abcdef1234567890abcdef1234567890abcdef1234567890123456"),
		FieldC: []byte("update entry"),
	}

	testBookmark = TestBookmark{
		FieldA: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0},
	}

	nonAddedBookmark = TestBookmark{
		FieldA: []byte{0, 2, 0, 0, 0, 0, 0, 0, 0},
	}

	testBookmark2 = TestBookmark{
		FieldA: []byte{0, 3, 0, 0, 0, 0, 0, 0, 0},
	}

	headerEntry = TestHeader{
		PacketType:   1,
		HeadLength:   29,
		StreamType:   1,
		TotalLength:  1053479,
		TotalEntries: 1304,
	}
)

func deleteFiles() error {
	// Delete test file from filesystem
	err := os.Remove(config.Filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Delete leveldb folder from filesystem
	err = os.RemoveAll(leveldb)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func TestServer(t *testing.T) {
	err := deleteFiles()
	if err != nil {
		panic(err)
	}
	streamServer, err = datastreamer.NewServer(config.Port, 1, 137, streamType,
		config.Filename, config.WriteTimeout, config.InactivityTimeout, 5*time.Second, &config.Log, 100)
	if err != nil {
		panic(err)
	}

	// Case: Add entry without starting atomic operation -> FAIL
	entryNumber, err := streamServer.AddStreamEntry(entryType1, testEntries[1].Encode())
	require.Equal(t, datastreamer.ErrAddEntryNotAllowed, err)
	require.Equal(t, uint64(0), entryNumber)

	// Case: Start atomic operation without starting the server -> FAIL
	err = streamServer.StartAtomicOp()
	require.Equal(t, datastreamer.ErrAtomicOpNotAllowed, err)
	require.Equal(t, uint64(0), entryNumber)

	// Case: Start server, start atomic operation, add entries, commit -> OK
	err = streamServer.Start()
	require.NoError(t, err)

	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	entryNumber, err = streamServer.AddStreamBookmark(testBookmark.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(0), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[1].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(1), entryNumber)

	entryNumber, err = streamServer.AddStreamBookmark(testBookmark2.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(2), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[2].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(3), entryNumber)

	// Case: Start atomic operation with atomic operation in progress -> FAIL
	_ = streamServer.StartAtomicOp()
	err = streamServer.StartAtomicOp()
	_ = streamServer.CommitAtomicOp()
	require.EqualError(t, datastreamer.ErrStartAtomicOpNotAllowed, err.Error())

	// Case: Commit atomic operation without starting atomic operation -> FAIL
	err = streamServer.CommitAtomicOp()
	require.EqualError(t, datastreamer.ErrCommitNotAllowed, err.Error())

	// Case: AddStreamBookmark without atomic operation in progress -> FAIL
	entryNumber, err = streamServer.AddStreamBookmark(testBookmark.Encode())
	require.Equal(t, uint64(0), entryNumber)
	require.EqualError(t, datastreamer.ErrAddEntryNotAllowed, err.Error())

	// Check get data between 2 bookmarks
	data, err := streamServer.GetDataBetweenBookmarks(testBookmark.Encode(), testBookmark2.Encode())
	require.NoError(t, err)
	require.Equal(t, testEntries[1].Encode(), data)

	// Truncate file
	err = streamServer.TruncateFile(2)
	require.NoError(t, err)

	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[2].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(2), entryNumber)

	err = streamServer.CommitAtomicOp()
	require.NoError(t, err)

	// Case: Get entry data of an entry number that exists -> OK
	entry, err := streamServer.GetEntry(2)
	require.NoError(t, err)
	require.Equal(t, testEntries[2], TestEntry{}.Decode(entry.Data))

	// Case: Get entry data of an entry number that doesn't exist -> FAIL
	entry, err = streamServer.GetEntry(3)
	require.EqualError(t, datastreamer.ErrInvalidEntryNumber, err.Error())
	require.Equal(t, datastreamer.FileEntry{}, entry)

	// Case: Get entry number pointed by bookmark that exists -> OK
	entryNumber, err = streamServer.GetBookmark(testBookmark.Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(0), entryNumber)

	// Case: Get entry number pointed by bookmark that doesn't exist -> FAIL
	_, err = streamServer.GetBookmark(nonAddedBookmark.Encode())
	require.EqualError(t, errors.New("leveldb: not found"), err.Error())

	// Case: Update entry data of an entry number that doesn't exist -> FAIL
	err = streamServer.UpdateEntryData(22, entryType1, badUpdateEntry.Encode())
	require.EqualError(t, datastreamer.ErrInvalidEntryNumber, err.Error())

	// Case: Update entry data present in atomic operation in progress -> FAIL
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[3].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(3), entryNumber)

	err = streamServer.UpdateEntryData(3, entryType1, badUpdateEntry.Encode())
	require.EqualError(t, datastreamer.ErrUpdateNotAllowed, err.Error())

	err = streamServer.CommitAtomicOp()
	require.NoError(t, err)

	// Case: Update entry data changing the entry type -> FAIL
	err = streamServer.UpdateEntryData(3, entryType2, badUpdateEntry.Encode())
	require.EqualError(t, datastreamer.ErrUpdateEntryTypeNotAllowed, err.Error())

	// Case: Update entry data changing data length -> FAIL
	err = streamServer.UpdateEntryData(3, entryType1, badUpdateEntry.Encode())
	require.EqualError(t, datastreamer.ErrUpdateEntryDifferentSize, err.Error())

	// Case: Update entry data not in atomic oper, same type, same data length -> OK
	var entryUpdated uint64 = 3
	err = streamServer.UpdateEntryData(entryUpdated, entryType1, okUpdateEntry.Encode())
	require.NoError(t, err)

	// Case: Get entry just updated and check it is modified -> OK
	entry, err = streamServer.GetEntry(entryUpdated)
	require.NoError(t, err)
	require.Equal(t, entryUpdated, entry.Number)
	require.Equal(t, okUpdateEntry, TestEntry{}.Decode(entry.Data))

	// Case: Get previous entry to the updated one and check not modified -> OK
	if entryUpdated > 1 {
		entry, err = streamServer.GetEntry(entryUpdated - 1)
		require.NoError(t, err)
		require.Equal(t, entryUpdated-1, entry.Number)
		require.Equal(t, testEntries[entryUpdated-1], TestEntry{}.Decode(entry.Data))
	}

	// Case: Add 3 new entries -> OK
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[1].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(4), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[2].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(5), entryNumber)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[2].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(6), entryNumber)

	err = streamServer.CommitAtomicOp()
	require.NoError(t, err)

	// Case: Atomic finished with rollback -> OK
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[1].Encode())
	require.NoError(t, err)
	require.Equal(t, uint64(7), entryNumber)

	err = streamServer.RollbackAtomicOp()
	require.NoError(t, err)

	// Case: Rollback operation without starting atomic operation -> FAIL
	err = streamServer.RollbackAtomicOp()
	require.EqualError(t, datastreamer.ErrRollbackNotAllowed, err.Error())

	// Case: Get entry data of previous rollback entry number (doesn't exist) -> FAIL
	entry, err = streamServer.GetEntry(7)
	require.EqualError(t, datastreamer.ErrInvalidEntryNumber, err.Error())
	require.Equal(t, datastreamer.FileEntry{}, entry)

	// Case: Truncate file with atomic operation in progress -> FAIL
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	err = streamServer.TruncateFile(5)
	require.EqualError(t, datastreamer.ErrTruncateNotAllowed, err.Error())

	err = streamServer.RollbackAtomicOp()
	require.NoError(t, err)

	// Case: Truncate file from an entry number invalid -> FAIL
	err = streamServer.TruncateFile(7)
	require.EqualError(t, datastreamer.ErrInvalidEntryNumber, err.Error())

	// Case: Truncate file from valid entry number, not atomic operation in progress -> OK
	err = streamServer.TruncateFile(5)
	require.NoError(t, err)

	// Case: Get entries included in previous file truncate (don't exist) -> FAIL
	entry, err = streamServer.GetEntry(6)
	require.EqualError(t, datastreamer.ErrInvalidEntryNumber, err.Error())
	require.Equal(t, datastreamer.FileEntry{}, entry)
	entry, err = streamServer.GetEntry(5)
	require.EqualError(t, datastreamer.ErrInvalidEntryNumber, err.Error())
	require.Equal(t, datastreamer.FileEntry{}, entry)

	// Case: Get entry not included in previous file truncate -> OK
	entry, err = streamServer.GetEntry(4)
	require.NoError(t, err)
	require.Equal(t, uint64(4), entry.Number)

	// Log file header before fill the first data page
	datastreamer.PrintHeaderEntry(streamServer.GetHeader(), "before fill page")

	// Case: Fill first data page with entries
	entryLength := len(testEntries[4].Encode()) + datastreamer.FixedSizeFileEntry
	bytesAvailable := datastreamer.PageDataSize - (streamServer.GetHeader().TotalLength - datastreamer.PageHeaderSize)
	numEntries := bytesAvailable / uint64(entryLength)
	log.Debugf(">>> totalLength: %d | bytesAvailable: %d | entryLength: %d | numEntries: %d",
		streamServer.GetHeader().TotalLength, bytesAvailable, entryLength, numEntries)

	lastEntry := entryNumber - 2 // 2 entries truncated
	lastEntry--
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	for i := 1; i <= int(numEntries); i++ {
		lastEntry++
		entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[4].Encode())
		require.NoError(t, err)
		require.Equal(t, lastEntry, entryNumber)
	}

	err = streamServer.CommitAtomicOp()
	require.NoError(t, err)

	bytesAvailable = datastreamer.PageDataSize -
		((streamServer.GetHeader().TotalLength - datastreamer.PageHeaderSize) % datastreamer.PageDataSize)
	numEntries = bytesAvailable / uint64(entryLength)
	log.Debugf(">>> totalLength: %d | bytesAvailable: %d | entryLength: %d | numEntries: %d",
		streamServer.GetHeader().TotalLength, bytesAvailable, entryLength, numEntries)

	// Case: Get latest entry stored in the first data page -> OK
	entry, err = streamServer.GetEntry(entryNumber)
	require.NoError(t, err)
	require.Equal(t, entryNumber, entry.Number)
	require.Equal(t, testEntries[4], TestEntry{}.Decode(entry.Data))

	// Case: Add new entry and will be stored in the second data page -> OK
	err = streamServer.StartAtomicOp()
	require.NoError(t, err)

	entryNumber, err = streamServer.AddStreamEntry(entryType1, testEntries[4].Encode())
	require.NoError(t, err)
	require.Equal(t, lastEntry+1, entryNumber)

	err = streamServer.CommitAtomicOp()
	require.NoError(t, err)

	// Case: Get entry stored in the second data page -> OK
	entry, err = streamServer.GetEntry(entryNumber)
	require.NoError(t, err)
	require.Equal(t, entryNumber, entry.Number)
	require.Equal(t, testEntries[4], TestEntry{}.Decode(entry.Data))

	// Log final file header
	datastreamer.PrintHeaderEntry(streamServer.GetHeader(), "final tests")
}

func TestClient(t *testing.T) {
	var fromBookmark []byte
	var fromEntry uint64
	var entry datastreamer.FileEntry
	var header datastreamer.HeaderEntry

	client, err := datastreamer.NewClient(fmt.Sprintf("localhost:%d", config.Port), streamType)
	require.NoError(t, err)

	err = client.Start()
	require.NoError(t, err)

	// Case: Query data from not existing bookmark -> FAIL
	fromBookmark = nonAddedBookmark.Encode()
	_, err = client.ExecCommandGetBookmark(fromBookmark)
	require.EqualError(t, datastreamer.ErrBookmarkNotFound, err.Error())

	// Case: Query data from existing bookmark -> OK
	fromBookmark = testBookmark.Encode()
	_, err = client.ExecCommandGetBookmark(fromBookmark)
	require.NoError(t, err)

	// Case: Query data for entry number that doesn't exist -> FAIL
	fromEntry = 5000
	_, err = client.ExecCommandGetEntry(fromEntry)
	require.EqualError(t, datastreamer.ErrEntryNotFound, err.Error())

	// Case: Query data for entry number that exists -> OK
	fromEntry = 2
	entry, err = client.ExecCommandGetEntry(fromEntry)
	require.NoError(t, err)
	require.Equal(t, testEntries[2], TestEntry{}.Decode(entry.Data))

	// Case: Query data for entry number that exists -> OK
	fromEntry = 1
	entry, err = client.ExecCommandGetEntry(fromEntry)
	require.NoError(t, err)
	require.Equal(t, testEntries[1], TestEntry{}.Decode(entry.Data))

	// Case: Query header info -> OK
	header, err = client.ExecCommandGetHeader()
	require.NoError(t, err)
	require.Equal(t, headerEntry.TotalEntries, header.TotalEntries)
	require.Equal(t, headerEntry.TotalLength, header.TotalLength)

	// Case: Start sync from not existing entry -> FAIL
	// client.FromEntry = 22
	// err = client.ExecCommand(datastreamer.CmdStart)
	// require.EqualError(t, datastreamer.ErrResultCommandError, err.Error())

	// Case: Start sync from not existing bookmark -> FAIL
	// client.FromBookmark = nonAddedBookmark.Encode()
	// err = client.ExecCommand(datastreamer.CmdStartBookmark)
	// require.EqualError(t, datastreamer.ErrResultCommandError, err.Error())

	// Case: Start sync from existing entry -> OK
	// client.FromEntry = 0
	// err = client.ExecCommand(datastreamer.CmdStart)
	// require.NoError(t, err)

	// Case: Start sync from existing bookmark -> OK
	fromBookmark = testBookmark.Encode()
	err = client.ExecCommandStartBookmark(fromBookmark)
	require.NoError(t, err)

	// Case: Query entry data with streaming started -> FAIL
	// client.FromEntry = 2
	// err = client.ExecCommand(datastreamer.CmdEntry)
	// require.EqualError(t, datastreamer.ErrResultCommandError, err.Error())

	// Case: Query bookmark data with streaming started -> FAIL
	// client.FromBookmark = testBookmark.Encode()
	// err = client.ExecCommand(datastreamer.CmdBookmark)
	// require.EqualError(t, datastreamer.ErrResultCommandError, err.Error())

	// Case: Stop receiving streaming -> OK
	err = client.ExecCommandStop()
	require.NoError(t, err)

	// Case: Query entry data after stop the streaming -> OK
	fromEntry = 2
	entry, err = client.ExecCommandGetEntry(fromEntry)
	require.NoError(t, err)
	require.Equal(t, testEntries[2], TestEntry{}.Decode(entry.Data))
}
