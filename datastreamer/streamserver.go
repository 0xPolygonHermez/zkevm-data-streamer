package datastreamer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

type Command uint64
type ClientStatus uint64
type AOStatus uint64
type EntryType uint32
type StreamType uint64
type CommandError uint32

const (
	// Commands
	CmdStart  Command = 1
	CmdStop   Command = 2
	CmdHeader Command = 3

	// Command errors
	CmdErrOK             CommandError = 0
	CmdErrAlreadyStarted CommandError = 1
	CmdErrAlreadyStopped CommandError = 2
	CmdErrBadFromEntry   CommandError = 3
	CmdErrInvalidCommand CommandError = 9

	// Client status
	csStarting ClientStatus = 1
	csStarted  ClientStatus = 2
	csStopped  ClientStatus = 3
	csKilled   ClientStatus = 0xff

	// Atomic operation status
	aoNone        AOStatus = 0
	aoStarted     AOStatus = 1
	aoCommitting  AOStatus = 2
	aoRollbacking AOStatus = 3
)

var (
	StrClientStatus = map[ClientStatus]string{
		csStarting: "Starting",
		csStarted:  "Started",
		csStopped:  "Stopped",
		csKilled:   "Killed",
	}

	StrCommand = map[Command]string{
		CmdStart:  "Start",
		CmdStop:   "Stop",
		CmdHeader: "Header",
	}

	StrCommandErrors = map[CommandError]string{
		CmdErrOK:             "OK",
		CmdErrAlreadyStarted: "Already started",
		CmdErrAlreadyStopped: "Already stopped",
		CmdErrBadFromEntry:   "Bad from entry",
	}
)

type StreamServer struct {
	port     uint16 // server stream port
	fileName string // stream file name

	streamType StreamType
	ln         net.Listener
	clients    map[string]*client

	lastEntry uint64
	atomicOp  streamAO
	sf        StreamFile

	entriesDefinition map[EntryType]EntityDefinition
}

type streamAO struct {
	status     AOStatus
	afterEntry uint64
	entries    []FileEntry
}

type client struct {
	conn   net.Conn
	status ClientStatus
}

type ResultEntry struct {
	packetType uint8 // 0xff:Result
	length     uint32
	errorNum   uint32 // 0:No error
	errorStr   []byte
}

func New(port uint16, streamType StreamType, fileName string) (StreamServer, error) {
	// Create the server data stream
	s := StreamServer{
		port:     port,
		fileName: fileName,

		streamType: streamType,
		ln:         nil,
		clients:    make(map[string]*client),
		lastEntry:  0,

		atomicOp: streamAO{
			status:     aoNone,
			afterEntry: 0,
			entries:    []FileEntry{},
		},
	}

	// Open (or create) the data stream file
	var err error
	s.sf, err = PrepareStreamFile(s.fileName, s.streamType)
	if err != nil {
		return s, err
	}

	// Initialize the entry number
	s.lastEntry = s.sf.header.totalEntries

	return s, nil
}

func (s *StreamServer) Start() error {
	// Start the server data stream
	var err error
	s.ln, err = net.Listen("tcp", ":"+strconv.Itoa(int(s.port)))
	if err != nil {
		log.Errorf("Error creating datastream server %d: %v", s.port, err)
		return err
	}

	// Wait for clients connections
	log.Infof("Listening on port: %d", s.port)
	go s.waitConnections()

	return nil
}

func (s *StreamServer) SetEntriesDefinition(entriesDefinition map[EntryType]EntityDefinition) {
	s.entriesDefinition = entriesDefinition
}

func (s *StreamServer) waitConnections() {
	defer s.ln.Close()

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Errorf("Error accepting new connection: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Goroutine to manage client (command requests and entries stream)
		go s.handleConnection(conn)
	}
}

func (s *StreamServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	clientId := conn.RemoteAddr().String()
	log.Infof("New connection: %s", clientId)

	s.clients[clientId] = &client{
		conn:   conn,
		status: csStopped,
	}

	reader := bufio.NewReader(conn)
	for {
		// Read command
		command, err := readFullUint64(reader)
		if err != nil {
			s.killClient(clientId)
			return
		}
		// Read stream type
		stUint64, err := readFullUint64(reader)
		if err != nil {
			s.killClient(clientId)
			return
		}
		st := StreamType(stUint64)

		// Check stream type
		if st != s.streamType {
			log.Errorf("Mismatch stream type, killed: %s", clientId)
			s.killClient(clientId)
			return
		}

		// Manage the requested command
		log.Infof("Command %d[%s] received from %s", command, StrCommand[Command(command)], clientId)
		err = s.processCommand(Command(command), clientId)
		if err != nil {
			// Kill client connection
			time.Sleep(1 * time.Second)
			s.killClient(clientId)
			return
		}
	}
}

func (s *StreamServer) StartAtomicOp() error {
	log.Debug("!!!Start AtomicOp")
	s.atomicOp.status = aoStarted
	s.atomicOp.afterEntry = s.lastEntry
	return nil
}

func (s *StreamServer) AddStreamEntry(etype EntryType, data []byte) (uint64, error) {
	// Log data entry fields
	entity := s.entriesDefinition[etype]
	if entity.Name != "" {
		// log.Infof("New data entry: %d", s.lastEntry+1)
		log.Infof("New data entry: %s", entity.toString(data))
	} else {
		log.Warn("New data entry: Unknown entry type")
	}

	// Generate data entry
	e := FileEntry{
		packetType: PtEntry,
		length:     1 + 4 + 4 + 8 + uint32(len(data)),
		entryType:  EntryType(etype),
		entryNum:   s.lastEntry + 1,
		data:       data,
	}

	// Write data entry in the file
	err := s.sf.AddFileEntry(e)
	if err != nil {
		return 0, nil
	}

	// Save the entry in the atomic operation in progress
	s.atomicOp.entries = append(s.atomicOp.entries, e)

	// Increase sequential entry number
	s.lastEntry++
	return s.lastEntry, nil
}

func (s *StreamServer) CommitAtomicOp() error {
	log.Debug("!!!Commit AtomicOp")
	s.atomicOp.status = aoCommitting

	// Update header in the file (commit new entries)
	err := s.sf.writeHeaderEntry()
	if err != nil {
		return err
	}

	// Do broadcast of the commited atomic operation to the stream clients
	s.broadcastAtomicOp() // TODO: call as goroutine

	return nil
}

func (s *StreamServer) RollbackAtomicOp() error {
	log.Debug("!!!Rollback AtomicOp")
	s.atomicOp.status = aoRollbacking

	// TODO: work

	return nil
}

func (s *StreamServer) broadcastAtomicOp() {
	// For each connected and started client
	log.Debugf("Broadcast clients length: %d", len(s.clients))
	for id, cli := range s.clients {
		log.Infof("Client %s status %d[%s]", id, cli.status, StrClientStatus[cli.status])
		if cli.status != csStarted {
			continue
		}

		// Send entries
		log.Debugf("Streaming to: %s", id)
		writer := bufio.NewWriter(cli.conn)
		for _, entry := range s.atomicOp.entries {
			log.Debugf("Sending data entry %d to %s", entry.entryNum, id)
			binaryEntry := encodeFileEntryToBinary(entry)

			// Send the file data entry
			_, err := writer.Write(binaryEntry)
			if err != nil {
				// Kill client connection
				log.Errorf("Error sending entry to %s", id)
				s.killClient(id)
			}

			// Flush buffers
			err = writer.Flush()
			if err != nil {
				log.Errorf("Error flushing socket data to %s", id)
				s.killClient(id)
			}
		}
	}

	s.atomicOp.status = aoNone
}

func (s *StreamServer) killClient(clientId string) {
	if s.clients[clientId].status != csKilled {
		s.clients[clientId].status = csKilled
		s.clients[clientId].conn.Close()
	}
}

func (s *StreamServer) processCommand(command Command, clientId string) error {
	cli := s.clients[clientId]

	// Manage each different kind of command request from a client
	var err error
	switch command {
	case CmdStart:
		if cli.status != csStopped {
			log.Error("Stream to client already started!")
			err = errors.New("client already started")
			_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], clientId)
		} else {
			// Perform work of start command
			cli.status = csStarting
			err = s.processCmdStart(clientId)
			if err == nil {
				cli.status = csStarted
			}
		}

	case CmdStop:
		if cli.status != csStarted {
			log.Error("Stream to client already stopped!")
			err = errors.New("client already stopped")
			_ = s.sendResultEntry(uint32(CmdErrAlreadyStopped), StrCommandErrors[CmdErrAlreadyStopped], clientId)
		} else {
			cli.status = csStopped
			err = s.processCmdStop(clientId)
		}

	case CmdHeader:
		if cli.status != csStopped {
			log.Error("Header command not allowed, stream started!")
			err = errors.New("header command not allowed")
			_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], clientId)
		} else {
			err = s.processCmdHeader(clientId)
		}

	default:
		log.Error("Invalid command!")
		err = errors.New("invalid command")
		_ = s.sendResultEntry(uint32(CmdErrInvalidCommand), StrCommandErrors[CmdErrInvalidCommand], clientId)
	}

	return err
}

func (s *StreamServer) processCmdStart(clientId string) error {
	// Read from entry number parameter
	reader := bufio.NewReader(s.clients[clientId].conn)
	fromEntry, err := readFullUint64(reader)
	if err != nil {
		return err
	}

	// Check received param
	if fromEntry > s.lastEntry {
		log.Errorf("Start command invalid from entry %d for client %s", fromEntry, clientId)
		err = errors.New("start command invalid param from entry")
		_ = s.sendResultEntry(uint32(CmdErrBadFromEntry), StrCommandErrors[CmdErrBadFromEntry], clientId)
		return err
	}

	// Send a command result entry OK
	err = s.sendResultEntry(0, "OK", clientId)
	if err != nil {
		return err
	}

	// Stream entries data from the requested entry number
	log.Infof("Streaming from entry %d for client %s", fromEntry, clientId)
	err = s.streamingFromEntry(clientId, fromEntry)

	return err
}

func (s *StreamServer) processCmdStop(clientId string) error {
	// Send a command result entry OK
	err := s.sendResultEntry(0, "OK", clientId)
	return err
}

func (s *StreamServer) processCmdHeader(clientId string) error {
	// Get current written/committed file header
	header := s.sf.getHeaderEntry()
	binaryHeader := encodeHeaderEntryToBinary(header)

	// Send header entry to the client
	conn := s.clients[clientId].conn
	writer := bufio.NewWriter(conn)
	_, err := writer.Write(binaryHeader)
	if err != nil {
		log.Errorf("Error sending header entry to %s: %v", clientId, err)
		return err
	}

	err = writer.Flush()
	if err != nil {
		log.Errorf("Error flushing socket data to %s: %v", clientId, err)
		return err
	}
	return nil
}

func (s *StreamServer) streamingFromEntry(clientId string, fromEntry uint64) error {
	return nil
}

// Send the response to a command that is a result entry
func (s *StreamServer) sendResultEntry(errorNum uint32, errorStr string, clientId string) error {
	// Prepare the result entry
	byteSlice := []byte(errorStr)

	entry := ResultEntry{
		packetType: PtResult,
		length:     1 + 4 + 4 + uint32(len(byteSlice)),
		errorNum:   errorNum,
		errorStr:   byteSlice,
	}
	// PrintResultEntry(entry) // TODO: remove

	// Convert struct to binary bytes
	binaryEntry := encodeResultEntryToBinary(entry)
	log.Debugf("result entry: %v", binaryEntry)

	// Send the result entry to the client
	conn := s.clients[clientId].conn
	writer := bufio.NewWriter(conn)
	_, err := writer.Write(binaryEntry)
	if err != nil {
		log.Errorf("Error sending result entry to %s", clientId)
		s.killClient(clientId)
		return err
	}

	err = writer.Flush()
	if err != nil {
		log.Errorf("Error flushing socket data to %s: %v", clientId, err)
		return err
	}
	return nil
}

func readFullUint64(reader *bufio.Reader) (uint64, error) {
	// Read 8 bytes (uint64 value)
	buffer := make([]byte, 8)
	n, err := io.ReadFull(reader, buffer)
	if err != nil {
		if err == io.EOF {
			log.Warn("Client close connection")
		} else {
			log.Errorf("Error reading from client: %v", err)
		}
		return 0, err
	}

	// Convert bytes to uint64
	var value uint64
	err = binary.Read(bytes.NewReader(buffer[:n]), binary.BigEndian, &value)
	if err != nil {
		log.Error("Error converting bytes to uint64")
		return 0, err
	}

	return value, nil
}

// Encode/convert from an entry type to binary bytes slice
func encodeResultEntryToBinary(e ResultEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.length)
	be = binary.BigEndian.AppendUint32(be, e.errorNum)
	be = append(be, e.errorStr...)
	return be
}

// Decode/convert from binary bytes slice to an entry type
func DecodeBinaryToResultEntry(b []byte) (ResultEntry, error) {
	e := ResultEntry{}

	if len(b) < FixedSizeResultEntry {
		log.Error("Invalid binary result entry")
		return e, errors.New("invalid binary result entry")
	}

	e.packetType = b[0]
	e.length = binary.BigEndian.Uint32(b[1:5])
	e.errorNum = binary.BigEndian.Uint32(b[5:9])
	e.errorStr = b[9:]

	if uint32(len(e.errorStr)) != e.length-FixedSizeResultEntry {
		log.Error("Error decoding binary result entry")
		return e, errors.New("error decoding binary result entry")
	}

	return e, nil
}

func PrintResultEntry(e ResultEntry) {
	log.Debug("--- RESULT ENTRY -------------------------")
	log.Debugf("packetType: [%d]", e.packetType)
	log.Debugf("length: [%d]", e.length)
	log.Debugf("errorNum: [%d]", e.errorNum)
	log.Debugf("errorStr: [%s]", e.errorStr)
}
