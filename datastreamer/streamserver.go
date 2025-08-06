package datastreamer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

// Command type for the TCP client commands
type Command uint64

// ClientStatus type for the status of the client
type ClientStatus uint64

// AOStatus type for the atomic operation internal states
type AOStatus uint64

// EntryType type for the entry event types
type EntryType uint32

// StreamType type for the stream types
type StreamType uint64

// CommandError type for the command responses
type CommandError uint32

// EntryTypeNotFound is the entry type value for CmdEntry/CmdBookmark when entry/bookmark not found
const EntryTypeNotFound = math.MaxUint32

const (
	maxConnections    = 100 // Maximum number of connected clients
	streamBuffer      = 256 // Buffers for the stream channel
	maxBookmarkLength = 16  // Maximum number of bytes for a bookmark
)

const (
	CmdStart         Command = iota + 1 // CmdStart for the start from entry TCP client command
	CmdStop                             // CmdStop for the stop TCP client command
	CmdHeader                           // CmdHeader for the header TCP client command
	CmdStartBookmark                    // CmdStartBookmark for the start from bookmark TCP client command
	CmdEntry                            // CmdEntry for the get entry TCP client command
	CmdBookmark                         // CmdBookmark for the get bookmark TCP client command
)

const (
	CmdErrOK              CommandError = iota // CmdErrOK for no error
	CmdErrAlreadyStarted                      // CmdErrAlreadyStarted for client already started error
	CmdErrAlreadyStopped                      // CmdErrAlreadyStopped for client already stopped error
	CmdErrBadFromEntry                        // CmdErrBadFromEntry for invalid starting entry number
	CmdErrBadFromBookmark                     // CmdErrBadFromBookmark for invalid starting bookmark
	CmdErrInvalidCommand  CommandError = 9    // CmdErrInvalidCommand for invalid/unknown command error
)

const (
	// Client status
	csSyncing ClientStatus = iota + 1
	csSynced
	csStopped
	csKilled ClientStatus = 0xff
)

const (
	// Atomic operation status
	aoNone AOStatus = iota + 1
	aoStarted
	aoCommitting
	aoRollbacking
)

var (
	// StrClientStatus for client status description
	StrClientStatus = map[ClientStatus]string{
		csSyncing: "Syncing",
		csSynced:  "Synced",
		csStopped: "Stopped",
		csKilled:  "Killed",
	}

	// StrCommand for TCP commands description
	StrCommand = map[Command]string{
		CmdStart:         "Start",
		CmdStop:          "Stop",
		CmdHeader:        "Header",
		CmdStartBookmark: "StartBookmark",
		CmdEntry:         "Entry",
		CmdBookmark:      "Bookmark",
	}

	// StrCommandErrors for TCP command errors description
	StrCommandErrors = map[CommandError]string{
		CmdErrOK:              "OK",
		CmdErrAlreadyStarted:  "Already started",
		CmdErrAlreadyStopped:  "Already stopped",
		CmdErrBadFromEntry:    "Bad from entry",
		CmdErrBadFromBookmark: "Bad from bookmark",
		CmdErrInvalidCommand:  "Invalid command",
	}
)

// StreamServer type to manage a data stream server
type StreamServer struct {
	port              uint16        // Server stream port
	fileName          string        // Stream file name
	writeTimeout      time.Duration // Timeout for write operations on client connection
	inactivityTimeout time.Duration // Inactivity timeout to kill a client connection
	// Time interval to check for client connections that have reached
	// the inactivity timeout and kill them
	inactivityCheckInterval time.Duration
	started                 bool // Flag server started

	version      uint8
	systemID     uint64
	streamType   StreamType
	ln           net.Listener
	clients      map[string]*client
	mutexClients sync.RWMutex // Mutex for write access to clients map

	nextEntry uint64 // Next sequential entry number
	initEntry uint64 // Only used by the relay (initial next entry in the master server)

	atomicOp   streamAO      // Current in progress (if any) atomic operation
	stream     chan streamAO // Channel to stream committed atomic operations
	streamFile *StreamFile
	bookmark   *StreamBookmark
}

// streamAO type to manage atomic operations
type streamAO struct {
	status     AOStatus
	startEntry uint64
	entries    []FileEntry
}

// client type for the server to manage clients
type client struct {
	conn         net.Conn
	status       ClientStatus
	fromEntry    uint64
	clientID     string
	lastActivity time.Time
}

func (c *client) updateActivity() {
	c.lastActivity = time.Now()
}

// ResultEntry type for a result entry
type ResultEntry struct {
	packetType uint8 // 0xff:Result
	length     uint32
	errorNum   uint32 // 0:No error
	errorStr   []byte
}

// NewServer creates a new data stream server
func NewServer(port uint16, version uint8, systemID uint64, streamType StreamType, fileName string,
	writeTimeout time.Duration, inactivityTimeout time.Duration, inactivityCheckInterval time.Duration,
	cfg *log.Config) (*StreamServer, error) {
	// Create the server data stream
	s := StreamServer{
		port:                    port,
		fileName:                fileName,
		writeTimeout:            writeTimeout,
		inactivityTimeout:       inactivityTimeout,
		inactivityCheckInterval: inactivityCheckInterval,
		started:                 false,

		version:    version,
		systemID:   systemID,
		streamType: streamType,
		ln:         nil,
		clients:    make(map[string]*client),
		nextEntry:  0,
		initEntry:  0,

		atomicOp: streamAO{
			status:     aoNone,
			startEntry: 0,
			entries:    []FileEntry{},
		},
		stream: make(chan streamAO, streamBuffer),
	}

	// Add file extension if not present
	ind := strings.IndexRune(s.fileName, '.')
	if ind == -1 {
		s.fileName += ".bin"
	}

	// Initialize the logger
	if cfg != nil {
		log.Init(*cfg)
	}

	// Open (or create) the data stream file
	var err error
	s.streamFile, err = NewStreamFile(s.fileName, version, systemID, s.streamType)
	if err != nil {
		return nil, err
	}

	// Initialize the data entry number
	s.nextEntry = s.streamFile.header.TotalEntries

	// Open (or create) the bookmarks DB
	name := s.fileName[:strings.LastIndex(s.fileName, ".")] + ".db"
	s.bookmark, err = NewBookmark(name)
	if err != nil {
		return &s, err
	}

	return &s, nil
}

// Start opens access to TCP clients and starts broadcasting
func (s *StreamServer) Start() error {
	// Start the server data stream
	var err error
	s.ln, err = net.Listen("tcp", ":"+strconv.Itoa(int(s.port)))
	if err != nil {
		log.Errorf("Error creating datastream server %d: %v", s.port, err)
		return err
	}

	// Goroutine to broadcast committed atomic operations
	go s.broadcastAtomicOp()

	// Goroutine to check inactivity timeout in client connections
	go s.checkClientInactivity()

	// Goroutine to wait for clients connections
	log.Infof("Listening on port: %d", s.port)
	go s.waitConnections()

	// Flag stared
	s.started = true

	return nil
}

// checkClientInactivity kills all the clients that reach write inactivity timeout
func (s *StreamServer) checkClientInactivity() {
	for {
		time.Sleep(s.inactivityCheckInterval)

		var clientsToKill = map[string]struct{}{}
		s.mutexClients.Lock()
		for _, client := range s.clients {
			if client.lastActivity.Add(s.inactivityTimeout).Before(time.Now()) {
				clientsToKill[client.clientID] = struct{}{}
			}
		}
		s.mutexClients.Unlock()

		for clientID := range clientsToKill {
			log.Warnf("killing inactive client %s", clientID)
			s.killClient(clientID)
		}
	}
}

// waitConnections waits for a new client connection and creates a goroutine to manages it
func (s *StreamServer) waitConnections() {
	defer s.ln.Close()

	const timeout = 2 * time.Second

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			log.Errorf("Error accepting new connection: %v", err)
			time.Sleep(timeout)
			continue
		}

		// Check max connections allowed
		if s.getSafeClientsLen() >= maxConnections {
			log.Warnf("Unable to accept client connection, maximum number of connections reached (%d)", maxConnections)
			conn.Close()
			time.Sleep(timeout)
			continue
		}

		// Goroutine to manage client (command requests and entries stream)
		go s.handleConnection(conn)
	}
}

// handleConnection reads from the client connection and processes the received commands
func (s *StreamServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	clientID := conn.RemoteAddr().String()
	log.Debugf("New connection: %s", clientID)

	s.mutexClients.Lock()
	client := &client{
		conn:         conn,
		status:       csStopped,
		fromEntry:    0,
		clientID:     clientID,
		lastActivity: time.Now(),
	}
	s.clients[clientID] = client
	s.mutexClients.Unlock()

	for {
		// Read command
		command, err := readFullUint64(client)
		if err != nil {
			s.killClient(clientID)
			return
		}
		// Read stream type
		stUint64, err := readFullUint64(client)
		if err != nil {
			s.killClient(clientID)
			return
		}
		st := StreamType(stUint64)

		// Check stream type
		if st != s.streamType {
			log.Errorf("Mismatch stream type: client %s killed", clientID)
			s.killClient(clientID)
			return
		}

		// Check if the client is nil
		safeClient := s.getSafeClient(clientID)
		if safeClient == nil {
			log.Errorf("Client %s is nil", clientID)
			s.killClient(clientID)
			return
		}

		// Manage the requested command
		log.Debugf("Command %d[%s] received from %s", command, StrCommand[Command(command)], clientID)
		err = s.processCommand(Command(command), safeClient)
		if err != nil {
			log.Errorf("Error processing command %d[%s] from %s: %v", command, StrCommand[Command(command)], clientID, err)
		}
	}
}

// StartAtomicOp starts a new atomic operation
func (s *StreamServer) StartAtomicOp() error {
	start := time.Now().UnixNano()
	defer log.Debugf("StartAtomicOp process time: %vns", time.Now().UnixNano()-start)

	log.Debugf("!AtomicOp START (%d)", s.nextEntry)
	// Check status of the server
	if !s.started {
		log.Errorf("AtomicOp not allowed. Server is not started")
		return ErrAtomicOpNotAllowed
	}
	// Check status of the atomic operation
	if s.atomicOp.status == aoStarted {
		log.Errorf("AtomicOp already started and in progress after entry %d", s.atomicOp.startEntry)
		return ErrStartAtomicOpNotAllowed
	}

	s.atomicOp.status = aoStarted
	s.atomicOp.startEntry = s.nextEntry
	return nil
}

// AddStreamEntry adds a new entry in the current atomic operation
func (s *StreamServer) AddStreamEntry(etype EntryType, data []byte) (uint64, error) {
	start := time.Now().UnixNano()
	defer log.Debugf("AddStreamEntry process time: %vns", time.Now().UnixNano()-start)

	// Add to the stream file
	entryNum, err := s.addStream("Data", etype, data)

	return entryNum, err
}

// AddStreamBookmark adds a new bookmark in the current atomic operation
func (s *StreamServer) AddStreamBookmark(bookmark []byte) (uint64, error) {
	start := time.Now().UnixNano()
	defer log.Debugf("AddStreamBookmark process time: %vns", time.Now().UnixNano()-start)

	// Add to the stream file
	entryNum, err := s.addStream("Bookmark", EtBookmark, bookmark)
	if err != nil {
		return 0, err
	}

	// Add to the bookmark index
	err = s.bookmark.AddBookmark(bookmark, entryNum)
	if err != nil {
		return entryNum, err
	}

	return entryNum, nil
}

// addStream adds a new stream entry in the current atomic operation
func (s *StreamServer) addStream(desc string, etype EntryType, data []byte) (uint64, error) {
	// Check atomic operation status
	if s.atomicOp.status != aoStarted {
		log.Errorf("Add stream entry not allowed, AtomicOp is not started")
		return 0, ErrAddEntryNotAllowed
	}

	// Generate data entry
	e := FileEntry{
		packetType: PtData,
		Length:     1 + 4 + 4 + 8 + uint32(len(data)),
		Type:       etype,
		Number:     s.nextEntry,
		Data:       data,
	}

	// Log data entry fields
	log.Debugf("%s entry: %d | %d | %d | %d | %d", desc, e.Number, e.packetType, e.Length, e.Type, len(data))

	// Update header (in memory) and write data entry into the file
	err := s.streamFile.AddFileEntry(e)
	if err != nil {
		return 0, nil
	}

	// Save the entry in the atomic operation in progress
	s.atomicOp.entries = append(s.atomicOp.entries, e)

	// Increase sequential entry number
	s.nextEntry++

	return e.Number, nil
}

// CommitAtomicOp commits the current atomic operation and streams it to the clients
func (s *StreamServer) CommitAtomicOp() error {
	start := time.Now()

	log.Debugf("committing datastream atomic operation, startEntry: %d", s.atomicOp.startEntry)
	if s.atomicOp.status != aoStarted {
		log.Errorf("commit not allowed, atomic operation is not in the started state")
		return ErrCommitNotAllowed
	}

	s.atomicOp.status = aoCommitting

	// Update header into the file (commit the new entries)
	err := s.streamFile.writeHeaderEntry()
	if err != nil {
		return err
	}

	// Do broadcast of the committed atomic operation to the stream clients
	atomic := streamAO{
		status:     s.atomicOp.status,
		startEntry: s.atomicOp.startEntry,
	}
	atomic.entries = make([]FileEntry, len(s.atomicOp.entries))
	copy(atomic.entries, s.atomicOp.entries)

	s.stream <- atomic

	// No atomic operation in progress
	s.clearAtomicOp()

	log.Debugf("committed datastream atomic operation, startEntry: %d, time: %v", s.atomicOp.startEntry, time.Since(start))

	return nil
}

// RollbackAtomicOp cancels the current atomic operation and rollbacks the changes
func (s *StreamServer) RollbackAtomicOp() error {
	start := time.Now().UnixNano()
	defer log.Debugf("RollbackAtomicOp process time: %vns", time.Now().UnixNano()-start)

	log.Debugf("rollback datastream atomic operation, startEntry: %d", s.atomicOp.startEntry)
	if s.atomicOp.status != aoStarted {
		log.Errorf("Rollback not allowed, AtomicOp is not in the started state")
		return ErrRollbackNotAllowed
	}

	s.atomicOp.status = aoRollbacking

	// Restore header in memory (discard current) from the file header (rollback entries)
	err := s.streamFile.rollbackHeader()
	if err != nil {
		return err
	}

	// Rollback the entry number
	s.nextEntry = s.atomicOp.startEntry

	// No atomic operation in progress
	s.clearAtomicOp()

	return nil
}

// TruncateFile truncates stream data file from an entry number onwards
func (s *StreamServer) TruncateFile(entryNum uint64) error {
	// Check the entry number
	if entryNum >= s.nextEntry {
		log.Errorf("Invalid entry number [%d], it doesn't exist", entryNum)
		return ErrInvalidEntryNumber
	}

	// Check atomic operation is not in progress
	if s.atomicOp.status != aoNone {
		log.Errorf("Truncate not allowed, atomic operation in progress")
		return ErrTruncateNotAllowed
	}

	// Log previous header
	PrintHeaderEntry(s.streamFile.header, "(before truncate)")

	// Truncate entries in the file
	err := s.streamFile.truncateFile(entryNum)
	if err != nil {
		return err
	}

	// Update entry number sequence
	s.nextEntry = s.streamFile.header.TotalEntries

	// Log current header
	log.Infof("File truncated! Removed entries from %d (included) until end of file", entryNum)
	PrintHeaderEntry(s.streamFile.header, "(after truncate)")

	return nil
}

// UpdateEntryData updates the internal data of an entry
func (s *StreamServer) UpdateEntryData(entryNum uint64, etype EntryType, data []byte) error {
	// Check the entry number
	if entryNum >= s.nextEntry {
		log.Errorf("Invalid entry number [%d], it doesn't exist", entryNum)
		return ErrInvalidEntryNumber
	}

	// Check entry not in current atomic operation
	if s.atomicOp.status != aoNone && entryNum >= s.atomicOp.startEntry {
		log.Errorf("Entry number [%d] not allowed for update, it's in the current atomic operation", entryNum)
		return ErrUpdateNotAllowed
	}

	// Update entry data in the stream file
	err := s.streamFile.updateEntryData(entryNum, etype, data)
	if err != nil {
		return err
	}

	return nil
}

// GetHeader returns the current committed header
func (s *StreamServer) GetHeader() HeaderEntry {
	// Get current file header
	header := s.streamFile.getHeaderEntry()
	return header
}

// GetEntry searches in the stream file and returns the data for the requested entry
func (s *StreamServer) GetEntry(entryNum uint64) (FileEntry, error) {
	// Initialize file stream iterator
	iterator, err := s.streamFile.iteratorFrom(entryNum, true)
	if err != nil {
		return FileEntry{}, err
	}

	// Get requested entry data
	_, err = s.streamFile.iteratorNext(iterator)
	if err != nil {
		return FileEntry{}, err
	}

	// Close iterator
	s.streamFile.iteratorEnd(iterator)

	return iterator.Entry, nil
}

// GetBookmark returns the entry number pointed by the bookmark
func (s *StreamServer) GetBookmark(bookmark []byte) (uint64, error) {
	return s.bookmark.GetBookmark(bookmark)
}

// GetFirstEventAfterBookmark searches in the stream file by bookmark and returns the first event entry data
func (s *StreamServer) GetFirstEventAfterBookmark(bookmark []byte) (FileEntry, error) {
	var err error
	entry := FileEntry{}

	// Get entry of the bookmark
	entryNum, err := s.bookmark.GetBookmark(bookmark)
	if err != nil {
		return entry, err
	}

	// Initialize file stream iterator from bookmark's entry
	iterator, err := s.streamFile.iteratorFrom(entryNum, true)
	if err != nil {
		return entry, err
	}

	// Loop until find the first event entry (skip bookmarks entries)
	for {
		// Get next entry data
		end, err := s.streamFile.iteratorNext(iterator)

		// Loop break conditions (error, end of file, entry type different from bookmark)
		if err != nil || end || iterator.Entry.Type != EtBookmark {
			break
		}
	}

	// Close iterator
	s.streamFile.iteratorEnd(iterator)

	return iterator.Entry, err
}

// GetDataBetweenBookmarks returns the data between two bookmarks
func (s *StreamServer) GetDataBetweenBookmarks(bookmarkFrom, bookmarkTo []byte) ([]byte, error) {
	var err error
	var response []byte

	// Get entry of the from bookmark
	fromEntryNum, err := s.bookmark.GetBookmark(bookmarkFrom)
	if err != nil {
		return response, err
	}

	// Get entry of the to bookmark
	toEntryNum, err := s.bookmark.GetBookmark(bookmarkTo)
	if err != nil {
		return response, err
	}

	// Check if the from bookmark is greater than the to bookmark
	if fromEntryNum > toEntryNum {
		return response, ErrInvalidBookmarkRange
	}

	// Initialize file stream iterator from bookmark's entry
	iterator, err := s.streamFile.iteratorFrom(fromEntryNum, true)
	if err != nil {
		return response, err
	}

	// Loop until we reach the to bookmark
	for {
		// Get next entry data
		end, err := s.streamFile.iteratorNext(iterator)

		// Loop break conditions
		if err != nil || end || iterator.Entry.Number >= toEntryNum {
			break
		}

		if iterator.Entry.Type != EtBookmark {
			response = append(response, iterator.Entry.Data...)
		}
	}

	// Close iterator
	s.streamFile.iteratorEnd(iterator)

	return response, err
}

// clearAtomicOp sets the current atomic operation to none
func (s *StreamServer) clearAtomicOp() {
	// No atomic operation in progress and empty entries slice
	s.atomicOp.entries = s.atomicOp.entries[:0]
	s.atomicOp.status = aoNone
}

// broadcastAtomicOp broadcasts committed atomic operations to the clients
func (s *StreamServer) broadcastAtomicOp() {
	defer s.streamFile.file.Close()
	defer s.bookmark.db.Close()

	var err error
	for {
		// Wait for new atomic operation to broadcast
		broadcastOp := <-s.stream
		start := time.Now()
		var killedClientMap = map[string]struct{}{}
		var clientMap = map[string]struct{}{}
		s.mutexClients.RLock()
		clientsSnapshot := make(map[string]*client, len(s.clients))
		for id, cli := range s.clients {
			clientsSnapshot[id] = cli
		}
		s.mutexClients.RUnlock()

		// For each connected and started client
		log.Debug("sending datastream entries, count: %d, clients: %d", len(broadcastOp.entries), len(clientsSnapshot))
		for id, cli := range clientsSnapshot {
			log.Debugf("client %s status %d (%s)", id, cli.status, StrClientStatus[cli.status])
			clientMap[id] = struct{}{}
			if cli.status != csSynced {
				continue
			}

			// Send entries
			for _, entry := range broadcastOp.entries {
				if entry.Number >= cli.fromEntry {
					log.Debugf("sending data entry %d (type %d) to %s", entry.Number, entry.Type, id)

					binaryEntry := encodeFileEntryToBinary(entry)

					// Send the file data entry
					if cli.conn != nil {
						_, err = TimeoutWrite(cli, binaryEntry, s.writeTimeout)
					} else {
						err = ErrNilConnection
					}
					if err != nil {
						// Kill client connection
						log.Warnf("error sending entry to %s, error: %v", id, err)
						killedClientMap[id] = struct{}{}
						break // skip rest of entries for this client
					}
				}
			}
		}

		for k := range killedClientMap {
			s.killClient(k)
		}

		sClients := ""
		for c := range clientMap {
			if sClients == "" {
				sClients = c
			} else {
				sClients += ", " + c
			}
		}

		log.Debugf("sent datastream entries, count: %d, clients: %d, time: %v, clients-ip: {%s}",
			len(broadcastOp.entries), len(clientsSnapshot), time.Since(start), sClients)
	}
}

// killClient disconnects the client and removes it from server clients struct
func (s *StreamServer) killClient(clientID string) {
	s.mutexClients.Lock()
	defer s.mutexClients.Unlock()

	client, ok := s.clients[clientID]
	if !ok {
		return
	}

	if client.status != csKilled {
		client.status = csKilled
		if client.conn != nil {
			client.conn.Close()
		}
		delete(s.clients, clientID)
	}
}

// processCommand manages the received TCP commands from the clients
func (s *StreamServer) processCommand(command Command, client *client) error {
	cli := client

	// Manage each different kind of command request from a client
	var err error

	switch command {
	case CmdStart:
		err = s.handleStartCommand(cli)

	case CmdStartBookmark:
		err = s.handleStartBookmarkCommand(cli)

	case CmdStop:
		err = s.handleStopCommand(cli)

	case CmdHeader:
		err = s.handleHeaderCommand(cli)

	case CmdEntry:
		err = s.handleEntryCommand(cli)

	case CmdBookmark:
		err = s.handleBookmarkCommand(cli)

	default:
		log.Error("Invalid command!")
		err = ErrInvalidCommand
		_ = s.sendResultEntry(uint32(CmdErrInvalidCommand), StrCommandErrors[CmdErrInvalidCommand], client)
	}

	return err
}

// handleStartCommand processes the CmdStart command
func (s *StreamServer) handleStartCommand(cli *client) error {
	if cli.status != csStopped {
		log.Error("Stream to client already started!")
		_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], cli)
		return ErrClientAlreadyStarted
	}

	cli.status = csSyncing
	err := s.processCmdStart(cli)
	if err == nil {
		cli.status = csSynced
	}

	return err
}

// handleStartBookmarkCommand processes the CmdStartBookmark command
func (s *StreamServer) handleStartBookmarkCommand(cli *client) error {
	if cli.status != csStopped {
		log.Error("Stream to client already started!")
		_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], cli)
		return ErrClientAlreadyStarted
	}

	cli.status = csSyncing
	err := s.processCmdStartBookmark(cli)
	if err == nil {
		cli.status = csSynced
	}

	return err
}

// handleStopCommand processes the CmdStop command
func (s *StreamServer) handleStopCommand(cli *client) error {
	if cli.status != csSynced {
		log.Error("Stream to client already stopped!")
		_ = s.sendResultEntry(uint32(CmdErrAlreadyStopped), StrCommandErrors[CmdErrAlreadyStopped], cli)
		return ErrClientAlreadyStopped
	}

	cli.status = csStopped
	return s.processCmdStop(cli)
}

// handleHeaderCommand processes the CmdHeader command
func (s *StreamServer) handleHeaderCommand(cli *client) error {
	if cli.status != csStopped {
		log.Error("Header command not allowed, stream started!")
		_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], cli)
		return ErrHeaderCommandNotAllowed
	}

	return s.processCmdHeader(cli)
}

// handleEntryCommand processes the CmdEntry command
func (s *StreamServer) handleEntryCommand(cli *client) error {
	if cli.status != csStopped {
		log.Error("Entry command not allowed, stream started!")
		_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], cli)
		return ErrEntryCommandNotAllowed
	}

	return s.processCmdEntry(cli)
}

// handleBookmarkCommand processes the CmdBookmark command
func (s *StreamServer) handleBookmarkCommand(cli *client) error {
	if cli.status != csStopped {
		log.Error("Bookmark command not allowed, stream started!")
		_ = s.sendResultEntry(uint32(CmdErrAlreadyStarted), StrCommandErrors[CmdErrAlreadyStarted], cli)
		return ErrBookmarkCommandNotAllowed
	}

	return s.processCmdBookmark(cli)
}

// processCmdStart processes the TCP Start command from the clients
func (s *StreamServer) processCmdStart(client *client) error {
	// Read from entry number parameter
	fromEntry, err := readFullUint64(client)
	if err != nil {
		return err
	}
	client.fromEntry = fromEntry

	// Log
	log.Debugf("Client %s command Start from %d", client.clientID, fromEntry)

	// Check received param
	if fromEntry > s.nextEntry && fromEntry > s.initEntry {
		log.Errorf("Start command invalid from entry %d for client %s", fromEntry, client.clientID)
		err = ErrStartCommandInvalidParamFromEntry
		_ = s.sendResultEntry(uint32(CmdErrBadFromEntry), StrCommandErrors[CmdErrBadFromEntry], client)
		return err
	}

	// Send a command result entry OK
	err = s.sendResultEntry(0, "OK", client)
	if err != nil {
		return err
	}

	// Stream entries data from the requested entry number
	if fromEntry < s.nextEntry {
		err = s.streamingFromEntry(client, fromEntry)
	}

	return err
}

// processCmdStartBookmark processes the TCP Start Bookmark command from the clients
func (s *StreamServer) processCmdStartBookmark(client *client) error {
	// Read bookmark length parameter
	length, err := readFullUint32(client)
	if err != nil {
		return err
	}

	// Check maximum length allowed
	if length > maxBookmarkLength {
		log.Errorf("Client %s exceeded [%d] maximum allowed length [%d] for a bookmark.",
			client.clientID, length, maxBookmarkLength)
		return ErrBookmarkMaxLength
	}

	// Read bookmark parameter
	bookmark, err := readFullBytes(length, client)
	if err != nil {
		return err
	}

	// Log
	log.Debugf("Client %s command StartBookmark [%v]", client.clientID, bookmark)

	// Get bookmark
	entryNum, err := s.bookmark.GetBookmark(bookmark)
	if err != nil {
		log.Errorf("StartBookmark command invalid from bookmark %v for client %s: %v", bookmark, client.clientID, err)
		err = ErrStartBookmarkInvalidParamFromBookmark
		_ = s.sendResultEntry(uint32(CmdErrBadFromBookmark), StrCommandErrors[CmdErrBadFromBookmark], client)
		return err
	}

	// Send a command result entry OK
	err = s.sendResultEntry(0, "OK", client)
	if err != nil {
		return err
	}

	// Stream entries data from the entry number marked by the bookmark
	log.Debugf("Client %s Bookmark [%v] is the entry number [%d]", client.clientID, bookmark, entryNum)
	if entryNum < s.nextEntry {
		err = s.streamingFromEntry(client, entryNum)
	}

	return err
}

// processCmdStop processes the TCP Stop command from the clients
func (s *StreamServer) processCmdStop(client *client) error {
	// Log
	log.Debugf("Client %s command Stop", client.clientID)

	// Send a command result entry OK
	err := s.sendResultEntry(0, "OK", client)
	return err
}

// processCmdHeader processes the TCP Header command from the clients
func (s *StreamServer) processCmdHeader(client *client) error {
	// Log
	log.Debugf("Client %s command Header", client.clientID)

	// Send a command result entry OK
	err := s.sendResultEntry(0, "OK", client)
	if err != nil {
		return err
	}

	// Get current written/committed file header
	header := s.streamFile.getHeaderEntry()
	binaryHeader := encodeHeaderEntryToBinary(header)

	// Send header entry to the client
	if client.conn != nil {
		_, err = TimeoutWrite(client, binaryHeader, s.writeTimeout)
	} else {
		err = ErrNilConnection
	}
	if err != nil {
		log.Errorf("Error sending header entry to %s: %v", client.clientID, err)
		return err
	}
	return nil
}

// processCmdEntry processes the TCP Entry command from the clients
func (s *StreamServer) processCmdEntry(client *client) error {
	// Read from entry number parameter
	entryNumber, err := readFullUint64(client)
	if err != nil {
		return err
	}

	// Log
	log.Debugf("Client %s command Entry %d", client.clientID, entryNumber)

	// Send a command result entry OK
	err = s.sendResultEntry(0, "OK", client)
	if err != nil {
		return err
	}

	// Get the requested entry
	entry, err := s.GetEntry(entryNumber)
	if err != nil {
		log.Warnf("Entry not found %d: %v", entryNumber, err)
		entry = FileEntry{}
		entry.Length = FixedSizeFileEntry
		entry.Type = EntryTypeNotFound
	}
	entry.packetType = PtDataRsp
	binaryEntry := encodeFileEntryToBinary(entry)

	// Send entry to the client
	if client.conn != nil {
		_, err = TimeoutWrite(client, binaryEntry, s.writeTimeout)
	} else {
		err = ErrNilConnection
	}
	if err != nil {
		log.Errorf("Error sending entry to %s: %v", client.clientID, err)
		return err
	}

	return err
}

// processCmdBookmark processes the TCP Bookmark command from the clients
func (s *StreamServer) processCmdBookmark(client *client) error {
	// Read bookmark length parameter
	length, err := readFullUint32(client)
	if err != nil {
		return err
	}

	// Check maximum length allowed
	if length > maxBookmarkLength {
		log.Errorf("Client %s exceeded [%d] maximum allowed length [%d] for a bookmark.",
			client.clientID, length, maxBookmarkLength)
		return ErrBookmarkMaxLength
	}

	// Read bookmark parameter
	bookmark, err := readFullBytes(length, client)
	if err != nil {
		return err
	}

	// Log
	log.Debugf("Client %s command Bookmark %v", client.clientID, bookmark)

	// Send a command result entry OK
	err = s.sendResultEntry(0, "OK", client)
	if err != nil {
		return err
	}

	// Get the requested bookmark
	entry, err := s.GetFirstEventAfterBookmark(bookmark)
	if err != nil {
		log.Warnf("Entry not found %v: %v", bookmark, err)
		entry = FileEntry{}
		entry.Length = FixedSizeFileEntry
		entry.Type = EntryTypeNotFound
	}
	entry.packetType = PtDataRsp
	binaryEntry := encodeFileEntryToBinary(entry)

	// Send entry to the client
	if client.conn != nil {
		_, err = TimeoutWrite(client, binaryEntry, s.writeTimeout)
	} else {
		err = ErrNilConnection
	}
	if err != nil {
		log.Errorf("Error sending entry to %s: %v", client.clientID, err)
		return err
	}

	return nil
}

// streamingFromEntry sends to the client the stream data starting from the requested entry number
func (s *StreamServer) streamingFromEntry(client *client, fromEntry uint64) error {
	// Log
	log.Debugf("SYNCING %s from entry %d...", client.clientID, fromEntry)

	// Start file stream iterator
	iterator, err := s.streamFile.iteratorFrom(fromEntry, true)
	if err != nil {
		return err
	}

	// Loop data entries from file stream iterator
	for {
		end, err := s.streamFile.iteratorNext(iterator)
		if err != nil {
			return err
		}

		// Check if end of iterator
		if end {
			break
		}

		// Send the file data entry
		binaryEntry := encodeFileEntryToBinary(iterator.Entry)
		log.Debugf("Sending data entry %d (type %d) to %s", iterator.Entry.Number, iterator.Entry.Type, client.clientID)
		if client.conn != nil {
			_, err = TimeoutWrite(client, binaryEntry, s.writeTimeout)
		} else {
			err = ErrNilConnection
		}
		if err != nil {
			log.Errorf("Error sending entry %d to %s: %v", iterator.Entry.Number, client.clientID, err)
			return err
		}
	}
	log.Debugf("Synced %s until %d!", client.clientID, iterator.Entry.Number)

	// Close iterator
	s.streamFile.iteratorEnd(iterator)

	return nil
}

// sendResultEntry sends the response to a TCP command for the clients
func (s *StreamServer) sendResultEntry(errorNum uint32, errorStr string, client *client) error {
	// Prepare the result entry
	byteSlice := []byte(errorStr)

	entry := ResultEntry{
		packetType: PtResult,
		length:     1 + 4 + 4 + uint32(len(byteSlice)),
		errorNum:   errorNum,
		errorStr:   byteSlice,
	}

	// Convert struct to binary bytes
	binaryEntry := encodeResultEntryToBinary(entry)
	log.Debugf("result entry: %v", binaryEntry)

	// Send the result entry to the client
	var err error
	if client.conn != nil {
		_, err = TimeoutWrite(client, binaryEntry, s.writeTimeout)
	} else {
		err = ErrNilConnection
	}
	if err != nil {
		log.Errorf("Error sending result entry to %s: %v", client.clientID, err)
		return err
	}
	return nil
}

func (s *StreamServer) getSafeClient(clientID string) *client {
	s.mutexClients.RLock()
	defer s.mutexClients.RUnlock()
	return s.clients[clientID]
}

func (s *StreamServer) getSafeClientsLen() int {
	s.mutexClients.RLock()
	defer s.mutexClients.RUnlock()
	return len(s.clients)
}

// BookmarkPrintDump prints all bookmarks
func (s *StreamServer) BookmarkPrintDump() {
	err := s.bookmark.PrintDump()
	if err != nil {
		log.Errorf("Error dumping bookmark database")
	}
}

// readFullUint64 reads from a connection a complete uint64
func readFullUint64(client *client) (uint64, error) {
	// Read 8 bytes (uint64 value)
	buffer, err := readFullBytes(8, client) //nolint:mnd
	if err != nil {
		return 0, err
	}

	// Convert bytes to uint64
	value := binary.BigEndian.Uint64(buffer[0:8])

	return value, nil
}

// readFullUint32 reads from a connection a complete uint32
func readFullUint32(client *client) (uint32, error) {
	// Read 4 bytes (uint32 value)
	buffer, err := readFullBytes(4, client) //nolint:mnd
	if err != nil {
		return 0, err
	}

	// Convert bytes to uint32
	value := binary.BigEndian.Uint32(buffer[0:4])

	return value, nil
}

// readFullBytes reads from a connection number length of bytes
func readFullBytes(length uint32, client *client) ([]byte, error) {
	// Read number length of bytes
	buffer := make([]byte, length)

	if length == 0 {
		return buffer, fmt.Errorf("read length must be greater than 0")
	}

	_, err := io.ReadFull(client.conn, buffer)
	if err != nil {
		if err == io.EOF {
			log.Debugf("Client %s close connection", client.conn.RemoteAddr().String())
		} else {
			log.Warnf("Error reading from client %s, error: %v", client.clientID, err)
		}
		return buffer, err
	}

	client.updateActivity()
	return buffer, nil
}

// encodeResultEntryToBinary encodes from a result entry type to binary bytes slice
func encodeResultEntryToBinary(e ResultEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.length)
	be = binary.BigEndian.AppendUint32(be, e.errorNum)
	be = append(be, e.errorStr...) //nolint:makezero
	return be
}

// DecodeBinaryToResultEntry decodes from binary bytes slice to a result entry type
func DecodeBinaryToResultEntry(b []byte) (ResultEntry, error) {
	e := ResultEntry{}

	if len(b) < FixedSizeResultEntry {
		log.Error("Invalid binary result entry")
		return e, ErrInvalidBinaryEntry
	}

	e.packetType = b[0]
	e.length = binary.BigEndian.Uint32(b[1:5])
	e.errorNum = binary.BigEndian.Uint32(b[5:9])
	e.errorStr = b[9:]

	if uint32(len(e.errorStr)) != e.length-FixedSizeResultEntry {
		log.Error("Error decoding binary result entry")
		return e, ErrDecodingBinaryResultEntry
	}

	return e, nil
}

// PrintResultEntry prints result entry type
func PrintResultEntry(e ResultEntry) {
	log.Debug("--- RESULT ENTRY -------------------------")
	log.Debugf("packetType: [%d]", e.packetType)
	log.Debugf("length: [%d]", e.length)
	log.Debugf("errorNum: [%d]", e.errorNum)
	log.Debugf("errorStr: [%s]", e.errorStr)
}

// IsACommand checks if a command is a valid command
func (c Command) IsACommand() bool {
	return c >= CmdStart && c <= CmdBookmark
}

// TimeoutWrite sets a deadline time before write
func TimeoutWrite(client *client, data []byte, timeout time.Duration) (int, error) {
	err := client.conn.SetWriteDeadline(time.Now().Add(timeout))
	if err != nil {
		log.Warnf("Error setting write deadline: %v", err)
	}
	n, err := client.conn.Write(data)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			log.Debugf("Write deadline exceeded for client %s, error: %v", client.clientID, err)
		}
	} else {
		client.updateActivity()
	}

	return n, err
}
