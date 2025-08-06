package datastreamer

import (
	"time"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

// StreamRelay type to manage a data stream relay
type StreamRelay struct {
	client *StreamClient
	server *StreamServer
}

// NewRelay creates a new data stream relay
func NewRelay(server string, port uint16, version uint8, systemID uint64,
	streamType StreamType, fileName string, writeTimeout time.Duration,
	inactivityTimeout time.Duration, inactivityCheckInterval time.Duration, cfg *log.Config) (*StreamRelay, error) {
	var r StreamRelay
	var err error

	// Create client side
	r.client, err = NewClient(server, streamType)
	if err != nil {
		log.Errorf("Error creating relay client side: %v", err)
		return nil, err
	}

	// Create server side
	r.server, err = NewServer(port, version, systemID, streamType, fileName, writeTimeout,
		inactivityTimeout, inactivityCheckInterval, cfg)
	if err != nil {
		log.Errorf("Error creating relay server side: %v", err)
		return nil, err
	}

	// Set function to process entry
	r.client.setProcessEntryFunc(relayEntry, r.server)

	return &r, nil
}

// Start connects and syncs with master server then opens access to relay clients
func (r *StreamRelay) Start() error {
	// Start client side
	err := r.client.Start()
	if err != nil {
		log.Errorf("Error starting relay client: %v", err)
		return err
	}

	// Get total entries from the master server
	header, err := r.client.ExecCommandGetHeader()
	if err != nil {
		log.Errorf("Error executing header command: %v", err)
		return err
	}
	r.server.initEntry = header.TotalEntries

	// Start server side before exec command `CmdStart`
	err = r.server.Start()
	if err != nil {
		log.Errorf("Error starting relay server: %v", err)
		return err
	}

	// Sync with master server from latest received entry
	fromEntry := r.server.GetHeader().TotalEntries
	log.Infof("TotalEntries: RELAY %d of MASTER %d", fromEntry, r.server.initEntry)
	err = r.client.ExecCommandStart(fromEntry)
	if err != nil {
		log.Errorf("Error executing start command: %v", err)
		return err
	}

	return nil
}

// relayEntry relays the entry received as client to the clients connected to the server
func relayEntry(e *FileEntry, c *StreamClient, s *StreamServer) error {
	// Start atomic operation
	err := s.StartAtomicOp()
	if err != nil {
		log.Errorf("Error starting atomic op: %v", err)
		return err
	}

	// Add entry
	if e.Type == EtBookmark {
		_, err = s.AddStreamBookmark(e.Data)
	} else {
		_, err = s.AddStreamEntry(e.Type, e.Data)
	}

	// Check if error adding entry
	if err != nil {
		log.Errorf("Error adding entry: %v", err)

		// Rollback atomic operation
		err2 := s.RollbackAtomicOp()
		if err2 != nil {
			log.Errorf("Error rollbacking atomic op: %v", err2)
		}
		return err
	}

	// Commit atomic operation
	err = s.CommitAtomicOp()
	if err != nil {
		log.Errorf("Error committing atomic op: %v", err)
		return err
	}

	return nil
}

// Stop gracefully shuts down the relay server and client
func (r *StreamRelay) Stop() error {
	// Stop the client first
	if r.client != nil {
		err := r.client.ExecCommandStop()
		if err != nil {
			log.Errorf("Error stopping relay client: %v", err)
			return err
		}
		r.client.closeConnection()
	}

	// Stop the server
	if r.server != nil {
		// Close the listener to stop accepting new connections
		if r.server.ln != nil {
			err := r.server.ln.Close()
			if err != nil {
				log.Errorf("Error closing server listener: %v", err)
				return err
			}
		}

		// Kill all connected clients
		r.server.mutexClients.Lock()
		for clientID := range r.server.clients {
			r.server.killClient(clientID)
		}
		r.server.mutexClients.Unlock()

		// Mark server as stopped to prevent new atomic operations
		r.server.started = false

		// Close the stream channel to stop the broadcast goroutine
		if r.server.stream != nil {
			close(r.server.stream)
		}
	}

	log.Info("Relay server stopped successfully")
	return nil
}
