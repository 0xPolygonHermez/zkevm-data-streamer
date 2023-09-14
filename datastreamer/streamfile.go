package datastreamer

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/0xPolygonHermez/zkevm-data-streamer/log"
)

const (
	// File config
	headerSize     = 29          // Header data size
	pageHeaderSize = 4096        // 4K size header page
	pageDataSize   = 1024 * 1024 // 1 MB size data page
	initPages      = 80          // Initial number of data pages
	nextPages      = 8           // Number of data pages to add when file is full

	// Packet types
	PtPadding = 0
	PtHeader  = 1    // Just for the header page
	PtData    = 2    // Data entry
	PtResult  = 0xff // Not stored/present in file (just for client command result)

	// Sizes
	FixedSizeFileEntry   = 17 // 1+4+4+8
	FixedSizeResultEntry = 9  // 1+4+4
)

type HeaderEntry struct {
	packetType   uint8      // 1:Header
	headLength   uint32     // 29
	streamType   StreamType // 1:Sequencer
	TotalLength  uint64     // Total bytes used in the file
	TotalEntries uint64     // Total number of data entries (entry type 2)
}

type FileEntry struct {
	packetType uint8     // 2:Data entry, 0:Padding, (1:Header)
	length     uint32    // Length of the entry
	entryType  EntryType // e.g. 1:L2 block, 2:L2 tx,...
	entryNum   uint64    // Entry number (sequential starting with 0)
	data       []byte
}

type StreamFile struct {
	fileName   string
	pageSize   uint32 // Data page size in bytes
	file       *os.File
	streamType StreamType
	maxLength  uint64 // File size in bytes

	header      HeaderEntry // Current header in memory (atomic operation in progress)
	writtenHead HeaderEntry // Current header written in the file
}

type iteratorFile struct {
	fromEntry uint64
	file      *os.File
	entry     FileEntry
}

func PrepareStreamFile(fn string, st StreamType) (StreamFile, error) {
	sf := StreamFile{
		fileName:   fn,
		pageSize:   pageDataSize,
		file:       nil,
		streamType: st,
		maxLength:  0,

		header: HeaderEntry{
			packetType:   PtHeader,
			headLength:   headerSize,
			streamType:   st,
			TotalLength:  0,
			TotalEntries: 0,
		},
	}

	// Open (or create) the data stream file
	err := sf.openCreateFile()

	printStreamFile(sf)

	return sf, err
}

func (f *StreamFile) openCreateFile() error {
	// Check if file exists (otherwise create it)
	_, err := os.Stat(f.fileName)

	if os.IsNotExist(err) {
		// File does not exists so create it
		log.Infof("Creating new file for datastream: %s", f.fileName)
		f.file, err = os.Create(f.fileName)

		if err != nil {
			log.Errorf("Error creating datastream file %s: %v", f.fileName, err)
		} else {
			err = f.initializeFile()
		}

	} else if err == nil {
		// File already exists
		log.Infof("Using existing file for datastream: %s", f.fileName)
		f.file, err = os.OpenFile(f.fileName, os.O_RDWR, 0666)
		if err != nil {
			log.Errorf("Error opening datastream file %s: %v", f.fileName, err)
		}
	} else {
		log.Errorf("Unable to check datastream file status %s: %v", f.fileName, err)
	}

	if err != nil {
		return err
	}

	// Max length of the file
	info, err := f.file.Stat()
	if err != nil {
		return err
	}
	f.maxLength = uint64(info.Size())

	// Check file consistency
	err = f.checkFileConsistency()
	if err != nil {
		return err
	}

	// Restore header from the file and check it
	err = f.readHeaderEntry()
	if err != nil {
		return err
	}
	err = f.checkHeaderConsistency()
	if err != nil {
		return err
	}

	return nil
}

func (f *StreamFile) initializeFile() error {
	// Create the header page
	err := f.createHeaderPage()
	if err != nil {
		return err
	}

	// Create initial data pages
	for i := 1; i <= initPages; i++ {
		err = f.createPage(f.pageSize)
		if err != nil {
			log.Error("Eror creating page")
			return err
		}
	}

	return err
}

func (f *StreamFile) createHeaderPage() error {
	// Create the header page (first page) of the file
	err := f.createPage(pageHeaderSize)
	if err != nil {
		log.Errorf("Error creating the header page: %v", err)
		return err
	}

	// Update total data length and max file length
	f.maxLength = f.maxLength + pageHeaderSize
	f.header.TotalLength = pageHeaderSize

	// Write header entry
	err = f.writeHeaderEntry()
	return err
}

// Create/add a new page on the stream file
func (f *StreamFile) createPage(size uint32) error {
	page := make([]byte, size)

	// Position at the end of the file
	_, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Errorf("Error seeking the end of the file: %v", err)
		return err
	}

	// Write the page
	_, err = f.file.Write(page)
	if err != nil {
		log.Errorf("Error writing a new page: %v", err)
		return err
	}

	// Flush
	err = f.file.Sync()
	if err != nil {
		log.Errorf("Error flushing new page to disk: %v", err)
		return err
	}

	// Update max file length
	f.maxLength = f.maxLength + uint64(size)

	return nil
}

func (f *StreamFile) extendFile() error {
	// Add data pages
	var err error = nil
	for i := 1; i <= nextPages; i++ {
		err = f.createPage(f.pageSize)
		if err != nil {
			log.Error("Error adding page")
			return err
		}
	}
	return err
}

// Read header from file to restore the header struct
func (f *StreamFile) readHeaderEntry() error {
	// Position at the beginning of the file
	_, err := f.file.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking the start of the file: %v", err)
		return err
	}

	// Read header stream bytes
	binaryHeader := make([]byte, headerSize)
	n, err := f.file.Read(binaryHeader)
	if err != nil {
		log.Errorf("Error reading the header: %v", err)
		return err
	}
	if n != headerSize {
		log.Error("Error getting header info")
		return errors.New("error getting header info")
	}

	// Convert to header struct
	f.header, err = decodeBinaryToHeaderEntry(binaryHeader)
	if err != nil {
		log.Error("Error decoding binary header")
		return err
	}

	// Current written header in file
	f.writtenHead = f.header
	return nil
}

func (f *StreamFile) getHeaderEntry() HeaderEntry {
	return f.writtenHead
}

func printHeaderEntry(e HeaderEntry) {
	log.Debug("--- HEADER ENTRY -------------------------")
	log.Debugf("packetType: [%d]", e.packetType)
	log.Debugf("headerLength: [%d]", e.headLength)
	log.Debugf("streamType: [%d]", e.streamType)
	log.Debugf("totalLength: [%d]", e.TotalLength)
	log.Debugf("totalEntries: [%d]", e.TotalEntries)
}

// Write the memory header struct into the file header
func (f *StreamFile) writeHeaderEntry() error {
	// Position at the beginning of the file
	_, err := f.file.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking the start of the file: %v", err)
		return err
	}

	// Write after convert header struct to binary stream
	binaryHeader := encodeHeaderEntryToBinary(f.header)
	log.Debugf("writing header entry: %v", binaryHeader)
	_, err = f.file.Write(binaryHeader)
	if err != nil {
		log.Errorf("Error writing the header: %v", err)
		return err
	}
	err = f.file.Sync()
	if err != nil {
		log.Errorf("Error flushing header data to disk: %v", err)
		return err
	}

	// Update the written header
	f.writtenHead = f.header
	return nil
}

// Encode/convert from a header entry type to binary bytes slice
func encodeHeaderEntryToBinary(e HeaderEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.headLength)
	be = binary.BigEndian.AppendUint64(be, uint64(e.streamType))
	be = binary.BigEndian.AppendUint64(be, e.TotalLength)
	be = binary.BigEndian.AppendUint64(be, e.TotalEntries)
	return be
}

// Decode/convert from binary bytes slice to a header entry type
func decodeBinaryToHeaderEntry(b []byte) (HeaderEntry, error) {
	e := HeaderEntry{}

	if len(b) != headerSize {
		log.Error("Invalid binary header entry")
		return e, errors.New("invalid binary header entry")
	}

	e.packetType = b[0]
	e.headLength = binary.BigEndian.Uint32(b[1:5])
	e.streamType = StreamType(binary.BigEndian.Uint64(b[5:13]))
	e.TotalLength = binary.BigEndian.Uint64(b[13:21])
	e.TotalEntries = binary.BigEndian.Uint64(b[21:29])

	return e, nil
}

func encodeFileEntryToBinary(e FileEntry) []byte {
	be := make([]byte, 1)
	be[0] = e.packetType
	be = binary.BigEndian.AppendUint32(be, e.length)
	be = binary.BigEndian.AppendUint32(be, uint32(e.entryType))
	be = binary.BigEndian.AppendUint64(be, e.entryNum)
	be = append(be, e.data...)
	return be
}

func (f *StreamFile) checkFileConsistency() error {
	// Get file info
	info, err := os.Stat(f.fileName)
	if err != nil {
		log.Error("Error checking file consistency")
		return err
	}

	// Check header page is present
	if info.Size() < pageHeaderSize {
		log.Error("Invalid file: missing header page")
		return errors.New("invalid file missing header page")
	}

	// Check data pages are not cut
	dataSize := info.Size() - pageHeaderSize
	uncut := dataSize % int64(f.pageSize)
	if uncut != 0 {
		log.Error("Inconsistent file size there is a cut data page")
		return errors.New("bad file size cut data page")
	}

	return nil
}

func (f *StreamFile) checkHeaderConsistency() error {
	var err error = nil

	if f.header.packetType != PtHeader {
		log.Error("Invalid header: bad packet type")
		err = errors.New("invalid header bad packet type")
	} else if f.header.headLength != headerSize {
		log.Error("Invalid header: bad header length")
		err = errors.New("invalid header bad header length")
	} else if f.header.streamType != f.streamType {
		log.Error("Invalid header: bad stream type")
		err = errors.New("invalid header bad stream type")
	}

	return err
}

// Write new data entry to the data stream file
func (f *StreamFile) AddFileEntry(e FileEntry) error {
	// Set the file position to write
	_, err := f.file.Seek(int64(f.header.TotalLength), io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking position to write: %v", err)
		return err
	}

	// Convert from data struct to bytes stream
	be := encodeFileEntryToBinary(e)

	// Check if the entry fits on current page
	entryLength := uint64(len(be))
	pageRemaining := pageDataSize - (f.header.TotalLength-pageHeaderSize)%pageDataSize
	if entryLength > pageRemaining {
		log.Debugf(">> Fill with pad entries. PageRemaining:%d, EntryLength:%d", pageRemaining, entryLength)
		err = f.fillPagePadEntries()
		if err != nil {
			return err
		}

		// Check if file is full
		if f.header.TotalLength == f.maxLength {
			// Add new data pages to the file
			log.Infof(">> FULL FILE (TotalLength: %d) -> extending!", f.header.TotalLength)
			err = f.extendFile()
			if err != nil {
				return err
			}

			log.Infof(">> New file max length: %d", f.maxLength)

			// Re-set the file position to write
			_, err = f.file.Seek(int64(f.header.TotalLength), io.SeekStart)
			if err != nil {
				log.Errorf("Error seeking position to write after file extend: %v", err)
				return err
			}
		}
	}

	// Write the data entry
	_, err = f.file.Write(be)
	if err != nil {
		log.Errorf("Error writing the entry: %v", err)
		return err
	}

	// Flush data to disk
	err = f.file.Sync()
	if err != nil {
		log.Errorf("Error flushing new entry to disk: %v", err)
		return err
	}

	// Update the current header in memory (on disk later when the commit arrives)
	f.header.TotalLength = f.header.TotalLength + entryLength
	f.header.TotalEntries = f.header.TotalEntries + 1

	// printHeaderEntry(f.header)
	return nil
}

// Fill remaining free space on the current data page with pad
func (f *StreamFile) fillPagePadEntries() error {
	// Set the file position to write
	_, err := f.file.Seek(int64(f.header.TotalLength), io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking fill pads position to write: %v", err)
		return err
	}

	// Page remaining free space
	pageRemaining := pageDataSize - (f.header.TotalLength-pageHeaderSize)%pageDataSize

	if pageRemaining > 0 {
		// Pad entries
		entries := make([]byte, pageRemaining)
		for i := 0; i < int(pageRemaining); i++ {
			entries[i] = 0
		}

		// Write pad entries
		_, err = f.file.Write(entries)
		if err != nil {
			log.Errorf("Error writing pad entries: %v", err)
			return err
		}

		// Sync/flush to disk will be done outside this function

		// Update the current header in memory (on disk later when the commit arrives)
		f.header.TotalLength = f.header.TotalLength + pageRemaining
	}

	return nil
}

func printStreamFile(f StreamFile) {
	log.Debug("--- STREAM FILE --------------------------")
	log.Debugf("fileName: [%s]", f.fileName)
	log.Debugf("pageSize: [%d]", f.pageSize)
	log.Debugf("streamType: [%d]", f.streamType)
	log.Debugf("maxLength: [%d]", f.maxLength)
	printHeaderEntry(f.header)
}

// Decode/convert from binary bytes slice to file entry type
func DecodeBinaryToFileEntry(b []byte) (FileEntry, error) {
	d := FileEntry{}

	if len(b) < FixedSizeFileEntry {
		log.Error("Invalid binary data entry")
		return d, errors.New("invalid binary data entry")
	}

	d.packetType = b[0]
	d.length = binary.BigEndian.Uint32(b[1:5])
	d.entryType = EntryType(binary.BigEndian.Uint32(b[5:9]))
	d.entryNum = binary.BigEndian.Uint64(b[9:17])
	d.data = b[17:]

	if uint32(len(d.data)) != d.length-FixedSizeFileEntry {
		log.Error("Error decoding binary data entry")
		return d, errors.New("error decoding binary data entry")
	}

	return d, nil
}

func (f *StreamFile) iteratorFrom(entryNum uint64) (*iteratorFile, error) {
	// Open file for read only
	file, err := os.OpenFile(f.fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Errorf("Error opening file for iterator: %v", err)
		return nil, err
	}

	// Set the file position to first data page
	_, err = file.Seek(pageHeaderSize, io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking file for iterator: %v", err)
		return nil, err
	}

	// Create iterator struct
	iterator := iteratorFile{
		fromEntry: entryNum,
		file:      file,
		entry: FileEntry{
			entryNum: 0,
		},
	}

	// TODO: Locate file start streaming point using dichotomic search
	// f.seekEntry(&iterator)
	if entryNum > 0 {
		for {
			end, err := f.iteratorNext(&iterator)
			if err != nil {
				return nil, err
			}

			if end {
				break
			}

			if iterator.entry.entryNum+1 == entryNum {
				break
			}
		}
	}

	return &iterator, err
}

func (f *StreamFile) iteratorNext(iterator *iteratorFile) (bool, error) {
	// Check end of entries condition
	if iterator.entry.entryNum == f.writtenHead.TotalEntries {
		return true, nil
	}

	// Read just the packet type
	packet := make([]byte, 1)
	_, err := iterator.file.Read(packet)
	if err != nil {
		log.Errorf("Error reading packet type for iterator: %v", err)
		return true, err
	}

	// Check if it is of type pad, if so forward to next data page on the file
	if packet[0] == PtPadding {
		// Current file position
		pos, err := iterator.file.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Errorf("Error seeking current pos for iterator: %v", err)
			return true, err
		}

		// Bytes to forward until next data page
		forward := pageDataSize - ((pos - pageHeaderSize) % pageDataSize)

		// Check end of data pages condition
		if pos+forward >= int64(f.writtenHead.TotalLength) {
			return true, nil
		}

		// Seek for the start of next data page
		_, err = iterator.file.Seek(forward, io.SeekCurrent)
		if err != nil {
			log.Errorf("Error seeking next page for iterator: %v", err)
			return true, err
		}

		// Read the new packet type
		_, err = iterator.file.Read(packet)
		if err != nil {
			log.Errorf("Error reading new packet type for iterator: %v", err)
			return true, err
		}

		// Should be of type data
		if packet[0] != PtData {
			log.Errorf("Error data page not starting with packet of type data. Type: %d", packet[0])
			return true, errors.New("page not starting with entry data")
		}
	}

	// Read the rest of fixed data entry bytes
	buffer := make([]byte, FixedSizeFileEntry-1)
	_, err = iterator.file.Read(buffer)
	if err != nil {
		log.Errorf("Error reading entry for iterator: %v", err)
		return true, err
	}
	buffer = append(packet, buffer...)

	// Read variable data
	length := binary.BigEndian.Uint32(buffer[1:5])
	bufferAux := make([]byte, length-FixedSizeFileEntry)
	_, err = iterator.file.Read(bufferAux)
	if err != nil {
		log.Errorf("Error reading data for iterator: %v", err)
		return true, err
	}
	buffer = append(buffer, bufferAux...)

	// Convert to data entry struct
	iterator.entry, err = DecodeBinaryToFileEntry(buffer)
	if err != nil {
		log.Errorf("Error decoding entry for iterator: %v", err)
		return true, err
	}

	return false, nil
}

func (f *StreamFile) iteratorEnd(iterator *iteratorFile) {
	iterator.file.Close()
}

// func (f *StreamFile) seekEntry(iterator *iteratorFile) error {
// 	// Start and end data page
// 	avg := 0
// 	beg := 1
// 	end := int((f.writtenHead.TotalLength - pageHeaderSize) / pageDataSize)
// 	if (f.writtenHead.TotalLength-pageHeaderSize)%pageDataSize != 0 {
// 		end = end + 1
// 	}

// 	// Custom binary search
// 	for beg <= end {
// 		avg = beg + (end-beg)/2

// 		// Seek for the start of avg data page
// 		newPos := (avg * pageDataSize) + pageHeaderSize
// 		_, err := iterator.file.Seek(int64(newPos), io.SeekStart)
// 		if err != nil {
// 			log.Errorf("Error seeking page for iterator seek entry: %v", err)
// 			return err
// 		}

// 		// Read fixed data entry bytes
// 		buffer := make([]byte, FixedSizeFileEntry)
// 		_, err = iterator.file.Read(buffer)
// 		if err != nil {
// 			log.Errorf("Error reading entry for iterator seek entry: %v", err)
// 			return err
// 		}

// 		// Decode packet type
// 		packetType := buffer[0]
// 		if packetType != PtData {
// 			log.Errorf("Error data page (%d) not starting with packet of type data. Type: %d", avg, packetType)
// 			return errors.New("page not starting with entry data")
// 		}

// 		// Decode entry number and compare with target value
// 		entryNum := binary.BigEndian.Uint64(buffer[9:17])
// 		if entryNum == iterator.fromEntry {
// 			break
// 		} else if entryNum > iterator.fromEntry {
// 			end = avg - 1
// 		} else if entryNum < iterator.fromEntry {
// 			beg = avg
// 		} else if beg == end {
// 			break
// 		}
// 	}

// 	// Back to the start of the current data page (where the entry number is present)
// 	_, err := iterator.file.Seek(-FixedSizeFileEntry, io.SeekCurrent)
// 	if err != nil {
// 		log.Errorf("Error seeking page for iterator seek entry: %v", err)
// 		return err
// 	}

// 	log.Debugf("Entry number %d is in the data page %d", iterator.fromEntry, avg)
// 	return nil
// }
