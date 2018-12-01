// Package ljh provides classes that read or write from the LJH x-ray pulse
// data file format.
package ljh

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/usnistgov/dastard/getbytes"
)

// PulseRecord is the interface for individual pulse records
type PulseRecord struct {
	TimeCode int64
	RowCount int64
	Pulse    []uint16
}

// VersionCode enumerates the LJH file version numbers.
type VersionCode int

// Enumeration for the LJH file version numbers
const (
	VersionInvalid VersionCode = iota
	Version2_1
	Version2_2
)

// Reader is the interface for reading an LJH file
type Reader struct {
	ChannelIndex    int
	Presamples      int
	Samples         int
	VersionNumber   VersionCode
	WordSize        int
	Timebase        float64
	TimestampOffset float64

	recordLength int
	headerLength int
	file         *os.File
	buffer       []byte
}

// Writer writes LJH2.2 files
type Writer struct {
	ChannelIndex              int
	Presamples                int
	Samples                   int
	FramesPerSample           int
	Timebase                  float64
	TimestampOffset           time.Time
	NumberOfRows              int
	NumberOfColumns           int
	NumberOfChans             int
	HeaderWritten             bool
	FileName                  string
	RecordsWritten            int
	DastardVersion            string
	GitHash                   string
	SourceName                string
	ChanName                  string
	ChannelNumberMatchingName int
	ColumnNum                 int
	RowNum                    int

	file   *os.File
	writer *bufio.Writer
}

// OpenReader returns an active LJH file reader, or an error.
func OpenReader(fileName string) (r *Reader, err error) {
	f, err := os.Open(fileName)
	if err != nil {
		return
	}

	r = &Reader{file: f}
	err = r.parseHeader()
	if err != nil {
		return
	}
	return r, nil
}

// Close closes the LJH file reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) setVersionNumber(line string) error {
	s := strings.TrimPrefix(line, "Save File Format Version: ")
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return fmt.Errorf("LJH file '%s': could not parse %d-part version number '%s'",
			r.file.Name(), len(parts), s)
	}
	if parts[0] != "2" {
		return fmt.Errorf("LJH file '%s' version number '%s' was not major version 2",
			r.file.Name(), s)
	}
	if parts[1] == "1" && parts[2] == "1" {
		r.VersionNumber = Version2_1
		r.recordLength = 6
		return nil
	}
	if parts[1] == "2" {
		r.VersionNumber = Version2_2
		r.recordLength = 16
		return nil
	}
	return fmt.Errorf("LJH file '%s': could not parse version number '%s' to valid value",
		r.file.Name(), s)
}

func extract(line, pattern string, i *int) bool {
	n, err := fmt.Sscanf(line, pattern, i)
	return n >= 1 && err != nil
}

func extractFloat(line, pattern string, f *float64) bool {
	n, err := fmt.Sscanf(line, pattern, f)
	return n >= 1 && err != nil
}

// CreateFile creates a file with filename .FileName and assigns it to .file
// you can't write records without doing this
func (w *Writer) CreateFile() error {
	if w.file == nil {
		file, err := os.Create(w.FileName)
		if err != nil {
			return err
		}
		w.file = file
	} else {
		return errors.New("file already exists")
	}
	w.writer = bufio.NewWriterSize(w.file, 32768)
	return nil
}

// WriteHeader writes a header to .file, returns err from WriteString
func (w *Writer) WriteHeader(firstRecord time.Time) error {
	starttime := w.TimestampOffset.Format("02 Jan 2006, 15:04:05 MST")
	timestamp := float64(w.TimestampOffset.UnixNano()) / 1e9
	firstrec := firstRecord.Format("02 Jan 2006, 15:04:05 MST")

	rowColText := fmt.Sprintf(`Number of rows: %d
Number of columns: %d
Row number (from 0-%d inclusive): %d
Column number (from 0-%d inclusive): %d`,
		w.NumberOfRows, w.NumberOfColumns,
		w.NumberOfRows-1, w.RowNum,
		w.NumberOfColumns-1, w.ColumnNum,
	)
	s := fmt.Sprintf(`#LJH Memorial File Format
Save File Format Version: 2.2.1
Software Version: DASTARD version %s
Software Git Hash: %s
Data source: %s
%s
Number of channels: %d
Channel name: %s
Channel: %d
ChannelIndex (in dastard): %d
Digitized Word Size In Bytes: 2
Presamples: %d
Total Samples: %d
Number of samples per point: %d
Timestamp offset (s): %.6f
Server Start Time: %s
First Record Time: %s
Timebase: %e
#End of Header
`, w.DastardVersion, w.GitHash, w.SourceName, rowColText, w.NumberOfChans,
		w.ChanName, w.ChannelNumberMatchingName, w.ChannelIndex, w.Presamples, w.Samples, w.FramesPerSample,
		timestamp, starttime, firstrec, w.Timebase,
	)
	_, err := w.writer.WriteString(s)
	w.HeaderWritten = true
	return err
}

// Flush flushes buffered data to disk
func (w Writer) Flush() {
	if w.writer != nil {
		w.writer.Flush()
	}
}

// Close closes the associated file, no more records can be written after this
func (w Writer) Close() {
	w.Flush()
	w.file.Close()
}

// WriteRecord writes a single record to the files
// timestamp should be a posix timestamp in microseconds
// rowcount is framecount*number_of_rows+row_number for TDM or TDM-like (eg CDM) data,
// rowcount=framecount for uMux data
// return error if data is wrong length (w.Samples is correct length)
func (w *Writer) WriteRecord(framecount int64, timestamp int64, data []uint16) error {
	if len(data) != w.Samples {
		return fmt.Errorf("ljh incorrect number of samples, have %v, want %v", len(data), w.Samples)
	}
	rowcount := framecount*int64(w.NumberOfRows) + int64(w.RowNum)
	if _, err := w.writer.Write(getbytes.FromInt64(rowcount)); err != nil {
		return err
	}
	if _, err := w.writer.Write(getbytes.FromInt64(timestamp)); err != nil {
		return err
	}
	if _, err := w.writer.Write(getbytes.FromSliceUint16(data)); err != nil {
		return err
	}
	w.RecordsWritten++
	return nil
}

// Writer3 writes LJH3.0 files
type Writer3 struct {
	ChannelIndex               int
	ChannelName                string
	ChannelNumberMatchingIndex int
	Timebase                   float64
	NumberOfRows               int
	NumberOfColumns            int
	Row                        int
	Column                     int
	HeaderWritten              bool
	FileName                   string
	RecordsWritten             int

	file   *os.File
	writer *bufio.Writer
}

// HeaderTDM contains info about TDM readout for placing in an LJH3 header
type HeaderTDM struct {
	NumberOfRows    int
	NumberOfColumns int
	Row             int
	Column          int
}

// Header is used to format the LJH3 json header
type Header struct {
	Frameperiod   float64   `json:"frameperiod"`
	Format        string    `json:"File Format"`
	FormatVersion string    `json:"File Format Version"`
	TDM           HeaderTDM `json:"TDM"`
}

// WriteHeader writes a header to the LJH3 file, return error if header already written
func (w *Writer3) WriteHeader() error {
	if w.HeaderWritten {
		return errors.New("header already written")
	}
	h := Header{Frameperiod: w.Timebase, Format: "LJH3", FormatVersion: "3.0.0",
		TDM: HeaderTDM{NumberOfRows: w.NumberOfRows, NumberOfColumns: w.NumberOfColumns,
			Row: w.Row, Column: w.Column}}
	s, err := json.MarshalIndent(h, "", "    ")
	if err != nil {
		panic("MarshallIndent error")
	}

	if _, err := w.writer.Write(s); err != nil {
		return err
	}

	if _, err := w.writer.WriteString("\n"); err != nil {
		return err
	}
	w.HeaderWritten = true
	return nil
}

// WriteRecord writes an LJH3 record
// firstRisingSample is the index in data of the sample after the pretrigger (zero or one indexed?)
// timestamp is posix timestamp in microseconds since epoch
// data can be variable length
func (w *Writer3) WriteRecord(firstRisingSample int32, framecount int64, timestamp int64, data []uint16) error {
	if _, err := w.writer.Write(getbytes.FromInt32(int32(len(data)))); err != nil {
		return err
	}
	if _, err := w.writer.Write(getbytes.FromInt32(firstRisingSample)); err != nil {
		return err
	}
	if _, err := w.writer.Write(getbytes.FromInt64(framecount)); err != nil {
		return err
	}
	if _, err := w.writer.Write(getbytes.FromInt64(timestamp)); err != nil {
		return err
	}
	if _, err := w.writer.Write(getbytes.FromSliceUint16(data)); err != nil {
		return err
	}
	w.RecordsWritten++
	return nil
}

// Flush flushes buffered data to disk
func (w Writer3) Flush() {
	if w.writer != nil {
		w.writer.Flush()
	}
}

// Close closes the LJH3 file
func (w Writer3) Close() {
	w.Flush()
	w.file.Close()
}

// CreateFile opens the LJH3 file for writing, must be called before wring RecordSlice
func (w *Writer3) CreateFile() error {
	if w.file != nil {
		return errors.New("file already exists")
	}
	file, err := os.Create(w.FileName)
	if err != nil {
		return err
	}
	w.file = file
	w.writer = bufio.NewWriterSize(w.file, 32768)
	return nil
}

func (r *Reader) parseHeader() error {
	scanner := bufio.NewScanner(r.file)
	lnum := 0
	textLength := 0
	endHeaderTag := "#End of Header"
header:
	for scanner.Scan() {
		line := scanner.Text()
		textLength += len(line)
		switch {
		case lnum == 0:
			firstLine := "#LJH Memorial File Format"
			if line != firstLine {
				return fmt.Errorf("File must begin with '%s'", firstLine)
			}
		case strings.Contains(line, "Save File Format Version:"):
			if err := r.setVersionNumber(line); err != nil {
				return err
			}

		case line == endHeaderTag:
			break header

		case extract(line, "Digitized Word Size in Bytes: %d", &r.WordSize):
		case extract(line, "Presamples: %d", &r.Presamples):
		case extract(line, "Total Samples: %d", &r.Samples):
		case extract(line, "Channel: %d", &r.ChannelIndex):
		case extract(line, "Channel: %d", &r.ChannelIndex):
		case extractFloat(line, "Timestamp offset (s): %f", &r.TimestampOffset):
		case extractFloat(line, "Timebase: %f", &r.Timebase):

		}
		lnum++
	}
	r.recordLength += r.WordSize * r.Samples

	// To find the header length is a big pain, because the bufio Reader and Scanner
	// outsmart us by not reporting the \r and/or \n line delimeters to us. We must
	// re-find the end-header line, then consume any \r and \n that follow it.
	// We can start at textLength, though, because that's a lower bound on the number of bytes
	// in the complete header.
	b := make([]byte, 1024) // Assume that # of bytes will cover all the missing newline chars.
	textLength -= len(endHeaderTag)
	r.file.ReadAt(b, int64(textLength))
	idx := strings.Index(string(b), endHeaderTag)
	if idx < 0 {
		return fmt.Errorf("Could not find '%s' in LJH file", endHeaderTag)
	}
	idx += len(endHeaderTag)
	// Consume all leading \r and/or \n bytes.
	for _, by := range b[idx:] {
		if by == '\n' || by == '\r' {
			idx++
		} else {
			break
		}
	}
	r.headerLength = textLength + idx
	r.file.Seek(int64(r.headerLength), 0) // rewind to end of header

	// log.Printf("Parsed %d lines in the header and %d bytes.\n", lnum, textLength)
	// log.Printf("%v\n", r)
	r.buffer = make([]byte, r.recordLength)
	return nil
}

// NextPulse returns the next pulse from the open LJH file
func (r *Reader) NextPulse() (*PulseRecord, error) {
	pr := new(PulseRecord)
	pr.Pulse = make([]uint16, r.Samples)
	if err := binary.Read(r.file, binary.LittleEndian, &pr.RowCount); err != nil {
		return nil, err
	}
	if err := binary.Read(r.file, binary.LittleEndian, &pr.TimeCode); err != nil {
		return nil, err
	}
	if err := binary.Read(r.file, binary.LittleEndian, pr.Pulse); err != nil {
		return nil, err
	}
	return pr, nil
}
