package reader

import (
	"fmt"
	"io"

	"github.com/sejvlond/go-kafkalog/common"
	"github.com/sejvlond/go-kafkalog/decoder"
	"github.com/sejvlond/go-kafkalog/reader/ioreader"
)

// kafkalog Writer interface
type KafkalogReader interface {
	io.Closer
	io.Seeker
	// Read will read value, key and kafkaOffset from kafkalog
	Read() (value, key []byte, kafkaOffset int64, err error)
}

// implementation of Reader
type reader struct {
	ioReader io.Reader
}

// New creates new kafkalog Reader with inner io.Reader for reading data
func New(ioReader io.Reader) (r KafkalogReader, err error) {
	r = &reader{
		ioReader: ioReader,
	}
	return
}

// Seek sets the offset for the next Read or Write to offset, interpreted
// according to whence:
//   0 means relative to the start of the file
//   1 means relative to the current offset
//   2 means relative to the end.
// Seek returns the new offset relative to the start of the file and an error,
// if any.
// If inner io.Redaer is not io.Seeker nothing happens.
func (this *reader) Seek(offset int64, whence int) (n int64, err error) {
	if ioSeeker, ok := this.ioReader.(io.Seeker); ok {
		return ioSeeker.Seek(offset, whence)
	}
	return
}

// Close closes the innerReader if it is io.Closer It returns an error, if any.
func (this *reader) Close() (err error) {
	if ioCloser, ok := this.ioReader.(io.Closer); ok {
		err = ioCloser.Close()
	}
	return
}

// rollback reading, returns default values
func (this *reader) rollback(n int, err error) ([]byte, []byte, int64, error) {
	_, e := this.Seek(int64(-n), 1)
	if e != nil {
		err = fmt.Errorf("Reading error(%q); Rollback error(%q)", err, e)
	}
	return nil, nil, 0, err
}

// read reads exactly len(buffer) bytes. Returns error when less bytes is read
func (this *reader) ioRead(buffer []byte) (n int, err error) {
	n, err = this.ioReader.Read(buffer)
	if err != nil {
		return
	}
	if n != len(buffer) {
		err = fmt.Errorf("Reading failed. Expected %v bytes but %v was read",
			len(buffer), n)
	}
	return
}

// Read reads data from kafkalog file format.
// It returns an error, if any.
func (this *reader) Read() (value, key []byte, offset int64, err error) {
	// read header
	buffer := make([]byte, common.MSGSET_HEADER_SIZE)
	n, err := this.ioRead(buffer)
	if err != nil {
		return this.rollback(n, err)
	}
	// parse header
	_, msglen, err := common.ParseMessageSetHeader(buffer)
	if err != nil {
		return this.rollback(n, err)
	}
	// realloc buffer
	tmpBuffer := make([]byte, msglen+len(buffer))
	copy(tmpBuffer, buffer)
	buffer = tmpBuffer
	// read message
	nn, err := this.ioRead(buffer[common.MSGSET_HEADER_SIZE:])
	n += nn
	if err != nil {
		return this.rollback(n, err)
	}
	// decode message
	value, key, offset, err = decoder.Decode(buffer)
	if err != nil {
		return this.rollback(n, err)
	}
	return
}

// ------ helper constructors for custom ioreaders ----------------------------
func NewFile(path string) (_ KafkalogReader, err error) {
	r, err := ioreader.NewFile(path)
	if err != nil {
		return
	}
	return New(r)
}
