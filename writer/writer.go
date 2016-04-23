package writer

import (
	"errors"
	"io"

	"github.com/sejvlond/go-kafkalog/encoder"
	"github.com/sejvlond/go-kafkalog/writer/iowriter"
)

// kafkalog Writer interface
type KafkalogWriter interface {
	io.Closer
	// Write will write value, key and kafkaOffset to kafkalog
	// key can be nil
	Write(value, key []byte, kafkaOffset int64) (int, error)
}

// implementation of Writer
type writer struct {
	encoder  encoder.Encoder
	ioWriter io.Writer
}

// New creates new kafkalog Writer with inner io.Writer for saving messages
func New(ioWriter io.Writer, compression uint8) (w KafkalogWriter, err error) {
	encoder, err := encoder.New(compression)
	if err != nil {
		return
	}
	w = &writer{
		encoder:  encoder,
		ioWriter: ioWriter,
	}
	return
}

// Close closes the innerWriter if it is io.Closer It returns an error, if any.
func (this *writer) Close() (err error) {
	if ioCloser, ok := this.ioWriter.(io.Closer); ok {
		err = ioCloser.Close()
	}
	return
}

// Write writes value into the kafkalog file format.
// It returns an error, if any.
func (this *writer) Write(value, key []byte, kafkaOffset int64) (
	n int, err error) {

	if value == nil {
		return n, errors.New("Value can not be nil")
	}
	bytes, err := this.encoder.Encode(nil, value, key, kafkaOffset)
	if err != nil {
		return
	}
	return this.ioWriter.Write(bytes)
}

// ------ helper constructors for custom iowriters ----------------------------

// NewFile returns a Writer which writes everything into single file
func NewFile(path string, compression uint8) (_ KafkalogWriter, err error) {
	ioWriter, err := iowriter.NewFile(path)
	if err != nil {
		return
	}
	return New(ioWriter, compression)
}

// NewRotate returns a Writer which writes everything into files and rotate
// them when needed
func NewRotate(name string, interval uint, dir string, compression uint8) (
	_ KafkalogWriter, err error) {

	ioWriter, err := iowriter.NewRotate(name, interval, dir)
	if err != nil {
		return
	}
	return New(ioWriter, compression)
}
