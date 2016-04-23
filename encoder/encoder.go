package encoder

import (
	"github.com/sejvlond/go-kafkalog/common"

	"errors"
)

// Encoder interface
type Encoder interface {
	// Encode will encode value and key to message and wraps it with offset
	// to messageSet. Each encode depends on its implementation
	Encode(dst, value, key []byte, offset int64) ([]byte, error)
}

// New returns Encoder for compression type
func New(compression byte) (Encoder, error) {
	switch compression {
	case common.COMPRESS_PLAIN:
		return NewPlain()
	case common.COMPRESS_GZIP:
		return NewGzip()
	case common.COMPRESS_SNAPPY:
		return NewSnappy()
	}
	return nil, errors.New("I don't know this type of compression")
}
