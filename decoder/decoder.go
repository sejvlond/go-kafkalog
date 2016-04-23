package decoder

import (
	"errors"

	"github.com/sejvlond/go-kafkalog/common"
)

func Decode(data []byte) (value, key []byte, offset int64, err error) {
	msgset, err := common.ParseMessageSet(data)
	if err != nil {
		return
	}
	decoder, err := New(msgset.Message.GetCompression())
	if err != nil {
		return
	}
	return decoder.Decode(msgset)
}

// Decoder interface
type Decoder interface {
	// Decode will decode value and key from message and offset from
	// messageSet. Each decode depends on its implementation
	Decode(*common.MessageSet) (value, key []byte, offset int64, err error)
}

// New returns Encoder for compression type
func New(compression byte) (Decoder, error) {
	switch compression {
	case common.COMPRESS_PLAIN:
		return NewPlain(), nil
	case common.COMPRESS_GZIP:
		return NewGzip(), nil
	case common.COMPRESS_SNAPPY:
		return NewSnappy(), nil
	}
	return nil, errors.New("I don't know this type of compression")
}
