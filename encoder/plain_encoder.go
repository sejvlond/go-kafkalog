package encoder

import (
	"io"
	"github.com/sejvlond/go-kafkalog/common"
)

// Plain Encoder
type PlainEncoder struct{}

// NewPlain returns plainEncoder for plain text
func NewPlain() (Encoder, error) {
	return new(PlainEncoder), nil
}

// Encode returns []byte with plain text message
func (this *PlainEncoder) Encode(dst, value, key []byte, offset int64) (
	[]byte, error) {

	msg := common.Message{Key: key, Value: value}
	msg.SetCompression(common.COMPRESS_PLAIN)
	set := common.MessageSet{Offset: offset, Message: msg}
	dst, err := set.Marshal(dst)
	if err == io.ErrShortBuffer {
		dst, err = set.Bytes(), nil
	}
	return dst, err
}
