package encoder

import (
	"github.com/golang/snappy"
	"github.com/sejvlond/go-kafkalog/common"

	"io"
)

// Snappy Encoder
type SnappyEncoder struct{}

// NewSnappy returns snappyEncoder for snappy compression
func NewSnappy() (Encoder, error) {
	return new(SnappyEncoder), nil
}

// Encode returns []byte with snappy compression
func (this *SnappyEncoder) Encode(dst, value, key []byte, offset int64) (
	_ []byte, err error) {

	plainEnc, err := NewPlain()
	if err != nil {
		return
	}
	plainValue, err := plainEnc.Encode(nil, value, key, offset)
	if err != nil {
		return
	}
	snappyValue := snappy.Encode(nil, plainValue)
	msg := common.Message{Value: snappyValue}
	msg.SetCompression(common.COMPRESS_SNAPPY)
	set := common.MessageSet{Message: msg}
	dst, err = set.Marshal(dst)
	if err == io.ErrShortBuffer {
		dst, err = set.Bytes(), nil
	}
	return dst, err
}
