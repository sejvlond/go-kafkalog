package decoder

import (
	"github.com/golang/snappy"
	"github.com/sejvlond/go-kafkalog/common"
)

// Snappy Decoder
type SnappyDecoder struct{}

// NewPlain returns PlainDecoder for plain text
func NewSnappy() Decoder {
	return new(SnappyDecoder)
}

// Decode Message Set to parts
func (this *SnappyDecoder) Decode(msgset *common.MessageSet) (
	value, key []byte, offset int64, err error) {

	plainBytes, err := snappy.Decode(nil, msgset.Message.Value)
	if err != nil {
		return
	}
	return Decode(plainBytes)
}
