package decoder

import (
	"github.com/sejvlond/go-kafkalog/common"
)

// Plain Decoder
type PlainDecoder struct{}

// NewPlain returns PlainDecoder for plain text
func NewPlain() Decoder {
	return new(PlainDecoder)
}

// Decode Message Set to parts
func (this *PlainDecoder) Decode(msgset *common.MessageSet) (
	value, key []byte, offset int64, err error) {

	return msgset.Message.Value, msgset.Message.Key, msgset.Offset, nil
}
