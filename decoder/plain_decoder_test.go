package decoder

import (
	"github.com/stretchr/testify/assert"
	"github.com/sejvlond/go-kafkalog/common"

	"testing"
)

func TestPlain(t *testing.T) {
	data := []byte("\x00\x00\x00\x00\x00\x00\x00\x2d\x00\x00\x00\x19\xb7\x65\x9c\xd3\x00\x00\x00\x00\x00\x04\x6b\x6c\x69\x63\x00\x00\x00\x07\x68\x6f\x64\x6e\x6f\x74\x61")

	msgset, err := common.ParseMessageSet(data)
	assert.Nil(t, err)
	assert.Equal(t, msgset.Message.GetCompression(), common.COMPRESS_PLAIN)

	decoder := NewPlain()
	v, k, o, err := decoder.Decode(msgset)
	assert.Nil(t, err)
	assert.Equal(t, o, int64(45))
	assert.Equal(t, v, []byte("hodnota"))
	assert.Equal(t, k, []byte("klic"))
}
