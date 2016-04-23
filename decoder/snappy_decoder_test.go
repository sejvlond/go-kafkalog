package decoder

import (
	"github.com/stretchr/testify/assert"
	"github.com/sejvlond/go-kafkalog/common"

	"testing"
)

func TestSnappy(t *testing.T) {
	data := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x30\x09\x02\xbf\x4b\x00\x02\xff\xff\xff\xff\x00\x00\x00\x22\x25\x00\x00\x09\x01\x20\x2d\x00\x00\x00\x19\xb7\x65\x9c\xd3\x05\x0f\x3c\x04\x6b\x6c\x69\x63\x00\x00\x00\x07\x68\x6f\x64\x6e\x6f\x74\x61")
	msgset, err := common.ParseMessageSet(data)
	assert.Nil(t, err)
	assert.Equal(t, msgset.Message.GetCompression(), common.COMPRESS_SNAPPY)

	decoder := NewSnappy()
	v, k, o, err := decoder.Decode(msgset)
	assert.Nil(t, err)
	assert.Equal(t, o, int64(45))
	assert.Equal(t, v, []byte("hodnota"))
	assert.Equal(t, k, []byte("klic"))
}
