package decoder

import (
	"github.com/stretchr/testify/assert"
	"github.com/sejvlond/go-kafkalog/common"

	"testing"
)

func TestGzip(t *testing.T) {
	data := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x35\xde\x8e\x7a\xe7\x00\x01\xff\xff\xff\xff\x00\x00\x00\x27\x78\x9c\x62\x80\x00\x5d\x20\x96\xdc\x9e\x3a\xe7\x32\x98\xc7\x92\x9d\x93\x99\x0c\xa4\xd9\x33\xf2\x53\xf2\xf2\x4b\x12\x01\x01\x00\x00\xff\xff\x65\xc4\x07\x6d")

	msgset, err := common.ParseMessageSet(data)
	assert.Nil(t, err)
	assert.Equal(t, msgset.Message.GetCompression(), common.COMPRESS_GZIP)

	decoder := NewGzip()
	v, k, o, err := decoder.Decode(msgset)
	assert.Nil(t, err)
	assert.Equal(t, o, int64(45))
	assert.Equal(t, v, []byte("hodnota"))
	assert.Equal(t, k, []byte("klic"))
}
