package encoder

import (
	"github.com/stretchr/testify/assert"
	"github.com/sejvlond/go-kafkalog/common"

	"bytes"
	"compress/zlib"
	"io"
	"testing"
)

func TestGzip(t *testing.T) {
	value := []byte("value")
	key := []byte("key")
	offset := int64(4)

	enc, err := NewGzip()
	assert.Nil(t, err)
	result, err := enc.Encode(nil, value, key, offset)
	assert.Nil(t, err)
	assert.Equal(t, result[17]&common.COMPRESS_MASK, common.COMPRESS_GZIP)

	gzipValue := bytes.NewReader(result[26:])
	gzipReader, err := zlib.NewReader(gzipValue)
	assert.Nil(t, err)
	plainValue := new(bytes.Buffer)
	io.Copy(plainValue, gzipReader)
	assert.Nil(t, err)

	plainBytes := plainValue.Bytes()
	assert.Equal(t, len(plainBytes), 26+len(key)+len(value))
	assert.Equal(t, plainBytes[22:22+len(key)], key)
	assert.Equal(t, plainBytes[26+len(key):], value)
}
