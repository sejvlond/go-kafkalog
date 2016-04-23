package encoder

import (
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/sejvlond/go-kafkalog/common"

	"testing"
)

func TestSnappy(t *testing.T) {
	value := []byte("value")
	key := []byte("key")
	offset := int64(4)

	enc, err := NewSnappy()
	assert.Nil(t, err)
	result, err := enc.Encode(nil, value, key, offset)
	assert.Nil(t, err)
	assert.Equal(t, result[17]&common.COMPRESS_MASK, common.COMPRESS_SNAPPY)

	plainBytes, err := snappy.Decode(nil, result[26:])
	assert.Nil(t, err)

	assert.Equal(t, len(plainBytes), 26+len(key)+len(value))
	assert.Equal(t, plainBytes[22:22+len(key)], key)
	assert.Equal(t, plainBytes[26+len(key):], value)
}
