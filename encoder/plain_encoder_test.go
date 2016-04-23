package encoder

import (
	"github.com/stretchr/testify/assert"
	"github.com/sejvlond/go-kafkalog/common"

	"testing"
)

func TestPlain(t *testing.T) {
	value := []byte("value")
	key := []byte("key")
	offset := int64(4)

	enc, err := NewPlain()
	assert.Nil(t, err)
	result, err := enc.Encode(nil, value, key, offset)
	assert.Nil(t, err)
	assert.Equal(t, result[17]&common.COMPRESS_MASK, common.COMPRESS_PLAIN)

	assert.Equal(t, len(result), 26+len(key)+len(value))
	assert.Equal(t, result[22:22+len(key)], key)
	assert.Equal(t, result[26+len(key):], value)
}
