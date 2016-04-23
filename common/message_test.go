package common

import (
	"github.com/stretchr/testify/assert"

	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestMsg(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	msg := Message{Key: key, Value: value}
	msg.SetCompression(COMPRESS_GZIP)
	msgBytes := msg.Bytes()

	assert.Equal(t, len(msgBytes), MSG_BASE_SIZE+len(key)+len(value))
	assert.Equal(t, msgBytes[MSG_MAGIC_START], byte(msg_MAGIC_VALUE)) // magic
	assert.Equal(t, msgBytes[MSG_ATTRS_START]&COMPRESS_GZIP, COMPRESS_GZIP)
	assert.Equal(t, binary.BigEndian.Uint32(
		msgBytes[MSG_KEY_SIZE_START:MSG_KEY_SIZE_END]),
		uint32(len(key)))
	assert.Equal(t, msgBytes[MSG_KEY_START:MSG_KEY_START+len(key)], key)
	assert.Equal(t, binary.BigEndian.Uint32(
		msgBytes[MSG_VALUE_SIZE_START+len(key):MSG_VALUE_SIZE_END+len(key)]),
		uint32(len(value)))
	assert.Equal(t, msgBytes[MSG_VALUE_START+len(key):], value)

	crc := crc32.ChecksumIEEE(msgBytes[MSG_CRC_END:])
	assert.Equal(t, binary.BigEndian.Uint32(
		msgBytes[MSG_CRC_START:MSG_CRC_END]), uint32(crc))
}

func TestMsgEmptyKey(t *testing.T) {
	value := []byte("value")
	key := make([]byte, 0, 0) // empty key
	msg := Message{Value: value, Key: key}
	msgBytes := msg.Bytes()

	assert.Equal(t, len(msgBytes), MSG_BASE_SIZE+len(value))
	assert.Equal(t, int32(binary.BigEndian.Uint32(
		msgBytes[MSG_KEY_SIZE_START:MSG_KEY_SIZE_END])), int32(0))
	assert.Equal(t, msgBytes[MSG_VALUE_START:], value)
}

func TestMsgNilKey(t *testing.T) {
	value := []byte("value")
	msg := Message{Value: value, Key: nil} // nil key
	msgBytes := msg.Bytes()

	assert.Equal(t, len(msgBytes), MSG_BASE_SIZE+len(value))
	assert.Equal(t, int32(binary.BigEndian.Uint32(
		msgBytes[MSG_KEY_SIZE_START:MSG_KEY_SIZE_END])), int32(-1))
	assert.Equal(t, msgBytes[MSG_VALUE_START:], value)
}

func TestMsgEmptyMsg(t *testing.T) {
	value := make([]byte, 0, 0) // empty value
	msg := Message{Value: value}
	msgBytes := msg.Bytes()

	assert.Equal(t, len(msgBytes), MSG_BASE_SIZE)
	assert.Equal(t, int32(binary.BigEndian.Uint32(
		msgBytes[MSG_VALUE_SIZE_START:MSG_VALUE_SIZE_END])), int32(0))
}

func TestMsgNilMsg(t *testing.T) {
	msg := Message{Value: nil} // nil value
	msgBytes := msg.Bytes()

	assert.Equal(t, len(msgBytes), MSG_BASE_SIZE)
	assert.Equal(t, int32(binary.BigEndian.Uint32(
		msgBytes[MSG_VALUE_SIZE_START:MSG_VALUE_SIZE_END])), int32(-1))
}

func TestMsgParse(t *testing.T) {
	data := []byte("\xb7\x65\x9c\xd3\x00\x00\x00\x00\x00\x04\x6b\x6c\x69\x63\x00\x00\x00\x07\x68\x6f\x64\x6e\x6f\x74\x61")
	msg, err := ParseMessage(data)
	assert.Nil(t, err)
	assert.Equal(t, msg.Key, []byte("klic"))
	assert.Equal(t, msg.Value, []byte("hodnota"))
	assert.Equal(t, msg.attrs, byte(0))

	faildata := []byte("\xff\x65\x9c\xd3\x00\x00\x00\x00\x00\x04\x6b\x6c\x69\x63\x00\x00\x00\x07\x68\x6f\x64\x6e\x6f\x74\x61")
	msg, err = ParseMessage(faildata)
	assert.NotNil(t, err)
}

func TestMsgParseNilEmpty(t *testing.T) {
	data := []byte("\x79\x57\x48\xe0\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00")
	msg, err := ParseMessage(data)
	assert.Nil(t, err)
	assert.Nil(t, msg.Key)
	assert.NotEqual(t, msg.Key, []byte(""))
	assert.NotNil(t, msg.Value)
	assert.Equal(t, msg.Value, []byte(""))
}
