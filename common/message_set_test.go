package common

import (
	"github.com/stretchr/testify/assert"

	"encoding/binary"
	"testing"
)

func TestMsgSet(t *testing.T) {
	msg := Message{}
	msgBytes := msg.Bytes()
	set := MessageSet{Message: msg, Offset: 1235468}
	setBytes := set.Bytes()

	assert.Equal(t, len(setBytes), MSGSET_OFFSET_LEN+MSGSET_MSG_SIZE_LEN+
		len(msgBytes))
	assert.Equal(t, binary.BigEndian.Uint64(
		setBytes[MSGSET_OFFSET_START:MSGSET_OFFSET_END]), uint64(1235468))
	assert.Equal(t, binary.BigEndian.Uint32(
		setBytes[MSGSET_MSG_SIZE_START:MSGSET_MSG_SIZE_END]),
		uint32(len(msgBytes)))
}

func TestMsgSetParseHeader(t *testing.T) {
	data := []byte("\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x40")
	off, len, err := ParseMessageSetHeader(data)
	assert.Nil(t, err)
	assert.Equal(t, off, int64(32))
	assert.Equal(t, len, int(64))

	faildata := make([]byte, 1)
	off, len, err = ParseMessageSetHeader(faildata)
	assert.NotNil(t, err)
}

func TestMsgSetParse(t *testing.T) {
	data := []byte("\x00\x00\x00\x00\x00\x00\x00\x2d\x00\x00\x00\x19\xb7\x65\x9c\xd3\x00\x00\x00\x00\x00\x04\x6b\x6c\x69\x63\x00\x00\x00\x07\x68\x6f\x64\x6e\x6f\x74\x61")
	msgset, err := ParseMessageSet(data)
	assert.Nil(t, err)
	assert.Equal(t, msgset.Offset, int64(45))
	assert.Equal(t, msgset.Message.Key, []byte("klic"))
	assert.Equal(t, msgset.Message.Value, []byte("hodnota"))

	faildata := []byte("\x00\x00\x00\x00\x00\x00\x00\x2d\x00\x00\x00\xff\xb7\x65\x9c\xd3\x00\x00\x00\x00\x00\x04\x6b\x6c\x69\x63\x00\x00\x00\x07\x68\x6f\x64\x6e\x6f\x74\x61")
	msgset, err = ParseMessageSet(faildata)
	assert.NotNil(t, err)
}
