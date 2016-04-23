package common

import (
	"encoding/binary"
	"fmt"
	"io"
)

// MessageSet
type MessageSet struct {
	Offset  int64
	Message Message
}

/*
MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32
*/
const (
	MSGSET_HEADER_SIZE = MSGSET_OFFSET_LEN + MSGSET_MSG_SIZE_LEN

	MSGSET_OFFSET_LEN   = 8
	MSGSET_OFFSET_START = 0
	MSGSET_OFFSET_END   = MSGSET_OFFSET_START + MSGSET_OFFSET_LEN

	MSGSET_MSG_SIZE_LEN   = 4
	MSGSET_MSG_SIZE_START = MSGSET_OFFSET_END
	MSGSET_MSG_SIZE_END   = MSGSET_MSG_SIZE_START + MSGSET_MSG_SIZE_LEN

	MSGSET_MSG_START = MSGSET_MSG_SIZE_END
)

// ParseMessageSetHeader parse header of message set (ignoring its message) and
// returns offset and message length. Returns error if any
func ParseMessageSetHeader(data []byte) (offset int64, msglen int, err error) {
	if len(data) < MSGSET_HEADER_SIZE {
		err = fmt.Errorf("Parsing message set error: header length (%v) is "+
			"less than expected (%v)", len(data), MSGSET_HEADER_SIZE)
		return
	}
	// OFFSET
	offset = int64(binary.BigEndian.Uint64(
		data[MSGSET_OFFSET_START:MSGSET_OFFSET_END]))
	// MESSAGE SIZE
	msglen = int(binary.BigEndian.Uint32(
		data[MSGSET_MSG_SIZE_START:MSGSET_MSG_SIZE_END]))
	return
}

// ParseMessageSet creates new message set from []byte data.
// Returns Error if data are invalid
func ParseMessageSet(data []byte) (msgset *MessageSet, err error) {
	offset, msglen, err := ParseMessageSetHeader(data)
	if err != nil {
		return
	}
	if len(data[MSGSET_MSG_START:]) != msglen {
		err = fmt.Errorf("Parsing message set error: data length (%v) is "+
			"less than header + message length (%v+%v=%v)", len(data),
			MSGSET_HEADER_SIZE, msglen, MSGSET_HEADER_SIZE+msglen)
		return
	}
	msg, err := ParseMessage(data[MSGSET_MSG_START:])
	if err != nil {
		return
	}
	return &MessageSet{Offset: offset, Message: *msg}, nil
}

// Size returns size of messageSet in bytes
func (this *MessageSet) Size() int {
	return MSGSET_OFFSET_LEN + MSGSET_MSG_SIZE_LEN + this.Message.Size()
}

// Bytes serialize messageSet to []byte
func (this *MessageSet) Bytes() []byte {
	msgset := make([]byte, 0, this.Size())
	msgset, err := this.Marshal(msgset)
	if err != nil {
		panic(err)
	}
	return msgset
}

// Serialize message to output buffer. Realloc when needed
func (this *MessageSet) Marshal(msgset []byte) ([]byte, error) {
	if cap(msgset) < this.Size() {
		return nil, io.ErrShortBuffer
	}
	msgset = msgset[:this.Size()] // set len
	// OFFSET
	binary.BigEndian.PutUint64(
		msgset[MSGSET_OFFSET_START:MSGSET_OFFSET_END],
		uint64(this.Offset))
	// MESSAGE SIZE
	binary.BigEndian.PutUint32(
		msgset[MSGSET_MSG_SIZE_START:MSGSET_MSG_SIZE_END],
		uint32(this.Message.Size()))
	// MESSAGE
	_, err := this.Message.Marshal(msgset[MSGSET_MSG_START:])
	return msgset, err
}
