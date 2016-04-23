package common

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Message
type Message struct {
	attrs uint8
	Key   []byte
	Value []byte
}

/*
Message => Crc MagicByte Attributes KeySize Key ValueSize Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  KeySize => int32
  Key => bytes
  ValueSize => int32
  Value => bytes
*/
const (
	MSG_CRC_LEN   = 4
	MSG_CRC_START = 0
	MSG_CRC_END   = MSG_CRC_START + MSG_CRC_LEN

	msg_MAGIC_VALUE = 0
	MSG_MAGIC_LEN   = 1
	MSG_MAGIC_START = MSG_CRC_END
	MSG_MAGIC_END   = MSG_MAGIC_START + MSG_MAGIC_LEN

	MSG_ATTRS_LEN   = 1
	MSG_ATTRS_START = MSG_MAGIC_END
	MSG_ATTRS_END   = MSG_ATTRS_START + MSG_ATTRS_LEN

	MSG_KEY_SIZE_LEN   = 4
	MSG_KEY_SIZE_START = MSG_ATTRS_END
	MSG_KEY_SIZE_END   = MSG_KEY_SIZE_START + MSG_KEY_SIZE_LEN

	MSG_KEY_START = MSG_KEY_SIZE_END

	MSG_VALUE_SIZE_LEN   = 4
	MSG_VALUE_SIZE_START = MSG_KEY_SIZE_END // + len(key)
	MSG_VALUE_SIZE_END   = MSG_VALUE_SIZE_START + MSG_VALUE_SIZE_LEN

	MSG_VALUE_START = MSG_VALUE_SIZE_END

	MSG_BASE_SIZE = MSG_CRC_LEN + MSG_MAGIC_LEN + MSG_ATTRS_LEN +
		MSG_KEY_SIZE_LEN + MSG_VALUE_SIZE_LEN
)

// effSize calculate efectiveSize from size parsed from data
func effSize(size int) int {
	if size == -1 {
		return 0
	}
	return size
}

// Parase parse []byte and returns new Message
func ParseMessage(data []byte) (msg *Message, err error) {
	if len(data) < MSG_BASE_SIZE {
		err = fmt.Errorf("Parsing message error: data length (%v) is less "+
			"than empty message length (%v)", len(data), MSG_BASE_SIZE)
		return
	}
	keySize := int(int32(binary.BigEndian.Uint32(
		data[MSG_KEY_SIZE_START:MSG_KEY_SIZE_END])))
	effKeySize := effSize(keySize)
	if len(data) < MSG_BASE_SIZE+effKeySize {
		err = fmt.Errorf("Parsing message error: data length (%v) is less "+
			"than empty message + key length (%v+%v=%v)", len(data),
			MSG_BASE_SIZE, effKeySize, MSG_BASE_SIZE+effKeySize)
		return
	}
	valueSize := int(int32(binary.BigEndian.Uint32(
		data[MSG_VALUE_SIZE_START+effKeySize : MSG_VALUE_SIZE_END+effKeySize])))
	effValueSize := effSize(valueSize)
	if len(data) < MSG_BASE_SIZE+effKeySize+effValueSize {
		err = fmt.Errorf("Parsing message error: data length (%v) is less "+
			"than empty message + key length + value length (%v+%v+%b=%v)",
			len(data), MSG_BASE_SIZE, effKeySize, effValueSize,
			MSG_BASE_SIZE+effKeySize+effValueSize)
		return
	}
	crc := binary.BigEndian.Uint32(data[MSG_CRC_START:MSG_CRC_END])
	if realCrc := crc32.ChecksumIEEE(data[MSG_CRC_END:]); crc != realCrc {
		err = fmt.Errorf("Parsing message error: Invalid data (CRC failed)")
		return
	}
	// length of data is valid
	var key, value []byte // nil
	if keySize >= 0 {
		key = make([]byte, keySize) // empty value
		copy(key, data[MSG_KEY_START:MSG_KEY_START+effKeySize])
	}
	if valueSize >= 0 {
		value = make([]byte, valueSize) // empty value
		copy(value, data[MSG_VALUE_START+effKeySize:])
	}
	return &Message{
		Value: value,
		Key:   key,
		attrs: data[MSG_ATTRS_START],
	}, nil
}

// SetCompression sets compression attribute of message
func (this *Message) SetCompression(compression uint8) {
	this.attrs |= compression
}

// GetCompression gets compression attribute of message
func (this *Message) GetCompression() uint8 {
	return this.attrs & COMPRESS_MASK
}

// Size returns size of message
func (this *Message) Size() int {
	return MSG_BASE_SIZE + len(this.Key) + len(this.Value)
}

// Bytes serialize message to []byte
func (this *Message) Bytes() []byte {
	msg := make([]byte, 0, this.Size())
	msg, err := this.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return msg
}

// Serialize message to output buffer. Realloc when needed
func (this *Message) Marshal(msg []byte) ([]byte, error) {
	if cap(msg) < this.Size() {
		return nil, io.ErrShortBuffer
	}
	msg = msg[:this.Size()] // set len
	// MAGIC
	msg[MSG_MAGIC_START] = msg_MAGIC_VALUE
	// ATTRS
	msg[MSG_ATTRS_START] = this.attrs
	// KEY SIZE
	keySize := len(this.Key)
	specKeySize := keySize
	if this.Key == nil {
		specKeySize = -1
	}
	binary.BigEndian.PutUint32(
		msg[MSG_KEY_SIZE_START:MSG_KEY_SIZE_END], uint32(specKeySize))
	// KEY, if any
	if keySize > 0 {
		copy(msg[MSG_KEY_START:MSG_KEY_START+keySize], this.Key)
	}
	// VALUE SIZE
	valueSize := len(this.Value)
	specValueSize := valueSize
	if this.Value == nil {
		specValueSize = -1
	}
	binary.BigEndian.PutUint32(
		msg[MSG_VALUE_SIZE_START+keySize:MSG_VALUE_SIZE_END+keySize],
		uint32(specValueSize))
	// VALUE, if any
	if valueSize > 0 {
		copy(msg[MSG_VALUE_START+keySize:], this.Value)
	}
	// CRC32
	crc := crc32.ChecksumIEEE(msg[MSG_CRC_END:])
	binary.BigEndian.PutUint32(msg[MSG_CRC_START:MSG_CRC_END], uint32(crc))
	return msg, nil
}
