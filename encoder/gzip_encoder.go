package encoder

import (
	"github.com/sejvlond/go-kafkalog/common"

	"bytes"
	"compress/zlib"
	"io"
)

// GzipEncoder
type GzipEncoder struct{}

// NewGzip returns GzipEncoder for zlib compression
func NewGzip() (Encoder, error) {
	return new(GzipEncoder), nil
}

// Encode returns []byte with zlib compression
func (this *GzipEncoder) Encode(dst, value, key []byte, offset int64) (
	_ []byte, err error) {

	plainEnc, err := NewPlain()
	if err != nil {
		return
	}
	plainValue, err := plainEnc.Encode(nil, value, key, offset)
	if err != nil {
		return
	}
	buffer := new(bytes.Buffer)
	gzipWriter := zlib.NewWriter(buffer)
	if _, err = gzipWriter.Write(plainValue); err != nil {
		return
	}
	if err = gzipWriter.Close(); err != nil {
		return
	}
	msg := common.Message{Value: buffer.Bytes()}
	msg.SetCompression(common.COMPRESS_GZIP)
	set := common.MessageSet{Message: msg}
	dst, err = set.Marshal(dst)
	if err == io.ErrShortBuffer {
		dst, err = set.Bytes(), nil
	}
	return dst, err
}
