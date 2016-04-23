package decoder

import (
	"bytes"
	"compress/zlib"
	"io"

	"github.com/sejvlond/go-kafkalog/common"
)

// Gzip Decoder
type GzipDecoder struct{}

// NewPlain returns PlainDecoder for plain text
func NewGzip() Decoder {
	return new(GzipDecoder)
}

// Decode Message Set to parts
func (this *GzipDecoder) Decode(msgset *common.MessageSet) (
	value, key []byte, offset int64, err error) {

	gzipValue := bytes.NewReader(msgset.Message.Value)
	gzipReader, err := zlib.NewReader(gzipValue)
	if err != nil {
		return
	}
	plainValue := new(bytes.Buffer)
	io.Copy(plainValue, gzipReader)
	if err = gzipReader.Close(); err != nil {
		return
	}
	return Decode(plainValue.Bytes())
}
