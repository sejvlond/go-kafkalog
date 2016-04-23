package encoder

import (
	"testing"
)

func benchEncoder(b *testing.B, e Encoder) {
	value := []byte("value")
	key := []byte("key")
	offset := int64(4)

	for i := 0; i < b.N; i++ {
		e.Encode(nil, value, key, offset)
	}
}

func BenchmarkPlain(b *testing.B) {
	enc, _ := NewPlain()
	benchEncoder(b, enc)
}

func BenchmarkGzip(b *testing.B) {
	enc, _ := NewGzip()
	benchEncoder(b, enc)
}

func BenchmarkSnapyy(b *testing.B) {
	enc, _ := NewSnappy()
	benchEncoder(b, enc)
}
