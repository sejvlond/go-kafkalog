package common

const (
	COMPRESS_MASK = 0x03
)

const (
	COMPRESS_PLAIN byte = iota
	COMPRESS_GZIP
	COMPRESS_SNAPPY
)
