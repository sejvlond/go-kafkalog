package ioreader

import (
	"io"
	"os"
)

func NewFile(path string) (io.ReadWriteSeeker, error) {
	return os.Open(path)
}
