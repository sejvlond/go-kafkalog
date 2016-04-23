package iowriter

import (
	"bytes"
	"errors"
	"path"
	"strconv"
	"sync"
	"time"
)

// rotateWriter is innerWriter which write messages to file and rotate it when
// needed
type RotateWriter struct {
	name           string      // file base name
	intervalLength int64       // rotate interval length in seconds
	intervalStart  int64       // unix timestamp in UTC of last interval start
	dir            string      // directory for saving files
	fileWriter     *FileWriter // pointer to faileWriter
	mutex          sync.Mutex  // mutex for sync
}

/// newRotate creates rotateWriter
func NewRotate(name string, interval uint, dir string) (
	*RotateWriter, error) {

	if 3600%interval != 0 {
		return nil, errors.New("3600 % interval != 0")
	}
	return &RotateWriter{
		name:           name,
		intervalLength: int64(interval),
		dir:            dir,
	}, nil
}

// Write writes []byte to file and returns number of bytes and error if any
// it does not FSync the file and it uses mutex for synchronization (not flock)
func (this *RotateWriter) Write(bytes []byte) (n int, err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if err = this.rotate(); err != nil {
		return
	}
	return this.fileWriter.Write(bytes)
}

// rotate rotates files, when needed
func (this *RotateWriter) rotate() (err error) {
	now := time.Now().UTC()
	if this.fileWriter != nil &&
		this.intervalStart+this.intervalLength > now.Unix() {
		return
	}
	// rotate
	if this.fileWriter != nil {
		if err = this.fileWriter.Close(); err != nil {
			return
		}
	}
	// length of final name: 16+max4+5+len(name)+4 = 29+len(name)
	fileName := bytes.NewBuffer(make([]byte, 0, 30+len(this.name)))
	fileName.WriteString(now.Format("20060102_150405_")) // %Y%m%d_%H%M%S_
	fileName.WriteString(strconv.FormatInt(this.intervalLength, 10))
	fileName.WriteString("_UTC-")
	fileName.WriteString(this.name)
	fileName.WriteString(".szn")
	// create unsafe file, because we are using own mutex for synchro
	this.fileWriter, err = NewUnsafeFile(path.Join(this.dir, fileName.String()))
	this.intervalStart = now.Unix() - now.Unix()%this.intervalLength
	return
}

// Close closes the File, rendering it unusable for I/O. It returns an error,
// if any.
func (this *RotateWriter) Close() (err error) {
	if this.fileWriter != nil {
		return this.fileWriter.Close()
	}
	return
}
