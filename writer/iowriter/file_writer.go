package iowriter

import (
	"os"
	"sync"
)

// FileWriter writes messages to a single file
type FileWriter struct {
	file                 *os.File
	mutex                sync.Mutex
	unsafe_doNotUseMutex bool
}

/// newFileWriter creates fileWriter and open file for append
func NewFile(path string) (fw *FileWriter, err error) {
	// os.O_SYNC   int = syscall.O_SYNC   // open for synchronous I/O.
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0666) // with default system mask will be 0644
	if err != nil {
		return
	}
	return &FileWriter{file: file}, nil
}

// NewUnsafeFile creates fileWriter and do not use mutex for synchronization
func NewUnsafeFile(path string) (fw *FileWriter, err error) {
	fw, err = NewFile(path)
	if fw != nil {
		fw.unsafe_doNotUseMutex = true
	}
	return
}

// Write writes []byte to file and returns number of bytes and error if any
// It does not FSync the file and it uses only mutex (no flock etc.) for
// synchronization
func (this *FileWriter) Write(bytes []byte) (int, error) {
	if !this.unsafe_doNotUseMutex {
		this.mutex.Lock()
		defer this.mutex.Unlock()
	}
	return this.file.Write(bytes)
}

// Close closes the File, rendering it unusable for I/O. It returns an error,
// if any.
func (this *FileWriter) Close() error {
	if !this.unsafe_doNotUseMutex {
		this.mutex.Lock()
		defer this.mutex.Unlock()
	}
	return this.file.Close()
}
