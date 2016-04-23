package iowriter

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const TMP_FILE_PREFIX = "kafkalog.iowriter.TestFile"

func write_helper(t *testing.T, w io.Writer, msg string, count int,
	done chan bool) {

	text := []byte(msg)
	for i := 0; i < count; i++ {
		n, err := w.Write(text)
		assert.Nil(t, err)
		assert.Equal(t, n, len(text))
	}
	done <- true
}

func BenchmarkFile(b *testing.B) {
	tmpFile, err := ioutil.TempFile("", TMP_FILE_PREFIX)
	assert.Nil(b, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	fw, err := NewFile(tmpFile.Name())
	assert.Nil(b, err)
	text := []byte("testing")
	for i := 0; i < b.N; i++ {
		_, err = fw.Write(text)
	}
	err = fw.Close()
	assert.Nil(b, err)
}

func TestFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", TMP_FILE_PREFIX)
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	fw, err := NewFile(tmpFile.Name())
	assert.Nil(t, err)
	done := make(chan bool)
	go write_helper(t, fw, "testmsg", 10, done)
	<-done

	err = fw.Close()
	assert.Nil(t, err)

	// TODO assert messages from file
}

func TestNotExistsUnsafeFile(t *testing.T) {
	fw, err := NewUnsafeFile("/todlebynemeloexistovatataktomuzevkliduselhat/a.txt")
	assert.NotNil(t, err)
	assert.Nil(t, fw)
}

func TestFileMulti(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", TMP_FILE_PREFIX)
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	fw, err := NewFile(tmpFile.Name())
	assert.Nil(t, err)
	const threads = 10
	done := make([]chan bool, threads)
	for i := 0; i < threads; i++ {
		done[i] = make(chan bool)
		go write_helper(t, fw, "testmsg\n", 10, done[i])
	}
	for i := 0; i < threads; i++ {
		<-done[i]
	}
	err = fw.Close()
	assert.Nil(t, err)

	// TODO assert messages from file
}
