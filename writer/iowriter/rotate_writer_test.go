package iowriter

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const TMP_ROTATE_PREFIX = "kafkalog.iowriter.TestRotate"

func TestRotate(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", TMP_ROTATE_PREFIX)
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	rw, err := NewRotate("test", 1, tmpDir)
	assert.Nil(t, err)
	done := make(chan bool)
	go write_helper(t, rw, "testmsg", 10, done)
	<-done
	time.Sleep(1 * time.Second)
	go write_helper(t, rw, "testmsg", 10, done)
	<-done

	files, _ := ioutil.ReadDir(tmpDir)
	assert.Equal(t, len(files), 2)

	err = rw.Close()
	assert.Nil(t, err)

	// TODO assert messages from file
}

func TestNotExistsRitate(t *testing.T) {
	rw, err := NewRotate(
		"name", 3600, "/todlebynemeloexistovatataktomuzevkliduselhat/a.txt")
	assert.NotNil(t, rw)
	assert.Nil(t, err)
	_, err = rw.Write([]byte("data"))
	assert.NotNil(t, err)
	_, err = rw.Write([]byte("data"))
	assert.NotNil(t, err)
}

func TestRotateMulti(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", TMP_ROTATE_PREFIX)
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	rw, err := NewRotate("test", 1, tmpDir)
	assert.Nil(t, err)

	const threads = 10
	done := make([]chan bool, threads)
	for i := 0; i < threads; i++ {
		done[i] = make(chan bool)
		go write_helper(t, rw, "testmsg\n", 10, done[i])
	}
	for i := 0; i < threads; i++ {
		<-done[i]
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < threads; i++ {
		go write_helper(t, rw, "testmsg\n", 10, done[i])
	}
	for i := 0; i < threads; i++ {
		<-done[i]
	}

	files, _ := ioutil.ReadDir(tmpDir)
	assert.Equal(t, len(files), 2)

	err = rw.Close()
	assert.Nil(t, err)

	// TODO assert messages from file
}
