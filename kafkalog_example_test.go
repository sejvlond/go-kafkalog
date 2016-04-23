package kafkalog

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	kafkalog "github.com/sejvlond/go-kafkalog/common"
	kafkalog_reader "github.com/sejvlond/go-kafkalog/reader"
	kafkalog_writer "github.com/sejvlond/go-kafkalog/writer"
)

func panika(err error) {
	if err != nil {
		panic(err)
	}
}

func ExampleKafkalog() {
	tmpFile, err := ioutil.TempFile("", "kafkalog.example")
	panika(err)
	// WRITER
	w, err := kafkalog_writer.NewFile(tmpFile.Name(), kafkalog.COMPRESS_SNAPPY)
	panika(err)
	value := []byte("hodnota")
	key := []byte("klic")
	offset := int64(45)
	_, err = w.Write(value, key, offset)
	panika(err)
	err = w.Close()
	panika(err)

	file, err := os.Open(tmpFile.Name())
	panika(err)
	// READER
	r, err := kafkalog_reader.New(file)
	panika(err)
	for {
		rvalue, rkey, roffset, err := r.Read()
		if err == io.EOF {
			break
		}
		panika(err)
		fmt.Println(rvalue)
		fmt.Println(rkey)
		fmt.Println(roffset)
	}
	err = r.Close()
	panika(err)
	// Output:
	// [104 111 100 110 111 116 97]
	// [107 108 105 99]
	// 45
}
