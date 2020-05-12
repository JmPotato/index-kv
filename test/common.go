package test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/JmPotato/index-kv/constdef"
)

func errorHandle(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

func clearIndex() {
	chunkFiles, _ := ioutil.ReadDir(constdef.CHUNK_DIR)
	for _, chunkFile := range chunkFiles {
		go func(fileName string) {
			os.Remove(fileName)
		}(chunkFile.Name())
	}
}
