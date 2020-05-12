package test

import (
	"fmt"
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

func clearChunks() {
	os.RemoveAll(constdef.CHUNK_DIR)
	os.MkdirAll(constdef.CHUNK_DIR, 0777)
}

func fileExist(fileName string) bool {
	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
