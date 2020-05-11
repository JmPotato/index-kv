package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/JmPotato/index-kv/data"
)

func TestGenerateData(t *testing.T) {
	// Generate random test data
	keyList, valueList := data.GenerateRandomData()
	dataFile, err := os.Open("kv-data.pingcap")
	errorHandle(err)
	dataFileStat, err := dataFile.Stat()
	errorHandle(err)

	var (
		key, value      []byte
		currentPosition int64
		keyListRead     = make([]string, 0)
		valueListRead   = make([]string, 0)
	)
	for currentPosition < dataFileStat.Size() {
		_, key, err = data.ReadSizeAndContent(dataFile)
		errorHandle(err)
		keyListRead = append(keyListRead, string(key))

		_, value, err = data.ReadSizeAndContent(dataFile)
		errorHandle(err)
		valueListRead = append(valueListRead, string(value))
		currentPosition, _ = dataFile.Seek(0, 1)
	}

	assertEqual(t, len(keyList), len(keyListRead), fmt.Sprintf("Mismatch keyList and keyListRead lengths, %d != %d", len(keyList), len(keyListRead)))
	assertEqual(t, len(valueList), len(valueListRead), fmt.Sprintf("Mismatch valueList and valueListRead lengths, %d != %d", len(valueList), len(valueListRead)))
	assertEqual(t, len(keyList), len(valueList), fmt.Sprintf("Mismatch keyList and valueList lengths, %d != %d", len(keyList), len(valueList)))

	for i := range keyList {
		assertEqual(t, keyList[i], keyListRead[i], fmt.Sprintf("Mismatch key[%d], %s != %s", i, keyList[i], keyListRead[i]))
		assertEqual(t, valueList[i], valueListRead[i], fmt.Sprintf("Mismatch key[%d], %s != %s", i, valueList[i], valueListRead[i]))
	}
}
