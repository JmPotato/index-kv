package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/JmPotato/index-kv/constdef"
	"github.com/JmPotato/index-kv/data"
)

func TestGenerateData(t *testing.T) {
	// Generate random test data
	keyList, valueList := data.GenerateRandomData()
	dataFile, err := os.Open(constdef.DATA_FILENAME)
	errorHandle(err)

	var (
		keyListRead   = make([]string, 0)
		valueListRead = make([]string, 0)
	)
	keyListRead, valueListRead = data.ReadKV(dataFile)

	assertEqual(t, len(keyList), len(keyListRead), fmt.Sprintf("Mismatch keyList and keyListRead lengths, %d != %d", len(keyList), len(keyListRead)))
	assertEqual(t, len(valueList), len(valueListRead), fmt.Sprintf("Mismatch valueList and valueListRead lengths, %d != %d", len(valueList), len(valueListRead)))
	assertEqual(t, len(keyList), len(valueList), fmt.Sprintf("Mismatch keyList and valueList lengths, %d != %d", len(keyList), len(valueList)))

	for i := range keyList {
		assertEqual(t, keyList[i], keyListRead[i], fmt.Sprintf("Mismatch key[%d], %s != %s", i, keyList[i], keyListRead[i]))
		assertEqual(t, valueList[i], valueListRead[i], fmt.Sprintf("Mismatch key[%d], %s != %s", i, valueList[i], valueListRead[i]))
	}
}
