package data

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/JmPotato/index-kv/constdef"
)

const (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_!@#$%^&*()-"
)

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

// GenerateRandomString generates random string with the given length, where letters come from charset.
func GenerateRandomString(length int) (randomString []byte) {
	randomString = make([]byte, length)
	for i := range randomString {
		randomString[i] = charset[seededRand.Intn(len(charset))]
	}
	return randomString
}

// GenerateRandomData generates KV_NUMBER key-value pairs and writes them into DATA_FILENAME file.
func GenerateRandomData() (fakeKeyList, fakeValueList []string) {
	fakeKeyList = make([]string, 0)
	fakeValueList = make([]string, 0)
	dataFile, err := os.OpenFile(constdef.DATA_FILENAME, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("[GenerateRandomData] Open file error: %v", err)
	}

	for i := 0; i < constdef.KV_NUMBER; i++ {
		// Generate random key & value string
		keySize := seededRand.Intn(constdef.MAX_KEY_SIZE-constdef.MIN_KEY_SIZE) + constdef.MIN_KEY_SIZE
		key := GenerateRandomString(keySize)
		fakeKeyList = append(fakeKeyList, string(key))
		valueSize := seededRand.Intn(constdef.MAX_VALUE_SIZE-constdef.MIN_VALUE_SIZE) + constdef.MIN_VALUE_SIZE
		value := GenerateRandomString(valueSize)
		fakeValueList = append(fakeValueList, string(value))

		// Combine the data item structure for DATA_FILENAME
		keySizeItem := make([]byte, 8)
		binary.PutUvarint(keySizeItem, uint64(keySize))
		valueSizeItem := make([]byte, 8)
		binary.PutUvarint(valueSizeItem, uint64(valueSize))
		kvItem := make([]byte, 0)
		kvItem = append(kvItem, keySizeItem...)
		kvItem = append(kvItem, key...)
		kvItem = append(kvItem, valueSizeItem...)
		kvItem = append(kvItem, value...)

		// Write into DATA_FILENAME
		if _, err := dataFile.Write(kvItem); err != nil {
			dataFile.Close()
			log.Fatalf("[GenerateRandomData] Write file error=%v", err)
		}
	}

	if err := dataFile.Close(); err != nil {
		log.Fatalf("[GenerateRandomData] Close file error: %v", err)
	}
	return fakeKeyList, fakeValueList
}

// GetCurrentOffset returns the current file position
func GetCurrentOffset(file *os.File) (offset int64, err error) {
	offset, err = file.Seek(0, 1)
	if err != nil {
		log.Printf("[GetCurrentOffset] Get file offset error=%v", err)
		return 0, err
	}

	return offset, nil
}

// ReadSizeAndContent reads key/value size and corresponding content from file
func ReadSizeAndContent(file *os.File) (size uint64, content []byte, err error) {
	sizeItem := make([]byte, 8)
	_, err = file.Read(sizeItem)
	if err != nil {
		log.Printf("[ReadSizeAndContent] Read size from file error=%v", err)
		return size, nil, err
	}
	size, err = binary.ReadUvarint(bytes.NewBuffer(sizeItem))
	if err != nil {
		log.Printf("[ReadSizeAndContent] Convert size bytes to uint64 error=%v, sizeItem=%v", err, sizeItem)
		return size, nil, err
	}
	content = make([]byte, size)
	_, err = file.Read(content)
	if err != nil {
		log.Printf("[ReadSizeAndContent] Read content from file error=%v", err)
		return size, content, err
	}
	return size, content, nil
}

// ReadKV reads key-value pair from existed data file.
func ReadKV(dataFile *os.File) (keyListRead, valueListRead []string) {
	keyListRead = make([]string, 0)
	valueListRead = make([]string, 0)
	dataFileStat, err := dataFile.Stat()
	if err != nil {
		log.Fatalf("[ReadKV] Get data file stat error=%v", err)
		return
	}

	var (
		key, value      []byte
		currentPosition int64
	)
	for currentPosition < dataFileStat.Size() {
		_, key, err = ReadSizeAndContent(dataFile)
		if err != nil {
			log.Fatalf("[ReadKV] Read key size and content error=%v", err)
			return
		}
		keyListRead = append(keyListRead, string(key))

		_, value, err = ReadSizeAndContent(dataFile)
		if err != nil {
			log.Fatalf("[ReadKV] Read value size and content error=%v", err)
			return
		}
		valueListRead = append(valueListRead, string(value))
		currentPosition, _ = dataFile.Seek(0, 1)
	}

	return keyListRead, valueListRead
}
