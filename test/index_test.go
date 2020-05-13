package test

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/JmPotato/index-kv/constdef"
	"github.com/JmPotato/index-kv/data"
	"github.com/JmPotato/index-kv/index"
)

var (
	testIndex          *index.Index = &index.Index{}
	keyList, valueList []string
)

func TestIndexCreate(t *testing.T) {
	clearChunks()
	testIndex.New(constdef.DATA_FILENAME)
}

func TestIndexGet(t *testing.T) {
	if fileExist(constdef.DATA_FILENAME) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			log.Printf("Reading Index first...\n")
			testIndex.New(constdef.DATA_FILENAME)
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			log.Printf("Reading all k-v pairs...\n")
			dataFile, err := os.Open(constdef.DATA_FILENAME)
			errorHandle(err)
			keyList, valueList = data.ReadAllKV(dataFile)
			wg.Done()
		}()
		wg.Wait()
	} else {
		log.Printf("Generating new k-v data...\n")
		keyList, valueList = data.GenerateRandomData()
		testIndex.New(constdef.DATA_FILENAME)
	}

	for i, key := range keyList {
		valueRead := testIndex.Get(key)
		assertEqual(t, valueList[i], valueRead, fmt.Sprintf("Mismatch for keyList[%d]", i))
	}
}

func TestIndexMGet(t *testing.T) {
	if fileExist(constdef.DATA_FILENAME) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			log.Printf("Reading Index first...\n")
			testIndex.New(constdef.DATA_FILENAME)
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			log.Printf("Reading all k-v pairs...\n")
			dataFile, err := os.Open(constdef.DATA_FILENAME)
			errorHandle(err)
			keyList, valueList = data.ReadAllKV(dataFile)
			wg.Done()
		}()
		wg.Wait()
	} else {
		log.Printf("Generating new k-v data...\n")
		keyList, valueList = data.GenerateRandomData()
		testIndex.New(constdef.DATA_FILENAME)
	}

	valueListRead := testIndex.MGet(&keyList)
	for idx, valueRead := range *valueListRead {
		chunkID := testIndex.Hash([]byte(keyList[idx])) % constdef.CHUNK_NUM
		assertEqual(t, valueList[idx], valueRead, fmt.Sprintf("Mismatch for keyList[%d], chunkID=%d", idx, chunkID))
	}
}
func TestIndexRandomGet(t *testing.T) {
	if !fileExist(constdef.DATA_FILENAME) {
		log.Printf("Generate data first.\n")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		log.Printf("Reading Index first...\n")
		testIndex.New(constdef.DATA_FILENAME)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		log.Printf("Reading random k-v pairs...\n")
		dataFile, err := os.Open(constdef.DATA_FILENAME)
		errorHandle(err)
		keyList, valueList = data.ReadRandomKV(dataFile, constdef.KV_NUMBER/10)
		wg.Done()
	}()
	wg.Wait()

	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 1000; i++ {
		randomIdx := seededRand.Intn(constdef.KV_NUMBER / 10)
		valueRead := testIndex.Get(keyList[randomIdx])
		assertEqual(t, valueList[randomIdx], valueRead, fmt.Sprintf("Mismatch for keyList[%d]", randomIdx))
	}
}

func TestIndexRandomMGet(t *testing.T) {
	if !fileExist(constdef.DATA_FILENAME) {
		log.Printf("Generate data first.\n")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		log.Printf("Reading Index first...\n")
		testIndex.New(constdef.DATA_FILENAME)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		log.Printf("Reading random k-v pairs...\n")
		dataFile, err := os.Open(constdef.DATA_FILENAME)
		errorHandle(err)
		keyList, valueList = data.ReadRandomKV(dataFile, constdef.KV_NUMBER/10)
		wg.Done()
	}()
	wg.Wait()

	valueListRead := testIndex.MGet(&keyList)
	for idx, valueRead := range *valueListRead {
		chunkID := testIndex.Hash([]byte(keyList[idx])) % constdef.CHUNK_NUM
		assertEqual(t, valueList[idx], valueRead, fmt.Sprintf("Mismatch for keyList[%d], chunkID=%d", idx, chunkID))
	}
}

func BenchmarkIndexCreate(b *testing.B) {
	if fileExist(constdef.DATA_FILENAME) {
		testIndex.New(constdef.DATA_FILENAME)
	}
}

func BenchmarkIndexRandomGet(b *testing.B) {
	if !fileExist(constdef.DATA_FILENAME) {
		log.Printf("Generate data first.\n")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		log.Printf("Reading Index first...\n")
		testIndex.New(constdef.DATA_FILENAME)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		log.Printf("Reading random k-v pairs...\n")
		dataFile, err := os.Open(constdef.DATA_FILENAME)
		errorHandle(err)
		keyList, valueList = data.ReadRandomKV(dataFile, constdef.KV_NUMBER/10)
		wg.Done()
	}()
	wg.Wait()

	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	t1 := time.Now()
	for i := 0; i < 1000; i++ {
		randomIdx := seededRand.Intn(constdef.KV_NUMBER / 10)
		testIndex.Get(keyList[randomIdx])
	}
	elapsed := time.Since(t1)
	log.Printf("Time elapsed: %v", elapsed)
}

func BenchmarkIndexRandomMGet(b *testing.B) {
	if !fileExist(constdef.DATA_FILENAME) {
		log.Printf("Generate data first.\n")
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		log.Printf("Reading Index first...\n")
		testIndex.New(constdef.DATA_FILENAME)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		log.Printf("Reading random k-v pairs...\n")
		dataFile, err := os.Open(constdef.DATA_FILENAME)
		errorHandle(err)
		keyList, valueList = data.ReadRandomKV(dataFile, constdef.KV_NUMBER/10)
		wg.Done()
	}()
	wg.Wait()

	t1 := time.Now()
	testIndex.MGet(&keyList)
	elapsed := time.Since(t1)
	log.Printf("Time elapsed: %v", elapsed)
}
