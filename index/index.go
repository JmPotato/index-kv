package index

import (
	"log"
	"os"
	"sync"

	"github.com/JmPotato/index-kv/constdef"
	"github.com/JmPotato/index-kv/data"
)

// Index is the core index model
type Index struct {
	dataFile     *os.File
	dataFileStat os.FileInfo
	chunkList    map[uint32]*Chunk
	chunkMutex   map[uint32]*sync.Mutex
	indexMutex   sync.Mutex
	wg           sync.WaitGroup
}

func (index *Index) New(fileName string) (err error) {
	index.dataFile, err = os.OpenFile(constdef.DATA_FILENAME, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("[Index.New] Open data file error=%v", err)
		return err
	}
	index.dataFileStat, err = index.dataFile.Stat()
	if err != nil {
		log.Fatalf("[Index.New] Get data file stat error=%v", err)
		return err
	}

	index.chunkList = make(map[uint32]*Chunk)
	index.chunkMutex = make(map[uint32]*sync.Mutex)

	// Start to create index
	currentOffset, _ := data.GetCurrentOffset(index.dataFile)
	var (
		key, value []byte
	)
	for currentOffset < index.dataFileStat.Size() {
		offset := currentOffset
		_, key, err = data.ReadSizeAndContent(index.dataFile)
		if err != nil {
			log.Fatalf("[Index.New] Read key at offset=%d error=%v", currentOffset, err)
			return err
		}
		_, value, err = data.ReadSizeAndContent(index.dataFile)
		if err != nil {
			log.Fatalf("[Index.New] Read value at offset=%d error=%v", currentOffset, err)
			return err
		}
		keyHash := index.Hash([]byte(key))
		chunkID := keyHash % constdef.CHUNK_SIZE
		// Concurrently write index chunk
		go func(cID uint32) {
			defer index.wg.Done()
			index.wg.Add(1)
			index.indexMutex.Lock()
			cMutex, exist := index.chunkMutex[cID]
			if !exist {
				cMutex = &sync.Mutex{}
				index.chunkMutex[cID] = cMutex
			}
			index.indexMutex.Unlock()
			cMutex.Lock()
			chunkHandle, exist := index.chunkList[cID]
			if !exist {
				chunkHandle = &Chunk{}
				index.chunkList[cID] = chunkHandle
			}
			if err := chunkHandle.New(cID); err != nil {
				log.Fatalf("[Index.New] Init chunk id=%d error=%v, key=%s, value=%s", chunkID, err, key, value)
				return
			}
			if err := chunkHandle.Append(keyHash, uint64(offset)); err != nil {
				log.Fatalf("[Index.New] Append chunk id=%d error=%v, keyHash=%d, offset=%d", chunkID, err, keyHash, offset)
				return
			}
			chunkHandle.Close()
			log.Printf("[Index.New] Created index for keyHash=%d", keyHash)
			cMutex.Unlock()
		}(chunkID)

		currentOffset, _ = data.GetCurrentOffset(index.dataFile)
	}
	index.wg.Wait()
	return nil
}

// Hash uses BKDR Hash algorithm to hash a key
func (index *Index) Hash(key []byte) (hash uint32) {
	for _, value := range key {
		hash = (hash * constdef.BKDR_HASH_SEED) + uint32(value)
	}

	return hash
}

// Get is single-thread
func (index *Index) Get(key string) (value string) {
	var (
		chunk   *Chunk = &Chunk{}
		offsets []uint64
		keyHash uint32
		chunkID uint32
	)
	keyHash = index.Hash([]byte(key))
	chunkID = keyHash % constdef.CHUNK_SIZE
	chunk.New(chunkID)
	offsets, err := chunk.Get(keyHash)
	if err != nil {
		log.Fatalf("[Index.Get] Offset not found for key=%s", key)
		return ""
	}

	// Locate the offset in data on disk
	for _, offset := range offsets {
		index.dataFile.Seek(int64(offset), 0)
		keySize, keyRead, _ := data.ReadSizeAndContent(index.dataFile)
		if keySize < constdef.MIN_KEY_SIZE || keySize > constdef.MAX_KEY_SIZE {
			log.Fatalf("[Index.Get] Invalid key size=%d", keySize)
			return ""
		}
		valueSize, valueRead, _ := data.ReadSizeAndContent(index.dataFile)
		if valueSize < constdef.MIN_VALUE_SIZE || valueSize > constdef.MAX_VALUE_SIZE {
			log.Fatalf("[Index.Get] Invalid key size=%d", valueSize)
			return ""
		}

		if key == string(keyRead) {
			value = string(valueRead)
			return value
		}
	}

	return ""
}

// MGet concurrently get key through index
func (index *Index) MGet(keys *[]string) *[]string {
	values := make([]string, len(*keys))
	for i, key := range *keys {
		index.wg.Add(1)
		go func(idx int, k string) {
			defer index.wg.Done()
			var (
				keyHash     uint32
				chunkID     uint32
				offsets     []uint64
				chunkHandle *Chunk = &Chunk{}
			)
			keyHash = index.Hash([]byte(k))
			chunkID = keyHash % constdef.CHUNK_SIZE
			chunkHandle.New(chunkID)
			offsets, err := chunkHandle.Get(keyHash)
			if err != nil {
				log.Fatalf("[Index.Get] Offset not found for key=%s", k)
				return
			}

			// Locate the offset in data on disk
			dataFile, _ := os.OpenFile(constdef.DATA_FILENAME, os.O_RDONLY|os.O_CREATE, 0644)
			for _, offset := range offsets {
				dataFile.Seek(int64(offset), 0)
				keySize, keyRead, _ := data.ReadSizeAndContent(dataFile)
				if keySize < constdef.MIN_KEY_SIZE || keySize > constdef.MAX_KEY_SIZE {
					log.Fatalf("[Index.Get] Invalid key size=%d", keySize)
					return
				}
				valueSize, valueRead, _ := data.ReadSizeAndContent(dataFile)
				if valueSize < constdef.MIN_VALUE_SIZE || valueSize > constdef.MAX_VALUE_SIZE {
					log.Fatalf("[Index.Get] Invalid key size=%d", valueSize)
					return
				}
				if k == string(keyRead) {
					values[idx] = string(valueRead)
					return
				}
			}
			dataFile.Close()
		}(i, key)
	}
	index.wg.Wait()
	return &values
}
