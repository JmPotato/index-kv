package index

import (
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/JmPotato/index-kv/cache"
	"github.com/JmPotato/index-kv/constdef"
	"github.com/JmPotato/index-kv/data"
)

// Index is the core index model
type Index struct {
	wg         sync.WaitGroup
	indexMutex sync.Mutex

	// KV original datafile info
	dataFile     *os.File
	dataFileStat os.FileInfo

	// KV index chunk info
	chunkList  map[uint32]*Chunk
	chunkMutex map[uint32]*sync.Mutex

	// KV Cache info
	kvCache *cache.KVCache
}

func (index *Index) New(fileName string) (err error) {
	index.kvCache, err = cache.New(constdef.CACHE_SIZE)

	dir, err := ioutil.ReadDir(constdef.CHUNK_DIR)
	if err == nil && len(dir) != 0 {
		return nil
	}

	log.Printf("Chunk file not found. Create index first...\n")
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
		key, value  []byte
		routinePool = make(chan struct{}, constdef.MAX_ROUTINE_LIMIT)
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
		chunkID := keyHash % constdef.CHUNK_NUM
		// Concurrently write index chunk
		routinePool <- struct{}{}
		go func(cID uint32) {
			index.wg.Add(1)
			defer func() {
				<-routinePool
				index.wg.Done()
			}()
			index.indexMutex.Lock()
			cMutex, exist := index.chunkMutex[cID]
			if !exist {
				cMutex = &sync.Mutex{}
				index.chunkMutex[cID] = cMutex
			}
			index.indexMutex.Unlock()
			cMutex.Lock()
			index.indexMutex.Lock()
			chunkHandle, exist := index.chunkList[cID]
			if !exist {
				chunkHandle = &Chunk{}
				index.chunkList[cID] = chunkHandle
			}
			index.indexMutex.Unlock()
			if err := chunkHandle.New(cID); err != nil {
				log.Fatalf("[Index.New] Init chunk id=%d error=%v, key=%s, value=%s", chunkID, err, key, value)
				return
			}
			chunkHandle.Append(keyHash, uint64(offset))
			chunkHandle.Close()
			// log.Printf("[Index.New] Created index for keyHash=%d", keyHash)
			cMutex.Unlock()
		}(chunkID)

		currentOffset, _ = data.GetCurrentOffset(index.dataFile)
	}
	index.wg.Wait()
	if err != nil {
		return err
	}

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
	valueCache, exist := index.kvCache.Get(key)
	if exist {
		return valueCache
	}

	var (
		chunk   *Chunk = &Chunk{}
		offsets []uint64
		keyHash uint32
		chunkID uint32
	)
	keyHash = index.Hash([]byte(key))
	chunkID = keyHash % constdef.CHUNK_NUM
	chunk.New(chunkID)
	offsets, err := chunk.Get(keyHash)
	if err != nil {
		log.Fatalf("[Index.Get] Offset not found for key=%s", key)
		return ""
	}

	// Locate the offset in data on disk
	dataFile, _ := os.OpenFile(constdef.DATA_FILENAME, os.O_RDONLY|os.O_CREATE, 0644)
	for _, offset := range offsets {
		dataFile.Seek(int64(offset), 0)
		keySize, keyRead, _ := data.ReadSizeAndContent(dataFile)
		if keySize < constdef.MIN_KEY_SIZE || keySize > constdef.MAX_KEY_SIZE {
			log.Fatalf("[Index.Get] Invalid key size=%d", keySize)
			return ""
		}
		valueSize, valueRead, _ := data.ReadSizeAndContent(dataFile)
		if valueSize < constdef.MIN_VALUE_SIZE || valueSize > constdef.MAX_VALUE_SIZE {
			log.Fatalf("[Index.Get] Invalid key size=%d", valueSize)
			return ""
		}

		if key == string(keyRead) {
			value = string(valueRead)
			index.kvCache.Add(key, value)
			return value
		}
	}

	return ""
}

// MGet concurrently get key through index
func (index *Index) MGet(keys *[]string) *[]string {
	values := make([]string, len(*keys))
	routinePool := make(chan struct{}, constdef.MAX_ROUTINE_LIMIT)
	for i, key := range *keys {
		routinePool <- struct{}{}
		go func(idx int, k string) {
			index.wg.Add(1)
			defer func() {
				<-routinePool
				index.wg.Done()
			}()
			var (
				keyHash     uint32
				chunkID     uint32
				offsets     []uint64
				chunkHandle *Chunk = &Chunk{}
			)
			keyHash = index.Hash([]byte(k))
			chunkID = keyHash % constdef.CHUNK_NUM
			chunkHandle.New(chunkID)
			offsets, err := chunkHandle.Get(keyHash)
			if err != nil {
				log.Fatalf("[Index.Get] Offset not found for key=%s", k)
				<-routinePool
				return
			}

			// Locate the offset in data on disk
			dataFile, _ := os.OpenFile(constdef.DATA_FILENAME, os.O_RDONLY|os.O_CREATE, 0644)
			for _, offset := range offsets {
				dataFile.Seek(int64(offset), 0)
				keySize, keyRead, _ := data.ReadSizeAndContent(dataFile)
				if keySize < constdef.MIN_KEY_SIZE || keySize > constdef.MAX_KEY_SIZE {
					log.Fatalf("[Index.Get] Invalid key size=%d", keySize)
					dataFile.Close()
					return
				}
				valueSize, valueRead, _ := data.ReadSizeAndContent(dataFile)
				if valueSize < constdef.MIN_VALUE_SIZE || valueSize > constdef.MAX_VALUE_SIZE {
					log.Fatalf("[Index.Get] Invalid key size=%d", valueSize)
					dataFile.Close()
					return
				}
				if k == string(keyRead) {
					values[idx] = string(valueRead)
					dataFile.Close()
					return
				}
			}
		}(i, key)
	}
	index.wg.Wait()
	return &values
}
