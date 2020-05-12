package index

import (
	"log"
	"os"

	"github.com/JmPotato/index-kv/constdef"
	"github.com/JmPotato/index-kv/data"
)

// Index is the core index model
type Index struct {
	dataFile     *os.File
	dataFileStat os.FileInfo
	chunkIDList  []uint16
}

func (index *Index) New(fileName string) (err error) {
	index.dataFile, err = os.Open(constdef.DATA_FILENAME)
	if err != nil {
		log.Fatalf("[Index.New] Open data file error=%v", err)
		return err
	}
	index.dataFileStat, err = index.dataFile.Stat()
	if err != nil {
		log.Fatalf("[Index.New] Get data file stat error=%v", err)
		return err
	}
	index.chunkIDList = make([]uint16, 0)

	// Start to create index
	currentOffset, _ := data.GetCurrentOffset(index.dataFile)
	var (
		key, value []byte
		chunk      *Chunk = &Chunk{}
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
		chunkID := uint16(keyHash % constdef.CHUNK_SIZE)
		if err := chunk.New(chunkID); err != nil {
			log.Fatalf("[Index.New] Init chunk id=%d error=%v, key=%s, value=%s", chunkID, err, key, value)
			return err
		}
		if err := chunk.Append(keyHash, uint64(offset)); err != nil {
			log.Fatalf("[Index.New] Append chunk id=%d error=%v, keyHash=%d, offset=%d", chunkID, err, keyHash, offset)
			return err
		}
		log.Printf("[Index.New] Create index successfully for keyHash=%d", keyHash)
		index.chunkIDList = append(index.chunkIDList, chunkID)
		currentOffset, _ = data.GetCurrentOffset(index.dataFile)
	}

	return nil
}

func (index *Index) Hash(key []byte) (hash uint32) {
	for _, value := range key {
		hash = (hash * constdef.BKDR_HASH_SEED) + uint32(value)
	}

	return hash
}

func (index *Index) Get(key string) (value string) {
	var (
		chunk   *Chunk = &Chunk{}
		offsets []uint64
		keyHash uint32
		chunkID uint16
	)
	keyHash = index.Hash([]byte(key))
	chunkID = uint16(keyHash % constdef.CHUNK_SIZE)
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
			log.Fatalf("[Index.Get] Invalid key size=%d, keyRead=%s", keySize, keyRead)
			return ""
		}
		valueSize, valueRead, _ := data.ReadSizeAndContent(index.dataFile)
		if valueSize < constdef.MIN_VALUE_SIZE || valueSize > constdef.MAX_VALUE_SIZE {
			log.Fatalf("[Index.Get] Invalid key size=%d, valueRead=%s", valueSize, valueRead)
			return ""
		}

		if key == string(keyRead) {
			value = string(valueRead)
			return value
		}
	}

	return ""
}
