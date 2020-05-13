package index

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"strconv"

	"github.com/JmPotato/index-kv/constdef"
	"github.com/JmPotato/index-kv/data"
)

// Chunk is the sharding unit of the index on disk
type Chunk struct {
	ID            uint32 // The identifier of a chunk
	chunkFile     *os.File
	chunkFileStat os.FileInfo
}

func (chunk *Chunk) New(chunkID uint32) (err error) {
	chunk.Open(chunkID)
	chunk.chunkFileStat, err = chunk.chunkFile.Stat()
	if err != nil {
		log.Fatalf("[Chunk.New] Get chunk file stat error=%v, id=%d", err, chunkID)
		return err
	}
	chunk.ID = chunkID
	return nil
}

func (chunk *Chunk) Open(chunkID uint32) (err error) {
	// To-do: Read & Write Isolation
	chunk.chunkFile, err = os.OpenFile(constdef.CHUNK_DIR+strconv.FormatUint(uint64(chunkID), 10), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("[Chunk.New] Open/Create chunk file error=%v, id=%d", err, chunkID)
		return err
	}
	return nil
}

func (chunk *Chunk) Close() (err error) {
	return chunk.chunkFile.Close()
}

func (chunk *Chunk) CreatNewIndexRecord(keyHash uint32, offset uint64) {
	keyHashItem := make([]byte, 8)
	binary.PutUvarint(keyHashItem, uint64(keyHash))
	offsetItem := make([]byte, 8)
	binary.PutUvarint(offsetItem, offset)
	indexRecordItem := make([]byte, 0)
	indexRecordItem = append(indexRecordItem, keyHashItem...)
	indexRecordItem = append(indexRecordItem, offsetItem...)
	if _, err := chunk.chunkFile.Write(indexRecordItem); err != nil {
		chunk.chunkFile.Close()
		log.Fatalf("[Chunk.Append] Write chunk file error=%v, id=%d", err, chunk.ID)
	}
}

func (chunk *Chunk) Append(keyHash uint32, offset uint64) {
	// Flush the buffer data to file
	chunk.chunkFile.Sync()
	chunk.chunkFileStat, _ = chunk.chunkFile.Stat()

	// Locate at EOF
	chunk.chunkFile.Seek(0, 2)
	chunk.CreatNewIndexRecord(keyHash, offset)

	return
}

func (chunk *Chunk) Get(keyHash uint32) (offsets []uint64, err error) {
	offsets = make([]uint64, 0)
	// Make sure we have the latest file stat info
	chunk.chunkFile.Sync()
	chunk.chunkFileStat, _ = chunk.chunkFile.Stat()
	chunk.chunkFile.Seek(0, 0)
	currentPosition, err := data.GetCurrentOffset(chunk.chunkFile)
	if err != nil {
		log.Fatalf("[Chunk.Get] Relocate chunk file error=%v", err)
		return offsets, err
	}
	for currentPosition < chunk.chunkFileStat.Size() {
		keyHashItem := make([]byte, 8)
		_, err := chunk.chunkFile.Read(keyHashItem)
		if err != nil {
			log.Fatalf("[Chunk.Get] Read keyHashItem error=%v", err)
			return offsets, err
		}
		keyHashRead, _ := binary.ReadUvarint(bytes.NewBuffer(keyHashItem))
		if uint32(keyHashRead) == keyHash {
			offsetRecord := make([]byte, 8)
			_, err = chunk.chunkFile.Read(offsetRecord)
			offsetRead, _ := binary.ReadUvarint(bytes.NewBuffer(offsetRecord))
			offsets = append(offsets, offsetRead)
		}
		currentPosition, _ = data.GetCurrentOffset(chunk.chunkFile)
	}
	return offsets, nil
}
