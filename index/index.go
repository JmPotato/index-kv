package index

// Chunk is the sharding unit of the index on disk
type Chunk struct {
	ID uint16 // The identifier of a chunk
}

// LoadChunk loads chunk from disk into memory
func (chunk *Chunk) LoadChunk(chunkID uint16) {

}
