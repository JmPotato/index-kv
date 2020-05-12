package test

import (
	"testing"

	"github.com/JmPotato/index-kv/index"
)

func TestChunkAppend(t *testing.T) {
	clearChunks()

	chunk := index.Chunk{}
	chunk.New(1)
	chunk.Append(12432434, 233)
	chunk.Append(12432434, 234)
	chunk.Append(22432434, 235)
	chunk.Append(32432434, 236)

	offsets, _ := chunk.Get(12432434)
	testOffsets := []uint64{233, 234}
	for idx, value := range offsets {
		assertEqual(t, value, testOffsets[idx], "")
	}
	offsets, _ = chunk.Get(22432434)
	testOffsets = []uint64{235}
	for idx, value := range offsets {
		assertEqual(t, value, testOffsets[idx], "")
	}
	offsets, _ = chunk.Get(32432434)
	testOffsets = []uint64{236}
	for idx, value := range offsets {
		assertEqual(t, value, testOffsets[idx], "")
	}
}
