package test

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

const (
	CACHE_SIZE = 500
	TEST_COUNT = 10000
)

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func TestCache(t *testing.T) {
	var cacheSize = 100
	l, _ := lru.New(cacheSize)
	for i := 0; i < 256; i++ {
		l.Add(i, i)
	}
	if l.Len() != cacheSize {
		panic(fmt.Sprintf("Bad cache size: %v", l.Len()))
	}

	var hitCount, totalCount float64
	for i := 0; i < TEST_COUNT; i++ {
		totalCount++
		key := seededRand.Intn(TEST_COUNT)
		value, ok := l.Get(key)
		if ok && key == value {
			hitCount++
			log.Printf("Hit key=%d", key)
			continue
		}
		log.Printf("Not hit key=%d", key)
	}

	log.Printf("Hit Rate: %.2f", hitCount/totalCount)
}
