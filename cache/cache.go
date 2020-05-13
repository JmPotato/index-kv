package cache

import (
	"log"

	lru "github.com/hashicorp/golang-lru"
)

type KVCache struct {
	cache *lru.Cache
}

func New(size int) (*KVCache, error) {
	var (
		kvc *KVCache = &KVCache{}
		err error    = nil
	)
	kvc.cache, err = lru.New(size)
	if err != nil {
		log.Fatalf("[Cache New] Create LRU cache error=%v", err)
		return nil, err
	}

	return kvc, nil
}

func (kvc *KVCache) Get(key string) (value string, exist bool) {
	valueInterface, ok := kvc.cache.Get(key)
	if !ok || valueInterface == nil {
		return "", false
	}
	value, ok = valueInterface.(string)
	if !ok {
		return "", false
	}

	return value, true
}

func (kvc *KVCache) Add(key, value string) {
	kvc.cache.Add(key, value)
}
