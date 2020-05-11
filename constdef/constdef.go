package constdef

const (
	MIN_KEY_SIZE   = 1
	MAX_KEY_SIZE   = 1024
	MIN_VALUE_SIZE = 1
	MAX_VALUE_SIZE = 1048576

	CHUNK_SIZE         = 67108864   // Sharding chunk size: 64MB
	CACHE_SIZE         = 1073741824 // Cache size: 1024MB
	BLOOM_FILTER_BITS  = 31         // The bit number of Bloom Filter
	BLOOM_FILTER_FUNCS = 3          // The hash function number of Bloom Filter
)
