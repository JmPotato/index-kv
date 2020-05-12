package constdef

const (
	DATA_FILENAME  = "kv-data.pingcap"
	MIN_KEY_SIZE   = 1
	MAX_KEY_SIZE   = 1024
	MIN_VALUE_SIZE = 1
	MAX_VALUE_SIZE = 1048576

	BKDR_HASH_SEED     = 131 // 31 131 1313 13131 131313 etc..
	CHUNK_DIR          = "./chunks/"
	CHUNK_SIZE         = 67108864   // Sharding chunk size: 64MB
	CACHE_SIZE         = 1073741824 // Cache size: 1024MB
	BLOOM_FILTER_BITS  = 31         // The bit number of Bloom Filter
	BLOOM_FILTER_FUNCS = 5          // The hash function number of Bloom Filter
)
