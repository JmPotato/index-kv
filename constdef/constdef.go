package constdef

const (
	MAX_ROUTINE_LIMIT = 700 // The process ulimit number of system
)

const (
	KV_NUMBER     = 1000
	CHUNK_DIR     = "./chunks/"
	DATA_FILENAME = "kv-data.pingcap"

	BLOOM_FILTER_BITS  = 31 // The bit number of Bloom Filter
	BLOOM_FILTER_FUNCS = 5  // The hash function number of Bloom Filter

	BKDR_HASH_SEED = 131 // 31 131 1313 13131 131313 etc..

	CHUNK_NUM = 1000 // Sharding chunk number

	CACHE_SIZE = 100 // Cache size
)

const (
	MIN_KEY_SIZE   = 1
	MAX_KEY_SIZE   = 1024
	MIN_VALUE_SIZE = 1
	MAX_VALUE_SIZE = 1048576
)
