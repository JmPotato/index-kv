# index-kv

📒A simple index model for special k-v storage

# Spec

* CPU 8 cores
* Memory 4G
* Disk HDD 4T

# Key Points

* 1T disordered data in a single file on disk.
    * Data struct: (key_size uint64, key bytes, value_size uint64, value bytes)
    * `key_size   uint64`
    * `key        bytes`    1B <= size < 1KB
    * `value_size uint64`
    * `value      bytes`    1B <= size < 1MB
* Getting muti values by keys concurrently.
* Pretreatment wiil be include in the total cost.

# Solution

* Bloom filter
* LRU Cache
* Hash & Sharding -> Index/Offset
    * Chunk struct: (key_hash uint64, n uint8, offset []uint64)
    * `key_hash uint64` The hash value of key
    * `n        uint8` The number of offset records
    * `offset   uint64 ...` The n offset records

# Reference

* [详解布隆过滤器的原理，使用场景和注意事项](https://zhuanlan.zhihu.com/p/43263751)
* [A Bloom Filter written in Go](https://github.com/willf/bloom)
* [General Purpose Hash Function Algorithms](https://www.partow.net/programming/hashfunctions/#AvailableHashFunctions)
* [哈希表之 BKDRHash 算法解析及扩展](https://blog.csdn.net/MyLinChi/article/details/79509455)
* [字符串哈希函数](https://blog.csdn.net/wanglx_/article/details/40300363)