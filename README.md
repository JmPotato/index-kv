# index-kv

📒A simple index model for special k-v storage

# Spec

* CPU 8 cores
* Memory 4G
* Disk HDD 4T

# Key Point

* 1T disordered data in a single file on disk.
    * Data struct: (key_size uint64, key bytes, value_size uint64, value bytes)
    * `key_size   uint64`
    * `key        bytes`    1B <= size < 1KB
    * `value_size uint64`
    * `value      bytes`    1B <= size < 1MB
* Getting muti values by keys concurrently.
* Pretreatment wiil be include in the total cost.

# Solution

* ~~`Bloom Filter` checks whether key exists~~
* `LRU Cache` speeds up the querying
* `Hash & Sharding` -> Index/Offset
    * Chunk struct: (key_hash uint64, offset uint64)
    * `key_hash uint64` The hash value of key
    * `offset   uint64` The offset of key in the real data file

# Unit test

* `data_test.go` Unit tests for data generator.
* `chunk_test.go` Unit tests for chunk Create/Append/Get.
* `cache_test.go` Unit tests for LRU cache.
* `index_test.go` Unit tests for Index model, including Create/Get/MGet.

# Benchmark

Because I'm using an old poor 13-inch MBP Early 2015 which only has less than 100GB disk storage and very low CPU performance. The best I can do is to generate around 100000 pairs k-v using the random k-v data generator I wrote. So the benchmarks below may not be very accurate. Sorry :-(

* Create index for 100000 pairs k-v random disordered data(52.51GB)
    * Time cost: 233.050s or 3.88mins
    * Storage cost: 29320 Chunks, total 1.8MB

```shell
goos: darwin
goarch: amd64
pkg: github.com/JmPotato/index-kv/test
BenchmarkIndexCreate
2020/05/13 18:03:07 Chunk file not found. Create index first...
BenchmarkIndexCreate-4   	       1	231705919706 ns/op	52982909896 B/op	 1593649 allocs/op
PASS
ok  	github.com/JmPotato/index-kv/test	233.050s
```

* Get random 10000 keys with index and cache
    * Time cost: 4500ms total, 0.45ms per key

* Concurrently get random 10000 keys with index and cache
    * Time cost: 3602ms total, 0.36ms per key

# Reference

* [详解布隆过滤器的原理，使用场景和注意事项](https://zhuanlan.zhihu.com/p/43263751)
* [A Bloom Filter written in Go](https://github.com/willf/bloom)
* [General Purpose Hash Function Algorithms](https://www.partow.net/programming/hashfunctions/#AvailableHashFunctions)
* [哈希表之 BKDRHash 算法解析及扩展](https://blog.csdn.net/MyLinChi/article/details/79509455)
* [字符串哈希函数](https://blog.csdn.net/wanglx_/article/details/40300363)