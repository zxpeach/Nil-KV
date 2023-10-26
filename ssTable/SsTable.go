package ssTable

import (
	"github.com/zxpeach/Nil-KV/bloomFilter"
	"os"
	"sync"
)

// SSTable 表，存储在磁盘文件中
type SSTable struct {
	// 文件句柄
	f        *os.File
	filePath string
	// 元数据
	tableMetaInfo MetaInfo
	// 文件的稀疏索引列表
	sparseIndex map[string]Position
	// 排序后的 key 列表
	sortIndex []string
	// SSTable 排他锁
	bloomfilter bloomFilter.BloomFilter
	// 布隆过滤器
	lock sync.Locker
}

func (table *SSTable) Init(path string) {
	table.filePath = path
	table.lock = &sync.Mutex{}
	table.loadFileHandle()
}
