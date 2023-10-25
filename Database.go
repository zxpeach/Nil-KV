package lsm

import (
	"github.com/zxpeach/Lsm-Tree/skipList"
	"github.com/zxpeach/Lsm-Tree/ssTable"
	"github.com/zxpeach/Lsm-Tree/wal"
)

type Database struct {
	// 内存表
	MemoryTree *skipList.SkipList
	// SSTable 列表
	TableTree *ssTable.TableTree
	// WalF 文件句柄
	Wal *wal.Wal
}

// 数据库，全局唯一实例
var database *Database
