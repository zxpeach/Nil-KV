package lsm

import (
	"github.com/zxpeach/Nil-KV/config"
	"github.com/zxpeach/Nil-KV/kv"
	"github.com/zxpeach/Nil-KV/skipList"
	"github.com/zxpeach/Nil-KV/ssTable"
	"github.com/zxpeach/Nil-KV/wal"
)

type (
	// 功能集合
	NilAPI interface {
		Start() error
		Set(key string, value []byte) error
		Get(key string) (*kv.Value, error)
		Del(key string) error
		Close() error
	}
	Database struct {
		con config.Config
		// 内存表
		MemoryTree *skipList.SkipList
		// SSTable 列表
		TableTree *ssTable.TableTree
		// WalF 文件句柄
		Wal *wal.Wal
	}
)

// 数据库，全局唯一实例
var database *Database
