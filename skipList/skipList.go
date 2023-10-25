package skipList

import "github.com/zxpeach/Lsm-Tree/kv"

import (
	"sync"
)

type Node struct {
	KV  kv.Value
	nxt *Node
	pre *Node
}

// skipList 跳表
type SkipList struct {
	header *Node
	count  int
	rWLock *sync.RWMutex
}
