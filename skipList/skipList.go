package skipList

import "github.com/whuanle/lsm/kv"

import (
	"sync"
)

type Node struct {
	KV  kv.Value
	nxt *Node
	pre *Node
}

// skipList 跳表
type skipList struct {
	header *Node
	count  int
	rWLock *sync.RWMutex
}
