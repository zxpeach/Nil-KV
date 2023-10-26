package skipList

import (
	"fmt"
	"github.com/zxpeach/Lsm-Tree/kv"
	"log"
	"math/rand"
)

import (
	"sync"
)

const MaxLevel int = 30

type Node struct {
	KV kv.Value
}

type Element struct { //跳表中的元素
	data    Node
	hashKey uint64     //进行快速计算的哈希值
	levels  []*Element //跳表某层指向后继的指针
}

// skipList 跳表

type SkipList struct {
	header *Element
	count  int
	rWLock *sync.RWMutex
}

func (list *SkipList) GetCount() int {
	return list.count
}

func (list *SkipList) Init() {
	fmt.Println("skiplist init")
	list.rWLock = &sync.RWMutex{}
	var newkv *kv.Value
	newNode := &Node{
		KV: *newkv,
	}
	newHeader := &Element{
		data:    *newNode,
		levels:  make([]*Element, MaxLevel),
		hashKey: 0,
	}
	list.header = newHeader
	list.count = 0
}

func (list *SkipList) Search(key string) (kv.Value, kv.SearchResult) {
	list.rWLock.RLock()
	defer list.rWLock.RUnlock()

	if list == nil {
		log.Fatal("The list is nil")
		return kv.Value{}, kv.None
	}

	pre := list.header
	hashKey := calcHashKey(key)
	for i := MaxLevel - 1; i >= 0; i-- {
		for nxt := pre.levels[i]; nxt != nil; nxt = pre.levels[i] {
			res := cmp(hashKey, key, nxt)
			if res == -1 {
				break
			}
			if res == 0 {
				return nxt.data.KV, kv.Success
			}
			pre = nxt
		}
	}
	return kv.Value{}, kv.None
}

func calcHashKey(key string) uint64 {
	len := len(key)
	if len > 8 {
		len = 8
	}
	var res uint64
	for i := 0; i < len; i++ {
		res |= uint64(key[i]) << (64 - (i+1)*8)
	}
	return res
}
func (a *Element) calcHashKey() {
	a.hashKey = calcHashKey(a.data.KV.Key)
}
func cmp(hashKey uint64, key string, b *Element) int {
	if hashKey == b.hashKey {
		if key == b.data.KV.Key {
			return 0
		}
		if key > b.data.KV.Key {
			return 1
		}
		return -1
	}
	if hashKey < b.hashKey {
		return -1
	} else {
		return 1
	}
}

func checkUp() bool {
	return rand.Intn(2) == 1
}
func (list *SkipList) Set(key string, value []byte) (oldValue kv.Value, hasOld bool) {
	list.rWLock.Lock()
	defer list.rWLock.Unlock()

	if list == nil {
		log.Fatal("The list is nil")
	}
	pre := list.header
	hashKey := calcHashKey(key)
	var preElemHeaders [MaxLevel]*Element //注意上界，可能需要调整或取min
	for i := len(list.header.levels) - 1; i >= 0; i-- {
		preElemHeaders[i] = pre
		for nxt := pre.levels[i]; nxt != nil; nxt = pre.levels[i] {
			res := cmp(hashKey, key, nxt)
			preElemHeaders[i] = pre
			if res == -1 {
				break
			}
			if res == 0 {
				oldKV := nxt.data.KV.Copy()
				nxt.data.KV.Value = value
				return *oldKV, true
			}
			pre = nxt
		}
	}
	len := 0
	for checkUp() {
		len++
	}
	list.count++
	var element *Element = new(Element)
	element.data.KV.Key = key
	element.data.KV.Value = value
	element.hashKey = hashKey
	element.levels = make([]*Element, len)
	for i := 0; i < len; i++ {
		element.levels[i] = preElemHeaders[i].levels[i]
		preElemHeaders[i].levels[i] = element
	}
	return kv.Value{}, false
}

func (list *SkipList) Delete(key string) (oldValue kv.Value, hasOld bool) {
	list.rWLock.Lock()
	defer list.rWLock.Unlock()
	var itor *Element
	if list == nil {
		log.Fatal("The list is nil")
	}
	pre := list.header
	hashKey := calcHashKey(key)
	var preElemHeaders [MaxLevel]*Element //注意上界，可能需要调整或取min
	for i := len(list.header.levels) - 1; i >= 0; i-- {
		preElemHeaders[i] = pre
		for nxt := pre.levels[i]; nxt != nil; nxt = pre.levels[i] {
			res := cmp(hashKey, key, nxt)
			preElemHeaders[i] = pre
			if res == -1 {
				break
			}
			if res == 0 {
				itor = nxt
				break
			}
			pre = nxt
		}
	}
	if itor == nil {
		return kv.Value{}, false
	}
	oldKV := itor.data.KV.Copy()
	for i := 0; i < len(itor.levels); i++ {
		preElemHeaders[i].levels[i] = itor.levels[i]
		if itor.levels[i] != nil {
			itor.levels[i].levels[i] = preElemHeaders[i]
		}
	} //旧数据由于不被指向会自动gc掉，不用管
	list.count--
	return *oldKV, true
}

func (list *SkipList) GetValues() []kv.Value {
	list.rWLock.RLock()
	defer list.rWLock.RUnlock()

	values := make([]kv.Value, 0)
	if list == nil {
		log.Fatal("The list is nil")
		return values
	}
	for now := list.header; now.levels[0] != nil; now = now.levels[0] {
		if now != list.header {
			values = append(values, now.data.KV)
		}
	}
	return values
}

func (list *SkipList) Swap() *SkipList {
	list.rWLock.Lock()
	defer list.rWLock.Unlock()

	newlist := &SkipList{}
	newlist.Init()
	cheader := newlist.header
	newlist.header = list.header
	newlist.count = list.count
	list.header = cheader
	list.count = 0
	return newlist
}
