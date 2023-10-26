package bloomFilter

import (
	"math"
	"math/rand"
)

type BloomFilter struct {
	size int      //data 数组大小
	len  uint32   // m
	k    int      // 哈希个数
	seed []uint32 // 哈希种子（进制哈希）
	data []uint64 // bitset本体
}

func calcHash(b []byte, seed uint32) uint32 {
	res := uint32(0)
	for c := range b {
		res *= seed
		res += uint32(c)
	}
	return res
}
func calcLen(n int, p float64) int { //n个元素，预期概率为 p 计算bitmap位数
	return int(-math.Log(p)*float64(n)*math.Log2E*math.Log2E) + 1
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
func (a *BloomFilter) init(n int, p float64) {
	a.len = uint32(calcLen(n, p))
	a.k = max(1, int(0.69*float64(a.len)/float64(n)))
	a.seed = make([]uint32, a.k)
	for i := 0; i < a.k; i++ {
		a.seed[i] = uint32(rand.Intn(1919810))
	}
	a.size = int(a.len)/64 + 1
	a.data = make([]uint64, a.size)
}

func (a *BloomFilter) Insert(s []byte) {
	for i := 0; i < a.k; i++ {
		key := calcHash(s, a.seed[i]) % a.len
		a.data[key/64] |= 1 << (key & 63)
	}
}

func (a *BloomFilter) Check(s []byte) bool {
	for i := 0; i < a.k; i++ {
		key := calcHash(s, a.seed[i]) % a.len
		if a.data[key/64]>>(key&63)&1 != 1 {
			return false
		}
	}
	return true
}
