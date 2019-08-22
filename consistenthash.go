// 一致性hash
// author: baoqiang
// time: 2019-08-20 21:17
package groupcache

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type ConsistentMap struct {
	hash     Hash
	replicas int
	keys     []int // sorted
	hashMap  map[int]string
}

func NewConsistentMap(replicas int, fn Hash) *ConsistentMap {
	m := &ConsistentMap{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *ConsistentMap) IsEmpty() bool {
	return len(m.keys) == 0
}

// 添加几个节点，replicas代表需要多少个虚拟节点
func (m *ConsistentMap) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i > m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// which replies to use
func (m *ConsistentMap) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash([]byte(key)))

	// search for closest replies
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
