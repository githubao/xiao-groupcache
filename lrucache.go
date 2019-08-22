// lru cache
// author: baoqiang
// time: 2019-08-22 19:49
package groupcache

import "container/list"

type LruCache struct {
	MaxEntries int                              // 超过大小会被逐出
	OnEvicted  func(key Key, value interface{}) //逐出后的callback函数
	ll         *list.List
	cache      map[interface{}]*list.Element
}

type Key interface{}

type entry struct {
	key   Key
	value interface{}
}

func NewLruCache(maxEntries int) *LruCache {
	return &LruCache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

func (c *LruCache) Add(key Key, value interface{}) {
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}

	// add exist one, update
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return
	}

	// add a new one, insert
	ele := c.ll.PushFront(&entry{key, value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

func (c *LruCache) Get(key Key) (value interface{}, ok bool) {
	if c.cache == nil {
		return
	}

	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

func (c *LruCache) Remove(key Key) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

func (c *LruCache) RemoveOldest() {
	if c.cache == nil {
		return
	}

	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *LruCache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

func (c *LruCache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

func (c *LruCache) Clear() {
	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry)
			c.OnEvicted(kv.key, kv.value)
		}
	}

	c.ll = nil
	c.cache = nil
}
