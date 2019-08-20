// 自己写single flight
// author: baoqiang
// time: 2019-08-20 20:27
package groupcache

import "sync"

type cell struct {
	data interface{}
	err  error
	wg   sync.WaitGroup
}

type SingleFlightGroup struct {
	m  map[string]*cell
	mu sync.Mutex
}

func (g *SingleFlightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*cell)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.data, c.err
	}

	c := &cell{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.data, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.data, c.err
}
