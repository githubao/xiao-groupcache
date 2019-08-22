// sinks
// author: baoqiang
// time: 2019-08-22 21:59
package groupcache

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
