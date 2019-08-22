// immutable string or bytes
// used as value type rather than pointer

// author: baoqiang
// time: 2019-08-22 21:48
package groupcache

import (
	"bytes"
	"errors"
	"io"
	"strings"
)

type ByteView struct {
	b []byte // first use
	s string
}

func (v ByteView) Len() int {
	if v.b != nil {
		return len(v.b)
	}
	return len(v.s)
}

// a data copy
func (v ByteView) ByteSlice() []byte {
	if v.b != nil {
		return cloneBytes(v.b)
	}
	return []byte(v.s)
}

func (v ByteView) At(i int) byte {
	if v.b != nil {
		return v.b[i]
	}
	return v.s[i]
}

// a new sub instance
func (v ByteView) Slice(from, to int) ByteView {
	if v.b != nil {
		return ByteView{b: v.b[from:to]}
	}
	return ByteView{s: v.s[from:to]}
}

func (v ByteView) SliceFrom(from int) ByteView {
	if v.b != nil {
		return ByteView{b: v.b[from:]}
	}
	return ByteView{s: v.s[from:]}
}

func (v ByteView) Copy(dest []byte) int {
	if v.b != nil {
		return copy(dest, v.b)
	}
	return copy(dest, v.s)
}

func (v ByteView) Equal(b2 ByteView) bool {
	if b2.b == nil {
		return v.EqualString(b2.s)
	}
	return v.EqualBytes(b2.b)
}

func (v ByteView) EqualString(s string) bool {
	if v.b == nil {
		return v.s == s
	}

	l := v.Len()
	if len(s) != l {
		return false
	}

	for i, bi := range v.b {
		if bi != s[i] {
			return false
		}
	}

	return true
}

func (v ByteView) EqualBytes(b2 []byte) bool {
	if v.b != nil {
		return bytes.Equal(v.b, b2)
	}
	l := v.Len()
	if len(b2) != l {
		return false
	}
	for i, bi := range b2 {
		if bi != v.s[i] {
			return false
		}
	}
	return true
}

func (v ByteView) Reader() io.ReadSeeker {
	if v.b != nil {
		return bytes.NewReader(v.b)
	}
	return strings.NewReader(v.s)
}

func (v ByteView) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("view: invalid offset")
	}
	if off >= int64(v.Len()) {
		return 0, io.EOF
	}

	n = v.SliceFrom(int(off)).Copy(p)
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (v ByteView) WriteTo(w io.Writer) (n int64, err error) {
	var m int

	if v.b != nil {
		m, err = w.Write(v.b)
	} else {
		m, err = io.WriteString(w, v.s)
	}

	if err == nil && m < v.Len() {
		err = io.ErrShortWrite
	}

	n = int64(m)

	return
}
