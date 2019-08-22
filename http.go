// http
// author: baoqiang
// time: 2019-08-22 20:32
package groupcache

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	pb "github.com/githubao/xiao-groupcache/groupcachepb"
	"github.com/golang/protobuf/proto"
)

const (
	defaultBasePath = "/_groupcache/"
	defaultReplicas = 50
)

type HTTPPool struct {
	Context     func(r *http.Request) Context
	Transport   func(Context) http.RoundTripper
	self        string // base url
	opts        HTTPPoolOptions
	mu          sync.Mutex
	peers       *ConsistentMap
	httpGetters map[string]*httpGetter
}

type HTTPPoolOptions struct {
	BasePath string //http server path
	Replicas int
	HashFn   Hash
}

func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	// once init
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	// instance
	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}

	// opts
	if o != nil {
		p.opts = *o
	}

	// set default val
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}

	// consistent hash
	p.peers = NewConsistentMap(p.opts.Replicas, p.opts.HashFn)

	RegisterPeerPicker(func() PeerPicker { return p })

	return p
}

// HTTPPool http Handler
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	//todo why make a new?
	p.peers = NewConsistentMap(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(peers...)

	// peer is a valid url prefix
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{transport: p.Transport, baseURL: peer + p.opts.BasePath}
	}
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.peers.IsEmpty() {
		return nil, false
	}

	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// parse request
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: %v" + r.URL.Path)
	}

	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	//key := parts[1]

	// get target group
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	// todo core logic
	var value []byte

	// marshal data
	body, err := proto.Marshal(&pb.GetResponse{Value: value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// write resp
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(body)

}

// http getter
type httpGetter struct {
	transport func(Context) http.RoundTripper
	baseURL   string
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// httpGetter impl protoGetter
func (h *httpGetter) Get(context Context, in *pb.GetRequest, out *pb.GetResponse) error {
	u := fmt.Sprintf("%v%v/%v",
		h.baseURL, url.QueryEscape(in.GetGroup()), url.QueryEscape(in.GetKey()))

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}

	// http request
	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport(context)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}

	// close resp
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}

	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()

	// close
	defer bufferPool.Put(b)

	// copy respBody to buffer
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	// unmarshal it
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	return nil
}
