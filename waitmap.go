//
//
//

package waitmap

import "time"
import "sync"
import "sync/atomic"

import "github.com/ondi/go-cache"
import "github.com/ondi/go-queue"

type WaitMap interface {
	Create(key interface{}, queue_size int) (ok bool)
	WaitCreate(key interface{}) (value interface{}, oki int)
	Wait(key interface{}) (value interface{}, oki int)
	Signal(key interface{}, value interface{}) int
	Remove(key interface{}) (ok bool)
	Close()
	Size() int
	Limit() int
	TTL() time.Duration
}

type WaitMap_t struct {
	mx sync.Mutex
	wm WaitMap
}

func New(limit int, ttl time.Duration) (self * WaitMap_t) {
	self = &WaitMap_t{}
	self.wm = NewOpen(&self.mx, limit, ttl)
	return
}

func (self * WaitMap_t) Create(key interface{}, queue_size int) (ok bool) {
	self.mx.Lock()
	ok = self.wm.Create(key, queue_size)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) WaitCreate(key interface{}) (value interface{}, oki int) {
	self.mx.Lock()
	value, oki = self.wm.WaitCreate(key)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Wait(key interface{}) (value interface{}, oki int) {
	self.mx.Lock()
	value, oki = self.wm.Wait(key)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Signal(key interface{}, value interface{}) (oki int) {
	self.mx.Lock()
	oki = self.wm.Signal(key, value)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Remove(key interface{}) (ok bool) {
	self.mx.Lock()
	ok = self.wm.Remove(key)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Close() {
	self.mx.Lock()
	self.wm.Close()
	self.wm = &WaitMapClosed_t{}
	self.mx.Unlock()
}

func (self * WaitMap_t) Size() (size int) {
	self.mx.Lock()
	size = self.wm.Size()
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Limit() (limit int) {
	self.mx.Lock()
	limit = self.wm.Limit()
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) TTL() (ttl time.Duration) {
	self.mx.Lock()
	ttl = self.wm.TTL()
	self.mx.Unlock()
	return
}

type Mapped_t struct {
	q queue.Queue
	ts time.Time
}

type WaitMapOpen_t struct {
	mx sync.Locker
	c * cache.Cache_t
	ttl time.Duration
	limit int
	running int32
}

func NewOpen(mx sync.Locker, limit int, ttl time.Duration) (self * WaitMapOpen_t) {
	self = &WaitMapOpen_t{mx: mx, c: cache.New()}
	if ttl <= 0 {
		ttl = time.Duration(1 << 63 - 1)
	}
	if limit <= 0 {
		limit = 1 << 63 - 1
	}
	self.ttl = ttl
	self.limit = limit
	self.running = 1
	go self.flushing()
	return
}

func (self * WaitMapOpen_t) __evict(ts time.Time, it * cache.Value_t, keep int) bool {
	if self.c.Size() > keep || ts.Sub(it.Value().(* Mapped_t).ts) > self.ttl {
		it.Value().(* Mapped_t).q.Close()
		self.c.Remove(it.Key())
		return true
	}
	return false
}

func (self * WaitMapOpen_t) __flush() {
	ts := time.Now()
	for it := self.c.Front(); it != self.c.End() && self.__evict(ts, it, self.limit); it = it.Next() {}
}

func (self * WaitMapOpen_t) flush() (ts time.Time, ok bool) {
	self.mx.Lock()
	self.__flush()
	if self.c.Size() > 0 {
		ts, ok = self.c.Front().Value().(* Mapped_t).ts, true
	}
	self.mx.Unlock()
	return
}

func (self * WaitMapOpen_t) flushing() {
	for atomic.LoadInt32(&self.running) > 0 {
		if least, ok := self.flush(); ok && time.Now().Sub(least) < self.ttl {
			time.Sleep(time.Now().Sub(least))
		} else {
			time.Sleep(self.ttl)
		}
	}
}

func (self * WaitMapOpen_t) Create(key interface{}, queue_size int) (ok bool) {
	_, ok = self.c.CreateBack(key, func() interface{} {return &Mapped_t{q: queue.NewOpen(self.mx, queue_size), ts: time.Now()}})
	self.__flush()
	return
}

func (self * WaitMapOpen_t) WaitCreate(key interface{}) (value interface{}, oki int) {
	it, ok := self.c.CreateBack(key, func() interface{} {return &Mapped_t{q: queue.NewOpen(self.mx, 0), ts: time.Now()}})
	self.__flush()
	if !ok {
		return nil, -1
	}
	v := it.Value().(* Mapped_t)
	if value, oki = v.q.PopFront(); oki == -1 {
		return
	}
	if v.q.Readers() == 0 {
		self.c.Remove(key)
	}
	return
}

func (self * WaitMapOpen_t) Wait(key interface{}) (value interface{}, oki int) {
	it, ok := self.c.PushBack(key, func() interface{} {return &Mapped_t{q: queue.NewOpen(self.mx, 0), ts: time.Now()}})
	self.__flush()
	v := it.Value().(* Mapped_t)
	if !ok {
		v.ts = time.Now()
	}
	if value, oki = v.q.PopFront(); oki == -1 {
		return
	}
	if v.q.Readers() == 0 {
		self.c.Remove(key)
	}
	return
}

func (self * WaitMapOpen_t) Signal(key interface{}, value interface{}) int {
	self.__flush()
	if it := self.c.Find(key); it != self.c.End() {
		it.Value().(* Mapped_t).q.PushBack(value)
		return 0
	}
	return -1
}

func (self * WaitMapOpen_t) Remove(key interface{}) (ok bool) {
	self.__flush()
	var it * cache.Value_t
	if it, ok = self.c.Remove(key); ok {
		it.Value().(* Mapped_t).q.Close()
	}
	return
}

func (self * WaitMapOpen_t) Close() {
	for it := self.c.Front(); it != self.c.End(); it = it.Next() {
		it.Value().(* Mapped_t).q.Close()
		self.c.Remove(it.Key())
	}
	atomic.StoreInt32(&self.running, 0)
}

func (self * WaitMapOpen_t) Size() int {
	self.__flush()
	return self.c.Size()
}

func (self * WaitMapOpen_t) Limit() int {
	return self.limit
}

func (self * WaitMapOpen_t) TTL() time.Duration {
	return self.ttl
}

type WaitMapClosed_t struct {}

func (* WaitMapClosed_t) Create(key interface{}, queue_size int) (ok bool) {
	return
}

func (* WaitMapClosed_t) WaitCreate(key interface{}) (value interface{}, oki int) {
	return nil, -1
}

func (* WaitMapClosed_t) Wait(key interface{}) (value interface{}, oki int) {
	return nil, -1
}

func (* WaitMapClosed_t) Signal(key interface{}, value interface{}) int {
	return -1
}

func (* WaitMapClosed_t) Remove(key interface{}) (ok bool) {
	return
}

func (* WaitMapClosed_t) Close() {
	return
}

func (* WaitMapClosed_t) Size() int {
	return 0
}

func (* WaitMapClosed_t) Limit() int {
	return 0
}

func (* WaitMapClosed_t) TTL() time.Duration {
	return 0
}
