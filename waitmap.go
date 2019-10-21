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
	Create(ts time.Time, key interface{}, queue_size int) (ok bool)
	WaitCreate(ts time.Time, key interface{}) (value interface{}, oki int)
	Wait(ts time.Time, key interface{}) (value interface{}, oki int)
	Signal(ts time.Time, key interface{}, value interface{}) int
	Remove(ts time.Time, key interface{}) (ok bool)
	Close()
	Size(ts time.Time) int
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

func (self * WaitMap_t) Create(ts time.Time, key interface{}, queue_size int) (ok bool) {
	self.mx.Lock()
	ok = self.wm.Create(ts, key, queue_size)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) WaitCreate(ts time.Time, key interface{}) (value interface{}, oki int) {
	self.mx.Lock()
	value, oki = self.wm.WaitCreate(ts, key)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Wait(ts time.Time, key interface{}) (value interface{}, oki int) {
	self.mx.Lock()
	value, oki = self.wm.Wait(ts, key)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Signal(ts time.Time, key interface{}, value interface{}) (oki int) {
	self.mx.Lock()
	oki = self.wm.Signal(ts, key, value)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Remove(ts time.Time, key interface{}) (ok bool) {
	self.mx.Lock()
	ok = self.wm.Remove(ts, key)
	self.mx.Unlock()
	return
}

func (self * WaitMap_t) Close() {
	self.mx.Lock()
	self.wm.Close()
	self.wm = &WaitMapClosed_t{}
	self.mx.Unlock()
}

func (self * WaitMap_t) Size(ts time.Time) (size int) {
	self.mx.Lock()
	size = self.wm.Size(ts)
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
	go self.evicting()
	return
}

func (self * WaitMapOpen_t) __evict_one(ts time.Time, it * cache.Value_t, keep int) bool {
	if self.c.Size() > keep || ts.After(it.Value().(* Mapped_t).ts) {
		it.Value().(* Mapped_t).q.Close()
		self.c.Remove(it.Key())
		return true
	}
	return false
}

func (self * WaitMapOpen_t) __evict(ts time.Time) {
	for it := self.c.Front(); it != self.c.End() && self.__evict_one(ts, it, self.limit); it = it.Next() {}
}

func (self * WaitMapOpen_t) evict(ts time.Time) (least time.Time, ok bool) {
	self.mx.Lock()
	self.__evict(ts)
	if self.c.Size() > 0 {
		least, ok = self.c.Front().Value().(* Mapped_t).ts, true
	}
	self.mx.Unlock()
	return
}

func (self * WaitMapOpen_t) evicting() {
	for atomic.LoadInt32(&self.running) > 0 {
		ts := time.Now()
		if least, ok := self.evict(ts); ok && ts.Sub(least) < self.ttl {
			time.Sleep(ts.Sub(least))
		} else {
			time.Sleep(self.ttl)
		}
	}
}

func (self * WaitMapOpen_t) Create(ts time.Time, key interface{}, queue_size int) (ok bool) {
	_, ok = self.c.CreateBack(key, func() interface{} {return &Mapped_t{q: queue.NewOpen(self.mx, queue_size), ts: ts.Add(self.ttl)}})
	self.__evict(ts)
	return
}

func (self * WaitMapOpen_t) WaitCreate(ts time.Time, key interface{}) (value interface{}, oki int) {
	it, ok := self.c.CreateBack(key, func() interface{} {return &Mapped_t{q: queue.NewOpen(self.mx, 0), ts: ts.Add(self.ttl)}})
	self.__evict(ts)
	if !ok {
		return nil, -2
	}
	v := it.Value().(* Mapped_t)
	if value, oki = v.q.PopFront(); oki == 0 && v.q.Readers() == 0 {
		self.c.Remove(key)
	}
	return
}

func (self * WaitMapOpen_t) Wait(ts time.Time, key interface{}) (value interface{}, oki int) {
	it, ok := self.c.PushBack(key, func() interface{} {return &Mapped_t{q: queue.NewOpen(self.mx, 0), ts: ts.Add(self.ttl)}})
	self.__evict(ts)
	v := it.Value().(* Mapped_t)
	if !ok {
		v.ts = ts
	}
	if value, oki = v.q.PopFront(); oki == 0 && v.q.Readers() == 0 {
		self.c.Remove(key)
	}
	return
}

func (self * WaitMapOpen_t) Signal(ts time.Time, key interface{}, value interface{}) int {
	self.__evict(ts)
	if it := self.c.Find(key); it != self.c.End() {
		it.Value().(* Mapped_t).q.PushBack(value)
		return 0
	}
	return -1
}

func (self * WaitMapOpen_t) Remove(ts time.Time, key interface{}) (ok bool) {
	self.__evict(ts)
	var it * cache.Value_t
	if it, ok = self.c.Remove(key); ok {
		it.Value().(* Mapped_t).q.Close()
	}
	return
}

func (self * WaitMapOpen_t) Close() {
	for it := self.c.Front(); it != self.c.End(); it = it.Next() {
		it.Value().(* Mapped_t).q.Close()
	}
	self.c = cache.New()
	atomic.StoreInt32(&self.running, 0)
}

func (self * WaitMapOpen_t) Size(ts time.Time) int {
	self.__evict(ts)
	return self.c.Size()
}

func (self * WaitMapOpen_t) Limit() int {
	return self.limit
}

func (self * WaitMapOpen_t) TTL() time.Duration {
	return self.ttl
}

type WaitMapClosed_t struct {}

func (* WaitMapClosed_t) Create(ts time.Time, key interface{}, queue_size int) (ok bool) {
	return
}

func (* WaitMapClosed_t) WaitCreate(ts time.Time, key interface{}) (value interface{}, oki int) {
	return nil, -1
}

func (* WaitMapClosed_t) Wait(ts time.Time, key interface{}) (value interface{}, oki int) {
	return nil, -1
}

func (* WaitMapClosed_t) Signal(ts time.Time, key interface{}, value interface{}) int {
	return -1
}

func (* WaitMapClosed_t) Remove(ts time.Time, key interface{}) (ok bool) {
	return
}

func (* WaitMapClosed_t) Close() {
	return
}

func (* WaitMapClosed_t) Size(ts time.Time) int {
	return 0
}

func (* WaitMapClosed_t) Limit() int {
	return 0
}

func (* WaitMapClosed_t) TTL() time.Duration {
	return 0
}
