//
//
//

package waitmap

import (
	"sync"
	"time"

	"github.com/ondi/go-queue"
	ttl_cache "github.com/ondi/go-ttl-cache"
)

type Evict func(key string)

func Drop(key string) {}

type WaitMap_t[Value_t any] struct {
	mx    sync.Mutex
	c     *ttl_cache.Cache_t[string, queue.Queue[Value_t]]
	evict Evict
}

func New[Value_t any](limit int, ttl time.Duration, evict Evict) (self *WaitMap_t[Value_t]) {
	self = &WaitMap_t[Value_t]{}
	if ttl < 0 {
		ttl = time.Duration(1<<63 - 1)
	}
	if limit < 0 {
		limit = 1<<63 - 1
	}
	self.c = ttl_cache.New(limit, ttl, self.__evict)
	self.evict = evict
	go self.evicting(ttl)
	return
}

func (self *WaitMap_t[Value_t]) __evict(key string, value queue.Queue[Value_t]) {
	value.Close()
	self.evict(key)
}

func (self *WaitMap_t[Value_t]) evicting(ttl time.Duration) {
	for {
		self.mx.Lock()
		now := time.Now()
		ts, ok := self.c.LeastTs(now)
		self.mx.Unlock()
		if ok {
			time.Sleep(ts.Sub(now))
		} else {
			time.Sleep(ttl)
		}
	}
}

func (self *WaitMap_t[Value_t]) Size(ts time.Time) (size int) {
	self.mx.Lock()
	size = self.c.Size(ts)
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) Limit() (limit int) {
	self.mx.Lock()
	limit = self.c.Limit()
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) TTL() (ttl time.Duration) {
	self.mx.Lock()
	ttl = self.c.TTL()
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) Create(ts time.Time, key string, queue_size int) (ok bool) {
	self.mx.Lock()
	_, ok = self.c.Create(
		ts,
		key,
		func() queue.Queue[Value_t] {
			return queue.NewOpen[Value_t](&self.mx, queue_size)
		},
		func(p *queue.Queue[Value_t]) {},
	)
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) CreateWait(ts time.Time, key string, queue_size int) (value Value_t, oki int) {
	self.mx.Lock()
	res, ok := self.c.Create(
		ts,
		key,
		func() queue.Queue[Value_t] {
			return queue.NewOpen[Value_t](&self.mx, queue_size)
		},
		func(p *queue.Queue[Value_t]) {},
	)
	if !ok {
		self.mx.Unlock()
		oki = -2
		return
	}
	if value, oki = res.PopFront(); oki == 0 && res.Readers() == 0 {
		self.c.Remove(ts, key)
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) PushWait(ts time.Time, key string, queue_size int) (value Value_t, oki int) {
	self.mx.Lock()
	res, _ := self.c.Push(
		ts,
		key,
		func() queue.Queue[Value_t] {
			return queue.NewOpen[Value_t](&self.mx, queue_size)
		},
		func(p *queue.Queue[Value_t]) {},
	)
	if value, oki = res.PopFront(); oki == 0 && res.Readers() == 0 {
		self.c.Remove(ts, key)
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) Signal(ts time.Time, key string, value Value_t) (ok bool) {
	self.mx.Lock()
	res, ok := self.c.Get(ts, key)
	if ok {
		res.PushBackNoWait(value)
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) Remove(ts time.Time, key string) (ok bool) {
	self.mx.Lock()
	res, ok := self.c.Remove(ts, key)
	if ok {
		res.Close()
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t[Value_t]) Range(ts time.Time, f func(key string, ts time.Time) bool) {
	self.mx.Lock()
	self.c.RangeTs(ts, func(key string, value queue.Queue[Value_t], ts time.Time) bool {
		return f(key, ts)
	})
	self.mx.Unlock()
}

func (self *WaitMap_t[Value_t]) Flush(ts time.Time) {
	self.mx.Lock()
	self.c.FlushLimit(ts, 0)
	self.mx.Unlock()
}
