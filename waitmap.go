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

type Evict func(key interface{})

func Drop(key interface{}) {}

type WaitMap_t struct {
	mx    sync.Mutex
	c     *ttl_cache.Cache_t
	evict Evict
}

func New(limit int, ttl time.Duration, evict Evict) (self *WaitMap_t) {
	self = &WaitMap_t{}
	if ttl <= 0 {
		ttl = time.Duration(1<<63 - 1)
	}
	if limit <= 0 {
		limit = 1<<63 - 1
	}
	self.c = ttl_cache.New(limit, ttl, self.__evict)
	self.evict = evict
	go self.evicting(ttl)
	return
}

func (self *WaitMap_t) __evict(key interface{}, value interface{}) {
	value.(queue.Queue).Close()
	self.evict(key)
}

func (self *WaitMap_t) evicting(ttl time.Duration) {
	for {
		self.mx.Lock()
		diff, ok := self.c.LeastDiff(time.Now())
		self.mx.Unlock()
		if ok {
			time.Sleep(diff)
		} else {
			time.Sleep(ttl)
		}
	}
}

func (self *WaitMap_t) Size() (size int) {
	self.mx.Lock()
	size = self.c.Size()
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) Limit() (limit int) {
	self.mx.Lock()
	limit = self.c.Limit()
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) TTL() (ttl time.Duration) {
	self.mx.Lock()
	ttl = self.c.TTL()
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) Create(ts time.Time, key interface{}, queue_size int) (ok bool) {
	self.mx.Lock()
	_, ok = self.c.Create(
		ts,
		key,
		func() interface{} {
			return queue.NewOpen(&self.mx, queue_size)
		},
	)
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) WaitCreate(ts time.Time, key interface{}, queue_size int) (value interface{}, oki int) {
	self.mx.Lock()
	res, ok := self.c.Create(
		ts,
		key,
		func() interface{} {
			return queue.NewOpen(&self.mx, queue_size)
		},
	)
	if !ok {
		self.mx.Unlock()
		return nil, -2
	}
	v := res.(queue.Queue)
	if value, oki = v.PopFront(); oki == 0 && v.Readers() == 0 {
		self.c.Remove(ts, key)
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) Wait(ts time.Time, key interface{}, queue_size int) (value interface{}, oki int) {
	self.mx.Lock()
	res, _ := self.c.Write(
		ts,
		key,
		func() interface{} {
			return queue.NewOpen(&self.mx, queue_size)
		},
		func(prev interface{}) interface{} {
			return prev
		},
	)
	v := res.(queue.Queue)
	if value, oki = v.PopFront(); oki == 0 && v.Readers() == 0 {
		self.c.Remove(ts, key)
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) Signal(ts time.Time, key interface{}, value interface{}) (ok bool) {
	self.mx.Lock()
	var res interface{}
	if res, ok = self.c.Get(ts, key); ok {
		res.(queue.Queue).PushBackNoWait(value)
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) Remove(ts time.Time, key interface{}) (ok bool) {
	self.mx.Lock()
	var res interface{}
	if res, ok = self.c.Remove(ts, key); ok {
		res.(queue.Queue).Close()
	}
	self.mx.Unlock()
	return
}

func (self *WaitMap_t) Range(ts time.Time, f func(key interface{}, diff time.Duration) bool) {
	self.mx.Lock()
	self.c.RangeTs(ts, func(key interface{}, value interface{}, diff time.Duration) bool {
		return f(key, diff)
	})
	self.mx.Unlock()
}

func (self *WaitMap_t) Flush(ts time.Time) {
	self.mx.Lock()
	self.c.FlushLimit(ts, 0)
	self.mx.Unlock()
}
