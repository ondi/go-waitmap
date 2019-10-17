//
//
//

package waitmap

import "sync"
import "time"
// import "runtime"

import "github.com/ondi/go-queue"

type queue_t struct {
	q queue.Queue
	readers int
}

type WaitMap_t struct {
	mx sync.Mutex
	dict map[interface{}]*queue_t
}

func New() (* WaitMap_t) {
	return &WaitMap_t{dict: map[interface{}]*queue_t{}}
}

func (self *WaitMap_t) WaitNew(key interface{}) (value interface{}, oki int) {
	self.mx.Lock()
	defer self.mx.Unlock()
	v, ok := self.dict[key];
	if !ok {
		v.q = queue.NewOpen(&self.mx, 0)
		self.dict[key] = v
	}
	v.readers++
	value, oki = v.q.PopFront()
	v.readers--
	if v.readers == 0 {
		delete(self.dict, key)
	}
	return
}

func (self *WaitMap_t) WaitNewTimeout(key interface{}, timeout time.Duration) (value interface{}, oki int) {
	self.mx.Lock()
	defer self.mx.Unlock()
	v, ok := self.dict[key]
	if !ok {
		v = &queue_t{q: queue.NewOpen(&self.mx, 0)}
		self.dict[key] = v
	}
	v.readers++
	start := time.Now()
	for {
		value, oki = v.q.PopFrontNoWait()
		if oki < 1 || time.Since(start) > timeout {
			v.readers--
			if v.readers == 0 {
				delete(self.dict, key)
			}
			return
		}
		self.mx.Unlock()
		time.Sleep(50 * time.Microsecond)
		// runtime.Gosched()
		self.mx.Lock()
	}
}

func (self *WaitMap_t) WaitExistingTimeout(key interface{}, timeout time.Duration) (value interface{}, oki int) {
	self.mx.Lock()
	defer self.mx.Unlock()
	v, ok := self.dict[key]
	if !ok {
		return nil, -1
	}
	v.readers++
	start := time.Now()
	for {
		value, oki = v.q.PopFrontNoWait()
		if oki < 1 || time.Since(start) > timeout {
			v.readers--
			if v.readers == 0 {
				delete(self.dict, key)
			}
			return
		}
		self.mx.Unlock()
		time.Sleep(50 * time.Microsecond)
		// runtime.Gosched()
		self.mx.Lock()
	}
}

func (self *WaitMap_t) Signal(key interface{}, value interface{}) int {
	self.mx.Lock()
	defer self.mx.Unlock()
	v, ok := self.dict[key]
	if !ok {
		return -1
	}
	return v.q.PushBack(value)
}

func (self *WaitMap_t) Size() int {
	self.mx.Lock()
	defer self.mx.Unlock()
	return len(self.dict)
}
