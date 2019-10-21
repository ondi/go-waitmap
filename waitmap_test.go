//
//
//

package waitmap

import "time"
// import "sync"
import "testing"

func TestWaitMap1(t * testing.T) {
	m := New(0, 1 * time.Second)
	_, oki := m.Wait(time.Now(), "lalala")
	if oki != -1 {
		t.Errorf("WaitNewTimeout: -1 != %v", oki)
	}
}

func TestWaitMap2(t * testing.T) {
	m := New(0, 1 * time.Second)
	
	m.Create(time.Now(), "lalala", 1)
	oki := m.Signal(time.Now(), "lalala", "bububu")
	if oki != 0 {
		t.Errorf("Signal: 0 != %v", oki)
	}
	
	value, oki := m.Wait(time.Now(), "lalala")
	if oki != 0 {
		t.Errorf("Wait: 0 != %v, %v", oki, value)
	}
}

func TestWaitMap3(t * testing.T) {
	m := New(0, 1 * time.Second)
	
	m.Create(time.Now(), "lalala", 1)
	oki := m.Signal(time.Now(), "lalala", "bububu")
	if oki != 0 {
		t.Errorf("Signal: 0 != %v", oki)
	}
	
	value, oki := m.WaitCreate(time.Now(), "lalala")
	if oki != -1 {
		t.Errorf("WaitCreate: -1 != %v, %v", oki, value)
	}
}
