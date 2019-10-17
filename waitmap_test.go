//
//
//

package waitmap

import "time"
import "testing"

func TestWaitMap1(t * testing.T) {
	m := New()
	_, oki := m.WaitNewTimeout("lalala", 1 * time.Second)
	if oki != 1 {
		t.Errorf("WaitNewTimeout: 1 != %v", oki)
	}
}

func TestWaitMap2(t * testing.T) {
	m := New()
	
	go func() {
		time.Sleep(100 * time.Millisecond)
		oki := m.Signal("lalala", "bububu")
		if oki != 0 {
			t.Errorf("Signal: 0 != %v", oki)
		}
	}()
	
	value, oki := m.WaitNew("lalala")
	if oki != 0 {
		t.Errorf("WaitNew: 0 != %v, %v", oki, value)
	}
}

func TestWaitMap3(t * testing.T) {
	m := New()
	
	go func() {
		time.Sleep(100 * time.Millisecond)
		oki := m.Signal("lalala", "bububu")
		if oki != 0 {
			t.Errorf("Signal: 0 != %v", oki)
		}
	}()
	
	value, oki := m.WaitNewTimeout("lalala", 1 * time.Second)
	if oki != 0 {
		t.Errorf("WaitNewTimeout: 0 != %v, %v", oki, value)
	}
}
