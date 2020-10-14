//
//
//

package waitmap

import (
	"testing"
	"time"
)

func TestWaitMap1(t *testing.T) {
	m := New(0, 1*time.Second, Drop)
	_, oki := m.Wait(time.Now(), "lalala", 0)
	if oki != -1 {
		t.Errorf("Wait: -1 != %v", oki)
	}
}

func TestWaitMap2(t *testing.T) {
	m := New(0, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	if !ok {
		t.Errorf("Signal: true != %v", ok)
	}

	value, oki := m.Wait(time.Now(), "lalala", 0)
	if oki != 0 {
		t.Errorf("Wait: 0 != %v, %v", oki, value)
	}
}

func TestWaitMap3(t *testing.T) {
	m := New(0, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	if !ok {
		t.Errorf("Signal: true != %v", ok)
	}

	value, oki := m.WaitCreate(time.Now(), "lalala", 0)
	if oki != -2 {
		t.Errorf("WaitCreate: -2 != %v, %v", oki, value)
	}
}
