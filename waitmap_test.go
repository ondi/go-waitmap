//
//
//

package waitmap

import (
	"fmt"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestWaitMap1(t *testing.T) {
	m := New[string](1, 1*time.Second, Drop)
	_, oki := m.PushWait(time.Now(), "lalala", 0, true)
	assert.Assert(t, oki == false, oki)
}

func TestWaitMap2(t *testing.T) {
	m := New[string](1, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	assert.Assert(t, ok, fmt.Sprintf("Signal: true != %v", ok))

	value, oki := m.PushWait(time.Now(), "lalala", 0, true)
	assert.Assert(t, oki, fmt.Sprintf("Wait: true != %v, %v", oki, value))
}

func TestWaitMap3(t *testing.T) {
	m := New[string](1, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	assert.Assert(t, ok, fmt.Sprintf("Signal: true != %v", ok))

	value, oki := m.FindWait(time.Now(), "lalala", true)
	assert.Assert(t, oki, fmt.Errorf("FindWait: true != %v, %v", oki, value))
}

func TestWaitMap4(t *testing.T) {
	m := New[string](1, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	assert.Assert(t, ok, fmt.Sprintf("Signal: true != %v", ok))

	value, oki := m.FindWait(time.Now(), "bububu", true)
	assert.Assert(t, oki == false, fmt.Errorf("FindWait: false != %v, %v", oki, value))
}
