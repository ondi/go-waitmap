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
	assert.Assert(t, oki == -1, fmt.Sprintf("Wait: -1 != %v", oki))
}

func TestWaitMap2(t *testing.T) {
	m := New[string](1, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	assert.Assert(t, ok, fmt.Sprintf("Signal: true != %v", ok))

	value, oki := m.PushWait(time.Now(), "lalala", 0, true)
	assert.Assert(t, oki == 0, fmt.Sprintf("Wait: 0 != %v, %v", oki, value))
}

func TestWaitMap3(t *testing.T) {
	m := New[string](1, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	assert.Assert(t, ok, fmt.Sprintf("Signal: true != %v", ok))

	value, oki := m.FindWait(time.Now(), "lalala", true)
	assert.Assert(t, oki == 0, fmt.Errorf("FindWait: 0 != %v, %v", oki, value))
}

func TestWaitMap4(t *testing.T) {
	m := New[string](1, 1*time.Second, Drop)

	m.Create(time.Now(), "lalala", 1)
	ok := m.Signal(time.Now(), "lalala", "bububu")
	assert.Assert(t, ok, fmt.Sprintf("Signal: true != %v", ok))

	value, oki := m.FindWait(time.Now(), "bububu", true)
	assert.Assert(t, oki == -3, fmt.Errorf("FindWait: -3 != %v, %v", oki, value))
}
