package ring

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentGoroutines mixes several writer and reader handles over one
// shared buffer. Every published sequence must be delivered exactly once.
func TestConcurrentGoroutines(t *testing.T) {
	cfg := testCfg(t, "conc", 4096)
	creator, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer creator.Close()

	const writers = 4
	const perWriter = 200
	var publishedCount int64

	// Each message embeds writer id + index for end-to-end verification.
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			h, _, err := Mount(cfg)
			if err != nil {
				t.Errorf("writer mount: %v", err)
				return
			}
			defer h.Close()
			for i := 0; i < perWriter; i++ {
				msg := fmt.Sprintf("w%02d-m%04d", wid, i)
				for {
					if _, err := h.Write([]byte(msg)); err != nil {
						if errors.Is(err, ErrBufferFull) {
							runtime.Gosched()
							continue
						}
						t.Errorf("write: %v", err)
						return
					}
					atomic.AddInt64(&publishedCount, 1)
					break
				}
			}
		}(w)
	}

	const readers = 3
	var mu sync.Mutex
	got := make(map[string]int) // message -> delivery count
	var readCount int64
	stop := make(chan struct{})

	for rd := 0; rd < readers; rd++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, _, err := Mount(cfg)
			if err != nil {
				t.Errorf("reader mount: %v", err)
				return
			}
			defer h.Close()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, p, err := h.Read()
				if errors.Is(err, ErrBufferEmpty) {
					if atomic.LoadInt64(&readCount) >= atomic.LoadInt64(&publishedCount) {
						// published counter may lag slightly; the closer
						// loop decides termination.
					}
					runtime.Gosched()
					continue
				}
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				mu.Lock()
				got[string(p)]++
				mu.Unlock()
				atomic.AddInt64(&readCount, 1)
			}
		}()
	}

	// Wait for writers, then drain until every message is received.
	done := make(chan struct{})
	go func() {
		// join writers by waiting on a separate waitgroup-like barrier:
		// reuse a counter poll.
		for atomic.LoadInt64(&publishedCount) < int64(writers*perWriter) {
			runtime.Gosched()
		}
		for atomic.LoadInt64(&readCount) < int64(writers*perWriter) {
			runtime.Gosched()
		}
		close(stop)
		close(done)
	}()
	wg.Wait() // note: readers return only after stop closes
	<-done

	if len(got) != writers*perWriter {
		t.Fatalf("unique messages = %d, want %d", len(got), writers*perWriter)
	}
	dup := 0
	for m, c := range got {
		if c != 1 {
			dup++
			t.Errorf("message %q delivered %d times", m, c)
		}
	}
	if dup > 0 {
		t.Fatalf("%d messages delivered more than once", dup)
	}
	w, rd, used := creator.Positions()
	t.Logf("concurrency done: published=%d read=%d writePos=%d readPos=%d used=%d",
		publishedCount, readCount, w, rd, used)
}

// TestFillThenRejectThenDrain: full buffer rejects further writes until a
// reader consumes records; positions stay consistent throughout.
func TestFillThenRejectThenDrain(t *testing.T) {
	cfg := testCfg(t, "full", 1024)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	item := make([]byte, 64) // total 96 each
	n := 0
	for {
		if _, err := r.Write(item); err != nil {
			if !errors.Is(err, ErrBufferFull) {
				t.Fatal(err)
			}
			break
		}
		n++
	}
	w, rd, used := r.Positions()
	t.Logf("full after %d records: writePos=%d readPos=%d used=%d (cap=%d)",
		n, w, rd, used, cfg.Capacity)
	if used != w-rd || used > uint64(cfg.Capacity) {
		t.Fatalf("inconsistent used=%d w=%d rd=%d", used, w, rd)
	}
	// One more write must still be rejected and not move positions.
	if _, err := r.Write(item); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("want full, got %v", err)
	}
	w2, rd2, used2 := r.Positions()
	if w2 != w || rd2 != rd || used2 != used {
		t.Fatalf("rejected write moved positions: %d,%d,%d -> %d,%d,%d",
			w, rd, used, w2, rd2, used2)
	}
	// Consume one; one write must now succeed.
	if _, _, err := r.Read(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write(item); err != nil {
		t.Fatalf("write after one consume: %v", err)
	}
	// Drain all and confirm emptiness.
	got := 1 // already read one
	for {
		if _, _, err := r.Read(); err != nil {
			if !errors.Is(err, ErrBufferEmpty) {
				t.Fatal(err)
			}
			break
		}
		got++
	}
	if got != n+1 {
		t.Fatalf("consumed %d, want %d", got, n+1)
	}
}
