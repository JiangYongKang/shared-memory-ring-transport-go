package shmring

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func testDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func TestCreateOpenBasic(t *testing.T) {
	dir := testDir(t)
	r, err := Create(Config{Name: "basic", Capacity: 4096}, &Options{Dir: dir})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	seq, err := r.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if seq != 1 {
		t.Fatalf("first seq = %d, want 1", seq)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r2, err := Open("basic", &Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rec, err := r2.Read()
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	if rec.Seq != 1 || string(rec.Payload) != "hello" {
		t.Fatalf("got seq=%d payload=%q", rec.Seq, rec.Payload)
	}
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadEmpty(t *testing.T) {
	dir := testDir(t)
	r, err := Create(Config{Name: "empty", Capacity: 256}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Read(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Read empty = %v, want ErrEmpty", err)
	}
	if _, err := r.Peek(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Peek empty = %v, want ErrEmpty", err)
	}
}

func TestPeekDoesNotConsumeAndDuplicateSeq(t *testing.T) {
	dir := testDir(t)
	r, _ := Create(Config{Name: "dup", Capacity: 256}, &Options{Dir: dir})
	defer r.Close()
	if _, err := r.Write([]byte("same")); err != nil {
		t.Fatal(err)
	}
	first, err := r.Peek()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		rec, err := r.Peek()
		if err != nil {
			t.Fatal(err)
		}
		if rec.Seq != first.Seq || !bytes.Equal(rec.Payload, first.Payload) {
			t.Fatalf("peek %d differs: %+v vs %+v", i, rec, first)
		}
	}
	// Peek 不推进读位置，写空间未释放。
	_, wp, rp := r.Positions()
	if wp <= rp {
		t.Fatalf("read pos %d should still trail write pos %d", rp, wp)
	}
	consumed, err := r.Read()
	if err != nil || consumed.Seq != first.Seq {
		t.Fatalf("Read = %+v, %v", consumed, err)
	}
	if _, err := r.Read(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("after consume Read = %v, want ErrEmpty", err)
	}
}

func TestMountConflicts(t *testing.T) {
	dir := testDir(t)
	r, err := Create(Config{Name: "cf", Capacity: 4096}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	// 保持 r 打开，模拟“缓冲区正在使用中”。
	defer r.Close()

	if _, err := Create(Config{Name: "cf", Capacity: 2048}, &Options{Dir: dir}); !errors.Is(err, ErrCapacityMismatch) {
		t.Fatalf("capacity conflict = %v, want ErrCapacityMismatch", err)
	}
	// 冲突后原缓冲区仍可正常读写。
	if _, err := r.Write([]byte("alive")); err != nil {
		t.Fatalf("write after rejected mount: %v", err)
	}
	rec, err := r.Read()
	if err != nil || string(rec.Payload) != "alive" {
		t.Fatalf("read after rejected mount = %q, %v", rec.Payload, err)
	}

	// 损坏魔数后再挂载应被识别为 magic 冲突。
	dat := filepath.Join(dir, "shmring-cf.dat")
	raw, err := os.OpenFile(dat, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.WriteAt([]byte("XXXX"), 0); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	if _, err := Open("cf", &Options{Dir: dir}); !errors.Is(err, ErrMagicMismatch) {
		t.Fatalf("magic conflict = %v, want ErrMagicMismatch", err)
	}
}

func TestInvalidParams(t *testing.T) {
	dir := testDir(t)
	if _, err := Create(Config{Name: "", Capacity: 256}, &Options{Dir: dir}); !errors.Is(err, ErrNameInvalid) {
		t.Fatalf("empty name = %v", err)
	}
	if _, err := Create(Config{Name: "a/b", Capacity: 256}, &Options{Dir: dir}); !errors.Is(err, ErrNameInvalid) {
		t.Fatalf("slash name = %v", err)
	}
	for _, c := range []int{0, 8, 63, 65, 100} {
		if _, err := Create(Config{Name: fmt.Sprintf("c%d", c), Capacity: c}, &Options{Dir: dir}); !errors.Is(err, ErrCapacityInvalid) {
			t.Fatalf("capacity %d = %v, want ErrCapacityInvalid", c, err)
		}
	}
	if _, err := Open("missing", &Options{Dir: dir}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("open missing = %v, want ErrNotFound", err)
	}
}

func TestConcurrentSingleProcess(t *testing.T) {
	dir := testDir(t)
	r, _ := Create(Config{Name: "conc", Capacity: 1 << 20}, &Options{Dir: dir})
	defer r.Close()

	const (
		writers   = 4
		perWriter = 100
		total     = writers * perWriter
	)
	var wg sync.WaitGroup
	errCh := make(chan error, total+writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				payload := []byte(fmt.Sprintf("w%d-m%d", id, i))
				if _, err := r.Write(payload); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	// 唯一读者 goroutine 独占 map；主 goroutine 只等待其结束。
	received := make(map[uint64]string)
	done := make(chan struct{})
	go func() {
		for len(received) < total {
			rec, err := r.Read()
			if errors.Is(err, ErrEmpty) {
				runtime.Gosched()
				continue
			}
			if err != nil {
				errCh <- err
				break
			}
			if _, dup := received[rec.Seq]; dup {
				errCh <- fmt.Errorf("duplicate seq %d", rec.Seq)
				break
			}
			received[rec.Seq] = string(rec.Payload)
		}
		close(done)
	}()

	wg.Wait()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout: got %d/%d", len(received), total)
	}
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if len(received) != total {
		t.Fatalf("got %d records, want %d", len(received), total)
	}
	// 去重依据也可用于内容核对。
	counts := map[string]int{}
	for _, p := range received {
		counts[p[:2]]++
	}
	for w := 0; w < writers; w++ {
		if counts[fmt.Sprintf("w%d", w)] != perWriter {
			t.Fatalf("writer %d delivered %d, want %d", w, counts[fmt.Sprintf("w%d", w)], perWriter)
		}
	}
}
