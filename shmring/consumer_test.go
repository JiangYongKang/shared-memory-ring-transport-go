package shmring

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestMultiConsumerExactlyOnce：同一进程内 4 个消费者（4 个 goroutine）
// 并发领取 500 条记录，每条必须恰好被一个消费者领取一次；
// 消费者各自推进、互不阻塞。
func TestMultiConsumerExactlyOnce(t *testing.T) {
	dir := testDir(t)
	r, err := Create(Config{Name: "mc1", Capacity: 1 << 16}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const (
		consumers = 4
		total     = 500
	)
	// 写者 goroutine：持续写入可校验内容的记录。
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < total; i++ {
			p := []byte(fmt.Sprintf("msg-%04d", i))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := r.Write(p); err == nil {
					break
				} else if !errors.Is(err, ErrFull) {
					t.Errorf("write %d: %v", i, err)
					return
				} else {
					time.Sleep(time.Millisecond)
				}
			}
		}
	}()

	type result struct {
		seq     uint64
		payload string
	}
	results := make(chan result, total)
	var wg sync.WaitGroup
	claimed := make([]int, consumers) // 每个消费者领取的数量（各自推进的凭据）
	for id := 0; id < consumers; id++ {
		c, err := r.Consumer()
		if err != nil {
			t.Fatalf("consumer %d: %v", id, err)
		}
		defer c.Close()
		wg.Add(1)
		go func(id int, c *Consumer) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec, err := c.Read()
				if errors.Is(err, ErrEmpty) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					select {
					case <-stop:
					default:
						t.Errorf("consumer %d read: %v", id, err)
					}
					return
				}
				claimed[id]++
				results <- result{rec.Seq, string(rec.Payload)}
			}
		}(id, c)
	}
	// 清理顺序：先停写者与消费者并等待退出，再由外层 defer 关闭 Ring。
	defer func() { close(stop); wg.Wait(); <-writerDone }()

	// 汇总：每条 seq 恰好出现一次，内容与写入顺序一致。
	seen := make(map[uint64]string)
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: got %d/%d records", len(seen), total)
		}
		select {
		case res := <-results:
			if _, dup := seen[res.seq]; dup {
				t.Fatalf("seq=%d claimed twice (duplicate delivery)", res.seq)
			}
			seen[res.seq] = res.payload
		case <-time.After(100 * time.Millisecond):
		}
	}
	for seq := uint64(1); seq <= total; seq++ {
		want := fmt.Sprintf("msg-%04d", seq-1)
		if seen[seq] != want {
			t.Fatalf("seq=%d payload=%q, want %q", seq, seen[seq], want)
		}
	}
	t.Logf("exactly-once: %d records, per-consumer claims=%v, claimPos=%d nextSeq=%d",
		len(seen), claimed, r.ClaimPos(), r.NextSeq())
	sum := 0
	for _, n := range claimed {
		sum += n
	}
	if sum != total {
		t.Fatalf("per-consumer claims sum=%d, want %d", sum, total)
	}
	// 全部确认后，各消费者读空自动 Sync，回收边界应很快收敛到写位置
	// （收敛是异步的：最慢的消费者下一次读空轮询时追平）。
	<-writerDone // 与写者 goroutine 建立同步，避免 Close 与其写路径竞争
	deadline = time.Now().Add(5 * time.Second)
	for {
		_, wp, rp := r.Positions()
		if rp == wp {
			t.Logf("reclaim boundary converged: write=%d reclaim=%d", wp, rp)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reclaim boundary=%d never converged to write pos=%d", rp, wp)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSlowConsumerBackpressure：慢消费者不确认时，写者回绕不得覆盖任何
// 消费者未确认的记录；空间被最慢消费者拖住时写者必须得到明确的 ErrFull，
// 而不是丢数据；慢消费者恢复活跃（读空自动 Sync）后写者立即可以继续。
func TestSlowConsumerBackpressure(t *testing.T) {
	dir := testDir(t)
	const cap = 512
	r, err := Create(Config{Name: "slow", Capacity: cap}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const payloadLen = 32
	const frameLen = FrameHeaderSize + payloadLen + FrameTrailerSize // 56
	mk := func(i int) []byte {
		p := make([]byte, payloadLen)
		copy(p, fmt.Sprintf("rec-%04d-", i))
		return p
	}
	write := func(i int) error {
		_, err := r.Write(mk(i))
		return err
	}

	slow, err := r.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	fast, err := r.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	t.Logf("consumers: slow slot=%d fast slot=%d", slow.Slot(), fast.Slot())

	// 慢消费者只领走第 1 条，随后“卡住”（不再调用 Read）。
	if err := write(1); err != nil {
		t.Fatal(err)
	}
	rec, err := slow.Read()
	if err != nil || rec.Seq != 1 {
		t.Fatalf("slow first read = %+v, %v", rec, err)
	}
	t.Logf("slow consumer claimed seq=%d then stalls (ack=%d)", rec.Seq, slow.AckPos())

	// 快消费者持续领取；写者持续写入，直到空间被慢消费者的 ack 游标拖住。
	var fastGot []Record
	stopped := false
	var fullAt int
	for i := 2; i < 200; i++ {
		err := write(i)
		if errors.Is(err, ErrFull) {
			stopped, fullAt = true, i
			break
		}
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		// 快消费者尽量领走新记录（其读空会自动 Sync 自己的 ack 游标）。
		for {
			rec, err := fast.Read()
			if errors.Is(err, ErrEmpty) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			fastGot = append(fastGot, rec)
		}
	}
	if !stopped {
		t.Fatal("writer never saw ErrFull despite stalled slow consumer")
	}
	resv, wp, rp := r.Positions()
	t.Logf("writer blocked at seq=%d: reserved=%d write=%d reclaim=%d used=%d slowAck=%d fastAck=%d",
		fullAt, resv, wp, rp, resv-rp, slow.AckPos(), fast.AckPos())
	if rp != slow.AckPos() {
		t.Fatalf("reclaim boundary=%d should be pinned by slow consumer ack=%d", rp, slow.AckPos())
	}

	// 卡住期间：已写记录一条不丢、一条不改——快消费者领到的内容逐字节可校验。
	for i, rec := range fastGot {
		want := string(mk(i + 2))
		if string(rec.Payload) != want {
			t.Fatalf("fast record %d corrupted: %q want %q", i, rec.Payload, want)
		}
	}
	t.Logf("integrity: %d records claimed by fast consumer verified byte-exact; none lost while writer was full", len(fastGot))

	// 继续写仍然是明确的 ErrFull（不是静默丢数据）。
	if err := write(fullAt); !errors.Is(err, ErrFull) {
		t.Fatalf("write while slow consumer stalls = %v, want ErrFull", err)
	}

	// 慢消费者恢复活跃：读空触发自动 Sync，回收边界立刻释放，写者继续。
	if _, err := slow.Read(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("slow resume read = %v, want ErrEmpty (fast claimed everything)", err)
	}
	_, _, rp = r.Positions()
	t.Logf("slow consumer synced: reclaim boundary=%d slowAck=%d", rp, slow.AckPos())
	if err := write(fullAt); err != nil {
		t.Fatalf("write after slow consumer synced: %v", err)
	}
	t.Logf("writer resumed after slow consumer caught up: seq=%d written", fullAt)

	// 收尾：全部记录必须恰好交付一次（慢 1 条 + 快 N 条）。
	rec, err = fast.Read()
	if err != nil {
		t.Fatal(err)
	}
	fastGot = append(fastGot, rec)
	total := 1 + len(fastGot)
	if total != fullAt {
		t.Fatalf("delivered %d records, want %d written", total, fullAt)
	}
	if string(fastGot[len(fastGot)-1].Payload) != string(mk(fullAt)) {
		t.Fatalf("last record = %q, want %q", fastGot[len(fastGot)-1].Payload, mk(fullAt))
	}
}

// TestDeadConsumerSlotReaped：消费者“死亡”（句柄被遗弃、锁随 fd 关闭而
// 释放）后，其槽位会被其它挂载者自动回收，不会永久拖住回收边界。
// 进程级 SIGKILL 场景见 proc 测试；这里用包内手段确定性模拟同一机制。
func TestDeadConsumerSlotReaped(t *testing.T) {
	dir := testDir(t)
	const cap = 256
	r, err := Create(Config{Name: "reap", Capacity: cap}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	mk := func(i int) []byte {
		p := make([]byte, 32)
		copy(p, fmt.Sprintf("rec-%04d-", i))
		return p
	}

	// 死亡消费者：注册后领走第 1 条，随后“死亡”——锁文件描述符被关闭
	// （等同进程退出时内核释放 OFD 锁），但位图里的槽位没有注销。
	dead, err := r.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	deadSlot := dead.Slot()
	if _, err := r.Write(mk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := dead.Read(); err != nil {
		t.Fatal(err)
	}
	_ = dead.lk.Close() // 模拟死亡：释放 OFD 锁，槽位位图残留
	dead.closed.Store(true)
	t.Logf("dead consumer slot=%d abandoned (lock released, bitmap still set)", deadSlot)

	// 活跃消费者注册：注册过程会先回收死亡槽位——若回收生效，
	// 它将直接复用死亡消费者的槽位号，而不是另取新槽位。
	live, err := r.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	t.Logf("live consumer registered at slot=%d (dead slot was %d)", live.Slot(), deadSlot)
	if live.Slot() != deadSlot {
		t.Fatalf("live consumer slot=%d: dead slot %d was not reaped before registration",
			live.Slot(), deadSlot)
	}

	// 写满整个容量：死亡槽位的 ack 游标（=56）若不被回收，
	// 回收边界将永远停在 56，第 5 条起必然永远 ErrFull。
	const total = 12
	written := 0
	deadline := time.Now().Add(10 * time.Second)
	for written < total {
		_, err := r.Write(mk(written + 2))
		if errors.Is(err, ErrFull) {
			if time.Now().After(deadline) {
				resv, wp, rp := r.Positions()
				t.Fatalf("writer stuck on ErrFull: reserved=%d write=%d reclaim=%d (dead slot not reaped)",
					resv, wp, rp)
			}
			time.Sleep(2 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		written++
		// 活跃消费者同步领取，保持回收边界只被死亡槽位拖住。
		for {
			if _, err := live.Read(); errors.Is(err, ErrEmpty) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
		}
	}
	resv, wp, rp := r.Positions()
	t.Logf("wrote %d records past dead consumer: reserved=%d write=%d reclaim=%d",
		total, resv, wp, rp)
	if rp != wp {
		t.Fatalf("reclaim boundary=%d stuck behind write pos=%d (dead consumer still pinning)", rp, wp)
	}
}

// TestTooManyConsumers：存活消费者达到 MaxConsumers 后，新消费者被明确拒绝。
func TestTooManyConsumers(t *testing.T) {
	dir := testDir(t)
	r, err := Create(Config{Name: "toomany", Capacity: 256}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var cs []*Consumer
	for i := 0; i < MaxConsumers; i++ {
		c, err := r.Consumer()
		if err != nil {
			t.Fatalf("consumer %d: %v", i, err)
		}
		cs = append(cs, c)
	}
	defer func() {
		for _, c := range cs {
			_ = c.Close()
		}
	}()
	if _, err := r.Consumer(); !errors.Is(err, ErrTooManyConsumers) {
		t.Fatalf("consumer #%d = %v, want ErrTooManyConsumers", MaxConsumers+1, err)
	}
	// 关闭一个后立即可注册。
	if err := cs[0].Close(); err != nil {
		t.Fatal(err)
	}
	c, err := r.Consumer()
	if err != nil {
		t.Fatalf("register after close: %v", err)
	}
	t.Logf("slot reused after close: slot=%d (freed slot=%d)", c.Slot(), cs[0].Slot())
	cs[0] = c
}

// TestLegacyVersionRejected：v1（旧格式）缓冲区被明确拒绝，且拒绝过程
// 是只读的——文件内容逐字节不变，正在使用该缓冲区的进程不受任何影响。
func TestLegacyVersionRejected(t *testing.T) {
	dir := testDir(t)
	name := "legacy"
	r, err := Create(Config{Name: name, Capacity: 256}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("old-data")); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// 把版本字段改回 1，模拟“旧库创建、仍在被旧进程使用”的缓冲区。
	dat := filepath.Join(dir, "shmring-"+name+".dat")
	corruptFileField(t, dat, offVersion, []byte{1, 0, 0, 0})
	before, err := os.ReadFile(dat)
	if err != nil {
		t.Fatal(err)
	}

	// Open 与 Create 都必须以版本不兼容明确拒绝。
	if _, err := Open(name, &Options{Dir: dir}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("open v1 buffer = %v, want ErrVersionMismatch", err)
	}
	if _, err := Create(Config{Name: name, Capacity: 256}, &Options{Dir: dir}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("create over v1 buffer = %v, want ErrVersionMismatch", err)
	}
	t.Logf("v1 buffer rejected: %v", ErrVersionMismatch)

	// 拒绝是只读的：文件逐字节不变（旧进程看到的数据不受任何影响）。
	after, err := os.ReadFile(dat)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected mount modified the in-use legacy buffer")
	}
	t.Logf("legacy buffer untouched by rejected mounts (%d bytes identical)", len(after))

	// 同时，其它 v2 缓冲区的正常使用不受拒绝路径影响。
	r2, err := Create(Config{Name: "v2-ok", Capacity: 256}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if _, err := r2.Write([]byte("v2-alive")); err != nil {
		t.Fatal(err)
	}
	rec, err := r2.Read()
	if err != nil || string(rec.Payload) != "v2-alive" {
		t.Fatalf("v2 ring broken by legacy rejection path: %+v, %v", rec, err)
	}
}
