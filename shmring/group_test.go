package shmring

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shrinkConsumerTTL 缩短心跳周期与过期阈值，加速崩溃消费者场景；
// 返回恢复函数。
func shrinkConsumerTTL() func() {
	oldI, oldT := heartbeatInterval, consumerTTL
	heartbeatInterval = 20 * time.Millisecond
	consumerTTL = 120 * time.Millisecond
	return func() { heartbeatInterval, consumerTTL = oldI, oldT }
}

// lengthenConsumerTTL 拉长发心跳过期阈值，避免高负载（-race）下
// 存活消费者被误判为崩溃；返回恢复函数。
func lengthenConsumerTTL() func() {
	oldI, oldT := heartbeatInterval, consumerTTL
	heartbeatInterval = 20 * time.Millisecond
	consumerTTL = 30 * time.Second
	return func() { heartbeatInterval, consumerTTL = oldI, oldT }
}

// logGroupState 打印消费者组模式下的游标、回收下限与槽位快照，
// 使测试日志能解释“写者凭什么满、记录凭什么只被领一次”。
func logGroupState(t *testing.T, r *Ring, label string) {
	t.Helper()
	resv, wp, rp := r.Positions()
	t.Logf("[%s] reserved=%d write=%d read=%d claim=%d floor=%d slots=%v",
		label, resv, wp, rp, r.ClaimPos(), r.reclaimFloor(), r.ConsumerSlots())
}

// TestMultiConsumerExactlyOnce：单进程内 4 个消费者并发领取，
// 每条记录必须恰好被一个消费者领取一次，且负载与序列号自洽。
func TestMultiConsumerExactlyOnce(t *testing.T) {
	restore := lengthenConsumerTTL()
	defer restore()
	dir := testDir(t)
	r, err := Create(Config{Name: "g-exact", Capacity: 1 << 20}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const total, consumers = 400, 4
	var (
		mu       sync.Mutex
		owner    = make(map[uint64]int) // seq -> 领取者编号
		perCons  = make([]int, consumers)
		done     atomic.Bool
		wg       sync.WaitGroup
		claimErr = make(chan error, consumers)
	)
	for i := 0; i < consumers; i++ {
		c, err := r.NewConsumer()
		if err != nil {
			t.Fatalf("NewConsumer %d: %v", i, err)
		}
		defer c.Close()
		wg.Add(1)
		go func(idx int, c *Consumer) {
			defer wg.Done()
			for {
				rec, err := c.Next()
				if errors.Is(err, ErrEmpty) {
					if done.Load() {
						return
					}
					runtime.Gosched()
					continue
				}
				if err != nil {
					claimErr <- err
					return
				}
				// 校验依据：负载由写者按 seq 生成，二者必须一致。
				want := fmt.Sprintf("w-%06d", rec.Seq)
				if string(rec.Payload) != want {
					claimErr <- fmt.Errorf("consumer %d: seq=%d payload=%q, want %q",
						idx, rec.Seq, rec.Payload, want)
					return
				}
				mu.Lock()
				if prev, dup := owner[rec.Seq]; dup {
					mu.Unlock()
					claimErr <- fmt.Errorf("seq %d claimed by both consumer %d and %d",
						rec.Seq, prev, idx)
					return
				}
				owner[rec.Seq] = idx
				perCons[idx]++
				mu.Unlock()
				if err := c.Confirm(); err != nil {
					claimErr <- err
					return
				}
			}
		}(i, c)
	}

	// 唯一写者：负载内嵌序号的预期值。
	for i := 1; i <= total; i++ {
		if _, err := r.Write([]byte(fmt.Sprintf("w-%06d", i))); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	done.Store(true)
	wg.Wait()
	close(claimErr)
	for err := range claimErr {
		t.Fatal(err)
	}
	if len(owner) != total {
		t.Fatalf("claimed %d unique records, want %d", len(owner), total)
	}
	for seq := uint64(1); seq <= total; seq++ {
		if _, ok := owner[seq]; !ok {
			t.Fatalf("seq %d never claimed", seq)
		}
	}
	t.Logf("exactly-once: %d records claimed by %d consumers, per-consumer=%v",
		len(owner), consumers, perCons)
	logGroupState(t, r, "after drain")
}

// TestConsumerRedeliversOwnHold：未确认前再次 Next 返回同一条记录
// （相同 Seq，可识别为重复），确认后才推进。
func TestConsumerRedeliversOwnHold(t *testing.T) {
	dir := testDir(t)
	r, _ := Create(Config{Name: "g-hold", Capacity: 1024}, &Options{Dir: dir})
	defer r.Close()
	for i := 1; i <= 3; i++ {
		if _, err := r.Write([]byte(fmt.Sprintf("m-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	c, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, err := c.Next()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		again, err := c.Next()
		if err != nil {
			t.Fatal(err)
		}
		if again.Seq != first.Seq || string(again.Payload) != string(first.Payload) {
			t.Fatalf("unconfirmed Next = %+v, want identical to %+v", again, first)
		}
	}
	t.Logf("duplicate recognized: unconfirmed Next returns seq=%d repeatedly", first.Seq)
	if err := c.Confirm(); err != nil {
		t.Fatal(err)
	}
	if err := c.Confirm(); !errors.Is(err, ErrNoHold) {
		t.Fatalf("double Confirm = %v, want ErrNoHold", err)
	}
	second, err := c.Next()
	if err != nil || second.Seq != 2 {
		t.Fatalf("after Confirm Next = %+v, %v, want seq=2", second, err)
	}
}

// TestSlowConsumerBackpressure：慢消费者持有未确认记录时，
// 回收下限被拖住，写者空间不足必须返回 ErrFull 且不得覆盖；
// 其他消费者不受影响继续领取；慢消费者确认后写者恢复。
func TestSlowConsumerBackpressure(t *testing.T) {
	restore := lengthenConsumerTTL()
	defer restore()
	dir := testDir(t)
	const cap = 1024
	r, _ := Create(Config{Name: "g-slow", Capacity: cap}, &Options{Dir: dir})
	defer r.Close()

	// 负载 100 -> 帧 124；8 帧 = 992，写满后剩余 32 字节。
	const plen = 100
	mk := func(i int) []byte {
		p := make([]byte, plen)
		copy(p, fmt.Sprintf("slow-%02d-", i))
		return p
	}
	for i := 1; i <= 8; i++ {
		if _, err := r.Write(mk(i)); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
	slow, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	fast, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()

	// 慢消费者领取第 1 条后按住不确认。
	held, err := slow.Next()
	if err != nil || held.Seq != 1 {
		t.Fatalf("slow Next = %+v, %v", held, err)
	}
	logGroupState(t, r, "slow holds seq=1")

	// 快消费者不被拖住：继续领完其余 7 条。
	for want := uint64(2); want <= 8; want++ {
		rec, err := fast.Next()
		if err != nil {
			t.Fatalf("fast Next seq=%d: %v", want, err)
		}
		if rec.Seq != want {
			t.Fatalf("fast got seq=%d, want %d", rec.Seq, want)
		}
		if err := fast.Confirm(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fast.Next(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("fast Next at drained claim cursor = %v, want ErrEmpty", err)
	}
	t.Logf("fast consumer claimed seq 2..8 while slow consumer holds seq=1 (no blocking)")

	// 写者：回收下限 = 慢消费者持有位置 0，空间不足必须 ErrFull。
	logGroupState(t, r, "before full check")
	if _, err := r.Write(mk(9)); !errors.Is(err, ErrFull) {
		t.Fatalf("write with slow consumer holding = %v, want ErrFull", err)
	}
	t.Logf("writer got ErrFull: floor pinned by slow consumer's unconfirmed seq=1")

	// 慢消费者确认后下限释放，写者立即恢复；已确认记录不被改写。
	if err := slow.Confirm(); err != nil {
		t.Fatal(err)
	}
	seq9, err := r.Write(mk(9))
	if err != nil {
		t.Fatalf("write after slow confirm: %v", err)
	}
	seq10, err := r.Write(mk(10))
	if err != nil {
		t.Fatalf("second write after slow confirm: %v", err)
	}
	t.Logf("after confirm: wrote seq=%d,%d (wrapped around)", seq9, seq10)
	logGroupState(t, r, "after wrap writes")

	// 回绕后全部记录仍完整：快消费者读到 seq 9、10，内容逐字节匹配。
	for _, want := range []uint64{9, 10} {
		rec, err := fast.Next()
		if err != nil {
			t.Fatalf("drain seq=%d: %v", want, err)
		}
		if rec.Seq != want || string(rec.Payload[:8]) != fmt.Sprintf("slow-%02d-", want) {
			t.Fatalf("drained = seq=%d payload=%q, want seq=%d", rec.Seq, rec.Payload, want)
		}
		if err := fast.Confirm(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fast.Next(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("final drain = %v, want ErrEmpty", err)
	}
}

// TestConsumerCrashSteal：消费者“崩溃”（心跳停止、槽位遗弃）后，
// 其持有未确认的记录被其他消费者按相同 Seq 接管重投。
func TestConsumerCrashSteal(t *testing.T) {
	restore := shrinkConsumerTTL()
	defer restore()
	dir := testDir(t)
	r, _ := Create(Config{Name: "g-steal", Capacity: 4096}, &Options{Dir: dir})
	defer r.Close()
	for i := 1; i <= 3; i++ {
		if _, err := r.Write([]byte(fmt.Sprintf("s-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	victim, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	held, err := victim.Next()
	if err != nil || held.Seq != 1 {
		t.Fatalf("victim Next = %+v, %v", held, err)
	}
	victim.simulateCrash() // 心跳停止，持有 seq=1 未确认
	logGroupState(t, r, "victim crashed holding seq=1")

	c2, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	// 崩溃尚未过期：c2 走正常领取路径拿到 seq=2。
	rec, err := c2.Next()
	if err != nil || rec.Seq != 2 {
		t.Fatalf("before TTL: Next = %+v, %v, want seq=2", rec, err)
	}
	if err := c2.Confirm(); err != nil {
		t.Fatal(err)
	}
	// 等待心跳过期后，c2 应接管 seq=1（相同 Seq 重投）。
	time.Sleep(2 * consumerTTL)
	stolen, err := c2.Next()
	if err != nil {
		t.Fatalf("steal Next: %v", err)
	}
	if stolen.Seq != 1 || string(stolen.Payload) != "s-1" {
		t.Fatalf("stolen = %+v, want redelivery of seq=1", stolen)
	}
	t.Logf("redelivery: crashed consumer's seq=1 reclaimed by survivor (same seq => recognizable duplicate)")
	if err := c2.Confirm(); err != nil {
		t.Fatal(err)
	}
	// 剩余 seq=3 正常领取，之后读空。
	rec, err = c2.Next()
	if err != nil || rec.Seq != 3 {
		t.Fatalf("Next = %+v, %v, want seq=3", rec, err)
	}
	if err := c2.Confirm(); err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Next(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("drained = %v, want ErrEmpty", err)
	}
	logGroupState(t, r, "drained after steal")
}

// TestConsumerCloseAbandon：消费者持有记录退出（Close）时，
// 记录不丢失：槽位被遗弃，其他消费者立即接管重投。
func TestConsumerCloseAbandon(t *testing.T) {
	dir := testDir(t)
	r, _ := Create(Config{Name: "g-abandon", Capacity: 1024}, &Options{Dir: dir})
	defer r.Close()
	if _, err := r.Write([]byte("held-record")); err != nil {
		t.Fatal(err)
	}
	c1, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	rec, err := c1.Next()
	if err != nil || rec.Seq != 1 {
		t.Fatalf("c1 Next = %+v, %v", rec, err)
	}
	if err := c1.Close(); err != nil { // 持有未确认记录退出
		t.Fatal(err)
	}
	logGroupState(t, r, "c1 closed with held seq=1")

	c2, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	again, err := c2.Next()
	if err != nil {
		t.Fatal(err)
	}
	if again.Seq != 1 || string(again.Payload) != "held-record" {
		t.Fatalf("abandoned record = %+v, want redelivery of seq=1", again)
	}
	t.Logf("abandoned seq=1 redelivered to c2 with identical seq (recognizable)")
	if err := c2.Confirm(); err != nil {
		t.Fatal(err)
	}
}

// TestModeConflict：单消费者游标与消费者组互斥，先占先得，
// 冲突被明确拒绝且不影响在用方。
func TestModeConflict(t *testing.T) {
	dir := testDir(t)

	// 先用消费者组：legacy Read/Peek/Advance 被拒绝。
	rg, _ := Create(Config{Name: "mode-g", Capacity: 1024}, &Options{Dir: dir})
	defer rg.Close()
	c, err := rg.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := rg.Read(); !errors.Is(err, ErrModeConflict) {
		t.Fatalf("Read on group buffer = %v, want ErrModeConflict", err)
	}
	if _, err := rg.Peek(); !errors.Is(err, ErrModeConflict) {
		t.Fatalf("Peek on group buffer = %v, want ErrModeConflict", err)
	}
	if err := rg.Advance(); !errors.Is(err, ErrModeConflict) {
		t.Fatalf("Advance on group buffer = %v, want ErrModeConflict", err)
	}

	// 先用单消费者游标：NewConsumer 被拒绝；拒绝后原用法不受影响。
	rl, _ := Create(Config{Name: "mode-l", Capacity: 1024}, &Options{Dir: dir})
	defer rl.Close()
	if _, err := rl.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := rl.Read(); err != nil {
		t.Fatal(err)
	}
	if _, err := rl.NewConsumer(); !errors.Is(err, ErrModeConflict) {
		t.Fatalf("NewConsumer on legacy buffer = %v, want ErrModeConflict", err)
	}
	if _, err := rl.Write([]byte("still-ok")); err != nil {
		t.Fatalf("legacy buffer broken after rejected NewConsumer: %v", err)
	}
	if rec, err := rl.Read(); err != nil || string(rec.Payload) != "still-ok" {
		t.Fatalf("legacy read after rejection = %+v, %v", rec, err)
	}
	t.Logf("mode conflict rejected both ways; in-use side unaffected")
}

// TestTooManyConsumers：槽位占满后第 MaxConsumers+1 个消费者被拒绝。
func TestTooManyConsumers(t *testing.T) {
	dir := testDir(t)
	r, _ := Create(Config{Name: "g-full", Capacity: 1024}, &Options{Dir: dir})
	defer r.Close()
	cs := make([]*Consumer, 0, MaxConsumers)
	for i := 0; i < MaxConsumers; i++ {
		c, err := r.NewConsumer()
		if err != nil {
			t.Fatalf("consumer %d: %v", i, err)
		}
		cs = append(cs, c)
	}
	if _, err := r.NewConsumer(); !errors.Is(err, ErrTooManyConsumers) {
		t.Fatalf("consumer #%d = %v, want ErrTooManyConsumers", MaxConsumers+1, err)
	}
	// 释放一个槽位后即可再注册。
	cs[0].Close()
	if _, err := r.NewConsumer(); err != nil {
		t.Fatalf("NewConsumer after one Close: %v", err)
	}
	for _, c := range cs[1:] {
		c.Close()
	}
}
