package shmring

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// 消费者心跳节奏。槽位心跳超过 consumerTTL 未刷新即被其他挂载者视为
// “消费者已崩溃”：其槽位可被回收，其持有未确认的记录可被重投。
// 包内测试可缩小这两个值以加速场景。
var (
	heartbeatInterval = 200 * time.Millisecond
	consumerTTL       = 1 * time.Second
)

var consumerIDCounter atomic.Uint64

// newConsumerID 生成跨进程唯一的消费者标识（同时也是槽位 owner 值）。
func newConsumerID() uint64 {
	for {
		id := uint64(os.Getpid())<<32 ^
			uint64(time.Now().UnixNano()) ^
			consumerIDCounter.Add(1)*0x9E3779B97F4A7C15
		if id != 0 {
			return id
		}
	}
}

// Consumer 是消费者组模式下一个已注册的消费者句柄。
//
// 多条完整记录通过共享领取游标 claimPos 被消费者“领取”：每条记录只会被
// 一个消费者领到（恰好一次领取）；领取后必须 Confirm 确认。消费者崩溃
// 或退出时，其持有未确认的记录会由其他消费者按相同序列号重新投递，
// 因此重复投递总能按 Record.Seq 识别。
//
// 消费者之间互不阻塞：领取是一次 CAS，慢消费者只会通过“回收下限”
// 拖住写者（写者返回 ErrFull），不会挡住其他消费者的领取。
type Consumer struct {
	r    *Ring
	slot int
	id   uint64

	mu      sync.Mutex
	held    *Record // 当前持有未确认的记录（nil 表示无）
	heldPos uint64

	stopHB    chan struct{}
	hbDone    chan struct{}
	closed    bool
	closeOnce sync.Once
}

// ID 返回该消费者的唯一标识（同时也是槽位 owner）。
func (c *Consumer) ID() uint64 { return c.id }

// SlotInfo 是消费者槽位的观测快照，用于运行日志与测试断言。
type SlotInfo struct {
	Index   int
	Owner   uint64 // 0 表示空闲
	Stale   bool   // 心跳已过期（消费者崩溃或退出时遗弃）
	Held    bool   // 持有未确认的记录
	HoldPos uint64 // 持有记录的逻辑位置
	HoldSeq uint64 // 持有记录的序列号
}

// ConsumerSlots 返回全部消费者槽位的快照（观测用）。
func (r *Ring) ConsumerSlots() []SlotInfo {
	out := make([]SlotInfo, 0, MaxConsumers)
	if r.legacyV1 || loadU32(r.header, offMode) != modeGroup {
		return out
	}
	now := time.Now().UnixNano()
	for i := 0; i < MaxConsumers; i++ {
		owner := loadU64(r.header, slotOff(i, slotOffOwner))
		if owner == 0 {
			continue
		}
		hb := int64(loadU64(r.header, slotOff(i, slotOffHeartbeat)))
		pending := loadU64(r.header, slotOff(i, slotOffPending))
		info := SlotInfo{
			Index: i,
			Owner: owner,
			Stale: now-hb > consumerTTL.Nanoseconds(),
			Held:  pending != holdNone,
		}
		if info.Held {
			info.HoldPos = pending
			info.HoldSeq = loadU64(r.header, slotOff(i, slotOffSeq))
		}
		out = append(out, info)
	}
	return out
}

// NewConsumer 在缓冲区上注册一个消费者（消费者组模式）。
//
// 缓冲区一旦进入消费者组模式，单消费者游标接口（Read/Peek/Advance）
// 将被拒绝（ErrModeConflict），反之亦然；版本 1 旧格式文件不支持
// 消费者组（ErrLegacyBuffer）。槽位占满时返回 ErrTooManyConsumers。
//
// 若没有空闲槽位，会回收心跳已过期的槽位；被回收槽位上未确认的记录
// 由新消费者接管，作为它的第一条记录重新投递（序列号不变）。
func (r *Ring) NewConsumer() (*Consumer, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.legacyV1 {
		return nil, ErrLegacyBuffer
	}
	// 固定为消费者组模式。
	for {
		switch m := loadU32(r.header, offMode); m {
		case modeGroup:
			goto pinned
		case modeLegacy:
			return nil, ErrModeConflict
		default:
			if casU32(r.header, offMode, m, modeGroup) {
				goto pinned
			}
		}
	}
pinned:
	id := newConsumerID()
	now := time.Now().UnixNano()
	h := r.header

	// 槽位获取协议：先写心跳再 CAS owner。写心跳失败无副作用（会被
	// 真正的槽主的心跳覆盖）；这样槽位从“被获取”的那一刻起心跳就是
	// 新鲜的，不存在“已占用但心跳仍旧”的被误回收窗口。
	slot := -1
	adopted := false
	for i := 0; i < MaxConsumers && slot < 0; i++ {
		if loadU64(h, slotOff(i, slotOffOwner)) != 0 {
			continue
		}
		storeU64(h, slotOff(i, slotOffHeartbeat), uint64(now))
		if casU64(h, slotOff(i, slotOffOwner), 0, id) {
			slot = i
		}
	}
	if slot < 0 {
		// 没有空闲槽位：回收心跳过期的槽位，并接管其在途记录。
		for i := 0; i < MaxConsumers && slot < 0; i++ {
			owner := loadU64(h, slotOff(i, slotOffOwner))
			if owner == 0 {
				continue
			}
			hb := int64(loadU64(h, slotOff(i, slotOffHeartbeat)))
			if now-hb <= consumerTTL.Nanoseconds() {
				continue
			}
			storeU64(h, slotOff(i, slotOffHeartbeat), uint64(now))
			if casU64(h, slotOff(i, slotOffOwner), owner, id) {
				slot = i
				adopted = true
			}
		}
	}
	if slot < 0 {
		return nil, ErrTooManyConsumers
	}

	c := &Consumer{
		r:      r,
		slot:   slot,
		id:     id,
		stopHB: make(chan struct{}),
		hbDone: make(chan struct{}),
	}
	if adopted {
		// 接管死消费者在途记录：验证它还没被回绕覆盖。
		ppos := loadU64(h, slotOff(slot, slotOffPending))
		pseq := loadU64(h, slotOff(slot, slotOffSeq))
		if ppos != holdNone {
			if rec, ok := r.readRecordIfIntact(ppos, pseq); ok {
				recCopy := rec
				c.held = &recCopy
				c.heldPos = ppos
			} else {
				// 记录已在无人认领期间被覆盖：释放占用标记。
				storeU64(h, slotOff(slot, slotOffPending), holdNone)
				storeU64(h, slotOff(slot, slotOffSeq), 0)
			}
		}
	} else {
		storeU64(h, slotOff(slot, slotOffPending), holdNone)
		storeU64(h, slotOff(slot, slotOffSeq), 0)
	}
	go c.heartbeatLoop()

	r.consMu.Lock()
	r.cons = append(r.cons, c)
	r.consMu.Unlock()
	return c, nil
}

// heartbeatLoop 周期刷新槽位心跳，直到 Close。
func (c *Consumer) heartbeatLoop() {
	t := time.NewTicker(heartbeatInterval)
	defer func() {
		t.Stop()
		close(c.hbDone)
	}()
	for {
		select {
		case <-c.stopHB:
			return
		case now := <-t.C:
			storeU64(c.r.header, slotOff(c.slot, slotOffHeartbeat), uint64(now.UnixNano()))
		}
	}
}

// Next 领取下一条完整记录。
//
// 若本消费者持有未确认的记录，再次 Next 返回同一条（相同 Seq）——
// 这是“至少一次”重投在单个消费者内的表现，调用方按 Seq 去重。
// 没有可领取的记录时返回 ErrEmpty。
//
// Next 还会接管心跳已过期消费者持有未确认的记录（相同 Seq 重投）。
func (c *Consumer) Next() (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Record{}, ErrClosed
	}
	if c.held != nil {
		return *c.held, nil // 重投自己持有的记录：Seq 相同，可识别
	}
	h := c.r.header
	// 刷新心跳：领取动作本身证明本消费者存活。
	storeU64(h, slotOff(c.slot, slotOffHeartbeat), uint64(time.Now().UnixNano()))

	// 1) 优先接管崩溃消费者的在途记录。
	if rec, pos, ok := c.stealLocked(); ok {
		c.held = &rec
		c.heldPos = pos
		return rec, nil
	}

	// 2) 在共享领取游标上竞争下一条记录。
	for {
		pos := loadU64(h, offClaimPos)
		written := loadU64(h, offWritePos)
		if pos >= written {
			return Record{}, ErrEmpty
		}
		// 空洞帧：协作跳过（不是数据，谁跳过都一样）。
		if gapLen, ok := c.r.committedGapAt(pos, written); ok {
			casU64(h, offClaimPos, pos, pos+gapLen)
			continue
		}
		var fh [FrameHeaderSize]byte
		ringReadAt(c.r.ring, pos, fh[:])
		if binary.LittleEndian.Uint32(fh[0:4]) != FrameMagic {
			// 可能刚被其他消费者推进了游标；否则是发布中的半帧。
			if loadU64(h, offClaimPos) != pos {
				continue
			}
			return Record{}, ErrEmpty
		}
		plen := frameLen(fh[:])
		total := frameTotal(int(plen))
		if pos+total > written {
			if loadU64(h, offClaimPos) != pos {
				continue
			}
			return Record{}, ErrEmpty
		}
		seq := frameSeq(fh[:])
		// 先占住回收下限保护（pendingPos），再 CAS 领取；这样写者
		// 要么看到旧 claimPos，要么看到本消费者的 pendingPos，
		// 不存在“记录已被领走但无人为它占位”的窗口。
		storeU64(h, slotOff(c.slot, slotOffSeq), seq)
		storeU64(h, slotOff(c.slot, slotOffPending), pos)
		if !casU64(h, offClaimPos, pos, pos+total) {
			storeU64(h, slotOff(c.slot, slotOffPending), holdNone)
			storeU64(h, slotOff(c.slot, slotOffSeq), 0)
			continue // 被其他消费者抢先，重试
		}
		// 领取成功：读出负载并校验 CRC（校验依据与单消费者一致）。
		payload := make([]byte, plen)
		ringReadAt(c.r.ring, pos+FrameHeaderSize, payload)
		var tr [FrameTrailerSize]byte
		ringReadAt(c.r.ring, pos+FrameHeaderSize+uint64(plen), tr[:])
		if binary.LittleEndian.Uint32(tr[4:8]) != CommitMagic ||
			binary.LittleEndian.Uint32(tr[0:4]) != frameCRC(seq, payload) {
			storeU64(h, slotOff(c.slot, slotOffPending), holdNone)
			storeU64(h, slotOff(c.slot, slotOffSeq), 0)
			return Record{}, fmt.Errorf("%w: claimed frame at logical %d failed CRC", ErrCorrupt, pos)
		}
		rec := Record{Seq: seq, Payload: payload}
		c.held = &rec
		c.heldPos = pos
		return rec, nil
	}
}

// stealLocked 接管心跳已过期消费者持有未确认的记录。
// 相同 Seq 重投，调用方按 Seq 识别重复。返回 false 表示没有可接管的。
func (c *Consumer) stealLocked() (Record, uint64, bool) {
	h := c.r.header
	now := time.Now().UnixNano()
	for i := 0; i < MaxConsumers; i++ {
		if i == c.slot {
			continue
		}
		owner := loadU64(h, slotOff(i, slotOffOwner))
		if owner == 0 {
			continue
		}
		hb := int64(loadU64(h, slotOff(i, slotOffHeartbeat)))
		if now-hb <= consumerTTL.Nanoseconds() {
			continue // 存活消费者：绝不打扰
		}
		ppos := loadU64(h, slotOff(i, slotOffPending))
		if ppos == holdNone {
			// 死亡消费者无在途记录：顺手释放槽位。
			casU64(h, slotOff(i, slotOffOwner), owner, 0)
			continue
		}
		pseq := loadU64(h, slotOff(i, slotOffSeq))
		// 先用自己的槽位为该记录占住回收下限，再 CAS 夺取；
		// 失败则撤销占用（无副作用，仅短暂保守地压低回收下限）。
		storeU64(h, slotOff(c.slot, slotOffSeq), pseq)
		storeU64(h, slotOff(c.slot, slotOffPending), ppos)
		if !casU64(h, slotOff(i, slotOffPending), ppos, holdNone) {
			storeU64(h, slotOff(c.slot, slotOffPending), holdNone)
			storeU64(h, slotOff(c.slot, slotOffSeq), 0)
			continue
		}
		// 夺取成功：验证记录尚未被回绕覆盖。
		rec, ok := c.r.readRecordIfIntact(ppos, pseq)
		if !ok {
			// 死亡消费者迟迟未被接管，记录空间已被写者复用：
			// 该记录丢失（未曾确认），释放占用标记继续。
			storeU64(h, slotOff(c.slot, slotOffPending), holdNone)
			storeU64(h, slotOff(c.slot, slotOffSeq), 0)
			casU64(h, slotOff(i, slotOffOwner), owner, 0)
			continue
		}
		casU64(h, slotOff(i, slotOffOwner), owner, 0) // 释放死槽
		return rec, ppos, true
	}
	return Record{}, 0, false
}

// readRecordIfIntact 读取逻辑位置 pos 处、序列号为 seq 的已提交记录。
// 若该位置的空间已被写者回绕复用（物理字节不再属于这条记录），
// 或帧头/帧尾/CRC 与 seq 不符，返回 false。
func (r *Ring) readRecordIfIntact(pos, seq uint64) (Record, bool) {
	// 逻辑位置 pos 的字节在 reservedPos 越过 pos+capacity 后被复用。
	if loadU64(r.header, offReservedPos) > pos+r.capacity {
		return Record{}, false
	}
	var fh [FrameHeaderSize]byte
	ringReadAt(r.ring, pos, fh[:])
	if binary.LittleEndian.Uint32(fh[0:4]) != FrameMagic || frameSeq(fh[:]) != seq {
		return Record{}, false
	}
	plen := frameLen(fh[:])
	payload := make([]byte, plen)
	ringReadAt(r.ring, pos+FrameHeaderSize, payload)
	var tr [FrameTrailerSize]byte
	ringReadAt(r.ring, pos+FrameHeaderSize+uint64(plen), tr[:])
	if binary.LittleEndian.Uint32(tr[4:8]) != CommitMagic ||
		binary.LittleEndian.Uint32(tr[0:4]) != frameCRC(seq, payload) {
		return Record{}, false
	}
	return Record{Seq: seq, Payload: payload}, true
}

// Confirm 确认当前持有的记录（处理完成后调用）。
// 确认后该记录占用的空间才允许写者回绕复用。没有持有记录时返回 ErrNoHold。
func (c *Consumer) Confirm() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.held == nil {
		return ErrNoHold
	}
	h := c.r.header
	storeU64(h, slotOff(c.slot, slotOffPending), holdNone)
	storeU64(h, slotOff(c.slot, slotOffSeq), 0)
	storeU64(h, slotOff(c.slot, slotOffHeartbeat), uint64(time.Now().UnixNano()))
	c.held = nil
	return c.r.flush()
}

// Close 注销消费者：停止心跳并释放槽位。若仍持有未确认记录，
// 槽位被标记为“遗弃”（心跳归零，立即可被其他消费者接管重投），
// 记录不会随退出丢失。
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		held := c.held != nil
		close(c.stopHB)
		c.mu.Unlock()
		<-c.hbDone // 等心跳 goroutine 退出，之后不再触碰映射

		h := c.r.header
		if held {
			// 遗弃：保留 owner 与在途记录，心跳归零使其立即可被接管。
			storeU64(h, slotOff(c.slot, slotOffHeartbeat), 0)
		} else {
			storeU64(h, slotOff(c.slot, slotOffPending), holdNone)
			storeU64(h, slotOff(c.slot, slotOffSeq), 0)
			storeU64(h, slotOff(c.slot, slotOffOwner), 0)
		}
		c.r.consMu.Lock()
		for i, other := range c.r.cons {
			if other == c {
				c.r.cons = append(c.r.cons[:i], c.r.cons[i+1:]...)
				break
			}
		}
		c.r.consMu.Unlock()
	})
	return nil
}

// simulateCrash 仅供包内测试：模拟消费者进程崩溃——心跳停止、句柄失效，
// 但槽位与在途记录原样遗弃（等待心跳过期后被其他消费者接管）。
func (c *Consumer) simulateCrash() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		close(c.stopHB)
		c.mu.Unlock()
		<-c.hbDone
		// 不释放槽位、不清心跳：让心跳自然过期。
	})
}

// reclaimFloor 计算写者回绕复用空间的下限（逻辑位置）：
// 写者不得覆盖该位置及之后的任何字节。
//
//   - 单消费者模式（含 v1 旧格式）：下限即 readPos；
//   - 消费者组模式：下限是“共享领取游标 claimPos”与“所有活跃消费者
//     持有未确认记录的位置”的最小值。心跳过期的槽位不参与下限
//     （其在途记录可被接管重投），因此崩溃消费者最终不会卡住写者；
//     而存活但慢的消费者会拖住下限，写者空间不足时返回 ErrFull。
func (r *Ring) reclaimFloor() uint64 {
	h := r.header
	if r.legacyV1 || loadU32(h, offMode) != modeGroup {
		return loadU64(h, offReadPos)
	}
	floor := loadU64(h, offClaimPos)
	now := time.Now().UnixNano()
	for i := 0; i < MaxConsumers; i++ {
		if loadU64(h, slotOff(i, slotOffOwner)) == 0 {
			continue
		}
		hb := int64(loadU64(h, slotOff(i, slotOffHeartbeat)))
		if now-hb > consumerTTL.Nanoseconds() {
			continue // 已崩溃/遗弃：不参与下限
		}
		if p := loadU64(h, slotOff(i, slotOffPending)); p != holdNone && p < floor {
			floor = p
		}
	}
	return floor
}
