package shmring

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Consumer 是挂载在同一缓冲区上的一个独立消费者。
//
// 多消费者语义（竞争领取 / competing consumers）：
//   - 所有消费者共享全局 claimPos 游标，通过 CAS 竞争领取下一条记录，
//     每条完整记录恰好被一个消费者领取一次；
//   - 每个消费者持有独立槽位与 ack 游标，各自推进、互不阻塞；
//   - 写者回绕复用空间以“所有存活消费者 ack 游标的最小值”为界，
//     慢消费者会让写者看到 ErrFull，而不会覆盖任何人未确认的记录；
//   - 消费者进程死亡（含 SIGKILL）后，其槽位由其它挂载者经 OFD 锁
//     探测自动回收，不会永久卡住空间释放；其已领取未确认的记录随之
//     放弃（不会转投给其它消费者，避免重复处理）。
type Consumer struct {
	r    *Ring
	slot int
	lk   *os.File // 持有 rlock 文件第 slot 字节的 OFD 锁

	mu     sync.Mutex // 串行化本消费者的 Read/Sync/Close
	closed atomic.Bool
}

// Consumer 在本 Ring 上注册一个新消费者并返回其句柄。
//
// 注册前会先回收死亡消费者遗留的槽位；仍耗尽（MaxConsumers 个存活
// 消费者）时返回 ErrTooManyConsumers。不再消费时必须调用 Close 注销；
// 进程退出会由内核释放 OFD 锁，槽位随后被其它挂载者自动回收。
func (r *Ring) Consumer() (*Consumer, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}
	// 先回收死亡槽位，避免被退出的消费者占满。
	r.reapDeadSlots()

	lk, err := os.OpenFile(r.rlockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	h := r.header
	for slot := 0; slot < MaxConsumers; slot++ {
		mask := uint64(1) << uint(slot)
		if loadU64(h, offSlotBitmap)&mask != 0 {
			continue
		}
		// OFD 锁绑定打开文件描述：持锁期间其它挂载者探测该槽位为“存活”；
		// 进程死亡（含 SIGKILL）时内核自动释放，槽位变为可回收。
		locked, err := ofdTryLockByte(lk.Fd(), int64(slot))
		if err != nil {
			lk.Close()
			return nil, err
		}
		if !locked {
			continue // 其它挂载者正在注册该槽位
		}
		// 先发布 ack 游标（从当前领取位置起算：此前记录与本消费者无关），
		// 再占用槽位，保证“已注册槽位必有合法 ack 游标”。
		base := offSlotBase + slot*slotStride
		storeU64(h, base+offSlotAck, loadU64(h, offClaimPos))
		storeU64(h, base+offSlotActive, uint64(time.Now().UnixNano()))
		published := false
		for {
			bm := loadU64(h, offSlotBitmap)
			if bm&mask != 0 {
				break // 协议下不会发生；保守退出
			}
			if casU64(h, offSlotBitmap, bm, bm|mask) {
				published = true
				break
			}
		}
		if !published {
			_ = ofdUnlockByte(lk.Fd(), int64(slot))
			continue
		}
		if err := r.flush(); err != nil {
			// 注册未完成：摘除槽位并释放锁。
			for {
				bm := loadU64(h, offSlotBitmap)
				if bm&mask == 0 || casU64(h, offSlotBitmap, bm, bm&^mask) {
					break
				}
			}
			_ = ofdUnlockByte(lk.Fd(), int64(slot))
			lk.Close()
			return nil, err
		}
		return &Consumer{r: r, slot: slot, lk: lk}, nil
	}
	lk.Close()
	return nil, ErrTooManyConsumers
}

// Slot 返回该消费者占用的槽位号（用于日志与测试断言）。
func (c *Consumer) Slot() int { return c.slot }

// AckPos 返回该消费者已确认到的逻辑位置。
func (c *Consumer) AckPos() uint64 {
	return loadU64(c.r.header, offSlotBase+c.slot*slotStride+offSlotAck)
}

// Sync 把本消费者的 ack 游标追平到当前领取位置。
//
// 本消费者确认语义是“领取即确认”（Read 内部完成），因此它在任意空闲
// 时刻都不持有未确认记录，可以安全地把 ack 游标推进到 claimPos，
// 释放回收边界。活跃消费者无需调用（Read 在读空时会自动同步）；
// 它面向“长时间不调用 Read 但又不想拖住写者”的消费者。
func (c *Consumer) Sync() error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.r.closed.Load() {
		return ErrClosed
	}
	c.syncLocked()
	return nil
}

// syncLocked 假定调用方持有 c.mu。
func (c *Consumer) syncLocked() {
	h := c.r.header
	base := offSlotBase + c.slot*slotStride
	claim := loadU64(h, offClaimPos)
	if loadU64(h, base+offSlotAck) < claim {
		storeU64(h, base+offSlotAck, claim)
		storeU64(h, base+offSlotActive, uint64(time.Now().UnixNano()))
		_ = c.r.flush()
	}
	c.r.refreshReclaimBoundary()
}

// Read 竞争领取下一条完整记录并确认它（推进本消费者的 ack 游标）。
//
// 领取经 CAS 完成：同一时刻只有一个消费者能领到同一条记录；领取失败
// 只会重试，不会阻塞其它消费者。没有可领取的记录时返回 ErrEmpty。
// 崩溃写者留下的撕裂区在此被整段跳过，不会当作数据交付。
func (c *Consumer) Read() (Record, error) {
	if c.closed.Load() {
		return Record{}, ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.r
	if r.closed.Load() {
		return Record{}, ErrClosed
	}
	h := r.header
	for {
		pos := loadU64(h, offClaimPos)
		wp := loadU64(h, offWritePos)
		if pos >= wp {
			// 空闲即同步：本消费者没有任何已领取未确认的记录，
			// 把 ack 游标追平到领取位置，避免“活跃但暂时没数据”的
			// 消费者无谓拖住回收边界。
			c.syncLocked()
			return Record{}, ErrEmpty
		}
		// 撕裂区：整段跳过（任何消费者都可执行跳过，CAS 保证只做一次）。
		if end, ok := tornEndAt(h, pos); ok {
			casU64(h, offClaimPos, pos, end)
			continue
		}
		var fh [FrameHeaderSize]byte
		ringReadAt(r.ring, pos, fh[:])
		if binary.LittleEndian.Uint32(fh[0:4]) != FrameMagic {
			return Record{}, fmt.Errorf("%w: frame at logical %d missing magic", ErrCorrupt, pos)
		}
		plen := frameLen(fh[:])
		total := frameTotal(int(plen))
		if pos+total > wp {
			return Record{}, fmt.Errorf("%w: frame at logical %d overruns write pos %d", ErrCorrupt, pos, wp)
		}
		seq := frameSeq(fh[:])
		payload := make([]byte, plen)
		ringReadAt(r.ring, pos+FrameHeaderSize, payload)
		var tr [FrameTrailerSize]byte
		ringReadAt(r.ring, pos+FrameHeaderSize+uint64(plen), tr[:])
		if binary.LittleEndian.Uint32(tr[4:8]) != CommitMagic ||
			binary.LittleEndian.Uint32(tr[0:4]) != frameCRC(seq, payload) {
			return Record{}, fmt.Errorf("%w: committed frame at logical %d failed CRC", ErrCorrupt, pos)
		}
		if !casU64(h, offClaimPos, pos, pos+total) {
			continue // 被其它消费者抢先领取，重取下一条
		}
		// 领取成功：确认（推进本槽 ack 游标）并刷新全局回收边界。
		// 帧空间只有在所有存活消费者的 ack 游标都越过它之后才可回绕复用，
		// 因此上面的负载拷贝绝不会被写者并发覆盖。
		base := offSlotBase + c.slot*slotStride
		storeU64(h, base+offSlotAck, pos+total)
		storeU64(h, base+offSlotActive, uint64(time.Now().UnixNano()))
		if err := r.flush(); err != nil {
			return Record{}, err
		}
		r.refreshReclaimBoundary()
		return Record{Seq: seq, Payload: payload}, nil
	}
}

// Close 注销消费者并释放槽位。幂等之外的重复调用返回 ErrClosed。
func (c *Consumer) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Ring 可能已先关闭并解除映射；此时只需释放锁文件描述符。
	if !c.r.closed.Load() && c.r.header != nil {
		h := c.r.header
		mask := uint64(1) << uint(c.slot)
		for {
			bm := loadU64(h, offSlotBitmap)
			if bm&mask == 0 || casU64(h, offSlotBitmap, bm, bm&^mask) {
				break
			}
		}
		_ = c.r.flush()
	}
	if c.lk != nil {
		_ = ofdUnlockByte(c.lk.Fd(), int64(c.slot))
		_ = c.lk.Close()
		c.lk = nil
	}
	return nil
}

// reapDeadSlots 回收属主进程已死亡的消费者槽位。
//
// 对每个已注册槽位探测其 OFD 锁字节：能拿到锁说明属主（进程或关闭的
// 句柄）已不存在，摘除槽位使其 ack 游标不再拖住回收边界。探测与摘除
// 都基于“先持锁、后改位图”的协议，不会误伤正在注册或正常关闭的槽位。
func (r *Ring) reapDeadSlots() {
	h := r.header
	if loadU64(h, offSlotBitmap) == 0 {
		return
	}
	f, err := os.OpenFile(r.rlockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return // 无法探测时保守跳过，绝不误摘
	}
	defer f.Close()
	for slot := 0; slot < MaxConsumers; slot++ {
		mask := uint64(1) << uint(slot)
		if loadU64(h, offSlotBitmap)&mask == 0 {
			continue
		}
		dead, err := ofdProbeDead(f.Fd(), int64(slot))
		if err != nil || !dead {
			continue
		}
		// 属主已死亡：摘除槽位。位图在我们探测期间不会被别人置位
		// （注册只挑空位），摘除是安全的。
		for {
			bm := loadU64(h, offSlotBitmap)
			if bm&mask == 0 || casU64(h, offSlotBitmap, bm, bm&^mask) {
				break
			}
		}
		_ = ofdUnlockByte(f.Fd(), int64(slot))
	}
}

// refreshReclaimBoundary 回收死亡槽位并把 readPos（回收边界）推进到
// “所有存活消费者 ack 游标的最小值”。返回是否发生了推进。
//
// 单调性：各槽 ack 游标只前进、死亡槽位只会被摘除、新槽位从当前
// claimPos 起算，因此最小值不会低于当前 readPos；CAS 守护保证并发
// 推进不会回退。写者的空间计算以 readPos 为界，这里每推进一步，
// 写者回绕可复用的空间就释放一步。
func (r *Ring) refreshReclaimBoundary() bool {
	r.reapDeadSlots()
	h := r.header
	min := loadU64(h, offClaimPos)
	bm := loadU64(h, offSlotBitmap)
	for slot := 0; slot < MaxConsumers; slot++ {
		if bm&(uint64(1)<<uint(slot)) == 0 {
			continue
		}
		if ack := loadU64(h, offSlotBase+slot*slotStride+offSlotAck); ack < min {
			min = ack
		}
	}
	for {
		cur := loadU64(h, offReadPos)
		if min <= cur {
			return false
		}
		if casU64(h, offReadPos, cur, min) {
			_ = r.flush() // 持久化回收边界；失败仅影响崩溃后的保守程度
			return true
		}
	}
}
