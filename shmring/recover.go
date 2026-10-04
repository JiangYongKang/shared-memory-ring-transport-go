package shmring

import (
	"encoding/binary"
	"syscall"
)

// Recover 在没有存活写者时处理崩溃写者残留的未提交分配。
//
// v2 缓冲区：残留分配被改写为“空洞帧”并推进 writePos 越过它——
// 这一次分配的空间永不回收重用，空洞帧永久留在逻辑流中，任何挂载者
// 都能看出“这里曾经有一次没完成的写入”。返回 recovered=true 时，
// tornBytes 为空洞的逻辑字节数。已提交记录与各消费游标不做任何改动。
//
// v1 旧格式缓冲区：保持旧行为，reservedPos 退回到 writePos（回收残片）。
//
// 若写锁正被存活写者持有，返回 (false, 0, nil)，因为进行中的帧可能
// 马上合法提交，不得由旁观者处理。恢复是幂等的。
func (r *Ring) Recover() (recovered bool, tornBytes int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recoverIfNoWriter()
}

// recoverIfNoWriter 在没有存活写者（写锁空闲）时执行崩溃恢复。
//
// 协议中写者的推进分两个逻辑游标：
//
//	reservedPos 已预留（含半条记录可能已占用）的逻辑末端
//	writePos    已完整提交、可供读者消费的逻辑末端
//
// 写者在预留之后、提交之前崩溃时，writePos 落在半条记录之前，
// reservedPos 指向半条记录之后。v2 的恢复动作即 healGapLocked：
// 把 [writePos, reservedPos) 固化为空洞帧。v1 则回退 reservedPos。
func (r *Ring) recoverIfNoWriter() (recovered bool, tornBytes int64, err error) {
	locked, err := flockNb(r.wlock, syscall.LOCK_EX)
	if err != nil {
		return false, 0, err
	}
	if !locked {
		// 有存活写者：它对自己正在发布的帧负责，不做回收。
		return false, 0, nil
	}
	defer func() { _ = syscall.Flock(int(r.wlock.Fd()), syscall.LOCK_UN) }()

	if !r.legacyV1 {
		return r.healGapLocked()
	}

	h := r.header
	reserved := loadU64(h, offReservedPos)
	written := loadU64(h, offWritePos)
	readPos := loadU64(h, offReadPos)

	// 游标自洽性检查（单调顺序必须成立）。
	if readPos > written || written > reserved {
		return false, 0, ErrCorrupt
	}
	if written == reserved {
		return false, 0, nil // 没有进行中的帧
	}

	// 从读位置顺序扫描已提交帧，定位到半条记录的起点。
	pos := readPos
	for pos < written {
		var fh [FrameHeaderSize]byte
		ringReadAt(r.ring, pos, fh[:])
		if binary.LittleEndian.Uint32(fh[0:4]) != FrameMagic {
			return false, 0, ErrCorrupt
		}
		n := frameTotal(int(frameLen(fh[:])))
		if pos+n > written {
			return false, 0, ErrCorrupt
		}
		pos += n
	}
	if pos != written {
		return false, 0, ErrCorrupt
	}

	// [written, reserved) 即崩溃写者残留：无论残片的帧头看起来像什么，
	// 帧尾不可能带匹配的 CommitMagic（见提交顺序），一律丢弃。
	torn := int64(reserved - written)
	storeU64(h, offReservedPos, written)
	return true, torn, nil
}

// healGapLocked 是 v2 缓冲区的在线自愈：调用方必须已持有写锁
// （或确认没有存活写者）。若 reservedPos > writePos，说明上一个写者
// 在发布前崩溃，[writePos, reservedPos) 是它没写完的那一次分配。
// 自愈把这段区间改写为一帧空洞帧（GapMagic 头 + 提交标记帧尾），
// 再把 writePos 推进到 reservedPos：
//
//   - 这一次分配的空间永不回收重用，空洞帧永久可见（“这里曾经有一次
//     没完成的写入”）；被烧掉的序列号同样保留为空洞；
//   - 已提交记录与 readPos/claimPos 不做任何改动；
//   - 消费者读到空洞帧时按长度跳过，不会把它当作数据；
//   - 自愈是确定性的（同一区间写出同样的空洞帧），崩溃在自愈中途
//     时，下一个写者会重写同一空洞帧，因此幂等。
func (r *Ring) healGapLocked() (healed bool, tornBytes int64, err error) {
	h := r.header
	reserved := loadU64(h, offReservedPos)
	written := loadU64(h, offWritePos)
	if written > reserved {
		return false, 0, ErrCorrupt
	}
	if written == reserved {
		return false, 0, nil // 没有进行中的帧
	}
	torn := reserved - written
	if torn < frameOverhead {
		// 预留的最小单位就是一帧开销，小于它说明头部已损坏。
		return false, 0, ErrCorrupt
	}
	// 1) 在 writePos 处写空洞帧头（长度覆盖整个残留区间）。
	var fh [FrameHeaderSize]byte
	putGapHeader(fh[:], uint32(torn)-frameOverhead)
	ringWriteAt(r.ring, written, fh[:])
	// 2) 在区间末尾写空洞帧尾（提交标记）。此后崩溃，下一个写者
	//    会在同一位置重写同样的空洞帧，读者仍不可见。
	var tr [FrameTrailerSize]byte
	putGapTrailer(tr[:])
	ringWriteAt(r.ring, written+torn-FrameTrailerSize, tr[:])
	if err := r.flush(); err != nil {
		return false, 0, err
	}
	// 3) 原子推进 writePos：空洞帧自此对读者可见（并被跳过）。
	storeU64(h, offWritePos, reserved)
	if err := r.flush(); err != nil {
		return false, 0, err
	}
	return true, int64(torn), nil
}
