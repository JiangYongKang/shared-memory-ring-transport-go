package shmring

import (
	"encoding/binary"
	"errors"
	"syscall"
)

// Recover 在没有存活写者时，把崩溃写者残留的半条记录登记为撕裂区并
// 让写游标在线跳过它（在线自愈的挂载时版本）。
//
// 返回 recovered=true 时，tornBytes 为被跳过的残片逻辑字节数：
//   - 残片对应的逻辑区间 [旧 writePos, reservedPos) 被登记进撕裂区表，
//     永不回收重用（reservedPos 只前进、不回退），TornCount 递增，
//     “这里曾经有一次没完成的写入”始终可观测；
//   - writePos 直接推进到 reservedPos，读者借撕裂区表整段跳过残片；
//   - 已提交记录与 readPos（回收边界）不做任何改动，已确认数据不丢失、
//     不被改写。
//
// 若写锁正被存活写者持有，返回 (false, 0, nil)：进行中的帧可能马上
// 合法提交，不得由旁观者动它；该写者自己的下一次 Write 会完成同样的
// 自愈（见 writer.go 的 healLocked）。
func (r *Ring) Recover() (recovered bool, tornBytes int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recoverIfNoWriter()
}

// healLocked 在持有写锁的前提下执行在线自愈。
//
// 协议不变量：任何写者都只在持有写锁期间推进 reservedPos/writePos，
// 因此拿到写锁时若发现 reservedPos > writePos，间隙必然是某个已死亡
// 写者留下的半条记录（存活写者不会在锁外留下间隙）。自愈动作：
//
//  1. 把间隙 [writePos, reservedPos) 登记进撕裂区表（不回收、不重用）；
//  2. writePos 推进到 reservedPos，消费者经撕裂区表跳过残片；
//  3. 已提交记录、序列号、各消费者 ack 游标一律不动。
//
// 返回跳过的字节数；无间隙时返回 0。
func (r *Ring) healLocked() int64 {
	h := r.header
	reserved := loadU64(h, offReservedPos)
	written := loadU64(h, offWritePos)
	if written >= reserved {
		return 0
	}
	torn := int64(reserved - written)
	appendTornLocked(h, written, reserved)
	storeU64(h, offWritePos, reserved)
	return torn
}

// recoverIfNoWriter 在没有存活写者（写锁空闲）时执行挂载时自愈。
//
// 先校验已提交区域自洽（从 readPos 顺序扫描到 writePos，撕裂区整段
// 跳过），再执行 healLocked。恢复是幂等的：无间隙时返回 (false, 0, nil)。
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

	h := r.header
	reserved := loadU64(h, offReservedPos)
	written := loadU64(h, offWritePos)
	readPos := loadU64(h, offReadPos)
	claim := loadU64(h, offClaimPos)

	// 游标自洽性检查（单调顺序必须成立）。
	if readPos > claim || claim > written || written > reserved {
		return false, 0, ErrCorrupt
	}

	// 从回收边界顺序扫描已提交帧，撕裂区整段跳过，核对帧结构自洽。
	pos := readPos
	for pos < written {
		if end, ok := tornEndAt(h, pos); ok {
			if end > written {
				return false, 0, ErrCorrupt
			}
			pos = end
			continue
		}
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

	torn := r.healLocked()
	if torn == 0 {
		return false, 0, nil
	}
	return true, torn, nil
}

// errTorn 是内部“遇到未提交/损坏帧”信号。
var errTorn = errors.New("shmring: torn frame encountered")
