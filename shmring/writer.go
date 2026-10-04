package shmring

import (
	"encoding/binary"
	"fmt"
	"syscall"
)

// preCommitHook 非 nil 时，写者在“帧已全部写入并 msync、但提交标记与
// 游标尚未发布”的时点回调该函数。它是测试钩子（见 *_test.go），用于在
// 子进程中稳定复现“写者中途被杀掉”的崩溃：此时文件里只留下半条记录。
var preCommitHook func(seq uint64)

// midPayloadHook 非 nil 时，写者在“帧头与前半段负载已写入、后半段负载
// 与帧尾尚未写入”的时点回调，用于复现更粗暴的帧字节中途崩溃。
var midPayloadHook func(seq uint64)

// Write 以原子发布方式写入一条完整记录，返回该记录的序列号。
//
// 语义：
//   - 剩余逻辑空间（capacity - (reservedPos-readPos)）不足以容纳
//     帧头+负载+帧尾时返回 ErrFull，绝不覆盖未消费记录；
//   - 负载超过 Capacity()-frameOverhead 时返回 ErrPayloadTooLarge；
//   - 跨进程写者通过写锁串行化，同进程写者通过互斥锁串行化；
//   - 序列号在预留前单调分配并持久化，因此崩溃造成的序列号空洞是可解释的，
//     消费者可据此识别“曾经尝试但未提交”的写入。
func (r *Ring) Write(payload []byte) (uint64, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(payload) > r.MaxPayload() {
		return 0, fmt.Errorf("%w: payload=%d, max=%d",
			ErrPayloadTooLarge, len(payload), r.MaxPayload())
	}

	if err := syscall.Flock(int(r.wlock.Fd()), syscall.LOCK_EX); err != nil {
		return 0, err
	}
	defer func() { _ = syscall.Flock(int(r.wlock.Fd()), syscall.LOCK_UN) }()

	h := r.header

	// 在线自愈：拿到写锁时若上一个写者留下了半条记录（reserved>write
	// 只可能是死亡写者的残片），登记撕裂区并跳过，本写者照常继续写入。
	// 不需要重新挂载、不需要人工清理；残片空间永不回收重用。
	if torn := r.healLocked(); torn > 0 {
		if err := r.flush(); err != nil {
			return 0, err
		}
	}

	reserved := loadU64(h, offReservedPos)
	readPos := loadU64(h, offReadPos)
	used := reserved - readPos // 注意：reserved >= readPos
	total := frameTotal(len(payload))
	if uint64(len(payload)) > r.capacity-frameOverhead ||
		total > r.capacity-used {
		// 写满时先回收死亡消费者的槽位、刷新回收边界再复核一次：
		// 死亡消费者不得把写者永久卡在 ErrFull 上。
		if r.refreshReclaimBoundary() {
			reserved = loadU64(h, offReservedPos)
			readPos = loadU64(h, offReadPos)
			used = reserved - readPos
		}
		if total > r.capacity-used {
			// ErrFull 不消耗序列号：只有真正预留成功的写入才分配 seq。
			return 0, ErrFull
		}
	}

	// 1) 单调分配序列号并持久化，使崩溃恢复后序列号不会回卷重用。
	seq := loadU64(h, offNextSeq)
	storeU64(h, offNextSeq, seq+1)

	// 2) 预留逻辑空间并发布预留游标。读者与其它写者据此判断空间占用。
	newReserved := reserved + total
	storeU64(h, offReservedPos, newReserved)
	storeU64(h, offReservedPos, newReserved)
	if err := r.flush(); err != nil {
		// 预留尚未被任何读者可见；本写者回退即可。
		storeU64(h, offReservedPos, reserved)
		return 0, err
	}

	// 3) 写帧头。
	var fh [FrameHeaderSize]byte
	putFrameHeader(fh[:], seq, uint32(len(payload)))
	ringWriteAt(r.ring, reserved, fh[:])

	// 4) 写负载。若注入了中途崩溃钩子，只写前半段并在此终止。
	if len(payload) > 0 {
		if midPayloadHook != nil {
			half := len(payload) / 2
			ringWriteAt(r.ring, reserved+FrameHeaderSize, payload[:half])
			_ = r.flush()
			midPayloadHook(seq)
			return 0, nil // 测试钩子场景：进程随后退出
		}
		ringWriteAt(r.ring, reserved+FrameHeaderSize, payload)
	}

	// 5) 写帧尾（CRC + 提交标记）。CRC 覆盖序列号与负载，是完整性依据。
	var tr [FrameTrailerSize]byte
	putTrailer(tr[:], frameCRC(seq, payload))
	ringWriteAt(r.ring, reserved+FrameHeaderSize+uint64(len(payload)), tr[:])

	// 6) 帧字节落盘。此刻之后崩溃，残片仍可由“帧尾提交标记 + CRC”识别。
	if err := r.flush(); err != nil {
		return 0, err
	}

	// 测试钩子：模拟“帧已写完但尚未发布”的中途崩溃窗口。
	if preCommitHook != nil {
		preCommitHook(seq)
	}

	// 7) 原子发布：推进写游标后该帧才对读者可见。
	storeU64(h, offWritePos, newReserved)
	if err := r.flush(); err != nil {
		return 0, err
	}
	return seq, nil
}

// isCommittedAt 报告逻辑位置 pos 处是否为一帧已完整提交的帧，并返回其长度。
// 供读者使用；返回 false 涵盖“帧头尚未写全”和“帧尾无提交标记/CRC 不符”。
func (r *Ring) isCommittedAt(pos uint64) (seq uint64, payloadLen uint32, ok bool, corrupt bool) {
	var fh [FrameHeaderSize]byte
	ringReadAt(r.ring, pos, fh[:])
	if binary.LittleEndian.Uint32(fh[0:4]) != FrameMagic {
		return 0, 0, false, false
	}
	plen := frameLen(fh[:])
	n := frameTotal(int(plen))
	if pos+n > loadU64(r.header, offReservedPos) {
		return 0, 0, false, false
	}
	var tr [FrameTrailerSize]byte
	ringReadAt(r.ring, pos+FrameHeaderSize+uint64(plen), tr[:])
	if binary.LittleEndian.Uint32(tr[4:8]) != CommitMagic {
		return 0, 0, false, false
	}
	seqv := frameSeq(fh[:])
	payload := make([]byte, plen)
	ringReadAt(r.ring, pos+FrameHeaderSize, payload)
	if binary.LittleEndian.Uint32(tr[0:4]) != frameCRC(seqv, payload) {
		return 0, 0, false, true
	}
	return seqv, plen, true, false
}
