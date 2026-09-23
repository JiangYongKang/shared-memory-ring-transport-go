package shmring

import (
	"encoding/binary"
	"errors"
	"syscall"
)

// Recover 在没有存活写者时回收崩溃写者残留的半条记录。
//
// 返回 recovered=true 时，tornBytes 为被丢弃的残片逻辑字节数，
// reservedPos 被退回到 writePos（最后一条完整记录之后），已提交记录与
// readPos 不做任何改动。若写锁正被存活写者持有，返回 (false, 0, nil)，
// 因为进行中的帧可能马上合法提交，不得由旁观者回收。
func (r *Ring) Recover() (recovered bool, tornBytes int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recoverIfNoWriter()
}

// recoverIfNoWriter 在没有存活写者（写锁空闲）时执行崩溃回收。
//
// 协议中写者的推进分两个逻辑游标：
//
//	reservedPos 已预留（含半条记录可能已占用）的逻辑末端
//	writePos    已完整提交、可供读者消费的逻辑末端
//
// 正常情况下二者相等或“相差一帧正在发布”。写者在预留之后、提交之前崩溃时，
// writePos 落在半条记录之前，reservedPos 指向半条记录之后。回收动作即：
// 从 readPos 起逐帧校验，丢弃末尾未提交/校验失败的残片，将两个游标
// 一并退回到最后一条完整记录之后。半条记录的字节残留在容量回绕后会被
// 后续写入覆盖，永远不会被读者当作有效数据。
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

// errTorn 是内部“遇到未提交/损坏帧”信号。
var errTorn = errors.New("shmring: torn frame encountered")
