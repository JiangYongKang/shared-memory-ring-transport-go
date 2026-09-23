package shmring

import "fmt"

// Record 是一条完整读出的记录。
type Record struct {
	// Seq 是该记录的单调序列号，在缓冲区整个生命周期内不重用。
	Seq uint64
	// Payload 是记录负载的拷贝，可在后续读取后继续安全使用。
	Payload []byte
}

// Peek 返回下一条已提交记录但不确认（不推进读位置）。
//
// 对同一位置的连续 Peek 会返回同一条记录、同一个序列号——消费者据此
// 识别“重复投递”而不是将其当作新数据；需要新记录时调用 Read/Advance。
// 若下一条记录尚未完整提交（写者正在发布或已崩溃），返回 ErrEmpty。
func (r *Ring) Peek() (Record, error) {
	if r.closed.Load() {
		return Record{}, ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, _, _, err := r.peekFrameLocked()
	return rec, err
}

// Read 原子地读取下一条记录并确认它（推进读位置）。
// 没有完整记录时返回 ErrEmpty。读位置推进后该记录占用的空间才允许
// 写者回绕复用，因此读不到“已被回绕覆盖”的旧记录。
func (r *Ring) Read() (Record, error) {
	if r.closed.Load() {
		return Record{}, ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, pos, n, err := r.peekFrameLocked()
	if err != nil {
		return Record{}, err
	}
	storeU64(r.header, offReadPos, pos+n)
	if err := r.flush(); err != nil {
		return Record{}, err
	}
	return rec, nil
}

// Advance 在 Peek 之后显式确认当前记录（推进到下一帧起点）。
// 与 Peek 配合可实现“先处理、后确认”的至少一次投递：确认前崩溃，
// 重新挂载后该记录会被再次投递，序列号相同，可安全去重。
func (r *Ring) Advance() error {
	if r.closed.Load() {
		return ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	_, pos, n, err := r.peekFrameLocked()
	if err != nil {
		return err
	}
	storeU64(r.header, offReadPos, pos+n)
	return r.flush()
}

// peekFrameLocked 假定调用方持有 r.mu。返回记录及其逻辑起点与帧长。
func (r *Ring) peekFrameLocked() (rec Record, pos, n uint64, err error) {
	h := r.header
	readPos := loadU64(h, offReadPos)
	written := loadU64(h, offWritePos)
	if readPos >= written {
		return Record{}, 0, 0, ErrEmpty
	}
	seq, plen, ok, corrupt := r.isCommittedAt(readPos)
	if corrupt {
		return Record{}, 0, 0, fmt.Errorf("%w: committed frame at logical %d failed CRC", ErrCorrupt, readPos)
	}
	if !ok {
		// 正常情况下不会发生：writePos 只会在完整提交后推进。
		return Record{}, 0, 0, ErrEmpty
	}
	total := frameTotal(int(plen))
	if readPos+total > written {
		return Record{}, 0, 0, ErrEmpty
	}
	payload := make([]byte, plen)
	ringReadAt(r.ring, readPos+FrameHeaderSize, payload)
	return Record{Seq: seq, Payload: payload}, readPos, total, nil
}
