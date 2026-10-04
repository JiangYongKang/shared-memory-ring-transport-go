package shmring

import (
	"encoding/binary"
	"fmt"
)

// Record 是一条完整读出的记录。
type Record struct {
	// Seq 是该记录的单调序列号，在缓冲区整个生命周期内不重用。
	// 重复投递（确认前崩溃导致的再次投递）序列号相同，消费者据此去重。
	Seq uint64
	// Payload 是记录负载的拷贝，可在后续读取后继续安全使用。
	Payload []byte
}

// legacyConsumer 返回本 Ring 的隐式消费者（旧式单消费者 API 使用），
// 首次调用时注册一个普通消费者槽位，语义与 Consumer() 完全一致。
func (r *Ring) legacyConsumer() (*Consumer, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}
	r.legacyMu.Lock()
	defer r.legacyMu.Unlock()
	if r.legacy != nil && !r.legacy.closed.Load() {
		return r.legacy, nil
	}
	c, err := r.Consumer()
	if err != nil {
		return nil, err
	}
	r.legacy = c
	return c, nil
}

// Peek 返回下一条待领取记录但不领取、不确认（不推进任何游标）。
//
// 单消费者场景下，对同一位置的连续 Peek 会返回同一条记录、同一个
// 序列号——消费者据此识别“重复投递”而不是将其当作新数据；需要新记录
// 时调用 Read/Advance。多消费者并发时，Peek 到的记录可能随后被其它
// 消费者领走，Peek 仅作观测用途。
// 若下一条记录尚未完整提交（写者正在发布或已崩溃），返回 ErrEmpty。
func (r *Ring) Peek() (Record, error) {
	if r.closed.Load() {
		return Record{}, ErrClosed
	}
	// 确保隐式消费者已注册：其 ack 游标参与回收边界，保证 Peek 到的
	// 记录在确认前不会被写者回绕覆盖。
	if _, err := r.legacyConsumer(); err != nil {
		return Record{}, err
	}
	h := r.header
	pos := loadU64(h, offClaimPos)
	wp := loadU64(h, offWritePos)
	// 本地跳过撕裂区（不推进 claimPos）。
	for pos < wp {
		if end, ok := tornEndAt(h, pos); ok {
			pos = end
			continue
		}
		break
	}
	if pos >= wp {
		return Record{}, ErrEmpty
	}
	seq, plen, ok, corrupt := r.isCommittedAt(pos)
	if corrupt {
		return Record{}, fmt.Errorf("%w: committed frame at logical %d failed CRC", ErrCorrupt, pos)
	}
	if !ok {
		return Record{}, ErrEmpty
	}
	payload := make([]byte, plen)
	ringReadAt(r.ring, pos+FrameHeaderSize, payload)
	return Record{Seq: seq, Payload: payload}, nil
}

// Read 原子地领取下一条记录并确认它（推进本消费者的 ack 游标与全局
// 领取游标）。没有完整记录时返回 ErrEmpty。确认后该记录占用的空间
// 才允许写者回绕复用，因此读不到“已被回绕覆盖”的旧记录。
//
// 多消费者场景请为每个消费者使用 Consumer().Read()；Ring.Read 等价于
// 本 Ring 隐式消费者的 Read，保持旧的单消费者用法不变。
func (r *Ring) Read() (Record, error) {
	c, err := r.legacyConsumer()
	if err != nil {
		return Record{}, err
	}
	return c.Read()
}

// Advance 在 Peek 之后显式确认当前记录（领取并推进到下一帧起点）。
// 与 Peek 配合可实现“先处理、后确认”的至少一次投递：确认前崩溃，
// 重新挂载后该记录会被再次投递，序列号相同，可安全去重。
// 多消费者并发时，若 Peek 到的记录已被其它消费者领走，Advance 确认
// 的是当前待领取的记录；需要严格配对时请使用单消费者。
func (r *Ring) Advance() error {
	if r.closed.Load() {
		return ErrClosed
	}
	c, err := r.legacyConsumer()
	if err != nil {
		return err
	}
	h := r.header
	for {
		pos := loadU64(h, offClaimPos)
		wp := loadU64(h, offWritePos)
		if pos >= wp {
			return ErrEmpty
		}
		if end, ok := tornEndAt(h, pos); ok {
			casU64(h, offClaimPos, pos, end)
			continue
		}
		var fh [FrameHeaderSize]byte
		ringReadAt(r.ring, pos, fh[:])
		if binary.LittleEndian.Uint32(fh[0:4]) != FrameMagic {
			return fmt.Errorf("%w: frame at logical %d missing magic", ErrCorrupt, pos)
		}
		total := frameTotal(int(frameLen(fh[:])))
		if pos+total > wp {
			return fmt.Errorf("%w: frame at logical %d overruns write pos %d", ErrCorrupt, pos, wp)
		}
		if !casU64(h, offClaimPos, pos, pos+total) {
			continue
		}
		base := offSlotBase + c.slot*slotStride
		storeU64(h, base+offSlotAck, pos+total)
		if err := r.flush(); err != nil {
			return err
		}
		r.refreshReclaimBoundary()
		return nil
	}
}
