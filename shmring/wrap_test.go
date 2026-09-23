package shmring

import (
	"errors"
	"fmt"
	"testing"
)

// logState 打印读写位置、下一序列号以及已提交帧的 CRC 校验依据，
// 使失败时可解释“读者凭什么认为这条记录完整且未被改写”。
func logState(t *testing.T, r *Ring, label string) {
	t.Helper()
	resv, wp, rp := r.Positions()
	t.Logf("[%s] cap=%d reserved=%d write=%d read=%d used=%d nextSeq=%d",
		label, r.Capacity(), resv, wp, rp, resv-rp, r.NextSeq())
}

// verifyCommittedChain 从 readPos 起逐条校验帧头、帧尾提交标记与 CRC，
// 返回校验到的 (seq, 负载) 列表，直到 writePos。
func verifyCommittedChain(t *testing.T, r *Ring) []Record {
	t.Helper()
	_, wp, rp := r.Positions()
	var out []Record
	pos := rp
	for pos < wp {
		seq, plen, ok, corrupt := r.isCommittedAt(pos)
		if corrupt {
			t.Fatalf("CRC basis failed at logical %d", pos)
		}
		if !ok {
			t.Fatalf("frame at logical %d is not committed (magic/trailer missing)", pos)
		}
		payload := make([]byte, plen)
		ringReadAt(r.ring, pos+FrameHeaderSize, payload)
		out = append(out, Record{Seq: seq, Payload: payload})
		t.Logf("  verify frame@%d seq=%d len=%d crc=0x%08x OK",
			pos, seq, plen, frameCRC(seq, payload))
		pos += frameTotal(int(plen))
	}
	return out
}

func TestWriteFullAndWrap(t *testing.T) {
	dir := testDir(t)
	const cap = 256
	r, err := Create(Config{Name: "wrap", Capacity: cap}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// 记录大小固定，使容量占用可精确预测：负载 32 字节，每帧 56 字节。
	const payloadLen = 32
	mkPayload := func(i int) []byte {
		p := make([]byte, payloadLen)
		s := fmt.Sprintf("record-%04d-", i)
		copy(p, s)
		return p
	}
	const frameLen = FrameHeaderSize + payloadLen + FrameTrailerSize // 56
	const fitFrames = cap / frameLen                                 // 4

	// 1) 写满：恰好 fitFrames 条后，下一条必须 ErrFull。
	for i := 0; i < fitFrames; i++ {
		if _, err := r.Write(mkPayload(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	logState(t, r, "after fill")
	if _, err := r.Write(mkPayload(fitFrames)); !errors.Is(err, ErrFull) {
		t.Fatalf("write into full ring = %v, want ErrFull", err)
	}

	// 2) 消费两条释放空间；写者此时可以回绕写入（逻辑位置越过容量边界）。
	for i := 0; i < 2; i++ {
		rec, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		if rec.Seq != uint64(i+1) {
			t.Fatalf("read seq = %d, want %d", rec.Seq, i+1)
		}
	}
	logState(t, r, "after consume 2")
	for i := fitFrames; i < fitFrames+2; i++ {
		if _, err := r.Write(mkPayload(i)); err != nil {
			t.Fatalf("wrapped write %d: %v", i, err)
		}
	}
	logState(t, r, "after wrap writes")
	// 再次写满。
	if _, err := r.Write(mkPayload(99)); !errors.Is(err, ErrFull) {
		t.Fatalf("write into refilled ring = %v, want ErrFull", err)
	}

	// 3) 逐条读出全部记录，序列号连续且内容逐字节匹配；
	//    读空后必须稳定返回 ErrEmpty。
	want := []int{2, 3, 4, 5}
	chain := verifyCommittedChain(t, r)
	if len(chain) != len(want) {
		t.Fatalf("committed chain len = %d, want %d", len(chain), len(want))
	}
	for i, idx := range want {
		rec, err := r.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		wantPayload := mkPayload(idx)
		if rec.Seq != chain[i].Seq || string(rec.Payload) != string(wantPayload) {
			t.Fatalf("record %d mismatch: seq=%d payload=%q", i, rec.Seq, rec.Payload)
		}
		t.Logf("read seq=%d matches expected record-%04d", rec.Seq, idx)
	}
	if _, err := r.Read(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("read drained ring = %v, want ErrEmpty", err)
	}
	logState(t, r, "drained")

	// 4) 读空后写入仍可继续，且新记录序列号不回卷。
	seq, err := r.Write(mkPayload(100))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 7 {
		t.Fatalf("post-wrap seq = %d, want 7 (no reuse)", seq)
	}
	rec, err := r.Read()
	if err != nil || string(rec.Payload[:8]) != "record-0" {
		t.Fatalf("post-wrap read = %+v, %v", rec, err)
	}
}
