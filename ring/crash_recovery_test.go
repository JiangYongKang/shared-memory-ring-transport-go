package ring

import (
	"errors"
	"testing"
)

// TestWriterCrashPartialRecord: a writer dies after writing only part of a
// record (header partly, or header+some payload), before publishing. The
// bytes must never be consumable; after remount a fresh writer overwrites
// them and all subsequent data is intact.
func TestWriterCrashPartialRecord(t *testing.T) {
	cfg := testCfg(t, "crash-partial", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// One committed record before the crash so readPos > 0.
	good := []byte("before-crash")
	if _, err := r.Write(good); err != nil {
		t.Fatal(err)
	}
	w0, rd0, _ := r.Positions()
	t.Logf("before crash: writePos=%d readPos=%d", w0, rd0)

	// Consume it, then crash while writing the next record at different
	// flush points.
	if _, _, err := r.Read(); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 400)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}

	for _, flush := range []int{0, 1, 8, 20, recHdrSize, recHdrSize + 7, recHdrSize + 200} {
		if err := r.crashWrite(payload, flush); err != nil {
			t.Fatalf("crashWrite(flush=%d): %v", flush, err)
		}
		// Handle is now "dead". Open a fresh process view.
		rr, info, err := Mount(cfg)
		if err != nil {
			t.Fatalf("remount after crash flush=%d: %v", flush, err)
		}
		w, rd, used := rr.Positions()
		t.Logf("flush=%3d recovered: abandoned=%d writePos=%d readPos=%d used=%d",
			flush, info.Abandoned, w, rd, used)

		// Unpublished half-record must not be readable.
		if _, _, err := rr.Read(); !errors.Is(err, ErrBufferEmpty) {
			t.Fatalf("flush=%d: half-record visible! err=%v", flush, err)
		}
		// writePos must be unmoved by the crash.
		if w != info.WritePos || used != 0 {
			t.Fatalf("flush=%d: positions moved by crash w=%d rd=%d", flush, w, rd)
		}

		// A new writer must be able to overwrite and proceed.
		msg := []byte("after-crash")
		seq, err := rr.Write(msg)
		if err != nil {
			t.Fatalf("write after crash flush=%d: %v", flush, err)
		}
		s, got, err := rr.Read()
		if err != nil || s != seq || string(got) != string(msg) {
			t.Fatalf("read after crash flush=%d: seq=%d/%d %q err=%v",
				flush, s, seq, got, err)
		}
		rr.Close()

		// Recreate r handle for next iteration (its mapping is still valid;
		// only its writer lock bookkeeping was killed). Open new view.
		r2, _, err := Mount(cfg)
		if err != nil {
			t.Fatal(err)
		}
		// Drain, then reuse as the crashing handle source.
		r = r2
	}
	r.Close()
}

// TestWriterCrashFullButUnpublished: whole record (checksum valid) is on
// disk but writePos was never advanced. It must still be invisible and get
// reclaimed.
func TestWriterCrashFullButUnpublished(t *testing.T) {
	cfg := testCfg(t, "crash-unpub", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("complete-but-unpublished")
	n := recHdrSize + len(payload)
	if err := r.crashWrite(payload, n); err != nil {
		t.Fatal(err)
	}
	rr, info, err := Mount(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Close()
	if info.Abandoned != 1 {
		t.Fatalf("expected abandoned=1, got %d", info.Abandoned)
	}
	if _, _, err := rr.Read(); !errors.Is(err, ErrBufferEmpty) {
		t.Fatalf("unpublished record visible: %v", err)
	}
	seq, err := rr.Write([]byte("real-one"))
	if err != nil {
		t.Fatal(err)
	}
	s, p, err := rr.Read()
	if err != nil || s != seq || string(p) != "real-one" {
		t.Fatalf("got seq=%d %q err=%v", s, p, err)
	}
}

// TestPeekDuplicateDetection: Peek must hand back the same record with the
// same sequence number repeatedly; only Commit acknowledges it.
func TestPeekDuplicateDetection(t *testing.T) {
	cfg := testCfg(t, "peek", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, m := range []string{"r1", "r2"} {
		if _, err := r.Write([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	s1, p1, err := r.Peek()
	if err != nil {
		t.Fatal(err)
	}
	// Peek again: identical seq/bytes -> caller can tell it is a duplicate.
	s2, p2, err := r.Peek()
	if err != nil || s1 != s2 || string(p1) != string(p2) {
		t.Fatalf("peek not repeatable: (%d,%q) vs (%d,%q) err=%v", s1, p1, s2, p2, err)
	}
	t.Logf("duplicate delivery recognized by seq=%d payload=%q", s1, p1)
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	s3, p3, err := r.Peek()
	if err != nil || s3 == s1 || string(p3) != "r2" {
		t.Fatalf("after commit peek: seq=%d %q err=%v", s3, p3, err)
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Peek(); !errors.Is(err, ErrBufferEmpty) {
		t.Fatalf("want empty, got %v", err)
	}
}

// TestChecksumCorruption: flip a payload byte without updating writePos
// machinery and ensure Read reports ErrCorruptRecord.
func TestChecksumCorruption(t *testing.T) {
	cfg := testCfg(t, "crc", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	msg := []byte("integrity-check")
	seq, err := r.Write(msg)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt one payload byte in the mapping directly. r.data is the ring
	// data area, so the first record's payload starts at recHdrSize.
	off := recHdrSize
	r.data[off] ^= 0xFF
	w, rd, used := r.Positions()
	t.Logf("corrupted seq=%d at phys payload off=%d w=%d rd=%d used=%d",
		seq, off, w, rd, used)
	if _, _, err := r.Read(); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("expected ErrCorruptRecord, got %v", err)
	}
}
