package ring

import (
	"errors"
	"testing"
)

func TestMaxPayloadAndTooLarge(t *testing.T) {
	cfg := testCfg(t, "maxp", 1024)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	max := cfg.MaxPayload()
	if _, err := r.Write(make([]byte, max)); err != nil {
		t.Fatalf("max payload %d: %v", max, err)
	}
	if _, _, err := r.Read(); err != nil {
		t.Fatal(err)
	}
	// One byte over the max must never fit: a record cannot exceed one lap.
	if _, err := r.Write(make([]byte, max+1)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("want ErrRecordTooLarge, got %v", err)
	}
	// Max payload occupies the whole lap, so with an empty readPos it fits
	// once but a second must be full.
	r2cfg := cfg
	r2cfg.Name = "maxp2"
	r2, _, err := Create(r2cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if _, err := r2.Write(make([]byte, r2cfg.MaxPayload())); err != nil {
		t.Fatal(err)
	}
	if _, err := r2.Write([]byte("x")); !errors.Is(err, ErrBufferFull) {
		t.Fatalf("want full after max record, got %v", err)
	}
}

func TestEmptyRecord(t *testing.T) {
	cfg := testCfg(t, "empty", 1024)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	seq, err := r.Write(nil)
	if err != nil {
		t.Fatal(err)
	}
	s, p, err := r.Read()
	if err != nil || s != seq || len(p) != 0 {
		t.Fatalf("empty record: seq=%d/%d len=%d err=%v", s, seq, len(p), err)
	}
}

// TestWrappingGapsPrecise: choose sizes so writePos lands at physical
// offsets that force every gap size (8,16,32,...,56) and verify reads
// reconstruct everything across many laps.
func TestWrappingGapsPrecise(t *testing.T) {
	cfg := testCfg(t, "gaps", 1024)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Vary payload sizes across 0..64 (sizes 8 apart plus odd lengths) so
	// all pad/naked-gap branches are exercised with wrap-around.
	total := 0
	lap := 0
	for lap < 30 {
		wrote := 0
		for sz := 0; sz <= 80; sz++ {
			p := make([]byte, sz)
			for i := range p {
				p[i] = byte(sz)
			}
			seq, werr := r.Write(p)
			if errors.Is(werr, ErrBufferFull) {
				break
			}
			if werr != nil {
				t.Fatal(werr)
			}
			_ = seq
			wrote++
			total++
		}
		read := 0
		for {
			_, p, rerr := r.Read()
			if errors.Is(rerr, ErrBufferEmpty) {
				break
			}
			if rerr != nil {
				t.Fatalf("read after varied writes: %v (lap=%d read=%d)", rerr, lap, read)
			}
			if len(p) > 0 && p[0] != byte(len(p)) {
				t.Fatalf("payload marker mismatch: len=%d marker=%d", len(p), p[0])
			}
			read++
		}
		if read != wrote {
			t.Fatalf("lap %d wrote=%d read=%d", lap, wrote, read)
		}
		w, rd, used := r.Positions()
		if used != 0 {
			t.Fatalf("lap %d not drained w=%d rd=%d used=%d", lap, w, rd, used)
		}
		lap++
	}
	t.Logf("varied-size wrap OK over %d records/%d laps", total, lap)
}

func TestSeqMonotonicAcrossRemounts(t *testing.T) {
	cfg := testCfg(t, "seq", 1024)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var last uint64
	for i := 0; i < 5; i++ {
		last, err = r.Write([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		r.Close()
		r, _, err = Mount(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if r.LastSeq() != last {
			t.Fatalf("after %d remounts LastSeq=%d want %d", i+1, r.LastSeq(), last)
		}
	}
	defer r.Close()
	// First 5 seq numbers in order after multiple remounts.
	for i := 1; i <= 5; i++ {
		s, _, err := r.Read()
		if err != nil || s != uint64(i) {
			t.Fatalf("seq order: got %d err=%v want %d", s, err, i)
		}
	}
}
