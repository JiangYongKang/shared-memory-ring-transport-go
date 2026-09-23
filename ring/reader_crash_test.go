package ring

import "testing"

// TestReaderCrashBeforeAck models a consumer that obtained a record (via
// Peek, the non-destructive path) and died before Commit. On remount the
// record must still be there with the SAME sequence number: no data loss,
// and the identical seq lets the restarted consumer recognize it as a
// re-delivery rather than new data.
func TestReaderCrashBeforeAck(t *testing.T) {
	cfg := testCfg(t, "rdcrash", 1024)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	msgs := []string{"job-1", "job-2", "job-3"}
	for _, m := range msgs {
		if _, err := r.Write([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}

	// Consumer process: Peek first record, then "die" (no Commit).
	s1, p1, err := r.Peek()
	if err != nil || string(p1) != "job-1" {
		t.Fatalf("peek: %d %q %v", s1, p1, err)
	}
	r.Close()

	// Restarted process: same record, same seq (re-delivery, not new).
	r2, info, err := Mount(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	t.Logf("reader restart recover: w=%d rd=%d abandoned=%d",
		info.WritePos, info.ReadPos, info.Abandoned)
	s2, p2, err := r2.Peek()
	if err != nil || s2 != s1 || string(p2) != "job-1" {
		t.Fatalf("redelivery mismatch: seq %d!=%d payload %q", s2, s1, p2)
	}
	t.Logf("re-delivered record recognized by unchanged seq=%d payload=%q", s2, p2)
	if err := r2.Commit(); err != nil {
		t.Fatal(err)
	}
	// After acknowledging once, it must not come back again.
	s3, p3, err := r2.Peek()
	if err != nil || s3 == s2 || string(p3) != "job-2" {
		t.Fatalf("post-commit peek: seq=%d %q err=%v", s3, p3, err)
	}
}

// TestReaderWriterInterleavedCrash: committed but unread data must survive a
// writer crash that happens after those commits.
func TestReaderWriterInterleavedCrash(t *testing.T) {
	cfg := testCfg(t, "rwcrash", 2048)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Two confirmed records.
	if _, err := r.Write([]byte("safe-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("safe-2")); err != nil {
		t.Fatal(err)
	}
	// Acknowledge safe-1, then the writer crashes on its next record.
	if _, p, err := r.Read(); err != nil || string(p) != "safe-1" {
		t.Fatalf("read safe-1: %q %v", p, err)
	}
	if err := r.CrashWrite([]byte("lost-ghost"), 40); err != nil {
		t.Fatal(err)
	}

	// New process mounts: safe-1 ack stands; safe-2 is still pending and
	// readable; ghost is invisible.
	r2, info, err := Mount(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if info.ReadPos == 0 {
		t.Fatal("acknowledgment of safe-1 was lost")
	}
	s, p, err := r2.Read()
	if err != nil || string(p) != "safe-2" {
		t.Fatalf("safe-2 not preserved: seq=%d %q err=%v", s, p, err)
	}
	if _, _, err := r2.Read(); err != ErrBufferEmpty {
		t.Fatalf("ghost consumed or wrong err: %v", err)
	}
	// And the buffer is writable again.
	if _, err := r2.Write([]byte("back-to-normal")); err != nil {
		t.Fatal(err)
	}
	s, p, err = r2.Read()
	if err != nil || string(p) != "back-to-normal" {
		t.Fatalf("post-recovery write/read: seq=%d %q err=%v", s, p, err)
	}
}
