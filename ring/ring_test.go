package ring

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testCfg(t *testing.T, name string, cap int) Config {
	t.Helper()
	dir := t.TempDir()
	return Config{Name: name, Dir: dir, Capacity: cap}
}

func TestBasicWriteRead(t *testing.T) {
	cfg := testCfg(t, "basic", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer r.Close()

	payloads := [][]byte{[]byte("hello-0"), []byte("hello-1"), []byte("world-2")}
	seqs := make([]uint64, len(payloads))
	for i, p := range payloads {
		seqs[i], err = r.Write(p)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("unexpected seqs: %v", seqs)
	}
	for i, want := range payloads {
		s, got, err := r.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if s != seqs[i] || string(got) != string(want) {
			t.Fatalf("read %d = (%d,%q), want (%d,%q)", i, s, got, seqs[i], want)
		}
	}
	if _, _, err := r.Read(); !errors.Is(err, ErrBufferEmpty) {
		t.Fatalf("expected empty, got %v", err)
	}
	w, rd, used := r.Positions()
	t.Logf("after drain: writePos=%d readPos=%d used=%d", w, rd, used)
	if used != 0 || w != rd {
		t.Fatalf("buffer not empty: w=%d rd=%d used=%d", w, rd, used)
	}
}

func TestFillAndWrap(t *testing.T) {
	cfg := testCfg(t, "wrap", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	item := make([]byte, 100)
	var totalWritten, totalRead int
	var lastSeq uint64

	// Alternate writes and reads across many laps to exercise wrapping and
	// padding at the physical end.
	laps := 20
	for lap := 0; lap < laps; lap++ {
		for {
			seq, err := r.Write(item)
			if errors.Is(err, ErrBufferFull) {
				break
			}
			if err != nil {
				t.Fatalf("write: %v", err)
			}
			lastSeq = seq
			totalWritten++
		}
		for {
			seq, p, err := r.Read()
			if errors.Is(err, ErrBufferEmpty) {
				break
			}
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if seq > lastSeq || len(p) != len(item) {
				t.Fatalf("bad record seq=%d len=%d", seq, len(p))
			}
			totalRead++
		}
		w, rd, used := r.Positions()
		if used != 0 {
			t.Fatalf("lap %d not drained: w=%d rd=%d", lap, w, rd)
		}
		t.Logf("lap=%d writePos=%d readPos=%d", lap, w, rd)
	}
	if totalWritten != totalRead || totalWritten == 0 {
		t.Fatalf("written=%d read=%d", totalWritten, totalRead)
	}
	t.Logf("fill/wrap OK: %d records over %d laps, final writePos=%d",
		totalWritten, laps, lastSeq)
}

func TestRemountPersistsPositions(t *testing.T) {
	cfg := testCfg(t, "remount", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	msgs := []string{"persisted-1", "persisted-2", "persisted-3"}
	for _, m := range msgs {
		if _, err := r.Write([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	// Acknowledge one, then "die" and remount.
	if _, _, err := r.Read(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	r2, info, err := Mount(cfg)
	if err != nil {
		t.Fatalf("remount: %v", err)
	}
	defer r2.Close()
	t.Logf("remount recover info: abandoned=%d w=%d rd=%d",
		info.Abandoned, info.WritePos, info.ReadPos)
	for i := 1; i < len(msgs); i++ {
		s, p, err := r2.Read()
		if err != nil {
			t.Fatalf("read after remount %d: %v", i, err)
		}
		if string(p) != msgs[i] {
			t.Fatalf("got %q want %q (seq=%d)", p, msgs[i], s)
		}
	}
	if _, _, err := r2.Read(); !errors.Is(err, ErrBufferEmpty) {
		t.Fatalf("want empty, got %v", err)
	}
	// Ensure files exist at documented paths.
	if _, err := os.Stat(filepath.Join(cfg.dir(), cfg.Name+dataFileExt)); err != nil {
		t.Fatalf("data file missing: %v", err)
	}
}

func TestMountConflicts(t *testing.T) {
	cfg := testCfg(t, "conflict", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Seed a committed record; it must survive every rejected mount.
	if _, err := r.Write([]byte("still-alive")); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	type tc struct {
		name      string
		mountCap  int
		tamper    int
		tamperVal uint64
		want      error
	}
	cases := []tc{
		{"magic", minCapacity, tamperMagic, 0xDEADBEEF, ErrMagicConflict},
		{"version", minCapacity, tamperVersion, Version + 99, ErrVersionConflict},
		{"capacity-arg", minCapacity * 2, -1, 0, ErrCapacityConflict},
		{"capacity-header", minCapacity, tamperCapacity, uint64(minCapacity * 2), ErrCapacityConflict},
		{"format", minCapacity, tamperFormat, formatFlags ^ 0x2, ErrFormatConflict},
		{"badcap-too-small", 100, -1, 0, ErrInvalidParam},
		{"badcap-unaligned", 1000, -1, 0, ErrInvalidParam},
		{"badname", minCapacity, -1, 0, ErrInvalidParam},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.tamper >= 0 {
				if err := tamperHeader(cfg, c.tamper, c.tamperVal); err != nil {
					t.Fatal(err)
				}
				// Restore afterwards so cases stay independent.
				defer func(field int) {
					var v uint64
					switch field {
					case tamperMagic:
						v = ringMagic
					case tamperVersion:
						v = Version
					case tamperCapacity:
						v = uint64(cfg.Capacity)
					case tamperFormat:
						v = formatFlags
					}
					_ = tamperHeader(cfg, field, v)
				}(c.tamper)
			}
			mcfg := cfg
			mcfg.Capacity = c.mountCap
			if c.name == "badname" {
				mcfg.Name = ""
			}
			_, _, err := Mount(mcfg)
			if !errors.Is(err, c.want) {
				t.Fatalf("%s: got %v, want %v", c.name, err, c.want)
			}
			t.Logf("%-18s rejected with %v", c.name, err)
		})
	}

	// The buffer must still mount normally and still hold the seeded data.
	good, info, err := Mount(cfg)
	if err != nil {
		t.Fatalf("mount after rejected mounts: %v", err)
	}
	defer good.Close()
	if info.WritePos == 0 {
		t.Fatal("seeded record lost after rejected mounts")
	}
	s, p, err := good.Read()
	if err != nil || string(p) != "still-alive" {
		t.Fatalf("read after conflicts: seq=%d %q err=%v", s, p, err)
	}
}

func TestCreateDuplicateRejected(t *testing.T) {
	cfg := testCfg(t, "dup", minCapacity)
	r, _, err := Create(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, _, err = Create(cfg)
	if !errIsExist(err) {
		t.Fatalf("expected ErrExist, got %v", err)
	}
	// Mount with mismatched capacity on an absent name must not create it.
	bad := cfg
	bad.Name = "absent-buffer"
	_, _, err = Mount(bad)
	if os.IsExist(err) {
		t.Fatalf("mount should not create file: %v", err)
	}
	if _, statErr := os.Stat(bad.dataPath()); !os.IsNotExist(statErr) {
		t.Fatalf("mount created a file unexpectedly: %v", statErr)
	}
}
