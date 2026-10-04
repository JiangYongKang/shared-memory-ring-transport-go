package shmring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// craftV1File 手工构造一个版本 1 格式的缓冲区文件（128 字节头），
// 模拟“上一版库创建、正在使用中的缓冲区”。
func craftV1File(t *testing.T, dir, name string, capacity int) {
	t.Helper()
	path := filepath.Join(dir, "shmring-"+name+".dat")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(int64(headerSizeV1 + capacity)); err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, headerSizeV1)
	binary.LittleEndian.PutUint32(hdr[offMagic:], HeaderMagic)
	binary.LittleEndian.PutUint32(hdr[offVersion:], formatVersionV1)
	binary.LittleEndian.PutUint32(hdr[offHeaderSize:], headerSizeV1)
	binary.LittleEndian.PutUint64(hdr[offCapacity:], uint64(capacity))
	binary.LittleEndian.PutUint64(hdr[offNextSeq:], 1)
	binary.LittleEndian.PutUint32(hdr[offInitFlag:], InitMagic)
	if _, err := f.WriteAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// TestV1LegacyBuffer：v1 旧格式文件继续按老语义工作（单消费者读写、
// 崩溃回退式恢复），消费者组被明确拒绝且拒绝不破坏在用缓冲区。
func TestV1LegacyBuffer(t *testing.T) {
	dir := testDir(t)
	name := "v1buf"
	craftV1File(t, dir, name, 256)

	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		t.Fatalf("open v1 buffer: %v", err)
	}
	defer r.Close()
	if !r.legacyV1 {
		t.Fatal("v1 file not detected as legacy")
	}

	// 老的单消费者用法完全可用。
	seq, err := r.Write([]byte("legacy-record"))
	if err != nil || seq != 1 {
		t.Fatalf("v1 write = seq %d, %v", seq, err)
	}
	rec, err := r.Read()
	if err != nil || rec.Seq != 1 || string(rec.Payload) != "legacy-record" {
		t.Fatalf("v1 read = %+v, %v", rec, err)
	}

	// 消费者组被明确拒绝（ErrLegacyBuffer），且拒绝后缓冲区仍可读写。
	if _, err := r.NewConsumer(); !errors.Is(err, ErrLegacyBuffer) {
		t.Fatalf("NewConsumer on v1 = %v, want ErrLegacyBuffer", err)
	}
	if _, err := r.Write([]byte("still-works")); err != nil {
		t.Fatalf("v1 buffer broken after rejected NewConsumer: %v", err)
	}
	if rec, err := r.Read(); err != nil || string(rec.Payload) != "still-works" {
		t.Fatalf("v1 read after rejection = %+v, %v", rec, err)
	}
	t.Logf("v1 buffer: legacy read/write OK, consumer group explicitly rejected")
}

// TestV1CrashRecoveryRewind：v1 文件保持旧的“回退式”崩溃恢复语义
// （reservedPos 退回 writePos），与旧库行为一致。
func TestV1CrashRecoveryRewind(t *testing.T) {
	dir := testDir(t)
	name := "v1crash"
	craftV1File(t, dir, name, 512)

	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("good-one")); err != nil {
		t.Fatal(err)
	}

	// 模拟写者负载写一半崩溃（v1 写路径不做在线自愈）。
	crashed := make(chan struct{})
	done := make(chan struct{})
	oldHook := midPayloadHook
	midPayloadHook = func(seq uint64) {
		close(crashed)
		runtime.Goexit()
	}
	go func() {
		defer close(done)
		_, _ = r.Write([]byte("torn-payload-on-v1-buffer"))
	}()
	<-crashed
	<-done
	midPayloadHook = oldHook
	r.Close()

	obs, err := Open(name, &Options{Dir: dir, disableAutoRecover: true})
	if err != nil {
		t.Fatal(err)
	}
	resv, wp, _ := obs.Positions()
	t.Logf("v1 torn: reserved=%d write=%d gap=%d", resv, wp, resv-wp)
	if resv <= wp {
		t.Fatalf("expected torn reservation on v1, reserved=%d write=%d", resv, wp)
	}
	recovered, torn, err := obs.Recover()
	if err != nil {
		t.Fatal(err)
	}
	resv2, wp2, _ := obs.Positions()
	t.Logf("v1 rewind recovery: recovered=%v torn=%d reserved=%d write=%d", recovered, torn, resv2, wp2)
	if !recovered || resv2 != wp2 {
		t.Fatalf("v1 recovery = %v, reserved=%d write=%d, want rewind", recovered, resv2, wp2)
	}
	// 恢复后读写正常，序列号不回卷。
	seq, err := obs.Write([]byte("after-v1-recovery"))
	if err != nil || seq != 3 {
		t.Fatalf("v1 write after recovery = seq %d, %v, want 3", seq, err)
	}
	rec, err := obs.Read()
	if err != nil || rec.Seq != 1 || string(rec.Payload) != "good-one" {
		t.Fatalf("v1 read = %+v, %v", rec, err)
	}
	rec, err = obs.Read()
	if err != nil || rec.Seq != 3 || string(rec.Payload) != "after-v1-recovery" {
		t.Fatalf("v1 read 2 = %+v, %v", rec, err)
	}
	obs.Close()
}

// TestV2RejectsUnknownVersion：未知版本号仍被明确拒绝（只读拒绝）。
func TestV2RejectsUnknownVersion(t *testing.T) {
	dir := testDir(t)
	r, err := Create(Config{Name: "v9", Capacity: 256}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	ver := make([]byte, 4)
	binary.LittleEndian.PutUint32(ver, 99)
	corruptFileField(t, filepath.Join(dir, "shmring-v9.dat"), offVersion, ver)
	if _, err := Open("v9", &Options{Dir: dir}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("open version=99 = %v, want ErrVersionMismatch", err)
	}
	fmt.Println("unknown version rejected read-only")
}
