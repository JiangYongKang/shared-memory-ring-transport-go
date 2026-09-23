package shmring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// 子进程场景通过环境变量 SHMRING_CRASH_CHILD 选择；重新执行测试二进制，
// 不依赖任何外部服务。
const childEnv = "SHMRING_CRASH_CHILD"

// 子进程因错误类别使用的可区分退出码。
const (
	exitOK               = 0
	exitErrCapacity      = 41
	exitErrMagic         = 42
	exitErrVersion       = 43
	exitErrOther         = 49
	exitChildSignalCrash = 77
)

// runChild 以指定场景重新执行当前测试二进制。
func runChild(t *testing.T, scene string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestChildProcess", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"="+scene)
	cmd.Env = append(cmd.Env, env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child (%s): %v", scene, err)
	}
	return cmd
}

func waitChild(t *testing.T, cmd *exec.Cmd, scene string) {
	t.Helper()
	err := cmd.Wait()
	if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == exitChildSignalCrash {
		t.Fatalf("child %s signaled crash unexpectedly", scene)
	}
	if err != nil {
		t.Fatalf("wait child %s: %v", scene, err)
	}
}

// TestChildProcess 是所有子进程场景的入口。
func TestChildProcess(t *testing.T) {
	scene := os.Getenv(childEnv)
	if scene == "" {
		t.Skip("parent process")
	}
	dir := os.Getenv("SHMRING_DIR")
	name := os.Getenv("SHMRING_NAME")
	cap, _ := strconv.Atoi(os.Getenv("SHMRING_CAP"))

	switch {
	case scene == "writer-crash-midpayload":
		childWriterCrash(t, dir, name, cap, true)
	case scene == "writer-crash-precommit":
		childWriterCrash(t, dir, name, cap, false)
	case strings.HasPrefix(scene, "writer-stream-"):
		id, _ := strconv.Atoi(strings.TrimPrefix(scene, "writer-stream-"))
		childWriterStream(t, dir, name, id)
	case scene == "reader-die":
		childReaderDie(t, dir, name)
	case scene == "mount-badcap":
		childMountBadCapacity(t, dir, name, cap)
	case scene == "mount-badmagic":
		childMountBadMagic(t, dir, name)
	case scene == "mount-badversion":
		childMountBadVersion(t, dir, name)
	default:
		t.Fatalf("unknown child scene: %q", scene)
	}
}

func childFatalf(code int, format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(code)
}

// childWriterCrash：写入 3 条完整记录后，第 4 条在发布前/帧字节中途死亡，
// 不推进 writePos。
func childWriterCrash(t *testing.T, dir, name string, cap int, midPayload bool) {
	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		childFatalf(exitErrOther, "child open: %v", err)
	}
	defer r.Close()
	for i := 1; i <= 3; i++ {
		p := []byte(fmt.Sprintf("good-%d-payload", i))
		if _, err := r.Write(p); err != nil {
			childFatalf(exitErrOther, "child write %d: %v", i, err)
		}
	}
	if midPayload {
		midPayloadHook = func(seq uint64) {
			fmt.Printf("CHILD: dying mid-payload while reserving seq=%d\n", seq)
			os.Exit(exitChildSignalCrash)
		}
	} else {
		preCommitHook = func(seq uint64) {
			fmt.Printf("CHILD: dying after full frame before commit, seq=%d\n", seq)
			os.Exit(exitChildSignalCrash)
		}
	}
	torn := []byte("TORN-HALF-RECORD-DO-NOT-CONSUME-XXXXXXXXXXXXXXXXXX")
	if _, err := r.Write(torn); err != nil {
		childFatalf(exitErrOther, "child torn write: %v", err)
	}
	childFatalf(exitErrOther, "child survived unexpectedly")
}

func TestWriterCrashRecovery(t *testing.T) {
	for _, midPayload := range []bool{true, false} {
		name := "crash-mid"
		if !midPayload {
			name = "crash-pre"
		}
		t.Run(name, func(t *testing.T) { runWriterCrash(t, midPayload) })
	}
}

func runWriterCrash(t *testing.T, midPayload bool) {
	dir := testDir(t)
	name := "wc"
	const cap = 512
	parent, err := Create(Config{Name: name, Capacity: cap}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })

	scene := "writer-crash-precommit"
	if midPayload {
		scene = "writer-crash-midpayload"
	}
	cmd := runChild(t, scene,
		"SHMRING_DIR="+dir, "SHMRING_NAME="+name, "SHMRING_CAP="+strconv.Itoa(cap))
	err = cmd.Wait()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != exitChildSignalCrash {
		t.Fatalf("child exit = %v, want crash code %d", err, exitChildSignalCrash)
	}

	// 父进程映射不会自动观察到其它进程的写入；重新挂载以读取最新状态，
	// 且先关闭自动回收，观察“半条记录尚在”的原始现场。
	parent.Close()
	obs, err := Open(name, &Options{Dir: dir, disableAutoRecover: true})
	if err != nil {
		t.Fatal(err)
	}
	resv, wp, rp := obs.Positions()
	t.Logf("before recovery: reserved=%d write=%d read=%d gap=%d nextSeq=%d",
		resv, wp, rp, resv-wp, obs.NextSeq())
	if resv <= wp {
		t.Fatalf("expected torn reservation gap, got reserved=%d write=%d", resv, wp)
	}
	tornLen := int64(frameTotal(len([]byte("TORN-HALF-RECORD-DO-NOT-CONSUME-XXXXXXXXXXXXXXXXXX"))))

	// 半条记录对读者必须表现为“空之后没有第四条”：只能读到 3 条好记录。
	var got int
	for {
		rec, err := obs.Read()
		if errors.Is(err, ErrEmpty) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("good-%d-payload", rec.Seq)
		if string(rec.Payload) != want || rec.Seq != uint64(got+1) {
			t.Fatalf("consumed unexpected record seq=%d payload=%q", rec.Seq, rec.Payload)
		}
		t.Logf("pre-recovery read seq=%d payload=%q", rec.Seq, rec.Payload)
		got++
	}
	if got != 3 {
		t.Fatalf("read %d records before recovery, want exactly 3 (torn must be invisible)", got)
	}

	// 显式回收：残片长度必须可解释，reserved 退回到 write。
	recovered, tornBytes, err := obs.Recover()
	if err != nil {
		t.Fatal(err)
	}
	resv2, wp2, rp2 := obs.Positions()
	t.Logf("recovery: recovered=%v tornBytes=%d; after: reserved=%d write=%d read=%d",
		recovered, tornBytes, resv2, wp2, rp2)
	if !recovered || tornBytes != tornLen {
		t.Fatalf("recovery = %v, %d, want torn len %d", recovered, tornBytes, tornLen)
	}
	if resv2 != wp2 {
		t.Fatalf("reserved %d != write %d after recovery", resv2, wp2)
	}

	// 回收后可继续写入，序列号不回卷（第 4 条尝试已消耗 seq=4）。
	seq, err := obs.Write([]byte("after-recovery"))
	if err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if seq != 5 {
		t.Fatalf("seq after recovery = %d, want 5", seq)
	}
	rec, err := obs.Read()
	if err != nil || string(rec.Payload) != "after-recovery" || rec.Seq != 5 {
		t.Fatalf("read after recovery = %+v, %v", rec, err)
	}

	// 重新挂载：自动回收幂等，已确认数据不丢失/不改写。
	if err := obs.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := Open(name, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	if _, err := re.Read(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("fresh mount read = %v, want ErrEmpty (all committed data was confirmed)", err)
	}
	recovered2, n2, err := re.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if recovered2 || n2 != 0 {
		t.Fatalf("idempotent recovery = %v,%d, want false,0", recovered2, n2)
	}
}

// childWriterStream：持续写入带进程编号与序号的记录。
func childWriterStream(t *testing.T, dir, name string, id int) {
	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		childFatalf(exitErrOther, "stream child open: %v", err)
	}
	defer r.Close()
	for i := 0; i < 60; i++ {
		p := []byte(fmt.Sprintf("p%d-m%d-pad-%024d", id, i, i))
		for {
			_, err := r.Write(p)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrFull) {
				childFatalf(exitErrOther, "stream write: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
}

func TestMultiProcessStream(t *testing.T) {
	dir := testDir(t)
	name := "mps"
	r, err := Create(Config{Name: name, Capacity: 4096}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const writers, perWriter = 3, 60
	cmds := make([]*exec.Cmd, writers)
	for i := 0; i < writers; i++ {
		cmds[i] = runChild(t, fmt.Sprintf("writer-stream-%d", i),
			"SHMRING_DIR="+dir, "SHMRING_NAME="+name)
	}

	// 父进程唯一读者，读到的每条 seq 都必须唯一（去重依据），
	// 内容必须符合格式且与 CRC 依据一致。
	seen := make(map[uint64]bool)
	payloadSeen := make(map[string]bool)
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < writers*perWriter {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: got %d/%d records", len(seen), writers*perWriter)
		}
		rec, err := r.Read()
		if errors.Is(err, ErrEmpty) {
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if seen[rec.Seq] {
			t.Fatalf("duplicate delivery seq=%d", rec.Seq)
		}
		if !strings.HasPrefix(string(rec.Payload), "p") {
			t.Fatalf("garbled payload seq=%d: %q", rec.Seq, rec.Payload)
		}
		if payloadSeen[string(rec.Payload)] {
			t.Fatalf("duplicate payload %q", rec.Payload)
		}
		seen[rec.Seq] = true
		payloadSeen[string(rec.Payload)] = true
	}
	for i, c := range cmds {
		waitChild(t, c, fmt.Sprintf("writer-%d", i))
	}
	_, wp, rp := r.Positions()
	t.Logf("multiprocess done: %d unique seqs, write=%d read=%d used=%d",
		len(seen), wp, rp, wp-rp)
	r.Close()
}

// childReaderDie：Peek 一条记录（不确认）后死亡，模拟读到一半崩溃。
func childReaderDie(t *testing.T, dir, name string) {
	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		childFatalf(exitErrOther, "reader child open: %v", err)
	}
	defer r.Close()
	rec, err := r.Peek()
	if err != nil {
		childFatalf(exitErrOther, "reader child peek: %v", err)
	}
	fmt.Printf("CHILD-READER: peeked seq=%d payload=%q then dying without ack\n",
		rec.Seq, rec.Payload)
	os.Exit(exitChildSignalCrash)
}

func TestReaderCrashRedelivery(t *testing.T) {
	dir := testDir(t)
	name := "rd"
	r, err := Create(Config{Name: name, Capacity: 256}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	const n = 4
	for i := 1; i <= n; i++ {
		if _, err := r.Write([]byte(fmt.Sprintf("rmsg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	cmd := runChild(t, "reader-die", "SHMRING_DIR="+dir, "SHMRING_NAME="+name)
	if e := cmd.Wait(); e == nil {
		t.Fatal("reader child unexpectedly survived")
	} else if ee, ok := e.(*exec.ExitError); !ok || ee.ExitCode() != exitChildSignalCrash {
		t.Fatalf("reader child exit = %v", e)
	}

	// 读位置未被确认推进：重新挂载后同一批记录必须原样重新投递，
	// 且 Peek 重复读取的序列号相同，可识别为重复数据。
	r.Close()
	re, err := Open(name, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	first, err := re.Peek()
	if err != nil || first.Seq != 1 {
		t.Fatalf("redelivery peek = %+v, %v, want seq=1", first, err)
	}
	again, err := re.Peek()
	if err != nil || again.Seq != first.Seq || string(again.Payload) != string(first.Payload) {
		t.Fatalf("repeat peek = %+v, %v; want identical to seq=%d", again, err, first.Seq)
	}
	t.Logf("duplicate recognized: repeated peek returns seq=%d (not new data)", first.Seq)
	for i := 1; i <= n; i++ {
		rec, err := re.Read()
		if err != nil {
			t.Fatalf("redelivered read %d: %v", i, err)
		}
		if rec.Seq != uint64(i) || string(rec.Payload) != fmt.Sprintf("rmsg-%d", i) {
			t.Fatalf("redelivered %d = seq=%d payload=%q", i, rec.Seq, rec.Payload)
		}
	}
	if _, err := re.Read(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("after redelivery drain = %v", err)
	}
}

func childMountBadCapacity(t *testing.T, dir, name string, cap int) {
	_, err := Create(Config{Name: name, Capacity: cap}, &Options{Dir: dir})
	switch {
	case errors.Is(err, ErrCapacityMismatch):
		fmt.Printf("CHILD: capacity conflict correctly rejected: %v\n", err)
		os.Exit(exitErrCapacity)
	case err == nil:
		childFatalf(exitErrOther, "bad-capacity mount unexpectedly succeeded")
	default:
		childFatalf(exitErrOther, "unexpected error: %v", err)
	}
}

func childMountBadMagic(t *testing.T, dir, name string) {
	_, err := Open(name, &Options{Dir: dir})
	switch {
	case errors.Is(err, ErrMagicMismatch):
		fmt.Printf("CHILD: magic conflict correctly rejected: %v\n", err)
		os.Exit(exitErrMagic)
	case err == nil:
		childFatalf(exitErrOther, "bad-magic mount unexpectedly succeeded")
	default:
		childFatalf(exitErrOther, "unexpected error: %v", err)
	}
}

func childMountBadVersion(t *testing.T, dir, name string) {
	// 版本字段由父进程预先改写，子进程只负责尝试挂载并报告可区分原因。
	_, err := Open(name, &Options{Dir: dir})
	switch {
	case errors.Is(err, ErrVersionMismatch):
		fmt.Printf("CHILD: version conflict correctly rejected: %v\n", err)
		os.Exit(exitErrVersion)
	case err == nil:
		childFatalf(exitErrOther, "bad-version mount unexpectedly succeeded")
	default:
		childFatalf(exitErrOther, "unexpected error: %v", err)
	}
}

// corruptFileField 在父进程不持有映射时改写文件字节，构造头部冲突现场。
func corruptFileField(t *testing.T, dat string, off int, b []byte) {
	t.Helper()
	f, err := os.OpenFile(dat, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(b, int64(off)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestCrossProcessMountConflicts(t *testing.T) {
	dir := testDir(t)

	// 场景一：容量冲突，父进程保持挂载（缓冲区“正在使用中”）。
	nameCap := "mpc-cap"
	r, err := Create(Config{Name: nameCap, Capacity: 1024}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	cmd := runChild(t, "mount-badcap",
		"SHMRING_DIR="+dir, "SHMRING_NAME="+nameCap, "SHMRING_CAP=512")
	if e := cmd.Wait(); e == nil || e.(*exec.ExitError).ExitCode() != exitErrCapacity {
		t.Fatalf("bad-capacity child exit=%v, want %d", e, exitErrCapacity)
	}
	t.Logf("capacity conflict rejected across processes: %v", ErrCapacityMismatch)
	if _, err := r.Write([]byte("still-alive")); err != nil {
		t.Fatalf("ring broken after rejected capacity mount: %v", err)
	}
	if rec, err := r.Read(); err != nil || string(rec.Payload) != "still-alive" {
		t.Fatalf("read after conflict = %+v, %v", rec, err)
	}
	r.Close()

	// 场景二：魔数冲突。父进程先关闭映射，直接损坏文件头部后由子进程挂载。
	nameMagic := "mpc-magic"
	rm, err := Create(Config{Name: nameMagic, Capacity: 1024}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	rm.Close()
	corruptFileField(t, filepath.Join(dir, "shmring-"+nameMagic+".dat"), 0, []byte("XXXX"))
	cmd = runChild(t, "mount-badmagic", "SHMRING_DIR="+dir, "SHMRING_NAME="+nameMagic)
	if e := cmd.Wait(); e == nil || e.(*exec.ExitError).ExitCode() != exitErrMagic {
		t.Fatalf("bad-magic child exit=%v, want %d", e, exitErrMagic)
	}
	t.Logf("magic conflict rejected across processes: %v", ErrMagicMismatch)

	// 场景三：版本冲突。
	nameVer := "mpc-ver"
	rv, err := Create(Config{Name: nameVer, Capacity: 1024}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	rv.Close()
	verBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(verBytes, 99)
	corruptFileField(t, filepath.Join(dir, "shmring-"+nameVer+".dat"), offVersion, verBytes)
	cmd = runChild(t, "mount-badversion", "SHMRING_DIR="+dir, "SHMRING_NAME="+nameVer)
	if e := cmd.Wait(); e == nil || e.(*exec.ExitError).ExitCode() != exitErrVersion {
		t.Fatalf("bad-version child exit=%v, want %d", e, exitErrVersion)
	}
	t.Logf("version conflict rejected across processes: %v", ErrVersionMismatch)
}

// 并发 goroutine 下反复冲突挂载，确保拒绝路径无死锁、无破坏。
func TestConcurrentRejectedMounts(t *testing.T) {
	dir := testDir(t)
	name := "crm"
	r, err := Create(Config{Name: name, Capacity: 2048}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := Create(Config{Name: name, Capacity: 1024}, &Options{Dir: dir})
			if !errors.Is(err, ErrCapacityMismatch) {
				t.Errorf("got %v, want ErrCapacityMismatch", err)
			}
		}()
	}
	wg.Wait()
	if _, err := r.Write([]byte("ok")); err != nil {
		t.Fatalf("ring broken after rejected mounts: %v", err)
	}
}

// TestCrashNoOverwriteUnread 验证：写者崩溃回收后，未被消费的完整记录
// 仍然占位，写者不能借“回收残片”覆盖它们；只有读者确认后空间才释放。
// 全程使用包内崩溃钩子做确定性模拟，无需子进程。
func TestCrashNoOverwriteUnread(t *testing.T) {
	dir := testDir(t)
	name := "cno"
	const cap = 512
	r, err := Create(Config{Name: name, Capacity: cap}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	// 容量布局：负载 100 -> 帧 124；4 条共 496，恰好写满。
	const plen = 100
	big := func(i int) []byte {
		p := make([]byte, plen)
		copy(p, fmt.Sprintf("bulk-%d-", i))
		return p
	}
	for i := 0; i < 4; i++ {
		if _, err := r.Write(big(i)); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
	if _, err := r.Write(big(4)); !errors.Is(err, ErrFull) {
		t.Fatalf("expect full, got %v", err)
	}

	// 消费者确认第 1 条，释放出 124 字节空洞（位于逻辑 0..123）。
	if _, err := r.Read(); err != nil {
		t.Fatal(err)
	}

	// 写者在新帧负载写到一半时“死亡”：占满刚释放的 124 字节，但
	// writePos 不推进。用新挂载者模拟崩溃后的进程重启。
	crashed := make(chan struct{})
	writerDone := make(chan struct{})
	oldHook := midPayloadHook
	midPayloadHook = func(seq uint64) {
		fmt.Printf("HOOK: writer goroutine dies mid-payload at seq=%d\n", seq)
		close(crashed)
		runtime.Goexit()
	}
	go func() {
		defer close(writerDone)
		_, _ = r.Write(big(5))
	}()
	select {
	case <-crashed:
	case <-time.After(5 * time.Second):
		t.Fatal("crash hook never fired")
	}
	// 等写者 goroutine 完全退出（其 Write defer 已释放写锁）再关闭 Ring，
	// 否则 Close 关闭 fd 会与释放锁路径产生竞争。
	<-writerDone
	// 崩溃点已过，立即还原全局钩子，避免后续合法 Write 再次触发。
	midPayloadHook = oldHook

	// 用 Goexit 模拟死亡后，当前 Ring 的互斥锁已释放，重新挂载观测现场。
	r.Close()
	obs, err := Open(name, &Options{Dir: dir, disableAutoRecover: true})
	if err != nil {
		t.Fatal(err)
	}
	resv, wp, rp := obs.Positions()
	t.Logf("before recover: reserved=%d write=%d read=%d gap=%d", resv, wp, rp, resv-wp)
	if resv-wp != 124 {
		t.Fatalf("torn gap = %d, want 124", resv-wp)
	}
	// 未消费的 bulk-1 仍在且可读，内容必须原样。
	rec, err := obs.Peek()
	if err != nil || rec.Seq != 2 || string(rec.Payload[:6]) != "bulk-1" {
		t.Fatalf("unread record lost/altered: %+v, %v", rec, err)
	}

	// 自动回收的新挂载：残片空间只能在“逻辑尾”逻辑中被后续写入复用，
	// 但已提交但未确认的 3 条记录（372 字节）绝不能被覆盖。
	obs.Close()
	recRing, err := Open(name, &Options{Dir: dir}) // 触发自动回收
	if err != nil {
		t.Fatal(err)
	}
	defer recRing.Close()
	resv, wp, rp = recRing.Positions()
	t.Logf("after recover: reserved=%d write=%d read=%d used=%d free=%d",
		resv, wp, rp, wp-rp, cap-int(wp-rp))
	if resv != wp {
		t.Fatalf("torn gap not reclaimed: reserved=%d write=%d", resv, wp)
	}
	// 可用空间 = 512 - 372 = 140，不足以再放 124 的帧？140 >= 124，可以放 1 条。
	if _, err := recRing.Write(big(9)); err != nil {
		t.Logf("note: no room even for one frame right at boundary (%v)", err)
	}
	// 再写第二条必须 ErrFull（不得覆盖未消费的 bulk-1..bulk-3）。
	if _, err := recRing.Write(big(8)); !errors.Is(err, ErrFull) {
		t.Fatalf("second write must be ErrFull, got %v", err)
	}
	// 逐条确认剩余记录，内容与序号连续。
	for i := 1; i <= 3; i++ {
		rec, err := recRing.Read()
		if err != nil {
			t.Fatalf("read unread %d: %v", i, err)
		}
		want := fmt.Sprintf("bulk-%d-", i)
		if string(rec.Payload[:len(want)]) != want {
			t.Fatalf("unread record %d altered: %q", i, rec.Payload)
		}
	}
	_, wp, rp = recRing.Positions()
	t.Logf("all confirmed: write=%d read=%d used=%d", wp, rp, wp-rp)
}
