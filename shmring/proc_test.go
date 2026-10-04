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
	// 可选：通过环境变量调整子进程的消费者心跳节奏。
	if ms, err := strconv.Atoi(os.Getenv("SHMRING_TTL_MS")); err == nil && ms > 0 {
		consumerTTL = time.Duration(ms) * time.Millisecond
	}
	if ms, err := strconv.Atoi(os.Getenv("SHMRING_HB_MS")); err == nil && ms > 0 {
		heartbeatInterval = time.Duration(ms) * time.Millisecond
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
	case scene == "writer-wait-go":
		childWriterWaitGo(t, dir, name)
	case scene == "consumer-claim":
		childConsumerClaim(t, dir, name)
	case scene == "consumer-hold-die":
		childConsumerHoldDie(t, dir, name)
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

	// 自动回收的新挂载：v2 语义下崩溃写者的那一次分配被固化为空洞帧，
	// 空间永不回收重用；已提交但未确认的 3 条记录（372 字节）同样占位。
	obs.Close()
	recRing, err := Open(name, &Options{Dir: dir}) // 触发自动在线自愈
	if err != nil {
		t.Fatal(err)
	}
	defer recRing.Close()
	resv, wp, rp = recRing.Positions()
	t.Logf("after heal: reserved=%d write=%d read=%d used=%d free=%d",
		resv, wp, rp, resv-rp, cap-int(resv-rp))
	if resv != wp {
		t.Fatalf("heal must advance writePos to reservedPos: reserved=%d write=%d", resv, wp)
	}
	// 空洞帧可见：逻辑位置 496 处是 124 字节的 GAP（“这里曾经有一次没完成的写入”）。
	gapLen, ok := recRing.committedGapAt(496, resv)
	if !ok || gapLen != 124 {
		t.Fatalf("gap frame at 496 = (%d, %v), want (124, true)", gapLen, ok)
	}
	t.Logf("torn allocation preserved as gap frame @496 len=124 (never reclaimed)")
	// 空洞仍占位：used = 620-124 = 496，free = 16 < 124，写入必须 ErrFull。
	if _, err := recRing.Write(big(9)); !errors.Is(err, ErrFull) {
		t.Fatalf("write while gap+unread occupy space = %v, want ErrFull", err)
	}
	// 逐条确认剩余记录，内容与序号连续（未被覆盖、未被改写）。
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
	t.Logf("all confirmed: write=%d read=%d used=%d (only the gap remains)", wp, rp, wp-rp)
	// 确认后只剩空洞本身占位（124 字节）：写者恢复，且序列号不回卷
	// （崩溃写者已消耗 seq=5）。
	seq, err := recRing.Write(big(9))
	if err != nil {
		t.Fatalf("write after confirming unread records: %v", err)
	}
	if seq != 6 {
		t.Fatalf("seq after torn write = %d, want 6 (seq=5 burned by crashed writer)", seq)
	}
	t.Logf("writer resumed after confirmations: seq=%d (seq=5 remains a visible hole)", seq)
}

// ---------------------------------------------------------------------------
// 多消费者与在线自愈的跨进程场景
// ---------------------------------------------------------------------------

// waitForFile 等待一个标记文件出现（子进程间握手用）。
func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("marker file %s never appeared", path)
}

// childConsumerClaim：注册一个消费者，持续领取记录并把 (seq, payload)
// 追加到结果文件；看到停止文件且无记录可领时退出。
func childConsumerClaim(t *testing.T, dir, name string) {
	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		childFatalf(exitErrOther, "consumer child open: %v", err)
	}
	defer r.Close()
	c, err := r.NewConsumer()
	if err != nil {
		childFatalf(exitErrOther, "consumer child NewConsumer: %v", err)
	}
	defer c.Close()
	result := os.Getenv("SHMRING_RESULT")
	stop := os.Getenv("SHMRING_STOP")
	f, err := os.OpenFile(result, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		childFatalf(exitErrOther, "consumer child result file: %v", err)
	}
	defer f.Close()
	for {
		rec, err := c.Next()
		if errors.Is(err, ErrEmpty) {
			if _, e := os.Stat(stop); e == nil {
				return
			}
			time.Sleep(3 * time.Millisecond)
			continue
		}
		if err != nil {
			childFatalf(exitErrOther, "consumer child next: %v", err)
		}
		fmt.Fprintf(f, "%d %s\n", rec.Seq, rec.Payload)
		_ = f.Sync()
		if err := c.Confirm(); err != nil {
			childFatalf(exitErrOther, "consumer child confirm: %v", err)
		}
	}
}

// childWriterWaitGo：先挂载（留下 mounted 标记证明“已在运行”），
// 等待 go 文件出现后写入 count 条记录再退出。
func childWriterWaitGo(t *testing.T, dir, name string) {
	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		childFatalf(exitErrOther, "wait-go writer open: %v", err)
	}
	defer r.Close()
	mounted := os.Getenv("SHMRING_MOUNTED")
	goFile := os.Getenv("SHMRING_GO")
	count, _ := strconv.Atoi(os.Getenv("SHMRING_COUNT"))
	if err := os.WriteFile(mounted, []byte("1"), 0o644); err != nil {
		childFatalf(exitErrOther, "wait-go writer mark: %v", err)
	}
	for {
		if _, err := os.Stat(goFile); err == nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	for i := 0; i < count; i++ {
		p := []byte(fmt.Sprintf("B-record-%d-pad", i))
		for {
			_, err := r.Write(p)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrFull) {
				childFatalf(exitErrOther, "wait-go writer write: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	fmt.Printf("CHILD-WRITER-B: wrote %d records after crash of writer A\n", count)
}

// childConsumerHoldDie：领取一条记录后不确认、不注销，直接退出，
// 模拟消费者进程崩溃（槽位随心跳过期被其他消费者接管）。
func childConsumerHoldDie(t *testing.T, dir, name string) {
	r, err := Open(name, &Options{Dir: dir})
	if err != nil {
		childFatalf(exitErrOther, "hold-die consumer open: %v", err)
	}
	defer r.Close()
	c, err := r.NewConsumer()
	if err != nil {
		childFatalf(exitErrOther, "hold-die consumer NewConsumer: %v", err)
	}
	rec, err := c.Next()
	if err != nil {
		childFatalf(exitErrOther, "hold-die consumer next: %v", err)
	}
	fmt.Printf("CHILD-CONSUMER: claimed seq=%d payload=%q, dying without confirm\n",
		rec.Seq, rec.Payload)
	os.Exit(exitChildSignalCrash)
}

// TestMultiConsumerCrossProcess：2 个真实消费者子进程 + 父进程消费者
// 并发领取 3 个写进程写入的 180 条记录，每条必须恰好被领取一次。
func TestMultiConsumerCrossProcess(t *testing.T) {
	dir := testDir(t)
	name := "mcp"
	r, err := Create(Config{Name: name, Capacity: 1 << 16}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	pc, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	stopFile := filepath.Join(dir, "stop")
	resultFiles := []string{
		filepath.Join(dir, "result-0.txt"),
		filepath.Join(dir, "result-1.txt"),
	}
	// 子进程 TTL 调大：避免高负载下心跳抖动被误判为崩溃（本场景不测试崩溃）。
	consumers := make([]*exec.Cmd, 2)
	for i := range consumers {
		consumers[i] = runChild(t, "consumer-claim",
			"SHMRING_DIR="+dir, "SHMRING_NAME="+name,
			"SHMRING_RESULT="+resultFiles[i], "SHMRING_STOP="+stopFile,
			"SHMRING_TTL_MS=30000", "SHMRING_HB_MS=100")
	}
	const writers, perWriter = 3, 60
	const total = writers * perWriter
	wcmds := make([]*exec.Cmd, writers)
	for i := 0; i < writers; i++ {
		wcmds[i] = runChild(t, fmt.Sprintf("writer-stream-%d", i),
			"SHMRING_DIR="+dir, "SHMRING_NAME="+name)
	}

	// union[seq] = payload：父进程自己的领取 + 两个子进程的结果文件。
	union := make(map[uint64]string)
	collect := func() {
		for _, rf := range resultFiles {
			data, err := os.ReadFile(rf)
			if err != nil {
				continue
			}
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				if line == "" {
					continue
				}
				var seq uint64
				var payload string
				if _, err := fmt.Sscanf(line, "%d %s", &seq, &payload); err != nil {
					t.Fatalf("bad result line %q: %v", line, err)
				}
				if prev, dup := union[seq]; dup && prev != payload {
					t.Fatalf("seq %d delivered twice with different payloads: %q vs %q",
						seq, prev, payload)
				}
				union[seq] = payload
			}
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for len(union) < total {
		if time.Now().After(deadline) {
			collect()
			t.Fatalf("timeout: claimed %d/%d unique records", len(union), total)
		}
		rec, err := pc.Next()
		if errors.Is(err, ErrEmpty) {
			collect()
			time.Sleep(3 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if prev, dup := union[rec.Seq]; dup && prev != string(rec.Payload) {
			t.Fatalf("seq %d duplicate with different payload", rec.Seq)
		}
		union[rec.Seq] = string(rec.Payload)
		if err := pc.Confirm(); err != nil {
			t.Fatal(err)
		}
	}
	// 全部领取完毕：通知子进程退出并等待所有人结束。
	if err := os.WriteFile(stopFile, []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	for i, c := range wcmds {
		waitChild(t, c, fmt.Sprintf("writer-%d", i))
	}
	for i, c := range consumers {
		waitChild(t, c, fmt.Sprintf("consumer-%d", i))
	}
	collect()

	// 校验依据一：序列号恰好覆盖 1..total，无重复、无缺失。
	if len(union) != total {
		t.Fatalf("claimed %d unique records, want %d", len(union), total)
	}
	for seq := uint64(1); seq <= total; seq++ {
		if _, ok := union[seq]; !ok {
			t.Fatalf("seq %d missing", seq)
		}
	}
	// 校验依据二：负载编码 (writer,msg) 对必须全局唯一且格式自洽。
	pairs := make(map[string]bool)
	for seq, p := range union {
		var w, m, pad int
		if n, _ := fmt.Sscanf(p, "p%d-m%d-pad-%024d", &w, &m, &pad); n != 3 || m != pad {
			t.Fatalf("seq %d payload %q does not match writer format", seq, p)
		}
		key := fmt.Sprintf("%d-%d", w, m)
		if pairs[key] {
			t.Fatalf("payload pair %s delivered twice", key)
		}
		pairs[key] = true
	}
	t.Logf("cross-process exactly-once: %d records, %d unique (writer,msg) pairs, claimed by 3 consumers in 2 processes + parent",
		len(union), len(pairs))
	resv, wp, _ := r.Positions()
	t.Logf("final: reserved=%d write=%d claim=%d floor=%d",
		resv, wp, r.ClaimPos(), r.reclaimFloor())
}

// TestWriterCrashOnlineHeal：写者 A 写到一半被杀，已挂载的写者 B
// 无需重挂载即可继续写入（首笔写触发在线自愈），父进程消费者
// 无需重挂载即可读到崩溃之后的新记录；残留分配固化为空洞帧。
func TestWriterCrashOnlineHeal(t *testing.T) {
	dir := testDir(t)
	name := "heal"
	r, err := Create(Config{Name: name, Capacity: 2048}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// 父进程消费者在崩溃发生前就已挂载（消费者组模式）。
	pc, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	goFile := filepath.Join(dir, "go")
	mountedFile := filepath.Join(dir, "mounted")
	// 写者 B：先挂载并等待放行信号——验证“已经挂载、还在运行的写者”。
	cmdB := runChild(t, "writer-wait-go",
		"SHMRING_DIR="+dir, "SHMRING_NAME="+name,
		"SHMRING_MOUNTED="+mountedFile, "SHMRING_GO="+goFile, "SHMRING_COUNT=5")
	waitForFile(t, mountedFile, 10*time.Second)
	t.Logf("writer B mounted and waiting; starting writer A (will crash mid-payload)")

	// 写者 A：写 3 条完整记录后，第 4 条负载写一半时被杀。
	cmdA := runChild(t, "writer-crash-midpayload",
		"SHMRING_DIR="+dir, "SHMRING_NAME="+name, "SHMRING_CAP=2048")
	err = cmdA.Wait()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != exitChildSignalCrash {
		t.Fatalf("writer A exit = %v, want crash code %d", err, exitChildSignalCrash)
	}
	t.Logf("writer A killed mid-payload; releasing writer B")

	// 放行 B：它的第一笔写会在写锁内把 A 的残留固化为空洞帧。
	if err := os.WriteFile(goFile, []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitChild(t, cmdB, "writer-B")

	// 父进程消费者（未重挂载）依次读到 8 条：seq 1,2,3（A 的好记录）
	// 与 seq 5..9（B 的记录）；seq=4 是崩溃写者烧掉的可见序列号空洞。
	wantSeqs := []uint64{1, 2, 3, 5, 6, 7, 8, 9}
	var got []uint64
	deadline := time.Now().Add(15 * time.Second)
	for len(got) < len(wantSeqs) {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: got seqs %v, want %v", got, wantSeqs)
		}
		rec, err := pc.Next()
		if errors.Is(err, ErrEmpty) {
			time.Sleep(3 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, rec.Seq)
		want := wantSeqs[len(got)-1]
		if rec.Seq != want {
			t.Fatalf("record %d: seq=%d, want %d (seq=4 must be a visible hole)", len(got), rec.Seq, want)
		}
		var wantPayload string
		if rec.Seq <= 3 {
			wantPayload = fmt.Sprintf("good-%d-payload", rec.Seq)
		} else {
			wantPayload = fmt.Sprintf("B-record-%d-pad", rec.Seq-5)
		}
		if string(rec.Payload) != wantPayload {
			t.Fatalf("seq=%d payload=%q, want %q", rec.Seq, rec.Payload, wantPayload)
		}
		if err := pc.Confirm(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("consumer (no remount) read seqs %v: seq=4 is the burned hole from crashed writer A", got)

	// 自愈后游标自洽，且残留分配以空洞帧形式可见、未被回收重用。
	resv, wp, _ := r.Positions()
	if resv != wp {
		t.Fatalf("after heal reserved=%d != write=%d", resv, wp)
	}
	// 3 条好记录各占 38 字节 -> 空洞位于逻辑 114，长度 = 16+50+8 = 74。
	gapLen, ok := r.committedGapAt(114, resv)
	if !ok || gapLen != 74 {
		t.Fatalf("gap frame at 114 = (%d, %v), want (74, true)", gapLen, ok)
	}
	t.Logf("torn allocation visible as gap frame @114 len=74; reserved=%d write=%d nextSeq=%d",
		resv, wp, r.NextSeq())
	if ns := r.NextSeq(); ns != 10 {
		t.Fatalf("nextSeq = %d, want 10 (seq=4 burned, never reused)", ns)
	}
	// 恢复幂等：再次 Recover 不再有任何动作。
	recovered, torn, err := r.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if recovered || torn != 0 {
		t.Fatalf("idempotent Recover = %v,%d, want false,0", recovered, torn)
	}
}

// TestConsumerCrashRedeliveryProcess：消费者子进程领取记录后未确认即死，
// 父进程消费者在心跳过期后按相同 Seq 接管重投。
func TestConsumerCrashRedeliveryProcess(t *testing.T) {
	// 父进程侧缩短 TTL；子进程用默认心跳（200ms），不会被误判。
	oldT, oldI := consumerTTL, heartbeatInterval
	consumerTTL = 600 * time.Millisecond
	heartbeatInterval = 50 * time.Millisecond
	defer func() { consumerTTL, heartbeatInterval = oldT, oldI }()

	dir := testDir(t)
	name := "crd"
	r, err := Create(Config{Name: name, Capacity: 1024}, &Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Write([]byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("beta")); err != nil {
		t.Fatal(err)
	}

	cmd := runChild(t, "consumer-hold-die", "SHMRING_DIR="+dir, "SHMRING_NAME="+name)
	err = cmd.Wait()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != exitChildSignalCrash {
		t.Fatalf("consumer child exit = %v, want crash code %d", err, exitChildSignalCrash)
	}
	t.Logf("consumer child died holding seq=1 (unconfirmed)")

	pc, err := r.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	// 崩溃尚未过期：先领到 seq=2（互不阻塞）。
	rec, err := pc.Next()
	if err != nil || rec.Seq != 2 || string(rec.Payload) != "beta" {
		t.Fatalf("Next before TTL = %+v, %v, want seq=2 beta", rec, err)
	}
	if err := pc.Confirm(); err != nil {
		t.Fatal(err)
	}
	// 心跳过期后：崩溃消费者的 seq=1 被接管重投，Seq 相同可识别。
	time.Sleep(2 * consumerTTL)
	stolen, err := pc.Next()
	if err != nil {
		t.Fatalf("steal after TTL: %v", err)
	}
	if stolen.Seq != 1 || string(stolen.Payload) != "alpha" {
		t.Fatalf("stolen = %+v, want redelivery of seq=1 alpha", stolen)
	}
	t.Logf("crashed process's seq=1 redelivered to surviving consumer (same seq => recognizable)")
	if err := pc.Confirm(); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.Next(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("drained = %v, want ErrEmpty", err)
	}
	logGroupState(t, r, "after cross-process redelivery")
}
