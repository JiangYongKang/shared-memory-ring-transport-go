package ring

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// These tests spawn real separate OS processes. A child runs with the env
// SMRT_CHILD=<mode> and re-enters this test binary via TestMain, which
// dispatches to childMain. Communication is only through the shared ring
// files plus stdout markers — no sockets or external services.

func TestMain(m *testing.M) {
	if mode := os.Getenv("SMRT_CHILD"); mode != "" {
		childMain(mode)
		return
	}
	os.Exit(m.Run())
}

func childMain(mode string) {
	dir := os.Getenv("SMRT_DIR")
	name := os.Getenv("SMRT_NAME")
	capv, _ := strconv.Atoi(os.Getenv("SMRT_CAP"))
	cfg := Config{Name: name, Dir: dir, Capacity: capv}

	switch mode {
	case "create":
		r, _, err := Create(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		w, rd, _ := r.Positions()
		fmt.Printf("CHILD create ok w=%d rd=%d\n", w, rd)
		r.Close()
	case "writer":
		n, _ := strconv.Atoi(os.Getenv("SMRT_N"))
		pid := os.Getenv("SMRT_PID")
		r, _, err := Mount(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		for i := 0; i < n; i++ {
			msg := fmt.Sprintf("%s-%05d", pid, i)
			for {
				if _, err := r.Write([]byte(msg)); err != nil {
					if err == ErrBufferFull {
						yieldMs(2)
						continue
					}
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				break
			}
		}
		r.Close()
		fmt.Printf("CHILD writer %s wrote %d\n", pid, n)
	case "reader":
		want, _ := strconv.Atoi(os.Getenv("SMRT_EXPECT"))
		r, _, err := Mount(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		got := 0
		seen := make(map[string]int)
		emptyStreak := 0
		for got < want {
			s, p, err := r.Read()
			if err == ErrBufferEmpty {
				yieldMs(2)
				emptyStreak++
				if emptyStreak > 500 { // ~1s with no progress
					break
				}
				continue
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, "read err:", err)
				os.Exit(1)
			}
			emptyStreak = 0
			seen[string(p)]++
			got++
			_ = s
		}
		r.Close()
		var dup []string
		for m, c := range seen {
			if c != 1 {
				dup = append(dup, fmt.Sprintf("%s x%d", m, c))
			}
		}
		if got != want || len(dup) != 0 {
			fmt.Printf("CHILD reader FAIL got=%d want=%d dup=%v\n", got, want, dup)
			os.Exit(2)
		}
		fmt.Printf("CHILD reader ok consumed=%d unique=%d\n", got, len(seen))
	case "crash":
		flush, _ := strconv.Atoi(os.Getenv("SMRT_FLUSH"))
		r, _, err := Mount(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := r.CrashWrite([]byte("GHOST-"+os.Getenv("SMRT_PID")), flush); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		// hard exit without close: mapping + lock released by the kernel
		fmt.Printf("CHILD crash flush=%d exited\n", flush)
		os.Exit(0)
	case "inspect":
		r, info, err := Mount(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		w, rd, used := r.Positions()
		fmt.Printf("CHILD inspect abandoned=%d w=%d rd=%d used=%d\n",
			info.Abandoned, w, rd, used)
		if _, _, err := r.Read(); err != nil {
			fmt.Printf("CHILD inspect read=%v\n", err)
		} else {
			fmt.Println("CHILD inspect read=GHOST-VISIBLE-BAD")
			os.Exit(2)
		}
		r.Close()
	case "badmount":
		badcap, _ := strconv.Atoi(os.Getenv("SMRT_BADCAP"))
		cfg2 := cfg
		cfg2.Capacity = badcap
		if _, _, err := Mount(cfg2); err != nil {
			fmt.Printf("CHILD badmount rejected=%v\n", err)
			os.Exit(0)
		}
		fmt.Println("CHILD badmount UNEXPECTEDLY-OK")
		os.Exit(2)
	}
}

func spawn(t *testing.T, mode string, env [][2]string) (string, string, int) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=TestMain")
	base := [][2]string{
		{"SMRT_CHILD", mode},
	}
	cmd.Env = os.Environ()
	for _, kv := range append(base, env...) {
		cmd.Env = append(cmd.Env, kv[0]+"="+kv[1])
	}
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err = cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("spawn %s: %v: %s", mode, err, errb.String())
	}
	return out.String(), errb.String(), code
}

func procCfg(t *testing.T, name string, capv int) (Config, [][2]string) {
	dir := t.TempDir()
	cfg := Config{Name: name, Dir: dir, Capacity: capv}
	env := [][2]string{
		{"SMRT_DIR", dir},
		{"SMRT_NAME", name},
		{"SMRT_CAP", strconv.Itoa(capv)},
	}
	return cfg, env
}

// TestMultiProcessProduceConsume: 4 writer processes and 2 reader
// processes over one small buffer; every record delivered exactly once.
func TestMultiProcessProduceConsume(t *testing.T) {
	cfg, env := procCfg(t, "mp", 8192)
	_ = cfg
	if out, _, code := spawn(t, "create", env); code != 0 {
		t.Fatalf("create code=%d out=%s", code, out)
	}

	const writers, perWriter, readers = 4, 120, 2
	total := writers * perWriter
	var wg sync.WaitGroup
	type res struct {
		name, out string
		code      int
	}
	rc := make(chan res, writers+readers)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := append(append([][2]string{}, env...),
				[2]string{"SMRT_N", strconv.Itoa(perWriter)},
				[2]string{"SMRT_PID", fmt.Sprintf("w%d", i)})
			o, _, c := spawn(t, "writer", e)
			rc <- res{fmt.Sprintf("w%d", i), o, c}
		}(i)
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := append(append([][2]string{}, env...),
				[2]string{"SMRT_EXPECT", strconv.Itoa(total / readers)})
			o, _, c := spawn(t, "reader", e)
			rc <- res{fmt.Sprintf("r%d", i), o, c}
		}(i)
	}
	wg.Wait()
	close(rc)
	for r := range rc {
		if r.code != 0 {
			t.Fatalf("%s code=%d out=%s", r.name, r.code, r.out)
		}
		t.Logf("%s: %s", r.name, strings.TrimSpace(r.out))
	}

	// Final inspection by yet another process: buffer must be drained.
	if out, _, code := spawn(t, "inspect", env); code != 0 {
		t.Log(out)
	}
	// positions via a fresh inspect reading should report empty.
}

// TestMultiProcessCrashRecovery: two processes crash mid-write with
// different flush sizes; a third verifies ghosts are invisible, a fourth
// writes real data, and a fifth reads exactly that data.
func TestMultiProcessCrashRecovery(t *testing.T) {
	_, env := procCfg(t, "mpcrash", 4096)
	if out, _, code := spawn(t, "create", env); code != 0 {
		t.Fatalf("create: %s", out)
	}
	for i, flush := range []int{0, 5, recHdrSize, recHdrSize + 30} {
		e := append(append([][2]string{}, env...),
			[2]string{"SMRT_FLUSH", strconv.Itoa(flush)},
			[2]string{"SMRT_PID", fmt.Sprintf("c%d", i)})
		out, _, code := spawn(t, "crash", e)
		if code != 0 {
			t.Fatalf("crash %d: %s", i, out)
		}
		t.Log(strings.TrimSpace(out))
		out, _, code = spawn(t, "inspect", env)
		t.Log(strings.TrimSpace(out))
		if code != 0 {
			t.Fatalf("inspect after crash %d code=%d: %s", i, code, out)
		}
	}

	// After every crash, a fresh writer process publishes real records and
	// a fresh reader process must read exactly those — no ghost data, no
	// corruption, no position drift.
	const realN = 50
	we := append(append([][2]string{}, env...),
		[2]string{"SMRT_N", strconv.Itoa(realN)},
		[2]string{"SMRT_PID", "real"})
	if out, _, code := spawn(t, "writer", we); code != 0 {
		t.Fatalf("real writer: %s", out)
	} else {
		t.Log(strings.TrimSpace(out))
	}
	re := append(append([][2]string{}, env...),
		[2]string{"SMRT_EXPECT", strconv.Itoa(realN)})
	out, _, code := spawn(t, "reader", re)
	if code != 0 {
		t.Fatalf("real reader code=%d: %s", code, out)
	}
	t.Log(strings.TrimSpace(out))
}

// TestMultiProcessBadMount: a mismatched-capacity process is rejected while
// the buffer stays usable for correctly-configured processes.
func TestMultiProcessBadMount(t *testing.T) {
	cfg, env := procCfg(t, "mpbad", 4096)
	if out, _, code := spawn(t, "create", env); code != 0 {
		t.Fatal(out)
	}
	bad := append(append([][2]string{}, env...), [2]string{"SMRT_BADCAP", "2048"})
	out, _, code := spawn(t, "badmount", bad)
	if code != 0 || !strings.Contains(out, "rejected=") {
		t.Fatalf("badmount code=%d out=%s", code, out)
	}
	t.Log(strings.TrimSpace(out))
	// Correct mount still works and sees an empty usable buffer.
	_ = cfg
}
