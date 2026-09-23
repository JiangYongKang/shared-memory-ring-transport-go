// Command shmring-demo 用本地映射文件演示跨进程共享内存环形缓冲。
//
// 用法（在不同终端/进程中执行）：
//
//	shmring-demo create <name> [capacity]   创建缓冲区
//	shmring-demo write  <name> <message>   写入一条完整记录
//	shmring-demo read   <name>             读取并确认一条记录
//	shmring-demo peek   <name>             查看但不确认（重复查看可识别重复）
//	shmring-demo state  <name>             打印读写位置与序列号
//
// 全部状态保存在系统临时目录的 shmring-<name>.dat 文件中，
// 不依赖任何外部服务。
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/highcumontoa/shared-memory-ring-transport-go/shmring"
)

func main() {
	if len(os.Args) < 3 {
		usage()
	}
	cmd, name := os.Args[1], os.Args[2]

	switch cmd {
	case "create":
		capacity := 4096
		if len(os.Args) > 3 {
			n, err := strconv.Atoi(os.Args[3])
			must(err)
			capacity = n
		}
		r, err := shmring.Create(shmring.Config{Name: name, Capacity: capacity}, nil)
		must(err)
		must(r.Close())
		fmt.Printf("created buffer %q capacity=%d\n", name, capacity)

	case "write":
		if len(os.Args) < 4 {
			usage()
		}
		r, err := shmring.Open(name, nil)
		must(err)
		defer r.Close()
		seq, err := r.Write([]byte(os.Args[3]))
		if errors.Is(err, shmring.ErrFull) {
			fail("buffer full: 消费者需要先 read 确认，写者不会覆盖未消费记录")
		}
		must(err)
		rsv, wp, rp := r.Positions()
		fmt.Printf("wrote seq=%d bytes=%d reserved=%d write=%d read=%d\n",
			seq, len(os.Args[3]), rsv, wp, rp)

	case "read", "peek":
		r, err := shmring.Open(name, nil)
		must(err)
		defer r.Close()
		var rec shmring.Record
		if cmd == "peek" {
			rec, err = r.Peek()
		} else {
			rec, err = r.Read()
		}
		if errors.Is(err, shmring.ErrEmpty) {
			rsv, wp, rp := r.Positions()
			fmt.Printf("empty: 没有已提交的完整记录 reserved=%d write=%d read=%d\n", rsv, wp, rp)
			return
		}
		must(err)
		rsv, wp, rp := r.Positions()
		fmt.Printf("%s seq=%d payload=%q reserved=%d write=%d read=%d\n",
			cmd, rec.Seq, rec.Payload, rsv, wp, rp)

	case "state":
		r, err := shmring.Open(name, nil)
		must(err)
		defer r.Close()
		rsv, wp, rp := r.Positions()
		fmt.Printf("name=%s capacity=%d reserved=%d write=%d read=%d used=%d nextSeq=%d\n",
			name, r.Capacity(), rsv, wp, rp, rsv-rp, r.NextSeq())

	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: shmring-demo {create|write|read|peek|state} <name> [args]")
	os.Exit(2)
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
