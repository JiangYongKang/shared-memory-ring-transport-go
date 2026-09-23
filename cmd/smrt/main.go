// Command smrt is a small multi-process driver for the shared-memory ring
// transport. It lets separate OS processes create, mount, write, read and
// crash against the same named buffer, so the transport is reproducible
// without any external service.
//
// Usage:
//
//	smrt -dir DIR -name N -cap C create
//	smrt -dir DIR -name N -cap C mount            # verify conventions, print positions
//	smrt -dir DIR -name N -cap C write -m MSG     # one record; prints seq
//	smrt -dir DIR -name N -cap C fill -n K -p L   # write K records of L bytes
//	smrt -dir DIR -name N -cap C read             # consume one; prints seq<TAB>payload
//	smrt -dir DIR -name N -cap C drain [-n K]     # consume until empty (or K)
//	smrt -dir DIR -name N -cap C positions        # print writePos readPos used
//	smrt -dir DIR -name N -cap C crash -m MSG -f B# write B bytes then die w/o publish
//
// Exit codes distinguish outcomes: 0 ok; 2 buffer full; 3 buffer empty;
// 4 conflict/invalid parameter; 5 corrupt; 1 other error.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/highcumontoa/shared-memory-ring-transport-go/ring"
)

func main() {
	dir := flag.String("dir", "", "directory holding ring files")
	name := flag.String("name", "", "buffer name")
	capStr := flag.String("cap", "4096", "capacity in bytes (multiple of 8, >=1024)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: smrt -dir DIR -name N -cap C <command> [flags]\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 || *name == "" {
		flag.Usage()
		os.Exit(1)
	}
	capv, err := strconv.Atoi(*capStr)
	if err != nil {
		fatal(4, fmt.Errorf("bad -cap: %w", err))
	}
	cfg := ring.Config{Name: *name, Dir: *dir, Capacity: capv}
	cmd, rest := args[0], args[1:]
	if err := run(cfg, cmd, rest); err != nil {
		switch {
		case errors.Is(err, ring.ErrBufferFull):
			os.Exit(2)
		case errors.Is(err, ring.ErrBufferEmpty):
			os.Exit(3)
		case errors.Is(err, ring.ErrInvalidParam), errors.Is(err, ring.ErrCapacityConflict),
			errors.Is(err, ring.ErrVersionConflict), errors.Is(err, ring.ErrMagicConflict),
			errors.Is(err, ring.ErrFormatConflict), errors.Is(err, ring.ErrRecordTooLarge):
			fmt.Fprintln(os.Stderr, "CONFLICT:", err)
			os.Exit(4)
		case errors.Is(err, ring.ErrCorruptRecord):
			os.Exit(5)
		default:
			fatal(1, err)
		}
	}
}

func run(cfg ring.Config, cmd string, args []string) error {
	switch cmd {
	case "create":
		r, _, err := ring.Create(cfg)
		if err != nil {
			return err
		}
		defer r.Close()
		fmt.Printf("created name=%s cap=%d file=%s\n", cfg.Name, cfg.Capacity, cfg.Dir)
		return printPositions(r)
	case "mount":
		r, info, err := ring.Mount(cfg)
		if err != nil {
			return err
		}
		defer r.Close()
		fmt.Printf("mounted name=%s cap=%d recovered={abandoned:%d w:%d rd:%d}\n",
			cfg.Name, cfg.Capacity, info.Abandoned, info.WritePos, info.ReadPos)
		return printPositions(r)
	case "write":
		fs := flag.NewFlagSet("write", flag.ContinueOnError)
		msg := fs.String("m", "", "message payload")
		if err := fs.Parse(args); err != nil {
			return err
		}
		return withRing(cfg, func(r *ring.Ring) error {
			seq, err := r.Write([]byte(*msg))
			if err != nil {
				return err
			}
			fmt.Printf("wrote seq=%d len=%d\n", seq, len(*msg))
			return printPositions(r)
		})
	case "fill":
		fs := flag.NewFlagSet("fill", flag.ContinueOnError)
		n := fs.Int("n", 1, "number of records")
		p := fs.Int("p", 32, "payload bytes")
		if err := fs.Parse(args); err != nil {
			return err
		}
		return withRing(cfg, func(r *ring.Ring) error {
			payload := make([]byte, *p)
			for i := range payload {
				payload[i] = byte('a' + i%26)
			}
			for i := 0; i < *n; i++ {
				seq, err := r.Write(payload)
				if err != nil {
					fmt.Fprintf(os.Stderr, "fill: stopped at %d: %v\n", i, err)
					printPositions(r)
					return err
				}
				_ = seq
			}
			return printPositions(r)
		})
	case "read":
		return withRing(cfg, func(r *ring.Ring) error {
			seq, p, err := r.Read()
			if err != nil {
				return err
			}
			fmt.Printf("read seq=%d len=%d payload=%q\n", seq, len(p), p)
			return printPositions(r)
		})
	case "drain":
		fs := flag.NewFlagSet("drain", flag.ContinueOnError)
		n := fs.Int("n", 0, "max records (0 = until empty)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		return withRing(cfg, func(r *ring.Ring) error {
			count := 0
			for *n == 0 || count < *n {
				seq, p, err := r.Read()
				if errors.Is(err, ring.ErrBufferEmpty) {
					break
				}
				if err != nil {
					return err
				}
				fmt.Printf("read seq=%d len=%d payload=%q\n", seq, len(p), p)
				count++
			}
			fmt.Printf("drained=%d\n", count)
			return printPositions(r)
		})
	case "peek":
		return withRing(cfg, func(r *ring.Ring) error {
			seq, p, err := r.Peek()
			if err != nil {
				return err
			}
			fmt.Printf("peek seq=%d len=%d payload=%q\n", seq, len(p), p)
			return printPositions(r)
		})
	case "commit":
		return withRing(cfg, func(r *ring.Ring) error {
			if err := r.Commit(); err != nil {
				return err
			}
			fmt.Println("committed")
			return printPositions(r)
		})
	case "positions":
		return withRing(cfg, printPositions)
	case "crash":
		return crashCmd(cfg, args)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func withRing(cfg ring.Config, fn func(*ring.Ring) error) error {
	r, info, err := ring.Mount(cfg)
	if err != nil {
		return err
	}
	defer r.Close()
	if info.Abandoned != 0 {
		fmt.Printf("recover: recognized 1 abandoned in-progress record at writePos=%d (unpublished; will be overwritten)\n",
			info.WritePos)
	}
	return fn(r)
}

func printPositions(r *ring.Ring) error {
	w, rd, used := r.Positions()
	fmt.Printf("positions writePos=%d readPos=%d used=%d lastSeq=%d\n",
		w, rd, used, r.LastSeq())
	return nil
}

func fatal(code int, err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(code)
}
