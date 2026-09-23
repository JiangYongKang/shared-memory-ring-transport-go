package main

import (
	"flag"
	"fmt"

	"github.com/highcumontoa/shared-memory-ring-transport-go/ring"
)

// crashCmd mounts the buffer then calls CrashWrite: the process prints the
// pre-crash writePos and exits WITHOUT advancing it, leaving a half record.
func crashCmd(cfg ring.Config, args []string) error {
	fs := flag.NewFlagSet("crash", flag.ContinueOnError)
	msg := fs.String("m", "crashed", "would-be payload")
	flush := fs.Int("f", 16, "bytes flushed before death")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, info, err := ring.Mount(cfg)
	if err != nil {
		return err
	}
	w0, rd0, used0 := r.Positions()
	fmt.Printf("crash: start writePos=%d readPos=%d used=%d recoveredAbandoned=%d\n",
		w0, rd0, used0, info.Abandoned)
	if err := r.CrashWrite([]byte(*msg), *flush); err != nil {
		return err
	}
	fmt.Printf("crash: wrote %d/%d bytes then DIED; writePos left at %d (unpublished)\n",
		*flush, recLen(len(*msg)), w0)
	// Do not close via r.Close (it would flush/use the killed lock). This
	// mirrors a hard process exit; the OS reclaims the mapping and lock.
	return nil
}

func recLen(payload int) int { return 32 + (payload+7)&^7 }
