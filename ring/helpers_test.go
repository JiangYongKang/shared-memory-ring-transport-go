package ring

import "time"

func yieldMs(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }
