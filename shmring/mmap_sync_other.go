//go:build !linux

package shmring

import "syscall"

func msync(b []byte, flags int) error {
	// 非 Linux 平台：标准库无统一 msync 包装。映射为 MAP_SHARED 时
	// 页面回收/关闭仍会写回；测试目标平台为 Linux。
	_ = b
	_ = flags
	return nil
}

var _ = syscall.MS_SYNC
