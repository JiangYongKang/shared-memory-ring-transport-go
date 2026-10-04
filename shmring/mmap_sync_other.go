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

// 非 Linux 平台无 OFD 锁。退化策略：注册照常成功（返回 true），
// 但死亡探测永远报告“存活”（返回 false），即不做崩溃消费者回收——
// 与 msync 空操作一致，崩溃恢复语义仅在 Linux 上完整保证。
func ofdTryLockByte(fd uintptr, off int64) (bool, error) {
	_ = fd
	_ = off
	return true, nil
}

func ofdUnlockByte(fd uintptr, off int64) error {
	_ = fd
	_ = off
	return nil
}

func ofdProbeDead(fd uintptr, off int64) (bool, error) {
	_ = fd
	_ = off
	return false, nil
}

var _ = syscall.MS_SYNC
