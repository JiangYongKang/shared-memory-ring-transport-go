//go:build linux

package shmring

import "syscall"

// msync 直接调用 Linux msync(2)。标准库 syscall 未提供包装。
func msync(b []byte, flags int) error {
	// 内核要求地址按页对齐；syscall.Mmap 返回的映射基址本身页对齐，
	// 而 r.data 正是映射基址。
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafeSlicePtr(b)), uintptr(len(b)), uintptr(flags))
	if errno != 0 {
		return errno
	}
	return nil
}
