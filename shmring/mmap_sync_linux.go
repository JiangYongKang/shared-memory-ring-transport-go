//go:build linux

package shmring

import (
	"syscall"
	"unsafe"
)

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

// fOfdSetlk 是 Linux 的 F_OFD_SETLK 命令号（标准库 syscall 未导出）。
const fOfdSetlk = 37

// ofdTryLockByte 尝试对 fd 指向文件的第 off 个字节加 OFD 写锁（非阻塞）。
//
// OFD 锁（F_OFD_SETLK，Linux 3.15+）绑定在“打开文件描述”而非进程上：
//   - 持有进程死亡（含 SIGKILL）时内核自动释放；
//   - 同一进程打开多个 fd 互不影响（不像 POSIX fcntl 锁那样被同进程
//     其它 fd 的 close 误释放），因此同进程多消费者各自持锁是安全的。
//
// 返回 (locked, err)。
func ofdTryLockByte(fd uintptr, off int64) (bool, error) {
	lk := &syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0, // SEEK_SET
		Start:  off,
		Len:    1,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd,
		uintptr(fOfdSetlk), uintptr(unsafe.Pointer(lk)))
	switch errno {
	case 0:
		return true, nil
	case syscall.EAGAIN, syscall.EACCES:
		return false, nil
	default:
		return false, errno
	}
}

// ofdUnlockByte 解除 fd 上第 off 个字节的 OFD 锁。
func ofdUnlockByte(fd uintptr, off int64) error {
	lk := &syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  off,
		Len:    1,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd,
		uintptr(fOfdSetlk), uintptr(unsafe.Pointer(lk)))
	if errno != 0 {
		return errno
	}
	return nil
}

// ofdProbeDead 探测第 off 字节的 OFD 锁是否无人持有（即槽位属主已死亡）。
// 探测成功时会短暂持锁，调用方须自行解锁。
func ofdProbeDead(fd uintptr, off int64) (dead bool, err error) {
	return ofdTryLockByte(fd, off)
}
