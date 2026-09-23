package ring

import (
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// mappedFile is a shared writable mapping of a regular file. All processes
// mapping the same file with MAP_SHARED observe one coherent byte array;
// writes by one process become visible to the others after the issuing
// store (and any necessary cache flush). Process death does not remove the
// file or its page-cache contents, so a buffer survives crashes and can be
// mounted again.
type mappedFile struct {
	data []byte
	f    *os.File
	size int
}

// createMappedFile atomically creates a new file (O_EXCL) of the given size
// and maps it shared. It never opens or truncates an existing file.
func createMappedFile(path string, size int) (*mappedFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return mapFile(f, size)
}

// openMappedFile opens an existing mapping file and maps it shared. The
// caller validates the header afterwards; this never mutates file contents.
func openMappedFile(path string) (*mappedFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return mapFile(f, int(st.Size()))
}

func mapFile(f *os.File, size int) (*mappedFile, error) {
	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &mappedFile{data: data, f: f, size: size}, nil
}

// flush makes prior writes visible to other mappings of the same file.
// MS_SYNC drains them to the page cache (and file); MS_INVALIDATE is not
// required because sibling mappings share the same page cache.
// msync(2) flags (not exposed by the syscall package on linux/amd64).
const (
	linuxMS_SYNC   = 4
	linuxSYS_MSYNC = 26 // amd64
)

func (m *mappedFile) flush() error {
	if m == nil || m.data == nil {
		return nil
	}
	_, _, errno := syscall.Syscall(linuxSYS_MSYNC,
		uintptr(unsafe.Pointer(&m.data[0])), uintptr(len(m.data)), linuxMS_SYNC)
	if errno != 0 {
		return errno
	}
	return nil
}

func (m *mappedFile) close() error {
	var err error
	if m.data != nil {
		err = syscall.Munmap(m.data)
		m.data = nil
	}
	if cerr := m.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// fileLock is a cross-process advisory lock backed by flock(2). The lock
// file is independent of the mapping file so a rejected mount never writes
// the buffer itself. If a process dies while holding the lock, the kernel
// releases it automatically when the file descriptor is closed.
type fileLock struct {
	f     *os.File
	taken bool
	mu    sync.Mutex
}

func newFileLock(path string) (*fileLock, error) {
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	return &fileLock{f: f}, nil
}

// lock acquires the exclusive lock, blocking until available.
func (l *fileLock) lock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.taken {
		return nil
	}
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	l.taken = true
	return nil
}

// tryLock attempts the exclusive lock without blocking.
func (l *fileLock) tryLock() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.taken {
		return true
	}
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return false
	}
	l.taken = true
	return true
}

func (l *fileLock) unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.taken {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if err == nil {
		l.taken = false
	}
	return err
}

func (l *fileLock) close() error {
	_ = l.unlock()
	return l.f.Close()
}

func filepathDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i == 0 {
				return "/"
			}

			return p[:i]
		}
	}
	return "."
}

func tempDir() string { return os.TempDir() }
