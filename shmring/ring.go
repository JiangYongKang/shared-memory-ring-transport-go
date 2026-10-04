package shmring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// Config 描述创建共享环形缓冲时的约定参数。挂载其它进程创建的缓冲区时，
// 容量（Capacity）、头部布局（HeaderSize）与格式版本（FormatVersion）
// 必须与创建时完全一致，否则挂载被拒绝（见 ConflictError）。
type Config struct {
	// Name 是缓冲区名称，只允许字母、数字、下划线、短横与点，
	// 不得包含路径分隔符。不同名称对应不同的映射文件。
	Name string
	// Capacity 是数据区容量（字节，不含 128 字节文件头）。
	// 必须 >= MinCapacity 且为 8 的倍数。
	Capacity int
}

// Options 控制挂载行为。
type Options struct {
	// Dir 存放映射文件与锁文件的目录，默认使用系统临时目录下的 shmring。
	Dir string

	// disableAutoRecover 仅供包内测试使用：挂载后不立即回收崩溃残片，
	// 由调用方显式调用 Recover，以便观测回收前后的游标差异。
	disableAutoRecover bool
}

// Ring 是一个已挂载的共享环形缓冲。
//
// Ring 可被同一进程内的多个 goroutine 并发使用；跨进程的写者之间通过
// 写锁串行化，单条记录的写入是原子发布单位。
type Ring struct {
	mu sync.Mutex

	file  *os.File
	mlock *os.File // 存活挂载期间持有共享锁；Destroy 借它探测是否仍有挂载
	wlock *os.File // 写锁文件描述符，仅在写入/恢复时短暂加独占锁

	data     []byte // 整个映射：文件头 + 数据区
	header   []byte
	ring     []byte // 数据区，长度 == capacity
	capacity uint64
	hdrSize  int  // 本文件实际的头部大小（v1=128，v2=640）
	legacyV1 bool // true 表示挂载的是版本 1 旧格式文件：仅限单消费者用法

	name string
	dir  string
	path string

	consMu sync.Mutex
	cons   []*Consumer // 本进程内经由此 Ring 注册的存活消费者

	closed atomic.Bool
}

const defaultDirName = "shmring"

func resolveDir(opts *Options) string {
	if opts != nil && opts.Dir != "" {
		return opts.Dir
	}
	return filepath.Join(os.TempDir(), defaultDirName)
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty name", ErrNameInvalid)
	}
	if name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("%w: %q", ErrNameInvalid, name)
	}
	return nil
}

func validateCapacity(capacity int) error {
	if capacity < MinCapacity || capacity%8 != 0 {
		return fmt.Errorf("%w: capacity=%d (need >= %d and multiple of 8)",
			ErrCapacityInvalid, capacity, MinCapacity)
	}
	return nil
}

func (cfg Config) paths(dir string) (dat, mlock, clock, wlock string) {
	base := filepath.Join(dir, "shmring-"+cfg.Name)
	return base + ".dat", base + ".mlock", base + ".clock", base + ".wlock"
}

// flockNb 尝试非阻塞加锁，返回 (locked, err)。
func flockNb(f *os.File, how int) (bool, error) {
	err := syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	}
	return false, err
}

// mountLive 阶段一：获取“存活挂载”共享锁。它与 Destroy 的排他探测互斥，
// 与其它挂载（含同进程的其它 Ring）共享兼容，因此可安全持有至 Close。
func acquireLiveLock(mlockPath string) (*os.File, error) {
	f, err := os.OpenFile(mlockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// mountWindow 阶段二：在存活锁保护下短暂排他，串行化“创建/校验”窗口，
// 防止两个挂载者同时初始化或与文件删除竞争。返回的释放函数会解锁并关闭。
func mountWindow(clockPath string) (release func(), err error) {
	f, err := os.OpenFile(clockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// Create 创建（或在不存在时初始化）一个共享环形缓冲。
//
// 若同名缓冲区已存在，则按等价于 Open 的方式挂载；此时容量必须与创建时
// 一致，否则返回可通过 errors.Is(err, ErrCapacityMismatch) 等识别的
// ConflictError，且不会改动既有缓冲区（校验为只读，失败时直接解除挂载）。
func Create(cfg Config, opts *Options) (*Ring, error) {
	if err := validateName(cfg.Name); err != nil {
		return nil, err
	}
	if err := validateCapacity(cfg.Capacity); err != nil {
		return nil, err
	}
	dir := resolveDir(opts)
	skipRecovery := opts != nil && opts.disableAutoRecover
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	datPath, mlockPath, clockPath, wlockPath := cfg.paths(dir)

	mlock, err := acquireLiveLock(mlockPath)
	if err != nil {
		return nil, err
	}
	releaseWindow, err := mountWindow(clockPath)
	if err != nil {
		mlock.Close()
		return nil, err
	}

	created := false
	f, err := os.OpenFile(datPath, os.O_RDWR, 0o644)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			releaseWindow()
			mlock.Close()
			return nil, err
		}
		size := int64(headerSize + cfg.Capacity)
		f, err = os.OpenFile(datPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			releaseWindow()
			mlock.Close()
			return nil, err
		}
		if err := f.Truncate(size); err != nil {
			f.Close()
			releaseWindow()
			mlock.Close()
			return nil, err
		}
		created = true
	}

	r := &Ring{file: f, mlock: mlock, name: cfg.Name, dir: dir, path: datPath,
		capacity: uint64(cfg.Capacity)}

	if created {
		if err := r.mapFile(); err != nil {
			r.file.Close()
			releaseWindow()
			r.mlock.Close()
			return nil, err
		}
		r.initHeader(cfg.Capacity)
		if err := r.flush(); err != nil {
			r.cleanupOnError()
			releaseWindow()
			return nil, err
		}
	} else {
		if err := r.mapFile(); err != nil {
			f.Close()
			releaseWindow()
			mlock.Close()
			return nil, err
		}
		if err := r.validateHeader(cfg.Capacity); err != nil {
			// 只读校验失败：不写一字节，直接解除挂载。
			r.munmap()
			f.Close()
			releaseWindow()
			mlock.Close()
			return nil, err
		}
	}

	if err := r.openWriterLock(wlockPath); err != nil {
		r.cleanupOnError()
		releaseWindow()
		return nil, err
	}
	if !skipRecovery {
		if _, _, err := r.Recover(); err != nil {
			r.cleanupOnError()
			releaseWindow()
			return nil, err
		}
	}
	if err := r.flush(); err != nil {
		r.cleanupOnError()
		releaseWindow()
		return nil, err
	}
	releaseWindow()
	return r, nil
}

// Open 按名称挂载一个已存在的共享环形缓冲。
//
// 若挂载点上存在崩溃写者残留的半条记录，且当前没有存活写者（写锁空闲），
// Open 会在发布任何数据之前完成回收。头部约定不一致时返回 ConflictError，
// 校验过程不修改文件。
func Open(name string, opts *Options) (*Ring, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	dir := resolveDir(opts)
	skipRecovery := opts != nil && opts.disableAutoRecover
	cfg := Config{Name: name}
	datPath, mlockPath, clockPath, wlockPath := cfg.paths(dir)

	mlock, err := acquireLiveLock(mlockPath)
	if err != nil {
		return nil, err
	}
	releaseWindow, err := mountWindow(clockPath)
	if err != nil {
		mlock.Close()
		return nil, err
	}

	f, err := os.OpenFile(datPath, os.O_RDWR, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		releaseWindow()
		mlock.Close()
		return nil, err
	}

	r := &Ring{file: f, mlock: mlock, name: name, dir: dir, path: datPath}
	if err := r.mapFile(); err != nil {
		f.Close()
		releaseWindow()
		mlock.Close()
		return nil, err
	}
	if err := r.validateHeader(-1); err != nil {
		r.munmap()
		f.Close()
		releaseWindow()
		mlock.Close()
		return nil, err
	}
	if err := r.openWriterLock(wlockPath); err != nil {
		r.cleanupOnError()
		releaseWindow()
		return nil, err
	}
	if !skipRecovery {
		if _, _, err := r.Recover(); err != nil {
			r.cleanupOnError()
			releaseWindow()
			return nil, err
		}
	}
	if err := r.flush(); err != nil {
		r.cleanupOnError()
		releaseWindow()
		return nil, err
	}
	releaseWindow()
	return r, nil
}

func (r *Ring) mapFile() error {
	fi, err := r.file.Stat()
	if err != nil {
		return err
	}
	size := int(fi.Size())
	if size < headerSizeV1 {
		return fmt.Errorf("%w: file too short (%d bytes)", ErrMagicMismatch, size)
	}
	data, err := syscall.Mmap(int(r.file.Fd()), 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	r.data = data
	// 头部大小取决于格式版本：先按 v1 最小头读出版本与头大小字段，
	// 再决定数据区起点。字段合法性由 validateHeader 最终把关。
	hs := int(binary.LittleEndian.Uint32(data[offHeaderSize : offHeaderSize+4]))
	switch hs {
	case headerSizeV1, headerSize:
		r.hdrSize = hs
	default:
		// 无法识别的头大小：先按当前版本切片，validateHeader 会拒绝。
		r.hdrSize = headerSize
	}
	if size < r.hdrSize {
		r.hdrSize = headerSizeV1
	}
	r.header = data[:r.hdrSize]
	r.ring = data[r.hdrSize:]
	r.capacity = uint64(size - r.hdrSize)
	return nil
}

func (r *Ring) munmap() {
	if r.data != nil {
		_ = syscall.Munmap(r.data)
		r.data = nil
		r.header = nil
		r.ring = nil
	}
}

func (r *Ring) cleanupOnError() {
	if r.wlock != nil {
		r.wlock.Close()
	}
	r.munmap()
	if r.file != nil {
		r.file.Close()
	}
	if r.mlock != nil {
		r.mlock.Close()
	}
}

func (r *Ring) openWriterLock(wlockPath string) error {
	f, err := os.OpenFile(wlockPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	r.wlock = f
	return nil
}

func (r *Ring) initHeader(capacity int) {
	storeU32(r.header, offMagic, HeaderMagic)
	storeU32(r.header, offVersion, FormatVersion)
	storeU32(r.header, offHeaderSize, uint32(headerSize))
	storeU64(r.header, offCapacity, uint64(capacity))
	storeU64(r.header, offReservedPos, 0)
	storeU64(r.header, offWritePos, 0)
	storeU64(r.header, offReadPos, 0)
	storeU64(r.header, offNextSeq, 1)
	// v2：消费模式未定，领取游标从 0 开始，所有消费者槽位空闲。
	storeU32(r.header, offMode, modeUndecided)
	storeU64(r.header, offClaimPos, 0)
	for i := 0; i < MaxConsumers; i++ {
		storeU64(r.header, slotOff(i, slotOffOwner), 0)
		storeU64(r.header, slotOff(i, slotOffHeartbeat), 0)
		storeU64(r.header, slotOff(i, slotOffPending), holdNone)
		storeU64(r.header, slotOff(i, slotOffSeq), 0)
	}
	// initFlag 最后写入：挂载者只有看到 INIT 才认为头部初始化完成。
	storeU32(r.header, offInitFlag, InitMagic)
}

func conflict(kind ConflictKind, detail string) error {
	return &ConflictError{Kind: kind, Detail: detail}
}

// validateHeader 只读取已映射头部并核对约定。expectedCapacity < 0 表示
// Open 场景：以文件中记录的容量为准，但仍校验其合法性。
//
// 版本 1 的既有文件被接受并标记为 legacyV1：布局与行为和旧库完全一致，
// 仅限单消费者用法；版本 2 文件支持全部能力。其它版本一律拒绝（只读）。
func (r *Ring) validateHeader(expectedCapacity int) error {
	h := r.header
	if len(h) < headerSizeV1 {
		return conflict(ConflictMagic, fmt.Sprintf("file shorter than header: %d bytes", len(h)))
	}
	// 初始化尚未完成（创建者在 initFlag 之前崩溃）。
	if loadU32(h, offMagic) != HeaderMagic {
		return conflict(ConflictMagic, "magic not found; file is not a shmring buffer or was never fully initialized")
	}
	if loadU32(h, offInitFlag) != InitMagic {
		return conflict(ConflictMagic, "initialization marker missing; buffer creation did not complete")
	}
	version := loadU32(h, offVersion)
	switch version {
	case formatVersionV1:
		if got := int(loadU32(h, offHeaderSize)); got != headerSizeV1 {
			return conflict(ConflictParams, fmt.Sprintf("v1 header size=%d, expect %d", got, headerSizeV1))
		}
		r.legacyV1 = true
	case FormatVersion:
		if got := int(loadU32(h, offHeaderSize)); got != headerSize {
			return conflict(ConflictParams, fmt.Sprintf("header size=%d, library expects %d", got, headerSize))
		}
	default:
		return conflict(ConflictVersion, fmt.Sprintf("file version=%d, library versions=%d/%d", version, formatVersionV1, FormatVersion))
	}
	cap := int(loadU64(h, offCapacity))
	if cap != int(r.capacity) {
		return conflict(ConflictParams, fmt.Sprintf("header capacity=%d but mapping data area=%d", cap, r.capacity))
	}
	if cap < MinCapacity || cap%8 != 0 {
		return fmt.Errorf("%w: stored capacity=%d", ErrCapacityInvalid, cap)
	}
	if expectedCapacity >= 0 && cap != expectedCapacity {
		return conflict(ConflictCapacity, fmt.Sprintf("requested capacity=%d, existing capacity=%d", expectedCapacity, cap))
	}
	return nil
}

// flush 将映射内容回写文件（msync）。崩溃后已 msync 的字节即为恢复依据。
func (r *Ring) flush() error {
	if r.data == nil {
		return ErrClosed
	}
	return msync(r.data, syscall.MS_SYNC)
}

// Name 返回缓冲区名称。
func (r *Ring) Name() string { return r.name }

// Capacity 返回数据区容量（字节）。
func (r *Ring) Capacity() int { return int(r.capacity) }

// MaxPayload 返回单条记录允许的最大负载长度。
func (r *Ring) MaxPayload() int { return int(r.capacity) - frameOverhead }

// Positions 返回当前的预留位置、写位置与读位置（单调递增的逻辑字节数），
// 用于运行日志与测试断言。
func (r *Ring) Positions() (reservedPos, writePos, readPos uint64) {
	return loadU64(r.header, offReservedPos),
		loadU64(r.header, offWritePos),
		loadU64(r.header, offReadPos)
}

// NextSeq 返回下一条记录将分配的序列号。
func (r *Ring) NextSeq() uint64 { return loadU64(r.header, offNextSeq) }

// ClaimPos 返回消费者组模式下共享领取游标的当前值（单消费者模式下无意义）。
func (r *Ring) ClaimPos() uint64 { return loadU64(r.header, offClaimPos) }

// Close 解除挂载并释放本进程持有的锁；不删除映射文件，其它进程与后续
// 挂载看到的数据不受影响。经由本 Ring 注册的消费者会被一并注销。
func (r *Ring) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	r.consMu.Lock()
	cons := r.cons
	r.cons = nil
	r.consMu.Unlock()
	for _, c := range cons {
		_ = c.Close()
	}
	var err error
	if r.wlock != nil {
		if e := r.wlock.Close(); e != nil {
			err = e
		}
	}
	r.munmap()
	if r.file != nil {
		if e := r.file.Close(); e != nil && err == nil {
			err = e
		}
	}
	if r.mlock != nil {
		// 关闭描述符即释放 flock。
		if e := r.mlock.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Destroy 在没有任何进程挂载时删除映射文件与锁文件。
// 其它进程（或本进程的其它 Ring）仍挂载时返回错误且不删除任何文件。
func (r *Ring) Destroy() error {
	if r.closed.Load() {
		return ErrClosed
	}
	datPath, mlockPath, clockPath, wlockPath := Config{Name: r.name}.paths(r.dir)

	// 先释放本 Ring 自己的存活共享锁，再用全新描述符非阻塞探测：
	// 若仍有任何存活挂载（含正处于挂载窗口者）持有共享锁，立即失败。
	if err := r.mlock.Close(); err != nil {
		return err
	}
	r.mlock = nil

	probe, err := os.OpenFile(mlockPath, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	locked, err := flockNb(probe, syscall.LOCK_EX)
	if err != nil {
		probe.Close()
		return err
	}
	if !locked {
		probe.Close()
		return fmt.Errorf("shmring: cannot destroy %q while other processes have it mounted", r.name)
	}
	// 此刻任何新挂载都无法获取存活共享锁；等待可能残留的挂载窗口结束。
	releaseWindow, err := mountWindow(clockPath)
	if err != nil {
		probe.Close()
		return err
	}

	// 持锁删除所有文件。已打开的映射在各进程 munmap/关闭前仍然有效，
	// 但新进程已无法再按名称挂载到它们。
	rmErr := os.Remove(datPath)
	_ = os.Remove(wlockPath)
	_ = os.Remove(clockPath)
	_ = os.Remove(mlockPath)

	if r.wlock != nil {
		_ = r.wlock.Close()
		r.wlock = nil
	}
	r.munmap()
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
	releaseWindow()
	_ = probe.Close()
	r.closed.Store(true)
	return rmErr
}
