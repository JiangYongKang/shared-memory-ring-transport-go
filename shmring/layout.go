package shmring

import (
	"encoding/binary"
	"hash/crc32"
	"sync/atomic"
	"unsafe"
)

// 格式与帧约定。所有多字节整数均使用小端编码，布局在不同位数/操作系统的
// Go 进程之间保持一致。
const (
	// FormatVersion 是当前库创建缓冲区时使用的共享段格式版本。
	// 版本 2 在版本 1 的基础上引入：消费模式字、消费者组领取游标、
	// 消费者槽位表与空洞帧。版本 1 的既有文件仍可挂载（仅限单消费者
	// 用法），其布局与行为与旧库完全一致。
	FormatVersion uint32 = 2
	// formatVersionV1 是上一版格式。
	formatVersionV1 uint32 = 1

	// HeaderMagic 内存字节为 "SHRM"，用于识别映射文件。
	HeaderMagic uint32 = 0x4D524853
	// InitMagic 写入头部初始化完成标记。
	InitMagic uint32 = 0x54494E49 // 内存字节 "INIT"
	// FrameMagic 内存字节 "SHRF"，标记一帧的起始。
	FrameMagic uint32 = 0x46524853
	// CommitMagic 内存字节 "CMIT"，位于帧尾，表示帧已完整发布。
	CommitMagic uint32 = 0x54494E43
	// GapMagic 内存字节 "SHRG"，标记一帧“空洞”：崩溃写者未完成的
	// 那一次分配被在线自愈改写为空洞帧，空间永不回收重用。
	GapMagic uint32 = 0x47524853

	// headerSizeV1 是版本 1 的文件头大小。
	headerSizeV1 = 128
	// headerSize 是当前版本（v2）的文件头大小：
	// 128 字节基础字段 + 16 个消费者槽位 × 32 字节。
	headerSize = 640

	// FrameHeaderSize 帧头：magic(u32) + seq(u64) + payloadLen(u32)。
	FrameHeaderSize = 16
	// FrameTrailerSize 帧尾：crc32(u32) + commitMagic(u32)。
	FrameTrailerSize = 8

	// MinCapacity 数据区允许的最小容量。
	MinCapacity = 64

	frameOverhead = FrameHeaderSize + FrameTrailerSize

	// 文件头各字段偏移（均保持 8 字节对齐以支持原子 64 位访问）。
	offMagic       = 0
	offVersion     = 4
	offHeaderSize  = 8
	offCapacity    = 16
	offReservedPos = 24
	offWritePos    = 32
	offReadPos     = 40
	offNextSeq     = 48
	offInitFlag    = 56
	// v2 新增：
	offMode     = 64  // 消费模式：0 未定 / 1 单消费者 / 2 消费者组
	offClaimPos = 72  // 消费者组共享领取游标
	offSlots    = 128 // 消费者槽位表起点

	// 消费模式取值。
	modeUndecided = 0
	modeLegacy    = 1
	modeGroup     = 2

	// MaxConsumers 是消费者槽位数量（同时存活的消费者上限）。
	MaxConsumers = 16
	// slotStride 是每个消费者槽位的字节数：
	// owner(u64) + heartbeat(u64) + pendingPos(u64) + pendingSeq(u64)。
	slotStride       = 32
	slotOffOwner     = 0
	slotOffHeartbeat = 8
	slotOffPending   = 16
	slotOffSeq       = 24

	// holdNone 表示槽位当前没有持有未确认的记录。
	holdNone = ^uint64(0)
)

// loadU64 / storeU64 在映射内存上做 64 位原子读写。
//
// 原子操作提供进程内与同机进程间（映射同一缓存行）的 release/acquire
// 可见性：写者 store 发布游标之前的所有帧字节写入，对之后 load 到该游标的
// 读者均可见。调用方保证 off 为 8 的倍数。
func loadU64(b []byte, off int) uint64 {
	p := unsafe.Pointer(&b[off])
	return (*atomic.Uint64)(p).Load()
}

func storeU64(b []byte, off int, v uint64) {
	p := unsafe.Pointer(&b[off])
	(*atomic.Uint64)(p).Store(v)
}

func loadU32(b []byte, off int) uint32 {
	p := unsafe.Pointer(&b[off])
	return (*atomic.Uint32)(p).Load()
}

func storeU32(b []byte, off int, v uint32) {
	p := unsafe.Pointer(&b[off])
	(*atomic.Uint32)(p).Store(v)
}

// ringWriteAt 将 data 写入逻辑位置 logicalPos，自动处理容量回绕，
// 支持一次写入跨过物理末尾。
func ringWriteAt(dst []byte, logicalPos uint64, data []byte) {
	capn := uint64(len(dst))
	start := logicalPos % capn
	n := uint64(len(data))
	first := capn - start
	if n <= first {
		copy(dst[start:start+n], data)
		return
	}
	copy(dst[start:], data[:first])
	copy(dst[:n-first], data[first:])
}

// ringReadAt 从逻辑位置 logicalPos 读取 n 字节到 dst，自动处理回绕。
func ringReadAt(src []byte, logicalPos uint64, dst []byte) {
	capn := uint64(len(src))
	start := logicalPos % capn
	n := uint64(len(dst))
	first := capn - start
	if n <= first {
		copy(dst, src[start:start+n])
		return
	}
	copy(dst[:first], src[start:])
	copy(dst[first:], src[:n-first])
}

// putFrameHeader 将帧头编码到 16 字节缓冲。
func putFrameHeader(b []byte, seq uint64, payloadLen uint32) {
	binary.LittleEndian.PutUint32(b[0:4], FrameMagic)
	binary.LittleEndian.PutUint64(b[4:12], seq)
	binary.LittleEndian.PutUint32(b[12:16], payloadLen)
}

func frameSeq(b []byte) uint64 { return binary.LittleEndian.Uint64(b[4:12]) }

func frameLen(b []byte) uint32 { return binary.LittleEndian.Uint32(b[12:16]) }

// frameCRC 计算帧内容的校验依据：序列号与负载拼接后的 IEEE CRC32。
// 序列号纳入 CRC，使旧帧残片与新帧恰好同长度时也无法伪造提交。
func frameCRC(seq uint64, payload []byte) uint32 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], seq)
	h := crc32.NewIEEE()
	h.Write(buf[:])
	h.Write(payload)
	return h.Sum32()
}

// putTrailer 将帧尾编码到 8 字节缓冲。
func putTrailer(b []byte, crc uint32) {
	binary.LittleEndian.PutUint32(b[0:4], crc)
	binary.LittleEndian.PutUint32(b[4:8], CommitMagic)
}

// frameTotal 返回一帧占用的逻辑字节数。
func frameTotal(payloadLen int) uint64 {
	return uint64(FrameHeaderSize + payloadLen + FrameTrailerSize)
}

// casU64 在映射内存上做 64 位 compare-and-swap（跨进程原子）。
func casU64(b []byte, off int, old, new uint64) bool {
	p := unsafe.Pointer(&b[off])
	return (*atomic.Uint64)(p).CompareAndSwap(old, new)
}

// casU32 在映射内存上做 32 位 compare-and-swap。
func casU32(b []byte, off int, old, new uint32) bool {
	p := unsafe.Pointer(&b[off])
	return (*atomic.Uint32)(p).CompareAndSwap(old, new)
}

// slotOff 返回第 i 个消费者槽位内 field 字段在文件头的绝对偏移。
func slotOff(i, field int) int { return offSlots + i*slotStride + field }

// putGapHeader 将空洞帧头编码到 16 字节缓冲。空洞帧的 seq 固定为 0，
// payloadLen 字段承载空洞总长（含帧头帧尾）减去帧开销后的值。
func putGapHeader(b []byte, payloadLen uint32) {
	binary.LittleEndian.PutUint32(b[0:4], GapMagic)
	binary.LittleEndian.PutUint64(b[4:12], 0)
	binary.LittleEndian.PutUint32(b[12:16], payloadLen)
}

// putGapTrailer 编码空洞帧尾。空洞帧不带有效 CRC（内容是崩溃残片），
// 仅保留提交标记，供读者确认“这是一个完整的空洞，可以跳过”。
func putGapTrailer(b []byte) {
	binary.LittleEndian.PutUint32(b[0:4], 0)
	binary.LittleEndian.PutUint32(b[4:8], CommitMagic)
}
