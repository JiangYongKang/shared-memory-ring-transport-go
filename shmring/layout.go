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
	// FormatVersion 是当前库使用的共享段格式版本。
	FormatVersion uint32 = 1

	// HeaderMagic 内存字节为 "SHRM"，用于识别映射文件。
	HeaderMagic uint32 = 0x4D524853
	// InitMagic 写入头部初始化完成标记。
	InitMagic uint32 = 0x54494E49 // 内存字节 "INIT"
	// FrameMagic 内存字节 "SHRF"，标记一帧的起始。
	FrameMagic uint32 = 0x46524853
	// CommitMagic 内存字节 "CMIT"，位于帧尾，表示帧已完整发布。
	CommitMagic uint32 = 0x54494E43

	headerSize = 128

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
