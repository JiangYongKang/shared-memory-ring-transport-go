// Package shmring 实现基于本地内存映射文件的跨进程共享内存环形缓冲传输通道。
//
// 通道使用一个本地文件经 mmap 映射到各进程的地址空间，模拟共享内存。
// 多个进程通过同一名称挂载同一缓冲区，以“预留/发布/确认”协议传递
// 完整记录：
//
//   - 多写者：写者先预留（reserve）逻辑空间，再发布“带提交标记”的
//     完整帧；跨进程写者由写锁串行化；
//   - 多消费者：消费者组通过共享领取游标竞争领取，每条记录恰好被一个
//     消费者领取一次，重复投递按序列号识别；也兼容单消费者游标用法；
//   - 写者崩溃残留的半条记录由存活写者在线固化为空洞帧（空间不回收
//     重用），消费者永远不会消费未提交内容，也无需重挂载；
//   - 消费者崩溃只会使其未确认的记录被按相同序列号重投，
//     已确认数据不会再次出现。
package shmring

import "errors"

// 挂载时可区分的冲突类型。
type ConflictKind string

const (
	// ConflictMagic 映射文件不是本格式（魔数不一致或已损坏）。
	ConflictMagic ConflictKind = "magic mismatch"
	// ConflictVersion 格式版本不一致。
	ConflictVersion ConflictKind = "version mismatch"
	// ConflictCapacity 数据区容量与创建时不一致。
	ConflictCapacity ConflictKind = "capacity mismatch"
	// ConflictParams 帧格式等其它约定参数不一致。
	ConflictParams ConflictKind = "parameter mismatch"
)

// ConflictError 表示一次挂载因约定不一致而被拒绝。
//
// 拒绝挂载是只读校验：不会修改映射文件头部，也不会影响正在使用缓冲区
// 的其它进程。可通过 errors.Is 区分具体原因。
type ConflictError struct {
	Kind   ConflictKind
	Detail string
}

func (e *ConflictError) Error() string {
	return "shmring: mount rejected: " + string(e.Kind) + ": " + e.Detail
}

// 可被 errors.Is 识别的哨兵错误。
var (
	// ErrMagicMismatch 目标文件不是本格式的共享缓冲（或头部已损坏）。
	ErrMagicMismatch = errors.New("shmring: bad magic (not a shmring file)")
	// ErrVersionMismatch 格式版本与当前库不兼容。
	ErrVersionMismatch = errors.New("shmring: incompatible format version")
	// ErrCapacityMismatch 挂载参数中的容量与缓冲区创建时不同。
	ErrCapacityMismatch = errors.New("shmring: capacity does not match the existing buffer")
	// ErrParamMismatch 帧格式等其它约定参数不一致。
	ErrParamMismatch = errors.New("shmring: framing parameters do not match the existing buffer")

	// ErrCapacityInvalid 容量不合法（为 0、非块大小对齐或过小）。
	ErrCapacityInvalid = errors.New("shmring: invalid capacity")
	// ErrNotFound 按名称找不到已创建的缓冲区。
	ErrNotFound = errors.New("shmring: buffer not found")
	// ErrNameInvalid 名称非法（空、含路径分隔符等）。
	ErrNameInvalid = errors.New("shmring: invalid buffer name")
	// ErrFull 缓冲区剩余空间不足以容纳本条记录（含帧头/帧尾），
	// 且不能覆盖尚未被消费的完整记录。
	ErrFull = errors.New("shmring: buffer full")
	// ErrEmpty 当前没有已提交、未确认的完整记录；正在写入中的半条记录
	// 对读者同样表现为空。
	ErrEmpty = errors.New("shmring: no committed record")
	// ErrPayloadTooLarge 单条记录负载超过容量允许的上限。
	ErrPayloadTooLarge = errors.New("shmring: payload larger than capacity allows")
	// ErrClosed 环形缓冲已关闭。
	ErrClosed = errors.New("shmring: ring closed")
	// ErrCorrupt 已提交区域内出现无法解释的结构损坏（正常崩溃恢复不会产生）。
	ErrCorrupt = errors.New("shmring: corrupt committed region")
	// ErrModeConflict 缓冲区已被固定为另一种消费模式：单消费者游标
	// （Read/Peek/Advance）与消费者组（NewConsumer）不能混用。
	ErrModeConflict = errors.New("shmring: buffer is pinned to the other consumption mode")
	// ErrLegacyBuffer 缓冲区是版本 1 的旧格式文件：老的单消费者用法
	// 完全可用，但消费者组等 v2 能力被明确拒绝。
	ErrLegacyBuffer = errors.New("shmring: v1 format buffer does not support consumer groups")
	// ErrTooManyConsumers 消费者槽位已满（MaxConsumers）。
	ErrTooManyConsumers = errors.New("shmring: too many consumers")
	// ErrNoHold 当前消费者没有持有待确认的记录。
	ErrNoHold = errors.New("shmring: no held record to confirm")
)

func (e *ConflictError) Is(target error) bool {
	switch target {
	case ErrMagicMismatch:
		return e.Kind == ConflictMagic
	case ErrVersionMismatch:
		return e.Kind == ConflictVersion
	case ErrCapacityMismatch:
		return e.Kind == ConflictCapacity
	case ErrParamMismatch:
		return e.Kind == ConflictParams
	default:
		return false
	}
}
