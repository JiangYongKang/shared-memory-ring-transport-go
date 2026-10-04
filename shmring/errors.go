// Package shmring 实现基于本地内存映射文件的跨进程共享内存环形缓冲传输通道。
//
// 通道使用一个本地文件经 mmap 映射到各进程的地址空间，模拟共享内存。
// 多个进程通过同一名称挂载同一缓冲区，以“预留/发布 + 竞争领取/确认”
// 协议传递完整记录：
//
//   - 写者先预留（reserve）逻辑空间，再发布“带提交标记”的完整帧；
//     多写者经写锁串行化，单条记录的发布是原子单位；
//   - 多消费者（多进程或同进程多句柄）经 CAS 竞争领取记录，每条完整
//     记录恰好被一个消费者领取一次；各消费者持有独立 ack 游标，彼此
//     不阻塞；重复投递可按记录序列号识别；
//   - 写者写到一半崩溃时，下一个拿到写锁的写者（或挂载者）在线自愈：
//     残片登记为撕裂区、写游标跳过它继续工作，无需重新挂载、无需人工
//     清理；那次未完成的分配永不回收重用，且始终可观测（TornCount）；
//   - 写者回绕复用空间以“所有存活消费者 ack 游标的最小值”为界，绝不
//     覆盖任何消费者未确认的记录；最慢的消费者拖住空间时写者得到明确
//     的 ErrFull；死亡消费者的槽位被自动回收，不会永久卡住写者；
//   - 旧式单消费者 API（Ring.Read/Peek/Advance）语义不变，确认前崩溃
//     只会导致该记录按原序列号再次投递。
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
	// ErrTooManyConsumers 存活消费者槽位已耗尽（MaxConsumers）。
	ErrTooManyConsumers = errors.New("shmring: too many live consumers")
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
