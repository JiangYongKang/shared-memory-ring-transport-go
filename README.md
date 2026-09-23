# shared-memory-ring-transport-go

本机可复现的**跨进程共享内存环形缓冲传输通道**。用一个本地映射文件
（`mmap(MAP_SHARED)` + `msync`）模拟共享内存：多个 OS 进程映射同一文件，
经由内核页缓存看到同一份字节，不依赖任何外部服务、真实凭据或联网运行时。

- 包：`ring/`
- 多进程驱动/手工复现工具：`cmd/smrt`
- Go ≥ 1.25，仅使用标准库；`syscall` 部分面向 `linux/amd64`（`mmap/flock/msync`）

## 快速开始

```bash
go test ./... -race -count=1     # 全部验证（含真实多进程子进程）
go build ./...

# 手工用多个进程复现
go build -o /tmp/smrt ./cmd/smrt
D=$(mktemp -d)
/tmp/smrt -dir "$D" -name b -cap 4096 create
/tmp/smrt -dir "$D" -name b -cap 4096 write -m hello      # 另一个进程
/tmp/smrt -dir "$D" -name b -cap 4096 drain
/tmp/smrt -dir "$D" -name b -cap 4096 crash -m ghost -f 12 # 写到一半"崩溃"
/tmp/smrt -dir "$D" -name b -cap 4096 mount                # 恢复，半记录不可见
```

## 一、共享参数与版本约定

映射文件 = **64 字节共享头** + `Capacity` 字节数据区。所有多字节字段均为
8 字节对齐、小端序，可用一条对齐的原子 8 字节读写访问。

共享头（所有进程必须一致的约定）：

| 偏移 | 字段 | 说明 |
|---:|----|----|
| 0  | magic   | 固定 `"SMRTRNG1"`，标识这是一个环形缓冲文件 |
| 8  | version | 协议版本（当前 `1`） |
| 16 | capacity| 数据区字节数，必须是 8 的倍数且 ≥ 1024 |
| 24 | format  | 格式标志位（当前仅 `CRC32-IEEE`） |
| 32 | writePos| **单调不减**的逻辑写位置（也是提交点） |
| 40 | readPos | **单调不减**的逻辑读位置（消费者确认点） |
| 48 | writeSeq| 已发布数据记录的条数（即最新记录序列号） |
| 56 | 保留 | 0 |

挂载规则（`ring.Mount`）：即使是挂载，`Config.Capacity` 也必须显式给出并与
文件中一致。按以下顺序检查，冲突以**可区分**的哨兵错误拒绝，且**在任何写入之前**
完成，因此拒绝挂载绝不修改正在使用的缓冲区：

| 不一致项 | 错误 |
|---|---|
| 名称为空/过长/含路径分隔符、容量 <1024 或非 8 倍数 | `ErrInvalidParam` |
| magic 不符（文件不是本格式） | `ErrMagicConflict` |
| version 不符 | `ErrVersionConflict` |
| capacity 与文件（或文件实际大小）不符 | `ErrCapacityConflict` |
| format 标志不符 | `ErrFormatConflict` |
| 同名 `Create` 已存在 | 包装 `fs.ErrExist` |

记录格式（固定 32 字节头，8 字节对齐，载荷按 8 字节向上填充）：

| 偏移 | 字段 | 说明 |
|---:|----|----|
| 0  | start  | 该记录起始的逻辑位置（代际标签，用于识别/校验） |
| 8  | length | 载荷字节数 |
| 12 | rtype  | `0=数据`，`1=尾部填充(pad)` |
| 16 | seq    | 单调递增数据记录序列号（pad 不递增） |
| 24 | crc    | CRC-32/IEEE，覆盖头的前 24 字节与原始载荷 |

容量约束：一条记录必须在**一圈内放下**，因此最大载荷为
`MaxPayload = Capacity - 32`；超过即 `ErrRecordTooLarge`。

## 二、并发可见性与覆盖语义

逻辑位置 `writePos`/`readPos` 单调不减（不回卷存储），物理偏移 =
`pos % Capacity`。已用字节数恒为 `writePos - readPos`，且始终满足
`readPos ≤ writePos ≤ readPos + Capacity`。

- **写者互斥**：进程内 `sync.Mutex` + 跨进程 `flock(LOCK_EX)`（独立的
  `.wlock` 文件）。读不加写锁。
- **原子可见性（发布点唯一）**：`Write` 先写记录头、载荷、CRC，并 `msync`；
  **最后**用一条对齐的原子 8 字节 store 推进 `writePos`。因此读者要么完全看不到
  一条记录，要么看到的是头/载荷/校验齐全的完整记录。
- **写满**：若写入后 `writePos+记录长 - readPos > Capacity`，直接返回
  `ErrBufferFull`，且**不写任何字节、位置不移动**（先判定空间，再写尾隙）。
  写者永远不会覆盖尚未被消费（`readPos` 之前）的完整记录。
- **读空**：读到的头位置 ≥ `writePos` 即 `ErrBufferEmpty`。
- **读者不重复 / 不撕裂**：`Read` 以 `readPos` 上的 CAS 领取下一条记录，多个读者
  各自恰好领取不同记录一次；读到的记录超过 `writePos`、`start` 标签不符或 CRC
  不符都判为 `ErrCorruptRecord`，绝不会读到未完整写入或已被回绕覆盖的内容。
- **重复读取可识别**：`Peek` 不推进 `readPos`，反复 `Peek` 得到**相同的 seq 与
  载荷**，消费方可据此识别"这是重新投递的同一条，而不是新数据"；`Commit` 才是
  确认。`Read` = 领取 + 确认一步完成。
- **尾部间隙**：当一圈剩余空间放不下 32 字节头时产生间隙，规则对读写双方完全
  相同（同一 `advanceGap`）：间隙 ≥32 字节时写入一条已发布的 `pad` 记录；
  8/16 字节的极小间隙不写任何字节，由读写双方共同跳过，读者用一次 CAS 跨过。
  记录头从不跨越物理尾端。

## 三、崩溃恢复规则

进程死亡后映射文件与页缓存内容保留，`flock` 由内核在关闭锁 fd 时**自动释放**，
不会留下永久写锁。

- **写者崩溃（半条记录）**：若在推进 `writePos` 之前死亡，记录从未发布，对任何
  读者都不可见（读空）。重新 `Mount` 时 `recoverAbandoned` 的判据是：在
  `advanceGap(writePos)` 处是否存在一条**起始标签 `start` 的前 8 字节已完整写入、
  且其值恰好等于该逻辑位置**的记录头。只写了不足 8 字节（标签未成形）判为 0；
  标签一旦成形即判为 1。上一圈残留记录的逻辑位置与当前位置相差 `k*Capacity`，
  `start` 永不相等，因此不会把旧数据误判为残留；创建时数据区还会填成非零擦除
  模式 `0xEE`，避免空文件零页在位置 0 处碰巧相等。残留无需搬移，由下一个写者
  **就地覆写回收**；`writePos/readPos` 保持自洽，已确认数据不丢不改。
- **整记录已落盘但未发布**：CRC 完整也仍不可见（发布只看 `writePos`），同样被
  识别为 abandoned 并覆写。
- **读者崩溃（取走未确认）**：使用 `Peek` 后崩溃、未 `Commit`，重新挂载时该记录
  仍在且 **seq 不变**——数据不丢，且重启方能凭相同 seq 判断这是重新投递而非新数据；
  `Commit`/`Read` 一旦推进 `readPos` 即永久确认，不会再次投递。
- **位置自洽**：恢复后仍满足 `readPos ≤ writePos ≤ readPos + Capacity`；序列号
  `writeSeq` 持久化，跨多次重挂载单调递增。
- **损坏检测**：任何载荷/头部字节被破坏导致 CRC 不符，返回 `ErrCorruptRecord`，
  不会把脏数据交给消费方。

`ring.CrashWrite(payload, flushBytes)` 是公开的崩溃注入 API：写入恰好
`flushBytes` 字节后像进程死亡一样释放锁、不推进 `writePos` 且令句柄不可再写；
`cmd/smrt crash` 用它在**独立进程**中复现。

## 四、验证方法

全部用工程既有方式运行，无外部依赖：

```bash
go test ./... -race -count=1       # 含真实多进程子进程，稳定可重复
go test ./... -run TestMultiProcess -v
```

测试矩阵：

| 场景 | 测试 |
|---|---|
| 基本读写 / seq 单调 / 读空 | `TestBasicWriteRead` |
| 写满、回绕、尾隙、排空一致 | `TestFillAndWrap`、`TestFillThenRejectThenDrain`、`TestWrappingGapsPrecise` |
| 重挂载位置持久、数据不丢 | `TestRemountPersistsPositions`、`TestSeqMonotonicAcrossRemounts` |
| 容量/版本/magic/格式/参数冲突拒绝且不破坏缓冲区 | `TestMountConflicts`、`TestCreateDuplicateRejected`、`TestMultiProcessBadMount` |
| 多 goroutine 并发（恰好一次） | `TestConcurrentGoroutines` |
| **真实多进程** 4 写 2 读 | `TestMultiProcessProduceConsume` |
| 写者半记录/未发布崩溃与回收 | `TestWriterCrash*`、`TestMultiProcessCrashRecovery` |
| 读者取走未确认崩溃、重投递识别 | `TestReaderCrashBeforeAck`、`TestReaderWriterInterleavedCrash` |
| CRC 损坏检测 | `TestChecksumCorruption` |
| 最大载荷 / 零载荷边界 | `TestMaxPayloadAndTooLarge`、`TestEmptyRecord` |

运行日志会打印 `writePos/readPos/used/lastSeq` 以及恢复信息
`abandoned/w/rd` 与 CRC 校验结论，便于对可见性与完整性给出一致、可解释的断言。

## 五、smrt 退出码（可区分失败原因）

| 码 | 含义 |
|---:|---|
| 0 | 成功 |
| 2 | 缓冲满 `ErrBufferFull` |
| 3 | 缓冲空 `ErrBufferEmpty` |
| 4 | 参数/容量/版本/magic/格式冲突，或记录过大 |
| 5 | 记录损坏 `ErrCorruptRecord` |
| 1 | 其他错误 |
