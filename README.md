# shmring — 跨进程共享内存环形缓冲传输通道

用一个**本地内存映射文件**（`mmap(MAP_SHARED)`）模拟共享内存，为多个本机
进程提供“完整记录”的并发读写通道。不依赖外部服务、真实凭据或需要联网获取
的运行时，所有能力均可通过本仓库的构建与测试方式在本机复现。

- 包：`github.com/highcumontoa/shared-memory-ring-transport-go/shmring`
- 平台：Linux（其它平台可编译，`msync` 为空操作，测试目标平台为 Linux）
- Go：1.25+

核心能力：

- **多写者**：跨进程/跨 goroutine 写者由写锁串行化，单条记录原子发布；
- **多消费者（消费者组）**：多个进程/协程可同时挂载为消费者并发领取，
  每条记录恰好被一个消费者领取一次；消费者崩溃后其在途记录按相同
  序列号被其他消费者重投；
- **写者崩溃在线自愈**：写者写到一半被杀，其他已挂载写者照常写入、
  消费者照常读取，无需重挂载、无需人工清理；未完成的分配固化为
  **空洞帧**，空间永不回收重用，现场可见；
- **慢消费者背压**：写者回绕复用以“最慢消费者”为界，空间不足返回
  明确的 `ErrFull`，绝不覆盖未确认记录；
- **兼容**：v1 旧格式文件继续按老的单消费者语义工作；不兼容的用法
  （混用消费模式、旧文件上用消费者组、未知版本）被明确拒绝，且拒绝
  过程只读、不破坏正在使用缓冲区的进程。

---

## 1. 共享缓冲区的参数与版本约定

### 1.1 文件布局

每个命名缓冲区对应系统临时目录（或 `Options.Dir`）下的三个文件：

| 文件 | 作用 |
| --- | --- |
| `shmring-<name>.dat` | 数据文件，被 `mmap` 为「文件头 + 环形数据区」 |
| `shmring-<name>.mlock` | 存活挂载锁：挂载期间持有共享锁，`Destroy` 借它探测存活挂载 |
| `shmring-<name>.wlock` | 写者锁：写入/恢复时短暂排他，跨进程串行化写者 |

另有一个短暂持有的挂载窗口锁 `shmring-<name>.clock`，仅在创建/校验期间排他。

### 1.2 文件头（v2，640 字节）

所有多字节整数使用**小端编码**，64 位字段均 8 字节对齐：

| 偏移 | 字段 | 含义 |
| --- | --- | --- |
| 0 | magic `SHRM` | 格式识别 |
| 4 | version | 格式版本：当前库创建为 `2`，可挂载 `1` |
| 8 | headerSize | 头大小：v1=128，v2=640 |
| 16 | capacity | 数据区容量（字节，不含头） |
| 24 | reservedPos | 已预留（可能含半条记录）的逻辑末端 |
| 32 | writePos | 已**完整提交**、可供消费的逻辑末端 |
| 40 | readPos | 单消费者游标：已被确认的逻辑末端 |
| 48 | nextSeq | 下一条记录的单调序列号（从 1 开始，永不重用） |
| 56 | initFlag `INIT` | 头部初始化完成标记，最后写入 |
| 64 | mode | 消费模式：0 未定 / 1 单消费者 / 2 消费者组（首次使用固定） |
| 72 | claimPos | 消费者组共享领取游标 |
| 128.. | 消费者槽位 ×16 | 每槽 32 字节：owner、心跳、持有位置、持有序列号 |

### 1.3 记录帧与空洞帧

每条记录是一帧，`总长度 = 16 + payload + 8`：

```
+----------------+------------------+----------+------------------+
| frameMagic u32 | seq u64 | len u32| payload  | crc32 u32 | CMIT |
+----------------+------------------+----------+------------------+
        16 字节帧头                    变长                 8 字节帧尾
```

- `crc32 = IEEE CRC32(seq 的小端 8 字节 || payload)`——**序列号纳入校验**，
  使“同长度旧帧残片”无法伪造新帧的提交；
- 帧尾最后的 `CMIT` 提交标记在整帧字节写完并 `msync(MS_SYNC)` 之后才写入。

**空洞帧**（v2）：写者崩溃留下的未提交分配被在线自愈改写为
`GapMagic(SHRG)` 头的空洞帧，长度恰好等于那次未完成的分配。
空洞帧不是数据，读者/消费者透明跳过；它占用的逻辑空间**永不回收重用**，
是“这里曾经有一次没完成的写入”的永久可见证据。

### 1.4 创建、挂载与版本兼容

所有挂载者必须与既有文件约定一致，否则返回 `*ConflictError`（可用
`errors.Is` 区分四种原因）。**校验是只读的：不写一字节，失败立即解除
挂载，正在使用该缓冲区的其它进程不受任何影响。**

| 哨兵错误 | 触发条件 |
| --- | --- |
| `ErrMagicMismatch` | 魔数不是 `SHRM` 或缺少 `INIT` 标记 |
| `ErrVersionMismatch` | 版本既不是 1 也不是 2 |
| `ErrCapacityMismatch` | `Create` 传入容量 ≠ 既有容量 |
| `ErrParamMismatch` | 头部大小等其它约定参数不一致 |

版本兼容矩阵：

| 既有文件 | 本库行为 |
| --- | --- |
| v1（128 字节头） | 正常挂载，**仅限单消费者用法**（Read/Peek/Advance/Write/Recover），行为与旧库完全一致；`NewConsumer` 返回 `ErrLegacyBuffer` |
| v2（640 字节头） | 全部能力 |
| 其它版本 | `ErrVersionMismatch`，只读拒绝 |

容量合法性：必须 `>= MinCapacity(64)` 且为 **8 的倍数**；名称只允许
字母/数字/`_`/`-`/`.`；单条负载上限为 `Capacity() - 24`。

---

## 2. 并发可见性与覆盖语义

### 2.1 逻辑游标

`reservedPos / writePos / readPos / claimPos` 都是**单调递增的逻辑字节
计数**（物理位置 = `pos % capacity`，帧允许跨过物理末尾）：

- `reservedPos`：写者预留到哪里（含发布中的一帧）；
- `writePos`：写者发布到哪里（**消费者唯一的可见性边界**）；
- `readPos`：单消费者模式下读者确认到哪里；
- `claimPos`：消费者组模式下共享领取游标（下一条待领取记录的位置）。

### 2.2 写入协议与在线自愈

1. 加跨进程写锁；
2. **（v2）在线自愈**：若 `reservedPos > writePos`，说明上一个写者在
   发布前崩溃——当前写者把 `[writePos, reservedPos)` 改写为空洞帧并
   推进 `writePos = reservedPos`，然后继续本次写入。其他已挂载写者
   与消费者**无需重挂载、无需人工清理**即可继续；
3. 检查 `capacity - (reservedPos - 回收下限) >= 帧长`，不足返回
   **`ErrFull`**，不预留、不分配序列号；
4. 分配单调 `seq`、推进 `reservedPos` 并 `msync`；
5. 写帧头、负载、帧尾 CRC，`msync` 后原子推进 `writePos`。

**回收下限**（写者回绕复用的边界，绝不越过）：

- 单消费者模式：`readPos`；
- 消费者组模式：`min(claimPos, 各活跃消费者持有未确认记录的位置)`。
  即**最慢的消费者**拖住下限——它落后到空间不够时写者得到明确的
  `ErrFull`，而不是丢数据或覆盖未确认记录。心跳过期的槽位不参与下限
  （其在途记录会被接管重投），因此崩溃的消费者最终不会卡住写者。

### 2.3 两种互斥的消费模式

同一个缓冲区的消费模式由**首次使用**固定，混用被明确拒绝
（`ErrModeConflict`），拒绝不影响在用方：

**单消费者游标**（`Read` / `Peek` / `Advance`，v1 文件的唯一模式）：
所有挂载者共享一个 `readPos`。`Peek` 不确认，重复 `Peek` 返回相同
序列号（可识别重复）；`Peek`+`Advance` 实现“先处理、后确认”的至少
一次投递。

**消费者组**（`NewConsumer` → `Next` / `Confirm` / `Close`）：

- 每个消费者注册一个槽位（上限 `MaxConsumers=16`，占满返回
  `ErrTooManyConsumers`），并周期刷新心跳；
- `Next` 通过一次 **CAS 推进 `claimPos`** 领取记录——每条完整记录
  **恰好被一个消费者领取一次**，两个消费者不会把同一条记录各自当成
  新数据处理；
- 消费者各自推进、互不阻塞：领取只是原子操作，慢消费者只会通过回收
  下限拖住**写者**（`ErrFull`），不会挡住其他消费者的领取；
- 领取后必须 `Confirm` 确认；确认前再次 `Next` 返回**同一条记录**
  （相同 `Seq`）——重复投递总能按记录本身的序列号识别；
- 消费者崩溃或退出（含 `Close` 时仍持有记录）时，其槽位心跳过期后，
  持有未确认的记录会被其他消费者**按相同序列号接管重投**
  （至少一次语义，重复按 `Seq` 去重）；若死亡消费者迟迟无人接管，
  其空间可能被写者复用，该在途记录丢失（它从未被确认）。

### 2.4 并发模型

- **写者**：多进程/多 goroutine，写锁 + 进程内互斥锁串行化；
- **消费者**：多进程/多 goroutine，各自注册槽位并发领取；
- 单条记录的发布是原子单位；消费者只能读到 `writePos` 之前、帧尾带
  `CMIT` 且 CRC 通过的帧。

---

## 3. 崩溃恢复规则

### 3.1 写者在写入中途崩溃（v2：在线自愈）

写者在推进 `writePos` 之前被杀，留下 `writePos < reservedPos` 的
未提交分配。自愈规则：

1. 任意写者下次获得写锁时（`Write` 内部），或挂载时（`Open`/`Create`
   自动 `Recover`），或显式 `Recover()` 时执行；
2. 若写锁被存活写者持有则不动（进行中的帧可能马上合法提交）；
3. 把残留分配改写为**空洞帧**并推进 `writePos`：那一次分配**不回收、
   不复用**，空洞帧永久可见；其序列号已烧掉，留下可解释的序列号空洞；
4. 已提交未确认的记录原样保留、继续占位；已确认数据不丢失、不被改写；
5. 自愈幂等：空洞帧内容确定，重复执行结果相同，`Recover` 再次调用
   返回 `(false, 0, nil)`。

**未完整写入的内容绝不被消费**：消费者只认 `writePos` 与 `CMIT+CRC`。

v1 旧格式文件保持旧的**回退式**恢复（`reservedPos` 退回 `writePos`），
与旧库行为逐字节一致。

### 3.2 消费者在消费中途崩溃

- 单消费者模式：确认前崩溃 → 重新挂载后该记录按相同序列号再次投递；
- 消费者组模式：见 2.3——在途记录被其他消费者按相同序列号接管重投；
  `flock` 与槽位都绑定在进程生命周期上，进程死亡（含 `SIGKILL`）后
  锁由内核释放、心跳停止，不需要任何人工清理。

---

## 4. 验证方法

### 4.1 构建与测试（本机既有方式）

```bash
go build ./...
go vet ./...
go test ./...                 # 全部测试
go test ./... -race           # 竞态检测
go test ./... -count=10       # 多轮稳定性
```

测试通过**重新执行测试二进制**派生多个真实本机进程（`SHMRING_CRASH_CHILD`
环境变量区分场景），无外部依赖。关键用例：

| 测试 | 场景与断言 |
| --- | --- |
| `TestMultiConsumerExactlyOnce` | 进程内 4 消费者并发领取 400 条：每条恰好一次，负载与 seq 自洽 |
| `TestMultiConsumerCrossProcess` | 3 写进程 × 60 条 + 2 消费者子进程 + 父进程消费者：180 个 seq 与 (writer,msg) 对全局唯一 |
| `TestConsumerRedeliversOwnHold` / `TestConsumerCloseAbandon` | 未确认重复 `Next` 返回相同 seq；持有记录退出后被接管重投 |
| `TestConsumerCrashSteal` / `TestConsumerCrashRedeliveryProcess` | 消费者“崩溃”（协程/真实进程）后，在途记录按相同 seq 被存活消费者重投 |
| `TestSlowConsumerBackpressure` | 慢消费者按住记录 → 写者 `ErrFull`；快消费者不受阻；确认后写者恢复且记录完好 |
| `TestWriterCrashOnlineHeal` | 写者 A 被杀 → 已挂载写者 B 继续写、父进程消费者不重挂载读到新记录；seq=4 为可见空洞；空洞帧 @114 长度 74 可观测 |
| `TestWriterCrashRecovery` / `TestCrashNoOverwriteUnread` | 原有崩溃恢复不回归：残片不可见、恢复幂等、未确认记录不被覆盖、空洞不回收 |
| `TestV1LegacyBuffer` / `TestV1CrashRecoveryRewind` | v1 旧文件：老用法正常、消费者组明确拒绝、回退式恢复语义不变 |
| `TestModeConflict` / `TestTooManyConsumers` | 两种消费模式互斥拒绝且不破坏在用方；槽位占满拒绝 |
| `TestWriteFullAndWrap` / `TestReadEmpty` / `TestMultiProcessStream` / `TestCrossProcessMountConflicts` 等 | 上一轮用例（写满回绕、读空、参数冲突重复挂载、多写者流）全部不回归 |

测试日志会打印 `reserved/write/read/claim/floor` 各位置、消费者槽位
快照，以及每条已提交帧的 CRC 校验依据，例如：

```
[slow holds seq=1] reserved=992 write=992 read=0 claim=124 floor=0 slots=[{0 ... true 0 1} ...]
writer got ErrFull: floor pinned by slow consumer's unconfirmed seq=1
consumer (no remount) read seqs [1 2 3 5 6 7 8 9]: seq=4 is the burned hole from crashed writer A
```

### 4.2 命令行手动复现

```bash
go build -o /tmp/shmring-demo ./cmd/shmring-demo
D=/tmp/shmring-demo
$D create demo 512
$D write demo hello-1            # 进程 A：写入
$D write demo hello-2            # 进程 B：写入
$D consume demo                  # 进程 C：以消费者组身份领取 seq=1
$D consume demo                  # 进程 D：领取 seq=2（每条恰好被领一次）
$D state   demo                  # 查看 reserved/write/read/claim 与消费者槽位
```

单消费者游标用法（`read`/`peek`）与上面消费者组用法**互斥**，同一
缓冲区混用会被明确拒绝。连续写满后 `write` 返回 full；消费者确认后
写者回绕复用空间。
