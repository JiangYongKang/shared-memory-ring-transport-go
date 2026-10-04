# shmring — 跨进程共享内存环形缓冲传输通道

用一个**本地内存映射文件**（`mmap(MAP_SHARED)`）模拟共享内存，为多个本机
进程提供“完整记录”的并发读写通道：多写者串行发布、**多消费者竞争领取**、
写者崩溃后**在线自愈**、慢消费者下**明确回满**。不依赖外部服务、真实凭据
或需要联网获取的运行时，所有能力均可通过本仓库的构建与测试方式在本机复现。

- 包：`github.com/highcumontoa/shared-memory-ring-transport-go/shmring`
- 平台：Linux（其它平台可编译，`msync` 与消费者存活探测退化为空操作，
  崩溃恢复语义仅在 Linux 上完整保证；测试目标平台为 Linux）
- Go：1.25+

---

## 1. 共享缓冲区的参数与版本约定

### 1.1 文件布局

每个命名缓冲区对应系统临时目录（或 `Options.Dir`）下的若干文件：

| 文件 | 作用 |
| --- | --- |
| `shmring-<name>.dat` | 数据文件，被 `mmap` 为「文件头 + 环形数据区」 |
| `shmring-<name>.mlock` | 存活挂载锁：挂载期间持有共享锁，`Destroy` 借它探测存活挂载 |
| `shmring-<name>.wlock` | 写者锁：写入/恢复时短暂排他，跨进程串行化写者 |
| `shmring-<name>.rlock` | 消费者槽位锁：每个消费者持有一个字节的 OFD 锁，用于死亡探测 |

另有一个短暂持有的挂载窗口锁 `shmring-<name>.clock`，仅在创建/校验期间排他，
不长期持有，因此同一进程内重复挂载不会与存活挂载的共享锁自死锁。

文件头固定 **768 字节**，所有多字节整数使用**小端编码**，64 位字段均
8 字节对齐：

| 偏移 | 字段 | 含义 |
| --- | --- | --- |
| 0 | magic `SHRM` | 格式识别 |
| 4 | version | 格式版本，当前为 `2` |
| 8 | headerSize | 头大小，固定 768 |
| 16 | capacity | 数据区容量（字节，不含头） |
| 24 | reservedPos | 已预留（可能含半条记录）的逻辑末端 |
| 32 | writePos | 已**完整提交**、可供领取的逻辑末端 |
| 40 | readPos | **回收边界**：所有存活消费者 ack 游标的最小值 |
| 48 | nextSeq | 下一条记录的单调序列号（从 1 开始，永不重用） |
| 56 | initFlag `INIT` | 头部初始化完成标记，最后写入 |
| 64 | claimPos | 下一条待消费者**领取**的记录位置（CAS 竞争） |
| 72 | slotBitmap | 消费者槽位占用位图（8 槽） |
| 80 | tornCount | 历史上“写一半崩溃”的分配次数（单调递增） |
| 128..255 | 消费者槽位表 | 每槽 16 字节：ackPos + 最近活跃时间 |
| 256..767 | 撕裂区表 | 32 条 × 16 字节：(start, end)，见 §3.1 |

### 1.2 记录帧格式

每条记录是一帧，`总长度 = 16 + payload + 8`：

```
+----------------+------------------+----------+------------------+
| frameMagic u32 | seq u64 | len u32| payload  | crc32 u32 | CMIT |
+----------------+------------------+----------+------------------+
        16 字节帧头                    变长                 8 字节帧尾
```

- `frameMagic = SHRF` 标记帧起始；
- `crc32 = IEEE CRC32(seq 的小端 8 字节 || payload)`——**序列号纳入校验**，
  使“同长度旧帧残片”无法伪造新帧的提交；
- 帧尾最后的 `CMIT` 提交标记在整帧字节写完并 `msync(MS_SYNC)` 之后才写入。

### 1.3 创建与挂载约定（冲突必被拒绝，且原因可区分）

所有挂载者必须与创建时约定一致，否则返回 `*ConflictError`，可用
`errors.Is` 区分四种原因。**校验是只读的：不写一字节，失败立即解除挂载，
正在使用该缓冲区的其它进程不受任何影响。**

| 哨兵错误 | ConflictKind | 触发条件 |
| --- | --- | --- |
| `ErrMagicMismatch` | magic mismatch | 魔数不是 `SHRM` 或缺少 `INIT` 标记 |
| `ErrVersionMismatch` | version mismatch | 版本字段 ≠ 库版本 `2`（含 v1 旧缓冲区） |
| `ErrCapacityMismatch` | capacity mismatch | `Create` 传入容量 ≠ 既有容量 |
| `ErrParamMismatch` | parameter mismatch | 头部大小等其它约定参数不一致 |

容量合法性：必须 `>= MinCapacity(64)` 且为 **8 的倍数**，否则
`ErrCapacityInvalid`；名称只允许字母/数字/`_`/`-`/`.`，不得含路径分隔符，
否则 `ErrNameInvalid`；挂载不存在的名称返回 `ErrNotFound`。
单条负载上限为 `Capacity() - 24`（帧头+帧尾），超过返回
`ErrPayloadTooLarge`。

**兼容范围**：v2 库拒绝挂载 v1 格式的旧缓冲区（`ErrVersionMismatch`，
只读拒绝、不影响在用进程）；旧版单消费者 **API 用法**（`Ring.Read` /
`Peek` / `Advance`）在 v2 缓冲区上语义不变，见 §2.4。

---

## 2. 并发可见性、多消费者与覆盖语义

### 2.1 四个逻辑游标

`reservedPos / writePos / readPos / claimPos` 都是**单调递增的逻辑字节
计数**（不是物理偏移；物理位置 = `pos % capacity`，帧允许跨过物理末尾）。

正常稳态下 `readPos <= claimPos <= writePos <= reservedPos`：

- `readPos`：**回收边界**，所有存活消费者 ack 游标的最小值；
- `claimPos`：消费者领取到哪里（下一条待领取记录的位置）；
- `writePos`：写者发布到哪里（**消费者唯一的可见性边界**）；
- `reservedPos`：写者预留到哪里（只前进、永不回退）。

### 2.2 写入是“自愈 → 预留 → 写字节 → 原子发布”

1. 加跨进程写锁；**在线自愈**：若发现 `reservedPos > writePos`（只可能是
   崩溃写者留下的半条记录），把该区间登记进撕裂区表、推进 `writePos`
   跳过它（见 §3.1）；
2. 检查 `capacity - (reservedPos - readPos) >= 帧长`，不足则先回收死亡
   消费者槽位、刷新回收边界再复核，仍不足返回 **`ErrFull`**，
   不预留、不分配序列号；
3. 分配单调 `seq`、推进 `reservedPos` 并 `msync`；
4. 写帧头、负载、帧尾 CRC，然后 `msync`；
5. **最后**写入帧尾 `CMIT`，再原子推进 `writePos` 并 `msync`。

第 5 步的 64 位原子 store 是 release 操作：此前所有帧字节写入，对之后
acquire 读到新 `writePos` 的消费者都可见。

### 2.3 多消费者：竞争领取、各自确认、恰好一次

- `Ring.Consumer()` 注册一个独立消费者（占用一个槽位，最多
  `MaxConsumers = 8` 个存活消费者，耗尽返回 `ErrTooManyConsumers`）；
  消费者可以是另一个进程，也可以是同进程的另一个句柄（ goroutine ）。
- `Consumer.Read()` 经 **CAS 推进 `claimPos`** 竞争领取：**每条完整记录
  恰好被一个消费者领取一次**，领取失败只是重试，消费者彼此不阻塞。
- 领取即确认：消费者的 ack 游标推进到所领记录的末端。各消费者 ack 游标
  独立推进；**回收边界 = 所有存活消费者 ack 游标的最小值**。
- 消费者读到空（`ErrEmpty`）时会自动把 ack 游标追平到 `claimPos`
  （它此时不持有任何未确认记录），也可显式调用 `Consumer.Sync()`——
  活跃但暂时没数据的消费者不会无谓拖住写者。
- **慢消费者**：不再读取的消费者，其 ack 游标停在前次位置，回收边界被
  拖住；写者空间不足时得到明确的 **`ErrFull`**，绝不覆盖任何消费者
  未确认的记录、绝不静默丢数据。慢消费者恢复读取（或 `Sync()`）后，
  写者立即可以继续。
- **死亡消费者**：消费者进程退出（含 `SIGKILL`）时内核自动释放其槽位的
  OFD 锁；其它挂载者在注册新消费者、确认记录或写满复核时探测到锁空闲，
  自动摘除该槽位，回收边界不再被它拖住。死亡消费者**已领取未确认**的
  记录随之放弃——不会转投给其它消费者，因此其它消费者绝不会重复处理
  同一条记录（重复投递只可能来自旧式 `Peek`/`Advance` 路径，且序列号
  相同，可按 `Record.Seq` 去重）。
- `Consumer.Close()` 正常注销槽位；同进程内不再使用的消费者句柄必须
  `Close()`（进程内 goroutine 退出不会触发内核放锁）。

### 2.4 旧式单消费者 API（兼容用法）

`Ring.Read` / `Peek` / `Advance` 语义不变，内部经由本 Ring 的隐式消费者
（首次读取时注册一个普通槽位）实现：

- `Peek()` 只查看不确认；单消费者下连续 `Peek` 返回**相同序列号**的同一
  条记录，据此识别重复投递；`Peek()` + `Advance()` 仍是“先处理、后确认”
  的至少一次投递（确认前崩溃，重新挂载后同 seq 重投）；
- 多消费者并发时 `Peek` 仅作观测（看到的记录可能随后被其它消费者领走）。

### 2.5 并发模型

- **写者**：多进程/多 goroutine。跨进程由 `wlock` 的 `flock(LOCK_EX)`
  串行化，进程内由互斥锁串行化；单条记录的发布是原子单位。
- **消费者**：多进程/多 goroutine 并发领取，CAS 竞争、无锁推进；
  单个 `Consumer` 句柄内部有互斥锁，可被同进程多 goroutine 共享。

---

## 3. 崩溃恢复规则

### 3.1 写者在写入中途崩溃：在线自愈

写者在推进 `writePos` 之前的任一时点被杀（帧字节写了一半、或整帧写完但
尚未发布），都会留下：

```
readPos <= claimPos <= writePos < reservedPos
                              └ 区间 = 崩溃写者残留的半条记录
```

**自愈不需要重新挂载、不需要人工清理**：任何写者拿到写锁时发现上述间隙，
即在线执行（挂载时的 `Open`/`Create` 自动恢复与显式 `Recover()` 走同一条
路径）：

1. 把间隙 `[writePos, reservedPos)` **登记进撕裂区表**（`tornCount` 递增，
   `Ring.TornRegions()` 可读出区间）——那次没写完的分配**永不回收重用**，
   `reservedPos` 只前进不回退，“这里曾经有一次没完成的写入”始终可观测；
2. `writePos` 直接推进到 `reservedPos`；消费者在领取路径上遇到撕裂区
   起点时**整段跳过**，残片永远不会被当作数据交付；
3. 已提交记录、各消费者 ack 游标、回收边界一律不动——已确认的数据不
   丢失、不被静默改写；
4. 残片对应的序列号已在预留前单调分配并持久化，留下一个**可解释的
   序列号空洞**（`TornCount` 与空洞一一对应），序列号绝不回卷重用；
5. 自愈是**幂等**的：无间隙时 `Recover()` 返回 `(false, 0, nil)`；
   写锁被存活写者持有时 `Recover()` 不动手（进行中的帧可能马上合法
   提交），该写者自己的下一次 `Write` 会完成自愈。

因此：一个写者被杀后，**其它存活写者照常写入、消费者照常读到崩溃之后
的新记录**，全链路无需重新挂载（见 `TestWriterCrashOnlineHealing`）。

### 3.2 消费者在读取中途崩溃

- 多消费者路径：领取即确认。消费者确认前崩溃，其槽位被自动回收，
  该记录不再投递给任何人（恰好一次领取；其它消费者不受影响、不重复
  处理）；确认后崩溃无任何残留影响。
- 旧式 `Peek`/`Advance` 路径：确认（推进 ack 游标）前崩溃，重新挂载后
  该记录被**再次投递**，序列号相同，消费者用 `seq` 去重（至少一次语义，
  见 `TestReaderCrashRedelivery`）。

### 3.3 进程异常退出时的锁

`flock` 与 OFD 锁都绑定在打开文件描述上，进程死亡（含 `SIGKILL`）时内核
自动释放。因此写者锁不会卡死后续写者，消费者槽位锁不会卡死回收边界。

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

测试通过**重新执行测试二进制**派生多个真实本机进程（设置
`SHMRING_CRASH_CHILD` 环境变量区分场景），无外部依赖。关键用例：

| 测试 | 场景与断言 |
| --- | --- |
| `TestMultiConsumerExactlyOnce` | 进程内 4 消费者 goroutine 竞争领取 500 条：每条恰好一次、内容逐字节一致；回收边界最终收敛到写位置 |
| `TestMultiConsumerCrossProcess` | 3 个真实消费者进程 × 父进程写 240 条：按各进程日志汇总，每条恰好被一个进程领取 |
| `TestWriterCrashOnlineHealing` | 写者 B 负载写一半被杀，写者 A 持续写、父进程消费者**不重新挂载**读完全部后续记录；撕裂区登记在案、序列号恰好一个空洞、自愈后同挂载继续写读 |
| `TestSlowConsumerBackpressure` | 慢消费者拖住回收边界：写者得到明确 `ErrFull` 且不丢不改任何记录；慢消费者恢复后写者立即继续 |
| `TestDeadConsumerSlotReaped` / `TestDeadConsumerDoesNotBlockProcess` | 消费者死亡（句柄遗弃 / 子进程被杀）后槽位被自动回收并被复用，写者不被永久卡满 |
| `TestTooManyConsumers` | 8 个存活消费者后第 9 个被 `ErrTooManyConsumers` 拒绝；关闭后可复用 |
| `TestLegacyVersionRejected` | v1 旧缓冲区被 `ErrVersionMismatch` 明确拒绝；拒绝前后文件逐字节不变，其它 v2 缓冲区照常工作 |
| `TestWriteFullAndWrap` | 写满→`ErrFull`；消费后跨物理末尾回绕写入；CRC 逐帧校验；读空→`ErrEmpty`；序列号不回卷 |
| `TestCrashNoOverwriteUnread` | 帧字节写一半“死亡”；残片对读者不可见；自愈后残片空间**不回收**、未消费记录仍占位不被覆盖；确认后才释放 |
| `TestWriterCrashRecovery` | 子进程在**负载写一半**与**整帧写完待发布**两种时点被杀；`Recover()` 报告的残片字节数精确可解释；重新挂载幂等 |
| `TestReaderCrashRedelivery` | 子进程 `Peek` 后未确认即死；重新挂载同一 seq 原样重投；重复 `Peek` 可识别为重复 |
| `TestMultiProcessStream` | 3 个真实写进程 × 60 条 + 父进程读者，180 个序列号全局唯一、无乱码、无重复投递 |
| `TestCrossProcessMountConflicts` / `TestMountConflicts` / `TestConcurrentRejectedMounts` | 容量/魔数/版本冲突可区分拒绝；拒绝只读、不破坏在用缓冲区；并发重复挂载无死锁 |
| `TestConcurrentSingleProcess` | 进程内 4 goroutine 并发写、唯一读者并发读，去重与内容核对 |

测试日志会打印 `reserved/write/reclaim/claim` 各位置、`used/free` 空间、
`nextSeq`、`tornCount` 与撕裂区区间，以及每条已提交帧的逻辑位置与 CRC
校验依据，例如：

```
online healing: torn=[172,246) reserved=2538 write=2538 reclaim=2538 (torn space never reused)
seq accounting: nextSeq=45 delivered=43 holes=[5] (hole = torn write's seq)
writer blocked at seq=11: reserved=560 write=560 reclaim=56 used=504 slowAck=56 fastAck=560
```

### 4.2 命令行手动复现

```bash
go build -o /tmp/shmring-demo ./cmd/shmring-demo
D=/tmp/shmring-demo
$D create demo 256
$D write demo hello            # 一个进程：写入完整记录
$D consume demo                # 另一个进程：以独立消费者身份领取（多消费者）
$D write demo world
$D read demo                   # 旧式单消费者读取并确认
$D peek demo                   # 查看（不确认）；重复 peek 得到相同 seq
$D state demo                  # 查看 reserved/write/reclaim/claim、nextSeq 与撕裂区
```

连续写满后 `write` 返回 full；消费者确认后空间释放，再次写入会回绕复用。
在两个终端各执行 `consume` 可以看到同一条记录只会被其中一个进程领到。
