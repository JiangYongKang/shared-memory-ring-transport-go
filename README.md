# shmring — 跨进程共享内存环形缓冲传输通道

用一个**本地内存映射文件**（`mmap(MAP_SHARED)`）模拟共享内存，为多个本机
进程提供“完整记录”的并发读写通道。不依赖外部服务、真实凭据或需要联网获取
的运行时，所有能力均可通过本仓库的构建与测试方式在本机复现。

- 包：`github.com/highcumontoa/shared-memory-ring-transport-go/shmring`
- 平台：Linux（其它平台可编译，`msync` 为空操作，测试目标平台为 Linux）
- Go：1.25+

---

## 1. 共享缓冲区的参数与版本约定

### 1.1 文件布局

每个命名缓冲区对应系统临时目录（或 `Options.Dir`）下的三个文件：

| 文件 | 作用 |
| --- | --- |
| `shmring-<name>.dat` | 数据文件，被 `mmap` 为「文件头 + 环形数据区」 |
| `shmring-<name>.mlock` | 存活挂载锁：挂载期间持有共享锁，`Destroy` 借它探测存活挂载 |
| `shmring-<name>.wlock` | 写者锁：写入/恢复时短暂排他，跨进程串行化写者 |

另有一个短暂持有的挂载窗口锁 `shmring-<name>.clock`，仅在创建/校验期间排他，
不长期持有，因此同一进程内重复挂载不会与存活挂载的共享锁自死锁。

文件头固定 **128 字节**，所有多字节整数使用**小端编码**（跨 32/64 位与
不同操作系统的 Go 进程布局一致），64 位字段均 8 字节对齐：

| 偏移 | 字段 | 含义 |
| --- | --- | --- |
| 0 | magic `SHRM` | 格式识别 |
| 4 | version | 格式版本，当前为 `1` |
| 8 | headerSize | 头大小，固定 128 |
| 16 | capacity | 数据区容量（字节，不含头） |
| 24 | reservedPos | 已预留（可能含半条记录）的逻辑末端 |
| 32 | writePos | 已**完整提交**、可供消费的逻辑末端 |
| 40 | readPos | 已被读者**确认**的逻辑末端 |
| 48 | nextSeq | 下一条记录的单调序列号（从 1 开始，永不重用） |
| 56 | initFlag `INIT` | 头部初始化完成标记，最后写入 |

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
| `ErrVersionMismatch` | version mismatch | 版本字段 ≠ 库版本 `1` |
| `ErrCapacityMismatch` | capacity mismatch | `Create` 传入容量 ≠ 既有容量 |
| `ErrParamMismatch` | parameter mismatch | 头部大小等其它帧约定不一致 |

容量合法性：必须 `>= MinCapacity(64)` 且为 **8 的倍数**，否则
`ErrCapacityInvalid`；名称只允许字母/数字/`_`/`-`/`.`，不得含路径分隔符，
否则 `ErrNameInvalid`；挂载不存在的名称返回 `ErrNotFound`。
单条负载上限为 `Capacity() - 24`（帧头+帧尾），超过返回
`ErrPayloadTooLarge`。

---

## 2. 并发可见性与覆盖语义

### 2.1 三个逻辑游标

`reservedPos / writePos / readPos` 都是**单调递增的逻辑字节计数**（不是物理
偏移；物理位置 = `pos % capacity`，帧允许跨过物理末尾）。

正常稳态下 `readPos <= writePos <= reservedPos`：

- `readPos`：读者确认到哪里；
- `writePos`：写者发布到哪里（**读者唯一的可见性边界**）；
- `reservedPos`：写者预留到哪里（通常等于 `writePos`，发布中的一帧除外）。

### 2.2 写入是“预留 → 写字节 → 原子发布”

1. 加跨进程写锁；检查 `capacity - (reservedPos - readPos) >= 帧长`，
   不足立即返回 **`ErrFull`**，不预留、不分配序列号；
2. 分配单调 `seq`、推进 `reservedPos` 并 `msync`；
3. 写帧头、负载、帧尾 CRC，然后 `msync`；
4. **最后**写入帧尾 `CMIT`，再原子推进 `writePos` 并 `msync`。

第 4 步的 64 位原子 store 是 release 操作：此前所有帧字节写入，对之后
acquire 读到新 `writePos` 的读者都可见。因此：

- **读者只能读到 `readPos..writePos` 区间、且帧尾带 `CMIT` 且 CRC 通过的帧**；
  写者正在发布的半条记录对读者表现为 `ErrEmpty`，绝不会被当作有效数据；
- **写者绝不覆盖未消费的完整记录**：空间计算以 `readPos` 为界，只有读者
  确认（`Read`/`Advance` 推进 `readPos`）后空间才允许回绕复用；
- 写满返回 `ErrFull`，读空返回 `ErrEmpty`，两者都是明确可验证的稳定状态。

### 2.3 重复读取可被识别

`Peek()` 只查看不确认；对同一位置连续 `Peek` 返回**相同序列号**的同一条
记录。消费者据此把“确认前崩溃导致的再次投递”识别为重复，而不是新数据。
`Read()` 读取并确认；`Peek()` + `Advance()` 可实现“先处理、后确认”的
至少一次投递。

### 2.4 并发模型

- **写者**：可多进程/多 goroutine。跨进程由 `wlock` 的 `flock(LOCK_EX)`
  串行化，进程内由互斥锁串行化；单条记录的发布是原子单位。
- **读者**：设计为单消费者（SPSC 风格）。多读者并发消费会竞争同一条记录，
  不属于本通道支持范围；但多个进程同时**挂载**、以及写者与读者并发是支持的。

---

## 3. 崩溃恢复规则

### 3.1 写者在写入中途崩溃

写者在推进 `writePos` 之前的任一时点被杀（帧字节写了一半、或整帧写完但
尚未发布），都会留下：

```
readPos <= writePos < reservedPos
                    └ 区间 = 崩溃写者残留的半条记录
```

恢复规则（`Open`/`Create` 挂载时自动执行，也可显式调用 `Recover()`）：

1. 先尝试**非阻塞**获取写锁；若写锁被存活写者持有，则**不回收**
   （进行中的帧可能马上合法提交），返回 `(false, 0, nil)`；
2. 拿到写锁后，从 `readPos` 顺序扫描到 `writePos`，核对每一帧的
   `SHRF / CMIT / CRC`（已提交区域必须自洽，否则返回 `ErrCorrupt`）；
3. 将 `reservedPos` 退回到 `writePos`，即**丢弃末尾残片**。残片的物理字节
   要到容量回绕时才被覆盖，但它永远在 `writePos` 之后，读者永远不可见；
4. 残片对应的序列号已在预留前单调分配并持久化，因此恢复后会留下一个
   **可解释的序列号空洞**（“曾经尝试但未提交”），序列号绝不回卷重用；
5. 恢复是**幂等**的：再次挂载/再次 `Recover()` 返回 `(false, 0, nil)`。

**未完整写入的内容绝不被消费**：读者只认 `writePos` 与 `CMIT+CRC`。
崩溃恢复后读写位置自洽：已提交未确认的记录原样保留且继续占位，写者不能借
回收残片覆盖它们（见 `TestCrashNoOverwriteUnread`）；已确认数据不丢失、
不被静默改写。

### 3.2 读者在读取中途崩溃

读者的确认只有一个动作：推进 `readPos`。

- 确认**之前**崩溃：`readPos` 未动，重新挂载后该记录被**再次投递**，
  序列号与之前相同，消费者用 `seq` 去重（至少一次语义，见
  `TestReaderCrashRedelivery`）；
- 确认**之后**崩溃：`readPos` 已 `msync`，该记录占用的空间可被复用，
  不会再次出现。

### 3.3 进程异常退出时的锁

`flock` 锁绑定在打开文件描述上，进程死亡（含 `SIGKILL`）时内核自动释放
全部锁。因此下一个挂载者能立即拿到写锁完成回收，不会出现“死写者永久
持锁”。

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
| `TestWriteFullAndWrap` | 写满→`ErrFull`；消费后跨物理末尾回绕写入；CRC 逐帧校验；读空→`ErrEmpty`；序列号不回卷 |
| `TestCrashNoOverwriteUnread` | 帧字节写到一半“死亡”；残片对读者不可见；回收后未消费记录仍占位、写者不得覆盖；确认后才释放 |
| `TestWriterCrashRecovery` | 子进程在**负载写一半**与**整帧写完待发布**两种时点被杀；`Recover()` 报告的残片字节数精确可解释；重新挂载幂等 |
| `TestReaderCrashRedelivery` | 子进程 `Peek` 后未确认即死；重新挂载同一 seq 原样重投；重复 `Peek` 可识别为重复 |
| `TestMultiProcessStream` | 3 个真实写进程 × 60 条 + 父进程读者，180 个序列号全局唯一、无乱码、无重复投递 |
| `TestCrossProcessMountConflicts` | 容量/魔数/版本冲突由子进程挂载触发，退出码与哨兵错误一一可区分；拒绝后在用缓冲区仍可读写 |
| `TestMountConflicts` / `TestConcurrentRejectedMounts` | 冲突拒绝只读、不破坏在用缓冲区；并发重复挂载无死锁 |
| `TestConcurrentSingleProcess` | 进程内 4 goroutine 并发写、唯一读者并发读，去重与内容核对 |

测试日志会打印 `reserved/write/read` 三个位置、`used/free` 空间、
`nextSeq`，以及每条已提交帧的逻辑位置与 CRC 校验依据，例如：

```
[after wrap writes] cap=256 reserved=336 write=336 read=112 used=224 nextSeq=7
  verify frame@280 seq=6 len=32 crc=0xbe006197 OK
```

### 4.2 命令行手动复现

```bash
go build -o /tmp/shmring-demo ./cmd/shmring-demo
D=/tmp/shmring-demo
$D create demo 256
$D write demo hello            # 另一个进程：写入完整记录
$D peek  demo                  # 查看（不确认）；重复 peek 得到相同 seq
$D read  demo                  # 读取并确认
$D state demo                  # 查看 reserved/write/read 与 nextSeq
```

连续写满后 `write` 返回 full；`read` 全部确认后再次写入会回绕复用空间。
