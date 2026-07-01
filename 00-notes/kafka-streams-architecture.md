# Kafka Streams 架构精简笔记

> 本文由一次代码走读会话整理，从 KAFKA-20731 这个 bug 出发，串起 Kafka Streams
> 的核心概念与内部架构。所有结论均对照 `streams/` 源码。

---

## 1. Kafka Streams 是什么

- 一个 **Java 类库**（不是独立集群），跑在你自己的应用进程里，`java -jar` 即可。
- 只跟 Kafka 打交道：**读 Kafka → 计算 → 写 Kafka**。
- 用几行 DSL 就能写实时流处理，框架帮你封装了容错、并行、状态管理。

对比 Flink/Spark：它们是"提交 job 上集群"，Kafka Streams 是"库跑在你的进程里"。

---

## 2. 核心概念层次：partition → task → thread

### Topology 与 Subtopology
- 你写的 DSL 被编译成**算子图（Topology）**。
- Topology 被 **repartition topic** 切成若干段，每段 = 一个 **subtopology**。
  - 段内：数据在内存里、同一 task 内流过，不走 Kafka。
  - 段间：必须写回 Kafka topic 按新 key 重新分区，再读出来。
- 触发切割（产生新 subtopology）的操作：任何"改 key 后重新分组"——
  `groupBy(...)`、需要 repartition 的 `join`。
  不切的：`filter` / `mapValues` / `groupByKey`（不改 key）。

### Task
- `TaskId = subtopology_partition`（如 `1_3` = 1 号子拓扑第 3 分区）。
  源码：`TaskId.java` 由 `subtopology` + `partition` 组成。
- **一个 task = 该 subtopology 各源 topic 中"同一分区号"的 partition 集合。**
  - 单输入 topic → 1 task 对应 1 partition。
  - join 多输入（co-partitioned）→ 1 task 含多个 partition（每个源各一个，分区号相同）。
- **每个 subtopology 的 task 数 = 它源 topic 分区数的最大值**；
  **总 task 数 = 各 subtopology 之和**。
- co-partition 约束（`CopartitionedTopicsEnforcer`）强制 join 各方分区数相同，
  所以那个 "max" 平时就等于"共同的分区数"。

### Thread
- `num.stream.threads` 决定线程数。
- **task 数固定**（由拓扑+分区数决定），与线程数无关；线程数只决定 task 怎么分。
- thread ↔ task 是一对多：一个线程可跑多个 task，一个 task 同一时刻只在一个线程。
- **持有 consumer 的是 thread，不是 task。** Task 没有自己的 consumer。

---

## 3. Consumer group：三种 consumer

| consumer | 数量 | 属于 group? | subscribe/assign | 用途 |
|---|---|---|---|---|
| **main** | 每线程 1 | ✅ group = `application.id` | `subscribe()` | 读源 topic，处理业务 |
| **restore** | 每线程 1 | ❌ 无 group | `assign()` | 读 changelog 恢复状态 |
| **global** | 每实例 1 | ❌ 无 group | `assign()` | 读 GlobalKTable |

- 全应用所有线程/实例的 **main consumer 共享一个 group**（= application.id），
  靠 group 协调器做分区分配、横向扩展。
- **restore consumer 故意不入 group**（`getRestoreConsumerConfigs` 里 remove 掉
  `GROUP_ID_CONFIG`），用手动 `assign()`，因为：
  1. 读哪些 changelog 由 Streams 精确算好，不能交给 rebalance 随机分。
  2. 恢复进度的真相在本地 RocksDB，不是 group committed offset。
  3. active 和多个 standby 要**同时读同一份 changelog**，入 group 会互斥。

---

## 4. Co-partition：相同 key 落同分区的真正条件

分区号 = `partitioner(序列化后的 key 字节, 分区数)`
默认算法：`toPositive(murmur2(serializedKey)) % numPartitions`（`BuiltInPartitioner`）。

**"同一个 key 落在同一分区号"需要三项全一致**（缺一不可）：
1. 分区数相同（Streams 会校验）
2. 分区器算法相同（你保证）
3. **key 序列化后的字节相同**（你保证）——最常见的坑：
   两 topic key 类型/serde 不同（`Long 42` vs `String "42"`）→ 字节不同 → 分区不同
   → join **静默丢数据**（不报错）。

- 内部 repartition topic：三项由 Streams 全保证。
- 外部手动 join 的 topic：Streams 只校验分区数，后两项靠你。
- 边角：null key 走随机/黏性分区，不按 key。

---

## 5. 内部架构：四个组件

### 组合关系（per-thread 配套）
```
StreamThread（工作线程）
 ├── TaskManager        账房/协调者
 ├── StateUpdater       后台恢复线程（DefaultStateUpdater）
 └── ChangelogReader ── 同一实例被三者共享（持有 1 个 restore consumer）
```
`StreamThread.create()` 里 `new StoreChangelogReader(...)`，把**同一实例**注入
StateUpdater 和 TaskManager。

### 各自角色
| 组件 | 身份 | 线程 | 职责 |
|---|---|---|---|
| **StreamThread** | 工头 | 自己（主循环）| `runOnce()`：poll → 处理 RUNNING 的 task → commit |
| **TaskManager** | 账房 | StreamThread 内 | 管 task 生命周期/归属；前台与后台的中间人 |
| **StateUpdater** | 恢复工 | **独立后台线程** | 把 task 状态追平；不处理业务数据 |
| **ChangelogReader** | 恢复引擎 | 被 StateUpdater 调用 | 掌控 restore consumer，从 changelog 拉字节写 state store |

### 两线程一交接线
- **StreamThread 线程**：处理数据（前台）。
- **StateUpdater 线程**：恢复状态（后台）。
- 二者通过 TaskManager 的三个接口显式交接 task：
  - `add(task)` —— 交给后台恢复
  - `drainRestoredActiveTasks()` —— 取回已恢复的 active，转 RUNNING
  - `drainExceptionsAndFailedTasks()` —— 取回出错的 task

### ChangelogReader 是 per-thread 共享，不是 per-task
- 内部 `Map<TopicPartition, ChangelogMetadata> changelogs`：**每 partition 一条**
  独立 metadata（各自绑定所属 task 的 stateManager）。
- 注册粒度是 partition：`register(TopicPartition, ProcessorStateManager)`。
- 一个 restore consumer 一次 poll 拉回所有恢复中的 partition，再按 map 分发。
  这样避免"每 task 一个 consumer"的巨大开销，也是它要集中调度 pause/resume 的原因。

### StreamThread 为何持有却不用 ChangelogReader
- 只在两端碰它：`create()` 里**创建+注入**、shutdown 时 `clear()` **销毁**。
- 运行期一次 `restore()` 都不调——恢复由 StateUpdater 线程驱动。
- 即"创建者+拥有者，负责生命周期两端，但不是使用者"。
  （历史上恢复曾在 StreamThread 主循环里做，后被剥离到 StateUpdater 线程。）

---

## 6. Task 生命周期与状态机

`Task.State`：`CREATED → RESTORING → RUNNING → SUSPENDED → CLOSED`

```
CREATED ─ RESTORING ─ RUNNING ─ SUSPENDED ─ CLOSED
          └── StateUpdater 管 ──┘└ StreamThread 管 ┘
              (追 changelog)      (处理数据)
```

一条 task 走完全程：
1. **rebalance**：`TaskManager.handleAssignment()` 创建 task → 入 `pendingTasksToInit` 队列。
2. **交接给后台**：`StreamThread.runOnce → checkStateUpdater →
   TaskManager.addTaskToStateUpdater`：先 `task.initializeIfNeeded()`（拿状态目录锁+初始化），
   成功后 `stateUpdater.add(task)`（交接点，`TaskManager.java:959`）。
3. **后台恢复**：StateUpdater `restoreTasks()` → `changelogReader.restore()` 追平。
4. **交还前台**：`drainRestoredActiveTasks()` → 转 RUNNING。
5. **处理数据**：`taskManager.process()` 真正处理记录、更新 state store（顺带写 changelog）。

- **standby task**：走到第 3 步就**永久留在 StateUpdater** 持续更新，无 4/5 步。
- active 与 standby 的意义：standby 是热备，机器挂了可立即顶上，省去从头读 changelog。

---

## 7. 数据流 + 容错闭环

正常流：`poll → StreamTask.process → 穿过算子图 → store.put → 转发到 sink`
- `store.put()` 被 `ChangeLoggingKeyValueBytesStore` 包装，**同时**：
  ① 写本地 RocksDB ② 写 changelog topic（备份）。这是容错的支点。

容错：机器挂 → rebalance → 新机器 RocksDB 为空 → StateUpdater 用 ChangelogReader
**回放 changelog 重建状态** → 恢复完转 RUNNING，计数从断点续，不从 0 开始。

---

## 8. KAFKA-20731 —— 本次讨论的引子

**Bug**：`transitToUpdateStandby()` 被调用时未检查 changelog reader 状态，
crash 掉 state updater 线程（`IllegalStateException`）。

**机制**：
- ChangelogReader 有全局状态 `ACTIVE_RESTORING ⇄ STANDBY_UPDATING`。
  规则：只要有 active 在恢复就进 `ACTIVE_RESTORING`（集中追 active，pause standby 的 partition），
  全部追完才切 `STANDBY_UPDATING`。
- `transitToUpdateStandby()` **不幂等**：state 不是 `ACTIVE_RESTORING` 就抛异常。
- 三个调用点（`DefaultStateUpdater`）用**任务集合视角**判断
  （`updatingTasks.size()==1` / `onlyStandbyTasksUpdating()`），
  而方法前提是**reader 全局状态**——两者会脱钩。
- 脱钩场景：standby 被 pause 时 reader 状态**不重置**，仍停在 `STANDBY_UPDATING`；
  再 resume/add 时 `size()==1` 又成立 → 再次调用 → 已在 STANDBY_UPDATING → 抛异常。

**崩溃半径**（比"一个 task 崩"严重）：
```
坏转换 → state updater 线程崩 → 名下所有 task(updating+paused)集体标记失败
       → 冒泡回 StreamThread → 默认 SHUTDOWN_CLIENT → 整个实例关闭
```
（配了 `REPLACE_THREAD` 才只重建线程，不关实例。）

**修复方向**（二选一）：
1. 三个调用点前用 `changelogReader.isRestoringActive()` 守卫。
2. 让 `transitToUpdateStandby()` 幂等（已在 STANDBY_UPDATING 时 no-op）。

---

## 关键源码索引

- `streams/.../StreamThread.java` —— 主循环 `runOnceWithoutProcessingThreads`、`checkStateUpdater`
- `streams/.../TaskManager.java` —— `addTaskToStateUpdater`（交接点 line 959）、`handleAssignment`
- `streams/.../DefaultStateUpdater.java` —— 后台 `runOnce`、`restoreTasks`、三个坏调用点
- `streams/.../StoreChangelogReader.java` —— `register`、`restore`、状态机、`transitToUpdateStandby`
- `streams/.../ChangeLoggingKeyValueBytesStore.java` —— `put` 触发 changelog 写
- `clients/.../BuiltInPartitioner.java` —— 默认分区算法 murmur2
