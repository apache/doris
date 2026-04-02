# Module 5: FE侧Publish后台线程

## 概述

本模块设计FE侧的Publish后台线程（`CloudPublishDaemon`），负责将Commit阶段完成后加入`committed txns set`的事务异步推进到VISIBLE状态。

**核心流程**：遍历committed事务 → 按BE分组下发CalcDeleteBitmapTask（每个BE一个请求包含所有涉及的tablet） → BE独立完成每个tablet的全流程（calc bitmap → update bitmap to MS → 拿MS tablet锁 → convert rowset → local apply） → BE返回per-tablet结果 → 所有tablet成功后FE调用轻量级publish。

**关键设计**：
- **Per-BE批量下发**：类似当前`sendCalcDeleteBitmaptask()`的模式，按BE分组
- **BE独立完成全流程**：FE不需要调用MS convert_tmp_rowset或通知BE apply
- **简化的FE端状态**：FE只跟踪per-BE任务状态（PENDING/SENT/DONE/FAILED），不需要per-tablet的复杂状态机
- **版本连续性检查在BE侧**：BE检查`max_version+1 == commit_version`，不满足返回`DELETE_BITMAP_LOCK_ERROR`，FE下轮重试

本模块依赖：
- Module 0（Proto/KV Schema）
- Module 2（MS侧Per-Tablet Rowset转正）
- Module 3（MS侧轻量级Publish）
- Module 4（FE侧Commit阶段 + Committed Txns管理）

---

## A. 参考：存算一体 PublishVersionDaemon 的工作方式

### 源码位置

`fe/fe-core/src/main/java/org/apache/doris/transaction/PublishVersionDaemon.java`

### 基本架构

`PublishVersionDaemon` 继承 `MasterDaemon`，以可配置间隔（`Config.publish_version_interval_ms`，默认10ms）周期执行。内部维护一个线程池数组（`Config.publish_thread_pool_num` 个单线程池，默认128个）用于并行finish事务。

### 主循环流程

1. **获取待publish事务**：调用 `GlobalTransactionMgr.getReadyToPublishTransactions()` 获取所有状态为COMMITTED的事务列表。
2. **下发PublishVersionTask**：遍历每个待publish事务，为每个涉及的BE生成 `PublishVersionTask`，加入 `AgentTaskQueue`，通过 `AgentBatchTask` 批量下发到BE。使用 `transactionState.setSendedTask()` 标记已发送，避免重复下发。
3. **尝试完成事务**：遍历所有待publish事务，检查各BE上的 `PublishVersionTask` 是否已完成：
   - 如果所有存活BE的任务都完成，或者超时，或者满足慢节点跳过条件，则尝试finish事务。
   - finish时如果启用了 `enable_parallel_publish_version`，则路由到线程池异步执行；否则同步执行。
4. **finish事务**：调用 `GlobalTransactionMgr.finishTransaction()` 将事务状态从COMMITTED改为VISIBLE，完成后从 `AgentTaskQueue` 清理任务。
5. **发送visible version**：将最新的partition visible version批量发送给相关BE。

### 关键设计要点

- **Agent Task机制**：任务加入 `AgentTaskQueue` 后，BE心跳中会通过 `getDiffTasks` 获取未执行的任务（即FE记录了但BE未在running中的任务），实现**失败自动重发**。BE执行完毕后通过 `finishTask` RPC回调FE的 `MasterImpl.finishCalcDeleteBitmap()`，标记任务完成。
- **CountDownLatch同步**：当前Cloud模式下 `CalcDeleteBitmapTask` 使用 `MarkedCountDownLatch` 做同步等待——commit线程阻塞等待所有BE完成bitmap计算，超时后抛出异常。
- **路由策略**：`enable_per_txn_publish` 为true时按txnId路由到线程池，使同DB不同事务可并行finish；否则按dbId路由保证同DB事务串行。

---

## B. 新 CloudPublishDaemon 设计

### B.1 设计目标

1. 将原本在commit线程中同步执行的 delete bitmap 计算和等待，改为异步后台执行。
2. 保证同一 partition 上 tablet 的版本连续性（version N 的事务必须在 version N-1 完成后才能开始）。
3. 不同 partition 的 tablet 之间完全并行。
4. 同一事务内的不同 tablet 完全并行。
5. 事务级别的并行——不同事务如果涉及不同 partition，可以完全并行推进。

### B.2 线程模型

```
CloudPublishDaemon（调度线程，继承 MasterDaemon）
  │
  ├── 周期运行（cloud_publish_interval_ms，默认 100ms）
  │
  └── 提交任务到 publishExecutorPool（固定大小线程池）
       │
       ├── Worker-1: 处理 partition-A 的 tablet publish 流水线
       ├── Worker-2: 处理 partition-B 的 tablet publish 流水线
       ├── Worker-3: 处理 partition-C 的 tablet publish 流水线
       └── ...
```

采用**一个调度线程 + 线程池执行**的模型：

- **调度线程**（`CloudPublishDaemon`）：继承 `MasterDaemon`，周期执行调度逻辑。负责遍历 committed txns set、检查版本依赖关系、为就绪的 tablet 提交任务到线程池。不执行任何阻塞操作。
- **执行线程池**（`cloudPublishExecutorPool`）：`ThreadPoolManager.newDaemonFixedThreadPool`，大小可配置（`cloud_publish_thread_pool_size`，默认 32）。执行具体的 tablet publish 流水线步骤（下发 CalcDeleteBitmapTask、调用 MS convert_tmp_rowset、通知 BE apply 等）。

### B.3 核心数据结构

```java
public class CloudPublishDaemon extends MasterDaemon {

    // 执行线程池
    private final ExecutorService publishExecutorPool;

    // 事务级别的publish状态跟踪
    // key: txnId, value: TxnPublishContext
    private final ConcurrentHashMap<Long, TxnPublishContext> publishingTxns = new ConcurrentHashMap<>();

    // 每个 <partitionId, tabletId> 最近一次完成的 commit version
    // 用于版本连续性检查
    // key: partitionId -> (tabletId -> lastAppliedCommitVersion)
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>> tabletAppliedVersions
            = new ConcurrentHashMap<>();
}

/**
 * 单个事务的publish上下文
 */
public class TxnPublishContext {
    private final long txnId;
    private final long dbId;
    private final long commitVersion;  // 本事务的 commit version（partition 粒度）
    private final long commitTimeMs;
    private final CountDownLatch publishLatch;  // 导入线程等待 publish 完成

    // 事务涉及的所有 tablet 的状态
    // key: tabletId, value: TabletPublishState
    private final ConcurrentHashMap<Long, TabletPublishState> tabletStates;

    // tablet 所属 partition 的映射
    private final Map<Long, Long> tabletToPartition;

    // 事务涉及的表信息（用于后续 MS RPC）
    private final List<OlapTable> mowTableList;

    // 失败重试计数
    private final AtomicInteger retryCount = new AtomicInteger(0);

    // 是否所有 tablet 都已完成
    public boolean isAllTabletsCompleted() { ... }
}
```

### B.4 主循环逻辑

```java
@Override
protected void runAfterCatalogReady() {
    try {
        schedulePublish();
    } catch (Throwable t) {
        LOG.error("errors in cloud publish daemon", t);
    }
}

private void schedulePublish() {
    // 1. 从 CommittedTxnManager（Module 4）获取所有待 publish 的事务
    List<CommittedTxnInfo> committedTxns = committedTxnManager.getCommittedTxns();
    if (committedTxns.isEmpty()) {
        return;
    }

    for (CommittedTxnInfo txnInfo : committedTxns) {
        long txnId = txnInfo.getTxnId();

        // 2. 如果这个事务还没开始 publish，创建 TxnPublishContext
        if (!publishingTxns.containsKey(txnId)) {
            TxnPublishContext context = buildTxnPublishContext(txnInfo);
            publishingTxns.putIfAbsent(txnId, context);
        }

        TxnPublishContext context = publishingTxns.get(txnId);

        // 3. 遍历事务的每个 tablet，检查状态并推进
        for (Map.Entry<Long, TabletPublishState> entry : context.getTabletStates().entrySet()) {
            long tabletId = entry.getKey();
            TabletPublishState state = entry.getValue();

            // 只处理可以推进的 tablet
            if (state.getPhase() == PublishPhase.PENDING) {
                // 检查版本连续性
                if (!isVersionReady(context, tabletId)) {
                    continue;  // 前置版本未完成，跳过
                }
                // 提交到线程池执行
                submitTabletPublish(context, tabletId, state);
            }
            // 对于已经在执行中的 tablet（CALC_BITMAP_SENT 等），由回调自动推进
        }

        // 4. 检查是否所有 tablet 已完成
        if (context.isAllTabletsCompleted()) {
            // 提交轻量级 publish
            submitLightweightPublish(context);
        }
    }
}
```

### B.5 Tablet Publish 流水线（在线程池中执行）

```java
private void submitTabletPublish(TxnPublishContext context, long tabletId, TabletPublishState state) {
    state.setPhase(PublishPhase.CALC_BITMAP_SENT);

    publishExecutorPool.submit(() -> {
        try {
            // Step 1: 下发 CalcDeleteBitmapTask 到 BE
            sendCalcDeleteBitmapAndWait(context, tabletId, state);
            // 任务完成后状态已在回调中更新为 BITMAP_DONE

            // Step 2: 调用 MS convert_tmp_rowset
            state.setPhase(PublishPhase.CONVERTING);
            convertTmpRowset(context, tabletId);
            state.setPhase(PublishPhase.CONVERTED);

            // Step 3: 通知 BE 本地 apply rowset
            state.setPhase(PublishPhase.APPLYING);
            notifyBeApplyRowset(context, tabletId);
            state.setPhase(PublishPhase.APPLIED);

            // Step 4: 更新 tabletAppliedVersions 以便后续事务检查
            updateTabletAppliedVersion(context, tabletId);

        } catch (Exception e) {
            LOG.warn("tablet publish failed, txnId={}, tabletId={}, phase={}",
                    context.getTxnId(), tabletId, state.getPhase(), e);
            state.setPhase(PublishPhase.FAILED);
            state.setErrorMsg(e.getMessage());
            state.incrementRetryCount();
            // 下一轮调度时重试（重置为 PENDING）
            if (state.getRetryCount() < MAX_RETRY_PER_TABLET) {
                state.setPhase(PublishPhase.PENDING);
            }
        }
    });
}
```

### B.6 轻量级 Publish

```java
private void submitLightweightPublish(TxnPublishContext context) {
    publishExecutorPool.submit(() -> {
        try {
            // 调用 MS 轻量级 publish：visible_version + 1
            // 参见 Module 3 的 MS RPC 设计
            msLightweightPublish(context.getDbId(), context.getTxnId());

            // 从 committed txns set 移除
            committedTxnManager.removeTxn(context.getTxnId());

            // 从 publishingTxns 移除
            publishingTxns.remove(context.getTxnId());

            // 唤醒等待 publish 完成的导入线程
            context.getPublishLatch().countDown();

            LOG.info("cloud publish completed, txnId={}, commitVersion={}",
                    context.getTxnId(), context.getCommitVersion());
        } catch (Exception e) {
            LOG.warn("lightweight publish failed, txnId={}, will retry", context.getTxnId(), e);
            // 不改变 tablet 状态，下一轮重试轻量级 publish
        }
    });
}
```

---

## C. Per-Tablet 状态机

### C.1 状态定义

```java
public enum PublishPhase {
    PENDING,            // 初始状态，等待版本依赖就绪
    CALC_BITMAP_SENT,   // CalcDeleteBitmapTask 已下发到 BE，等待完成
    BITMAP_DONE,        // delete bitmap 计算完成（BE 已将 bitmap 写入 MS）
    CONVERTING,         // 正在调用 MS convert_tmp_rowset 转正
    CONVERTED,          // MS 转正完成
    APPLYING,           // 正在通知 BE 本地 apply rowset
    APPLIED,            // BE 本地 apply 完成，tablet 的 max_version 已更新
    FAILED              // 执行失败，等待重试
}
```

### C.2 状态转换图

```
                         ┌─────────────────────────────┐
                         │           FAILED             │
                         │  (重试次数 < MAX_RETRY       │
                         │   时回退到 PENDING)          │
                         └──────┬──────────────────────┘
                                │ 重试
                                v
┌──────────┐  版本就绪   ┌──────────────────┐  BE完成    ┌─────────────┐
│ PENDING  │ ─────────> │ CALC_BITMAP_SENT │ ────────> │ BITMAP_DONE │
└──────────┘            └──────────────────┘           └──────┬──────┘
                                                              │
                                                              v
                        ┌──────────┐  MS转正完成  ┌────────────┐
                        │CONVERTED │ <──────────  │ CONVERTING │
                        └─────┬────┘              └────────────┘
                              │
                              v
                        ┌──────────┐  BE apply    ┌──────────┐
                        │ APPLYING │ ──────────>  │ APPLIED  │
                        └──────────┘              └──────────┘
```

### C.3 状态转换条件

| 当前状态 | 目标状态 | 转换条件 |
|---------|---------|---------|
| PENDING | CALC_BITMAP_SENT | 前置版本依赖满足（同 tablet 的前序事务已达到 APPLIED）+ 线程池提交成功 |
| CALC_BITMAP_SENT | BITMAP_DONE | BE 回调 `finishCalcDeleteBitmap` 报告成功 |
| CALC_BITMAP_SENT | FAILED | BE 回调报告失败，或等待超时 |
| BITMAP_DONE | CONVERTING | 上一步完成后立即转换（同一线程池任务中） |
| CONVERTING | CONVERTED | MS `convert_tmp_rowset` RPC 返回成功 |
| CONVERTING | FAILED | MS RPC 失败 |
| CONVERTED | APPLYING | 上一步完成后立即转换 |
| APPLYING | APPLIED | BE apply rowset 完成 |
| APPLYING | FAILED | BE 不可达或 apply 失败 |
| FAILED | PENDING | 重试次数未超限时，由调度线程下一轮重置 |

### C.4 状态持久化说明

Per-tablet 的 publish 状态**不需要持久化**。原因：
- 所有状态可以从 MS 中恢复：已 committed 的事务信息持久化在 MS 的 TxnInfoPB 中。
- FE重启/切主后，从 MS 恢复 committed txns set（参见 Module 6），重新从 PENDING 开始推进。
- 已经完成 `convert_tmp_rowset` 的 tablet 在 MS 中已标记，重新执行时 MS 可以识别幂等性。

---

## D. 版本连续性处理

### D.1 问题描述

对于 MOW 表，delete bitmap 的计算依赖于前序版本的 rowset 数据。对于同一个 tablet，version N+1 的事务在计算 delete bitmap 时，必须能看到 version N 的 rowset 数据（包括 delete bitmap）。因此：

- **同一 tablet**：version N+1 的事务必须等待 version N 的事务在该 tablet 上达到 APPLIED 状态后才能开始 CALC_BITMAP_SENT。
- **不同 tablet**：即使属于同一 partition，不同 tablet 之间无依赖（各自独立管理 delete bitmap）。
- **不同 partition**：完全独立，无任何依赖。

### D.2 版本追踪机制

使用一个内存数据结构记录每个 tablet 最近完成 apply 的 commit version：

```java
// partitionId -> (tabletId -> lastAppliedCommitVersion)
ConcurrentHashMap<Long, ConcurrentHashMap<Long, Long>> tabletAppliedVersions;
```

当一个 tablet 完成 APPLIED 状态转换时，更新此映射：

```java
private void updateTabletAppliedVersion(TxnPublishContext context, long tabletId) {
    long partitionId = context.getTabletToPartition().get(tabletId);
    long commitVersion = context.getCommitVersion();
    tabletAppliedVersions
        .computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>())
        .merge(tabletId, commitVersion, Math::max);
}
```

### D.3 版本就绪检查

```java
private boolean isVersionReady(TxnPublishContext context, long tabletId) {
    long partitionId = context.getTabletToPartition().get(tabletId);
    long commitVersion = context.getCommitVersion();

    // 如果是 partition 的第一个 commit version（commitVersion == visibleVersion + 1），
    // 不需要等待前置版本
    if (commitVersion <= getPartitionVisibleVersion(partitionId) + 1) {
        return true;
    }

    // 检查前序版本（commitVersion - 1）是否在该 tablet 上已完成 apply
    long requiredVersion = commitVersion - 1;
    ConcurrentHashMap<Long, Long> partitionTablets = tabletAppliedVersions.get(partitionId);
    if (partitionTablets == null) {
        return false;
    }
    Long appliedVersion = partitionTablets.get(tabletId);
    return appliedVersion != null && appliedVersion >= requiredVersion;
}
```

### D.4 性能优化

1. **按 partition 分组处理**：调度线程先按 partition 分组事务，同一 partition 内按 commitVersion 排序，确保低版本优先调度。
2. **跳过不涉及的 tablet**：如果事务 A 涉及 tablet-1 和 tablet-2，事务 B 只涉及 tablet-1，则事务 B 的 tablet-1 需要等事务 A 的 tablet-1 完成，但事务 A 的 tablet-2 完成与否不影响事务 B。
3. **FE 重启后初始化**：从 MS 获取每个 partition 的 visible version 作为初始值。在此之上的 committed 事务从 PENDING 开始重新推进，version 就绪检查自然满足。

---

## E. 失败重试

### E.1 CalcDeleteBitmapTask 失败重试

**Agent Task 自动重发机制**：
- CalcDeleteBitmapTask 加入 `AgentTaskQueue` 后，如果 BE 没有在 running 任务中汇报该任务，FE 会在 BE 心跳时通过 `getDiffTasks` 自动重新下发。这保证了网络抖动等瞬时故障下的自动恢复。
- 如果 BE 通过 `finishTask` 报告失败（如 `DELETE_BITMAP_LOCK_ERROR`），则由 Publish Daemon 将 tablet 状态重置为 PENDING，下一轮调度时重新下发。

**超时处理**：
- 每个 CalcDeleteBitmapTask 设置超时时间（`cloud_publish_calc_bitmap_timeout_seconds`，默认 120s）。
- 超时后将 tablet 状态置为 FAILED，递增重试计数器，重置为 PENDING。
- 不使用 `MarkedCountDownLatch` 同步等待（区别于现有同步模式），而是通过回调异步推进状态。

**重试策略**：
```java
// Per-tablet 重试限制
private static final int MAX_RETRY_PER_TABLET = 10;

// 重试退避
private long getRetryDelayMs(int retryCount) {
    // 指数退避：100ms, 200ms, 400ms, ... 最大 30s
    return Math.min(100L * (1L << retryCount), 30_000L);
}
```

### E.2 MS RPC 失败重试

**convert_tmp_rowset RPC**：
- 幂等操作（MS 端通过检查 rowset 是否已转正来保证幂等）。
- 重试 `Config.metaServiceRpcRetryTimes()` 次，每次间隔随机 [20, 200]ms（与现有 MS RPC 重试策略一致）。
- 如果遇到 `KV_TXN_CONFLICT`，重试。
- 重试耗尽后将 tablet 状态置为 FAILED。

**轻量级 publish RPC**：
- 同样幂等（MS 端检查 visible version 是否已更新）。
- 失败后整个轻量级 publish 重试，不影响已完成的 tablet 状态。

### E.3 BE 不可达的处理

**核心优势**：在异步发布中，delete bitmap 计算不依赖特定 BE（区别于存算一体，数据存储在共享存储上）。因此：

```java
private long selectBeForCalcBitmap(TxnPublishContext context, long tabletId) {
    // 优先选择原始提交时使用的 BE（已有本地缓存）
    long preferredBe = context.getOriginalBackendForTablet(tabletId);
    if (isBackendAlive(preferredBe)) {
        return preferredBe;
    }

    // 原始 BE 不可达，选择任意存活的 BE
    // 因为是存算分离，任何 BE 都可以从共享存储读取数据计算 delete bitmap
    List<Long> aliveBackends = Env.getCurrentSystemInfo().getAllBackendIds(true);
    if (aliveBackends.isEmpty()) {
        throw new RuntimeException("No alive backend available for calc delete bitmap");
    }

    // 负载均衡：按照当前 pending task 数选择负载最低的 BE
    return selectLeastLoadedBackend(aliveBackends);
}
```

**BE apply rowset 失败**：
- 如果通知 BE apply 时 BE 不可达，将 tablet 状态回退到 CONVERTED。
- 下一轮重试时，可以选择其他 BE 来 apply（需要该 BE 先从共享存储加载 rowset meta）。
- 或者等原始 BE 恢复后重试。

---

## F. 和导入等待的交互

### F.1 等待机制设计

导入线程在 commit 阶段完成后，需要等待 publish 完成才能返回用户导入成功。使用 `CountDownLatch` 实现等待：

```java
// === 在 commit 路径中（Module 4 改造后） ===
public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList,
        long transactionId, List<TabletCommitInfo> tabletCommitInfos,
        long timeoutMillis) throws UserException {

    // 1. 执行快速 commit（只更新 MS commit version）
    commitTransactionFast(db, tableList, transactionId, tabletCommitInfos);

    // 2. 创建 publish 等待 latch
    CountDownLatch publishLatch = new CountDownLatch(1);

    // 3. 将事务加入 committed txns set，附带 latch
    committedTxnManager.addTxn(transactionId, dbId, tabletCommitInfos,
            mowTableList, publishLatch);

    // 4. 等待 publish 完成或超时
    boolean published;
    try {
        published = publishLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
        LOG.warn("interrupted while waiting for publish, txnId={}", transactionId);
        published = false;
    }

    if (!published) {
        // 超时但不是错误——事务已 committed，publish 会继续在后台进行
        LOG.warn("publish timeout for txn {}, will continue in background", transactionId);
        return false;  // 返回 false 表示 publish 超时
    }

    return true;  // publish 完成
}
```

### F.2 超时行为

```
导入线程                    CloudPublishDaemon
   │                              │
   ├── commit (快速) ────────────>│
   │                              │
   ├── await(publishLatch,        │── 遍历 committed txns
   │         timeout)             │── 推进 tablet publish
   │    │                         │── ...
   │    │                         │── 所有 tablet APPLIED
   │    │                         │── lightweight publish
   │    │                         │── publishLatch.countDown()
   │    │<────────────────────────│
   │    v                         │
   ├── return true                │
   │   (publish 完成)             │
   │                              │
   │  === 超时情况 ===             │
   │    │                         │── 仍在推进中...
   │    v (timeout)               │
   ├── return false               │
   │   (publish 超时)             │
   │   但事务已 committed         │
   │   后台继续 publish           │── 最终完成 publish
                                  │── countDown()（虽然没人等了）
```

### F.3 超时返回给用户的语义

- `return true`：事务已 publish，对用户可见。
- `return false`：事务已 committed，保证最终会 publish，但当前尚未完成。用户可以通过 `show transaction` 查看状态。
- 抛出异常：commit 阶段失败，事务未提交。

### F.4 相关配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `cloud_publish_interval_ms` | 100 | CloudPublishDaemon 调度间隔（毫秒） |
| `cloud_publish_thread_pool_size` | 32 | publish 执行线程池大小 |
| `cloud_publish_calc_bitmap_timeout_seconds` | 120 | 单次 CalcDeleteBitmapTask 超时时间 |
| `cloud_publish_max_retry_per_tablet` | 10 | 单个 tablet 最大重试次数 |
| `cloud_publish_timeout_seconds` | 300 | 导入等待 publish 完成的默认超时时间 |

---

## G. 监控和告警

### G.1 Metrics

| Metric 名称 | 类型 | 说明 |
|-------------|------|------|
| `cloud_publish_committed_txn_count` | Gauge | 当前 committed txns set 中等待 publish 的事务数量。如果持续增长，说明 publish 速度跟不上 commit 速度。 |
| `cloud_publish_tablet_pending_count` | Gauge | 当前处于 PENDING 状态的 tablet 数量。 |
| `cloud_publish_tablet_in_progress_count` | Gauge | 当前正在执行 publish 流水线的 tablet 数量。 |
| `cloud_publish_latency` | Histogram | 从事务 commit 到 publish 完成的延迟分布。 |
| `cloud_publish_tablet_latency` | Histogram | 单个 tablet 完成 publish 流水线的延迟分布。 |
| `cloud_publish_calc_bitmap_latency` | Histogram | CalcDeleteBitmapTask 执行耗时分布。 |
| `cloud_publish_convert_rowset_latency` | Histogram | MS convert_tmp_rowset RPC 耗时分布。 |
| `cloud_publish_retry_count` | Counter | tablet publish 重试总次数。 |
| `cloud_publish_version_gap` | Gauge | 每个 partition 的 commit_version - visible_version 的最大值。反映 publish 积压程度。 |

### G.2 告警规则建议

| 告警条件 | 级别 | 说明 |
|---------|------|------|
| `cloud_publish_committed_txn_count > 1000` 持续 5 分钟 | WARNING | publish 积压，可能线程池不够 |
| `cloud_publish_committed_txn_count > 5000` 持续 5 分钟 | CRITICAL | 严重积压，需要立即处理 |
| `cloud_publish_version_gap > 100` 持续 10 分钟 | WARNING | 某个 partition 的 publish 严重滞后 |
| `cloud_publish_retry_count` 5 分钟内增量 > 1000 | WARNING | 大量重试，可能 BE/MS 有问题 |

### G.3 日志

关键日志点（均带 txnId 和 tabletId 方便排查）：
- 事务开始 publish：`LOG.info("start publish txnId={}, tabletCount={}")`
- tablet 状态转换：`LOG.info("tablet state change, txnId={}, tabletId={}, {} -> {}")`
- tablet publish 完成：`LOG.info("tablet publish done, txnId={}, tabletId={}, costMs={}")`
- 事务 publish 完成：`LOG.info("txn publish done, txnId={}, totalCostMs={}, tabletCount={}")`
- 失败重试：`LOG.warn("tablet publish retry, txnId={}, tabletId={}, retryCount={}, error={}")`

---

## H. 关键文件和修改点

### H.1 新增文件

| 文件路径 | 说明 |
|---------|------|
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CloudPublishDaemon.java` | 核心：Publish 后台调度线程 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/TxnPublishContext.java` | 事务 publish 上下文 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/TabletPublishState.java` | Per-tablet 状态机 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/PublishPhase.java` | Tablet publish 阶段枚举 |

### H.2 修改文件

| 文件路径 | 修改内容 |
|---------|---------|
| `fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java` | 启动 `CloudPublishDaemon` 线程（在 cloud 模式下替代/补充原有逻辑） |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CloudGlobalTransactionMgr.java` | 改造 `commitAndPublishTransaction`：commit 阶段不再同步等待 CalcDeleteBitmapTask，改为加入 committed txns set + CountDownLatch 等待 |
| `fe/fe-core/src/main/java/org/apache/doris/task/CalcDeleteBitmapTask.java` | 适配异步回调模式：移除 `MarkedCountDownLatch`，改为状态回调方式。新增针对单个 tablet 的任务粒度（现有是 per-BE 粒度）。或者保留现有机制，CloudPublishDaemon 中按新方式使用 |
| `fe/fe-core/src/main/java/org/apache/doris/master/MasterImpl.java` | 修改 `finishCalcDeleteBitmap`，支持异步回调通知 `CloudPublishDaemon` |
| `fe/fe-common/src/main/java/org/apache/doris/common/Config.java` | 新增 `cloud_publish_*` 系列配置项 |

### H.3 依赖的 Module 4 接口

- `CommittedTxnManager.getCommittedTxns()`：获取所有待 publish 的事务列表
- `CommittedTxnManager.addTxn(txnId, ...)`：commit 后加入待 publish 集合
- `CommittedTxnManager.removeTxn(txnId)`：publish 完成后移除

### H.4 依赖的 Module 2/3 MS RPC

- `MetaServiceProxy.convertTmpRowset(ConvertTmpRowsetRequest)`：per-tablet rowset 转正
- `MetaServiceProxy.lightweightPublish(LightweightPublishRequest)`：轻量级 publish（visible version + 1）

---

## I. 单元测试要点

### I.1 状态机测试

- **正常流水线**：验证 tablet 从 PENDING -> CALC_BITMAP_SENT -> BITMAP_DONE -> CONVERTING -> CONVERTED -> APPLYING -> APPLIED 的完整状态转换。
- **状态合法性**：验证不允许的状态跳转被拒绝（如 PENDING 直接到 BITMAP_DONE）。

### I.2 版本连续性测试

- **基本依赖**：两个事务 A（commitVersion=2）和 B（commitVersion=3）涉及同一 tablet，验证 B 的 tablet 在 A 的 tablet 达到 APPLIED 之前不会被调度。
- **不同 tablet 无依赖**：事务 A 涉及 tablet-1 和 tablet-2，事务 B 只涉及 tablet-1。B 的 tablet-1 需要等 A 的 tablet-1，但不需要等 A 的 tablet-2。
- **不同 partition 并行**：两个事务分别涉及不同 partition 的 tablet，验证它们并行推进。
- **连续版本链**：三个事务 A（v=2）、B（v=3）、C（v=4）涉及同一 tablet，验证它们按顺序处理。

### I.3 失败重试测试

- **CalcDeleteBitmapTask 失败重试**：Mock BE 返回失败，验证 tablet 状态回退到 PENDING 并在下一轮重试。
- **MS RPC 失败重试**：Mock convert_tmp_rowset 失败，验证重试逻辑和退避策略。
- **BE 切换**：Mock 原始 BE 宕机，验证 CalcDeleteBitmapTask 被发送到其他存活 BE。
- **重试次数耗尽**：验证超过 MAX_RETRY_PER_TABLET 后 tablet 状态停留在 FAILED，不再重试。

### I.4 并发测试

- **多事务并行**：多个事务涉及不同 partition，验证它们同时推进。
- **线程池满**：提交超过线程池大小的任务，验证不会丢失任务。
- **调度线程和执行线程并发**：验证调度线程遍历 tablet 状态时不会与执行线程的状态更新产生竞态。

### I.5 导入等待测试

- **正常完成**：导入线程等待 publishLatch，publish 完成后被唤醒。
- **超时返回**：publish 未在超时时间内完成，导入线程超时返回 false。
- **commit 失败不等待**：commit 阶段失败时，不应加入 committed txns set，不应创建 latch。

### I.6 轻量级 Publish 测试

- **所有 tablet 完成触发**：验证只有所有 tablet 都达到 APPLIED 后才触发轻量级 publish。
- **部分 tablet 完成不触发**：验证有 tablet 未完成时不会触发轻量级 publish。
- **轻量级 publish 失败重试**：Mock MS RPC 失败，验证下一轮重新尝试轻量级 publish。

### I.7 监控测试

- 验证各 Metric 在正常和异常流程中正确更新。
- 验证 `cloud_publish_version_gap` 在 publish 积压时正确增长，完成后正确下降。

### I.8 Mock 策略

测试时需要 Mock 的外部依赖：
- `MetaServiceProxy`：Mock MS 的 convert_tmp_rowset 和 lightweight_publish RPC。
- `AgentTaskExecutor`：Mock CalcDeleteBitmapTask 的下发和 BE 回调。
- `Env.getCurrentSystemInfo()`：Mock BE 列表和存活状态。
- `CommittedTxnManager`：可使用内存实现。
