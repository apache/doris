# Module 4: FE侧Commit阶段 + Committed Txns管理

## 1. 当前Commit流程分析

### 1.1 当前一阶段提交的完整步骤

当前在 `CloudGlobalTransactionMgr` 中，`commitAndPublishTransaction()` 方法的完整调用链如下（以普通导入为例）：

```
commitAndPublishTransaction()                    // 入口（含重试逻辑）
  └─ commitAndPublishTransaction(带attachment)    // 实际执行
       ├─ beforeCommitTransaction()              // 步骤1: 获取FE表级commit锁
       ├─ commitTransactionWithoutLock()          // 步骤2-5全在锁内
       │    ├─ getMowTableList()                  // 识别MOW表
       │    ├─ getDeleteBitmapUpdateLock()         // 步骤2: 获取MS分布式表锁
       │    │    ├─ getPartitionInfo()             //   构建tablet/partition映射
       │    │    └─ MetaServiceProxy.getDeleteBitmapUpdateLock()  //   RPC获取锁
       │    ├─ getCalcDeleteBitmapInfo()           // 步骤3: 获取partition version
       │    │    └─ getPartitionVersions()         //   从MS或本地缓存获取version
       │    ├─ sendCalcDeleteBitmaptask()          // 步骤4: 下发delete bitmap计算
       │    │    ├─ AgentTaskExecutor.submit()      //   发送到BE
       │    │    └─ countDownLatch.await()           //   同步等待BE计算完成
       │    ├─ builder.build() CommitTxnRequest    // 步骤5: 构建提交请求
       │    └─ executeCommitTxnRequest()           // 步骤5: 提交到MS
       │         └─ commitTxn()                    //   RPC调用MS commit_txn
       │              └─ afterCommitTxnResp()       //   更新版本、通知follower
       └─ afterCommitTransaction()                // 步骤6: 释放FE表级commit锁
```

### 1.2 持FE表锁期间的各步骤耗时分析

FE表级commit锁（`Table.commitLock`，一个 ReentrantLock）在 `beforeCommitTransaction()` 中获取，在 `afterCommitTransaction()` 中释放。**所有MOW相关操作都在锁内完成**，这是当前方案的核心瓶颈。

| 步骤 | 操作 | 持锁 | 典型耗时 | 说明 |
|------|------|:----:|---------|------|
| 1 | `beforeCommitTransaction()` 获取FE commit锁 | 获取锁 | 0-数秒（等待） | `tryCommitLock` 超时由 `try_commit_lock_timeout_seconds` 控制（默认5s） |
| 2 | `getDeleteBitmapUpdateLock()` 获取MS分布式表锁 | 是 | 数十ms-数秒 | RPC到MS，可能因锁冲突重试。与compaction互斥 |
| 3 | `getPartitionVersions()` 获取partition version | 是 | 数ms-数十ms | 从MS获取或本地缓存读取 |
| 4 | `sendCalcDeleteBitmaptask()` 下发并等待delete bitmap计算 | 是 | **数百ms-数十秒** | **最慢**。同步等待BE完成计算，超时60s |
| 5 | `executeCommitTxnRequest()` 提交到MS | 是 | 数十ms-数百ms | RPC调用，KV操作量与tablet数量成正比 |
| 6 | `afterCommitTransaction()` 释放锁、清理 | 释放锁 | <1ms | |

**核心问题**：步骤2-5全在FE表锁内串行执行，其中步骤4（delete bitmap计算同步等待）耗时最长，严重阻塞其他并发导入获取同一张表的commit锁。

### 1.3 新方案中各步骤的归属

| 步骤 | 新方案归属 | 说明 |
|------|-----------|------|
| 获取FE commit锁 | **Commit阶段**（保留） | 与其他并发导入互斥，但持锁时间极短 |
| 获取MS分布式表锁 | **移除** | 异步发布不再需要MS分布式锁 |
| 获取partition version | **Publish阶段** | 在后台线程中执行 |
| delete bitmap计算 | **Publish阶段** | 在后台线程中异步下发 |
| 提交到MS | **Commit阶段**（仅轻量commit） | 仅更新 partition commit version+1 和 TxnInfoPB |
| 版本更新和通知 | **Publish阶段** | 在轻量级publish完成后执行 |

---

## 2. Committed Txns 内存数据结构设计

### 2.1 数据结构定义

```java
package org.apache.doris.cloud.transaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 记录一个已经完成两阶段commit（但尚未publish）的事务的全部信息。
 * 生命周期：commit阶段加入 → publish完成后移除。
 */
public class CommittedTxnEntry {
    // ---- 基本标识 ----
    private final long txnId;
    private final long dbId;
    private final long tableId;

    // ---- Partition commit versions ----
    // commit阶段从MS返回的每个partition的commit version
    // Map<partitionId, commitVersion>
    private final Map<Long, Long> partitionCommitVersions;

    // ---- Tablet commit infos ----
    // 来自BE上报的tablet commit信息，publish阶段下发CalcDeleteBitmapTask时需要
    // 包含 tabletId 和所在 backendId
    private final List<TabletCommitInfo> tabletCommitInfos;

    // ---- 附加信息 ----
    private final TxnCommitAttachment txnCommitAttachment;
    private final long commitTimeMs;  // commit完成的时间戳，用于超时检测

    // ---- Publish等待机制 ----
    // 导入线程在commit完成后通过此latch等待publish完成
    private final CountDownLatch publishLatch = new CountDownLatch(1);
    // publish是否成功
    private volatile boolean publishSucceeded = false;
    // publish失败时的错误信息
    private volatile String publishErrorMsg = null;

    public CommittedTxnEntry(long txnId, long dbId, long tableId,
                             Map<Long, Long> partitionCommitVersions,
                             List<TabletCommitInfo> tabletCommitInfos,
                             TxnCommitAttachment txnCommitAttachment) {
        this.txnId = txnId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionCommitVersions = partitionCommitVersions;
        this.tabletCommitInfos = tabletCommitInfos;
        this.txnCommitAttachment = txnCommitAttachment;
        this.commitTimeMs = System.currentTimeMillis();
    }

    /** Publish线程调用：标记publish成功，唤醒等待的导入线程 */
    public void markPublishSucceeded() {
        this.publishSucceeded = true;
        this.publishLatch.countDown();
    }

    /** Publish线程调用：标记publish失败，唤醒等待的导入线程 */
    public void markPublishFailed(String errorMsg) {
        this.publishSucceeded = false;
        this.publishErrorMsg = errorMsg;
        this.publishLatch.countDown();
    }

    /** 导入线程调用：等待publish完成，返回是否超时 */
    public boolean awaitPublish(long timeoutMs) throws InterruptedException {
        return publishLatch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    // getter 方法省略
    public long getTxnId() { return txnId; }
    public long getDbId() { return dbId; }
    public long getTableId() { return tableId; }
    public Map<Long, Long> getPartitionCommitVersions() { return partitionCommitVersions; }
    public List<TabletCommitInfo> getTabletCommitInfos() { return tabletCommitInfos; }
    public TxnCommitAttachment getTxnCommitAttachment() { return txnCommitAttachment; }
    public long getCommitTimeMs() { return commitTimeMs; }
    public boolean isPublishSucceeded() { return publishSucceeded; }
    public String getPublishErrorMsg() { return publishErrorMsg; }
}
```

### 2.2 Committed Txns Manager

```java
package org.apache.doris.cloud.transaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 管理所有已commit但尚未publish的异步发布事务。
 * 线程安全：使用 ConcurrentHashMap 存储，支持多线程并发访问。
 *
 * 存放位置：作为 CloudGlobalTransactionMgr 的成员变量。
 */
public class CommittedTxnManager {

    // 按txnId索引：快速查找单个事务
    // key: txnId, value: CommittedTxnEntry
    private final ConcurrentHashMap<Long, CommittedTxnEntry> txnIdToEntry = new ConcurrentHashMap<>();

    // 按tableId索引：publish后台线程按表遍历committed txns
    // key: tableId, value: {txnId -> CommittedTxnEntry}
    // 内层也使用 ConcurrentHashMap 保证线程安全
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, CommittedTxnEntry>> tableToTxns
            = new ConcurrentHashMap<>();

    /**
     * Commit阶段调用：将一个事务加入committed set。
     * 在FE commit锁内调用，但由于ConcurrentHashMap操作是O(1)，不会增加锁持有时间。
     */
    public void addCommittedTxn(CommittedTxnEntry entry) {
        txnIdToEntry.put(entry.getTxnId(), entry);
        tableToTxns.computeIfAbsent(entry.getTableId(), k -> new ConcurrentHashMap<>())
                   .put(entry.getTxnId(), entry);
    }

    /**
     * Publish完成后调用：移除一个事务。
     */
    public void removeCommittedTxn(long txnId) {
        CommittedTxnEntry entry = txnIdToEntry.remove(txnId);
        if (entry != null) {
            ConcurrentHashMap<Long, CommittedTxnEntry> tableTxns = tableToTxns.get(entry.getTableId());
            if (tableTxns != null) {
                tableTxns.remove(txnId);
                // 如果该表已无committed txn，清理空map（减少内存占用）
                if (tableTxns.isEmpty()) {
                    tableToTxns.remove(entry.getTableId(), tableTxns);
                }
            }
        }
    }

    /** 按txnId查找 */
    public CommittedTxnEntry getByTxnId(long txnId) {
        return txnIdToEntry.get(txnId);
    }

    /** 获取某张表所有待publish的事务（按commit时间排序） */
    public List<CommittedTxnEntry> getByTableId(long tableId) {
        ConcurrentHashMap<Long, CommittedTxnEntry> tableTxns = tableToTxns.get(tableId);
        if (tableTxns == null) {
            return List.of();
        }
        return tableTxns.values().stream()
                .sorted((a, b) -> Long.compare(a.getCommitTimeMs(), b.getCommitTimeMs()))
                .collect(Collectors.toList());
    }

    /** 获取所有有待publish事务的tableId集合 */
    public Collection<Long> getTablesWithCommittedTxns() {
        return tableToTxns.keySet();
    }

    /** 当前committed txn总数 */
    public int size() {
        return txnIdToEntry.size();
    }

    /** 判断某个txn是否在committed set中 */
    public boolean contains(long txnId) {
        return txnIdToEntry.containsKey(txnId);
    }
}
```

### 2.3 生命周期

```
                      ┌──────────────────────┐
                      │   导入BE数据写入完成    │
                      └──────────┬───────────┘
                                 │
                                 v
                    ┌────────────────────────┐
                    │  FE commitTransaction  │
                    │  获取FE commit锁       │
                    │  调用MS两阶段commit API  │
                    │  加入CommittedTxnManager │ <-- addCommittedTxn()
                    │  释放FE commit锁        │
                    └────────────┬───────────┘
                                 │
              ┌──────────────────┼──────────────────────┐
              │ 导入线程                                  │ 后台Publish线程
              │ awaitPublish(timeout)                    │ 遍历committed txns
              │ 阻塞等待...                               │ 下发delete bitmap计算
              │                                          │ 调用MS convert rowset
              │                                          │ 调用MS轻量级publish
              │                                          │ markPublishSucceeded()
              │ <───────────── 被唤醒 ──────────────────── │
              │                                          │ removeCommittedTxn()
              │ 返回成功/超时                              │
              └──────────────────────────────────────────┘
```

---

## 3. 新的Commit流程

### 3.1 流程概述

新的 `commitAndPublishTransaction()` 方法在启用异步发布的表上，执行以下步骤：

```java
// 伪代码
public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList,
        long transactionId, List<TabletCommitInfo> tabletCommitInfos,
        long timeoutMillis, TxnCommitAttachment txnCommitAttachment) throws UserException {

    OlapTable table = (OlapTable) tableList.get(0);  // 假设单表

    if (table.isEnableTwoPhaseCommit()) {
        // ===== 新的异步发布流程 =====
        return commitAndPublishTwoPhase(db, tableList, transactionId,
                tabletCommitInfos, timeoutMillis, txnCommitAttachment);
    } else {
        // ===== 原有一阶段提交流程（不变）=====
        return commitAndPublishOnePhase(db, tableList, transactionId,
                tabletCommitInfos, timeoutMillis, txnCommitAttachment);
    }
}
```

### 3.2 两阶段Commit详细步骤

```java
private boolean commitAndPublishTwoPhase(DatabaseIf db, List<Table> tableList,
        long transactionId, List<TabletCommitInfo> tabletCommitInfos,
        long timeoutMillis, TxnCommitAttachment txnCommitAttachment) throws UserException {

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    // ===== 步骤1: 获取FE表级commit锁 =====
    // 与其他并发导入互斥，和当前方案一样使用 beforeCommitTransaction()
    beforeCommitTransaction(tableList, transactionId, timeoutMillis);

    try {
        // ===== 步骤2: 调用MS两阶段commit API =====
        // 构建请求：仅需 dbId, txnId, tableId 和 tabletCommitInfos
        // MS端操作：partition commit version+1, 更新TxnInfoPB状态为COMMITTED
        // 返回值中包含每个partition的commit version
        CommitTxnResponse response = callMsTwoPhaseCommit(
                db.getId(), transactionId, tabletCommitInfos, txnCommitAttachment);

        // ===== 步骤3: 构建CommittedTxnEntry并加入内存set =====
        Map<Long, Long> partitionCommitVersions = extractPartitionVersions(response);
        CommittedTxnEntry entry = new CommittedTxnEntry(
                transactionId, db.getId(), tableList.get(0).getId(),
                partitionCommitVersions, tabletCommitInfos, txnCommitAttachment);
        committedTxnManager.addCommittedTxn(entry);

    } finally {
        // ===== 步骤4: 释放FE表级commit锁 =====
        afterCommitTransaction(tableList, transactionId);

        stopWatch.stop();
        LOG.info("two-phase commit transaction {} cost {} ms", transactionId, stopWatch.getTime());
    }

    // ===== 步骤5: 等待publish完成 =====
    // 注意：此处已释放FE表锁，不阻塞其他导入
    CommittedTxnEntry entry = committedTxnManager.getByTxnId(transactionId);
    boolean completed = entry.awaitPublish(timeoutMillis);

    if (!completed) {
        // publish超时，但commit已成功，后台线程会继续尝试publish
        LOG.warn("two-phase publish timeout for txn {}, will continue in background", transactionId);
        return false;  // 返回false表示publish未在超时时间内完成
    }

    if (!entry.isPublishSucceeded()) {
        throw new UserException("publish failed for txn " + transactionId
                + ": " + entry.getPublishErrorMsg());
    }

    return true;
}
```

### 3.3 Commit阶段持锁范围对比

| 对比项 | 当前方案（一阶段） | 新方案（两阶段） |
|--------|------------------|-----------------|
| **持FE锁期间的操作** | MS分布式锁 + partition version + delete bitmap计算 + MS提交 | **仅MS轻量commit RPC** |
| **持FE锁典型耗时** | 数百ms - 数十秒 | **数ms - 数十ms** |
| **MS RPC操作量** | O(tablets) KV写入 | **O(partitions) KV写入** |
| **是否同步等待BE** | 是（delete bitmap计算） | **否** |

### 3.4 等待机制设计

导入线程在commit完成后需要同步等待publish完成（或超时），使用 `CountDownLatch` 实现：

```
导入线程                     Publish后台线程
    │                             │
    │  commit完成                  │
    │  entry.awaitPublish(timeout) │
    │  ──阻塞──                    │
    │                             │  处理该txn的publish
    │                             │  所有tablet完成
    │                             │  调用MS轻量级publish
    │                             │  entry.markPublishSucceeded()
    │  <──被唤醒──                 │
    │  检查结果，返回               │
```

**超时处理**：
- 超时时间复用现有的 `timeoutMillis` 参数（由上层调用者传入）
- 超时后导入返回 `PUBLISH_TIMEOUT` 状态，但 **commit已成功**，后台线程会继续publish
- 用户可通过 `SHOW TRANSACTION` 查看事务的publish进度

**配置项**：

```java
// fe.conf 新增配置
@ConfField(mutable = true, description = {
    "异步发布模式下，导入等待publish完成的默认超时时间，单位秒。"
    + "超时后commit仍然有效，publish会在后台继续进行。默认 300s"})
public static int mow_async_publish_publish_timeout_seconds = 300;
```

---

## 4. 表属性设计

### 4.1 新增表属性

| 属性名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `enable_mow_async_publish` | boolean | false | 是否启用异步发布。仅对Cloud模式下的MOW表有效 |

### 4.2 PropertyAnalyzer中的定义

在 `PropertyAnalyzer.java` 中添加：

```java
// 文件: fe/fe-core/src/main/java/org/apache/doris/common/util/PropertyAnalyzer.java

public static final String PROPERTIES_ENABLE_TWO_PHASE_COMMIT = "enable_mow_async_publish";

public static boolean analyzeEnableTwoPhaseCommit(Map<String, String> properties)
        throws AnalysisException {
    if (properties == null || properties.isEmpty()) {
        return false;
    }
    String value = properties.get(PROPERTIES_ENABLE_TWO_PHASE_COMMIT);
    if (value == null) {
        return false;
    }
    properties.remove(PROPERTIES_ENABLE_TWO_PHASE_COMMIT);
    if (value.equalsIgnoreCase("true")) {
        return true;
    } else if (value.equalsIgnoreCase("false")) {
        return false;
    }
    throw new AnalysisException(PROPERTIES_ENABLE_TWO_PHASE_COMMIT + " must be `true` or `false`");
}
```

### 4.3 TableProperty中的存取

在 `TableProperty.java` 中添加：

```java
// 文件: fe/fe-core/src/main/java/org/apache/doris/catalog/TableProperty.java

// 成员变量
private boolean enableTwoPhaseCommit = false;

// setter（建表和ALTER TABLE时调用）
public void setEnableTwoPhaseCommit(boolean enable) {
    properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_TWO_PHASE_COMMIT, Boolean.toString(enable));
    this.enableTwoPhaseCommit = enable;
}

// getter
public boolean getEnableTwoPhaseCommit() {
    return Boolean.parseBoolean(properties.getOrDefault(
            PropertyAnalyzer.PROPERTIES_ENABLE_TWO_PHASE_COMMIT, "false"));
}

// 在 buildProperty() 或 gsonPostProcess() 中添加从 properties map 恢复到成员变量的逻辑
// this.enableTwoPhaseCommit = getEnableTwoPhaseCommit();
```

### 4.4 OlapTable中的快捷方法

在 `OlapTable.java` 中添加：

```java
// 文件: fe/fe-core/src/main/java/org/apache/doris/catalog/OlapTable.java

/**
 * 判断该表是否启用了异步发布。
 * 前置条件：必须是 Cloud 模式 + MOW 表。
 */
public boolean isEnableTwoPhaseCommit() {
    if (!Config.isCloudMode()) {
        return false;
    }
    if (!isUniqKeyMergeOnWrite()) {
        return false;
    }
    if (tableProperty == null) {
        return false;
    }
    return tableProperty.getEnableTwoPhaseCommit();
}
```

### 4.5 建表时设置

```sql
-- 创建启用异步发布的MOW表
CREATE TABLE example_table (
    k1 INT,
    v1 VARCHAR(100)
)
UNIQUE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 8
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "enable_mow_async_publish" = "true"
);
```

**校验规则**（在 `InternalCatalog.createTable()` 或相关校验逻辑中添加）：
- 仅在 Cloud 模式下允许设置 `enable_mow_async_publish = true`
- 仅在 MOW 表（`enable_unique_key_merge_on_write = true`）上允许设置
- 非 Cloud 模式或非 MOW 表设置该属性时抛出 `AnalysisException`

---

## 5. 和现有流程的兼容

### 5.1 分叉点

在 `commitAndPublishTransaction()` 方法中，根据表属性决定走哪条路径：

```java
@Override
public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList,
        long transactionId, List<TabletCommitInfo> tabletCommitInfos,
        long timeoutMillis, TxnCommitAttachment txnCommitAttachment) throws UserException {

    // 判断是否有表启用了异步发布
    boolean hasTwoPhaseTable = tableList.stream()
            .filter(t -> t instanceof OlapTable)
            .map(t -> (OlapTable) t)
            .anyMatch(OlapTable::isEnableTwoPhaseCommit);

    if (hasTwoPhaseTable) {
        // 新的异步发布流程
        return commitAndPublishTwoPhase(db, tableList, transactionId,
                tabletCommitInfos, timeoutMillis, txnCommitAttachment);
    } else {
        // 原有一阶段提交流程（完全不变）
        return commitAndPublishOnePhase(db, tableList, transactionId,
                tabletCommitInfos, timeoutMillis, txnCommitAttachment);
    }
}
```

### 5.2 兼容性保证

| 场景 | 行为 |
|------|------|
| 未启用异步发布的MOW表 | 走原有的一阶段提交流程，完全不受影响 |
| 非MOW表（DUP/AGG等） | 走原有流程，不受影响 |
| 非Cloud模式 | `isEnableTwoPhaseCommit()` 始终返回false，走原有流程 |
| 混合场景（同一事务涉及两阶段和非两阶段表） | 初期不支持。校验时如果发现事务涉及的表属性不一致，抛出异常 |

### 5.3 与现有机制的交互

| 机制 | 影响 |
|------|------|
| **Lazy commit** (`enable_cloud_txn_lazy_commit`) | 启用异步发布的表不再使用lazy commit。两个属性互斥 |
| **2PC协议** (`commitTransaction2PC`) | 异步发布表不支持外部2PC协议（已有的Stream Load 2PC），两者互斥 |
| **SubTransaction** | 初期不支持异步发布表的sub transaction |
| **重试机制** | `mow_calculate_delete_bitmap_retry_times` 相关重试逻辑保留在一阶段路径中，两阶段路径的重试在publish后台线程中处理 |
| **Commit lock** | 异步发布仍然使用FE表级commit锁，但持锁时间极短 |
| **commitTransactionWithoutLock** | 此方法是 BE 直接调用 FE 的 thrift 接口提交事务时使用。对于异步发布表，需要相应改造 |

---

## 6. 关键文件和修改点

### 6.1 需要新增的文件

| 文件 | 说明 |
|------|------|
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CommittedTxnEntry.java` | 单个committed事务的数据结构 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CommittedTxnManager.java` | committed事务的内存管理器 |

### 6.2 需要修改的文件

| 文件 | 修改内容 |
|------|---------|
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CloudGlobalTransactionMgr.java` | 核心改造：新增 `commitAndPublishTwoPhase()` 方法，在 `commitAndPublishTransaction()` 中增加分叉逻辑，新增 `CommittedTxnManager` 成员变量 |
| `fe/fe-core/src/main/java/org/apache/doris/common/util/PropertyAnalyzer.java` | 新增 `PROPERTIES_ENABLE_TWO_PHASE_COMMIT` 常量和 `analyzeEnableTwoPhaseCommit()` 方法 |
| `fe/fe-core/src/main/java/org/apache/doris/catalog/TableProperty.java` | 新增 `enableTwoPhaseCommit` 字段、getter/setter，在 `buildProperty()` 中恢复该字段 |
| `fe/fe-core/src/main/java/org/apache/doris/catalog/OlapTable.java` | 新增 `isEnableTwoPhaseCommit()` 方法 |
| `fe/fe-core/src/main/java/org/apache/doris/datasource/InternalCatalog.java` | 建表时校验 `enable_mow_async_publish` 的合法性（必须是Cloud + MOW） |
| `fe/fe-common/src/main/java/org/apache/doris/common/Config.java` | 新增 `mow_async_publish_publish_timeout_seconds` 配置项 |

### 6.3 涉及但不修改的文件（需理解）

| 文件 | 说明 |
|------|------|
| `fe/fe-core/src/main/java/org/apache/doris/transaction/TabletCommitInfo.java` | 数据结构：`tabletId` + `backendId`，CommittedTxnEntry 中直接持有 |
| `fe/fe-core/src/main/java/org/apache/doris/common/util/MetaLockUtils.java` | commit锁工具类，异步发布仍然使用 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/DeleteBitmapUpdateLockContext.java` | 仅在一阶段提交路径中使用，两阶段路径不使用 |
| `gensrc/thrift/Types.thrift` | `TTabletCommitInfo` 定义：`tabletId` (i64) + `backendId` (i64) |

---

## 7. 单元测试要点

### 7.1 CommittedTxnEntry 测试

| 测试用例 | 验证点 |
|---------|--------|
| `testMarkPublishSucceeded` | 调用 `markPublishSucceeded()` 后，`awaitPublish()` 立即返回 true，`isPublishSucceeded()` 为 true |
| `testMarkPublishFailed` | 调用 `markPublishFailed("error")` 后，`awaitPublish()` 立即返回 true，`isPublishSucceeded()` 为 false，`getPublishErrorMsg()` 正确 |
| `testAwaitPublishTimeout` | 不调用 mark 方法时，`awaitPublish(100)` 在 100ms 后返回 false |
| `testAwaitPublishConcurrency` | 多个线程同时等待同一个 entry 的 publish 完成，均能正确被唤醒 |

### 7.2 CommittedTxnManager 测试

| 测试用例 | 验证点 |
|---------|--------|
| `testAddAndGet` | `addCommittedTxn()` 后，`getByTxnId()` 和 `getByTableId()` 均能正确返回 |
| `testRemove` | `removeCommittedTxn()` 后，`getByTxnId()` 返回 null，`getByTableId()` 不再包含该 txn |
| `testRemoveLastTxnForTable` | 表的最后一个 txn 被移除后，`getTablesWithCommittedTxns()` 不再包含该 tableId |
| `testConcurrentAddAndRemove` | 多线程并发调用 add/remove/get，不抛异常，结果一致 |
| `testGetByTableIdOrdering` | `getByTableId()` 返回的列表按 `commitTimeMs` 升序排列 |

### 7.3 Commit流程分叉测试

| 测试用例 | 验证点 |
|---------|--------|
| `testNonTwoPhaseTableUsesOriginalPath` | 未启用异步发布的表，commit流程走原有路径，不创建 CommittedTxnEntry |
| `testTwoPhaseTableUsesNewPath` | 启用异步发布的表，commit后 CommittedTxnManager 中存在对应 entry |
| `testTwoPhaseCommitLockHoldTime` | 验证异步发布持锁时间极短（不包含delete bitmap计算） |
| `testTwoPhaseCommitThenAwaitPublish` | commit成功后，导入线程进入等待状态，publish线程 markPublishSucceeded 后导入线程被唤醒 |
| `testTwoPhaseCommitPublishTimeout` | publish超时后导入返回 false，但 CommittedTxnManager 中 entry 仍然存在（等待后台继续publish） |
| `testCommitFailedNoEntryAdded` | 如果MS两阶段commit RPC失败，不应向 CommittedTxnManager 添加 entry |

### 7.4 表属性测试

| 测试用例 | 验证点 |
|---------|--------|
| `testCreateTableWithTwoPhaseCommit` | Cloud模式MOW表可以成功设置 `enable_mow_async_publish = true` |
| `testCreateTableNonCloudReject` | 非Cloud模式设置 `enable_mow_async_publish = true` 抛出 `AnalysisException` |
| `testCreateTableNonMowReject` | 非MOW表设置 `enable_mow_async_publish = true` 抛出 `AnalysisException` |
| `testIsEnableTwoPhaseCommit` | `OlapTable.isEnableTwoPhaseCommit()` 在各种条件组合下返回正确结果 |
| `testTwoPhaseAndLazyCommitMutualExclusion` | 同时设置 `enable_mow_async_publish` 和 `enable_cloud_txn_lazy_commit` 时报错 |

### 7.5 兼容性测试

| 测试用例 | 验证点 |
|---------|--------|
| `testMixedTablesReject` | 同一事务中同时包含两阶段和非两阶段表时，抛出异常 |
| `testOriginalPathUnchanged` | 启用异步发布功能后，未设置该属性的表的导入行为与原来完全一致 |
| `testCommitTransactionWithoutLockTwoPhase` | BE 直接调用 `commitTransactionWithoutLock` 时，对异步发布表的正确处理 |
