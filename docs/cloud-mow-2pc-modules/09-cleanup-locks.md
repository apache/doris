# Module 8: 锁机制变更 - 新增MS Tablet锁、移除表锁和Pending Delete Bitmap

## 概述

在Cloud MOW异步发布方案中，锁机制发生如下变更（仅对启用异步发布的表，**所有老代码完整保留**）：

1. **新增MS tablet级分布式锁**：在publish阶段BE的convert_tmp_rowset前获取，和compaction互斥。大多数情况BE内存锁（rowset_update_lock）已够用，MS tablet锁主要处理跨BE场景。
2. **移除MS表级分布式锁**（`get_delete_bitmap_update_lock`）：对启用异步发布的表不再使用。未启用的表继续使用。
3. **移除Pending delete bitmap KV**：commit后事务必然成功，不需要残留清理机制。
4. **移除大事务/lazy commit机制**：最终MS publish变为O(1)操作，不需要分批提交。

本模块的清理工作需在其他所有模块完成后进行，且仅对启用异步发布的表生效，不影响未启用的表。

---

## A. 当前MS表级锁机制分析

### A.1 锁的用途

MS表级分布式锁（Delete Bitmap Update Lock）用于保证同一张表上的导入和compaction/schema change操作在计算和更新delete bitmap时互斥。其本质是一个存储在FDB中的KV：

```
Key:   0x01 "meta" ${instance_id} "delete_bitmap_lock" ${table_id} ${partition_id}
Value: DeleteBitmapUpdateLockPB (包含 lock_id, expiration, initiators)
```

其中`partition_id`固定传`-1`，实际锁的粒度是**表级**。

### A.2 工作流程

**导入流程（FE驱动）：**

1. FE在`commitTransactionWithoutLock()`中，对MOW表列表调用`getDeleteBitmapUpdateLock()`（文件：`CloudGlobalTransactionMgr.java`，第1120行起）
2. FE通过MS RPC `getDeleteBitmapUpdateLock`获取锁，`lock_id`设为`transactionId`（正数），`initiator`设为`-1`
3. 获取锁后，FE下发`CalcDeleteBitmapTask`到BE计算delete bitmap
4. BE计算完成后，FE调用MS `commit_txn`提交事务
5. 在`commit_txn`的`process_mow_when_commit_txn()`中（文件：`meta_service_txn.cpp`，第1341行起），MS验证锁仍属于当前事务，然后删除锁KV和pending delete bitmap KV
6. 如果提交失败，FE在catch块中调用`removeDeleteBitmapUpdateLock()`释放锁

**Compaction流程（BE驱动）：**

1. BE在`CloudFullCompaction::_cloud_full_compaction_update_delete_bitmap()`中调用`get_delete_bitmap_update_lock()`获取锁（文件：`cloud_full_compaction.cpp`，第363行），`lock_id`传`-1`（`COMPACTION_DELETE_BITMAP_LOCK_ID`）
2. 计算并更新delete bitmap后，BE通过MS `finish_tablet_job` RPC完成compaction
3. MS在`finish_tablet_job`中通过`check_and_remove_delete_bitmap_update_lock()`验证并释放锁（文件：`meta_service_job.cpp`，第1066行）
4. 如果compaction失败abort，锁也会在`abort_tablet_job`中释放（第915行）

**Schema Change流程（BE驱动）：**

1. BE在`CloudSchemaChangeJob::_process_delete_bitmap()`中获取锁（文件：`cloud_schema_change_job.cpp`，第555行），`lock_id`传`-2`（`SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID`）
2. Schema change完成/失败时释放锁（第603行）

### A.3 锁的版本

存在v1和v2两个版本的锁实现：

- **v1**：表级单一锁KV，导入和compaction互斥。锁冲突时compaction需要等待或被拒绝。
- **v2**：在v1基础上增加了`mow_tablet_job`粒度的KV（按table_id+initiator_id），允许同表不同tablet的compaction并行，但导入仍与所有compaction互斥。

通过`DeleteBitmapLockWhiteList`配置决定使用哪个版本。

### A.4 锁冲突的影响

- 导入与compaction互斥：导入需等待compaction释放锁，最长等待`delete_bitmap_lock_expiration_seconds`秒
- 高并发场景下，导入请求被compaction阻塞导致超时
- FE对锁冲突有重试机制（`metaServiceRpcRetryTimes`次，带退避），但整体commit持FE表锁期间blocked
- 导入可设置`urgent`标志强制抢占compaction持有的锁

---

## B. 移除MS表级锁

### B.1 设计原则

对启用异步发布的表，整个`getDeleteBitmapUpdateLock` -> `calcDeleteBitmap` -> `commitTxn(remove lock)`的流程发生根本变化：

- **Commit阶段**不再涉及delete bitmap计算，无需获取锁
- **Publish阶段**的delete bitmap计算通过BE内存锁（tablet级`rowset_update_lock`）保证tablet内互斥

因此，对启用异步发布的表，应跳过整个MS表级锁的获取和释放。

### B.2 FE侧修改

**文件：`fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CloudGlobalTransactionMgr.java`**

1. `commitTransactionWithoutLock()`（第427行附近）：在检测到MOW表启用异步发布时，跳过`getDeleteBitmapUpdateLock()`和`getCalcDeleteBitmapInfo()`调用，直接走新的commit路径。

```java
// 修改前
if (!mowTableList.isEmpty()) {
    ...
    getDeleteBitmapUpdateLock(transactionId, mowTableList, tabletCommitInfos, lockContext);
    ...
    backendToPartitionInfos = getCalcDeleteBitmapInfo(lockContext, null);
}

// 修改后
List<OlapTable> twoPCMowTables = filter2PCEnabledTables(mowTableList);
List<OlapTable> legacyMowTables = filterLegacyTables(mowTableList);
if (!legacyMowTables.isEmpty()) {
    // 走原有路径
    getDeleteBitmapUpdateLock(transactionId, legacyMowTables, ...);
    ...
}
// twoPCMowTables 走新的两阶段commit路径（不需要获取锁）
```

2. catch块中的`removeDeleteBitmapUpdateLock()`同样只对未启用异步发布的表执行。

3. `commitTransactionWithSubTxns()`（第1555行附近）中的相同逻辑也需要修改。

### B.3 MS侧修改

MS侧的`get_delete_bitmap_update_lock`和`remove_delete_bitmap_update_lock` RPC本身不需要修改。它们仍然为未启用异步发布的表服务。调用端（FE/BE）通过判断表属性决定是否调用。

MS侧`commit_txn`中的`process_mow_when_commit_txn()`也需按条件判断：
- 启用异步发布的表：跳过锁验证和pending delete bitmap清理（因为本来就没有锁和pending KV）
- 未启用的表：走原有路径

### B.4 BE侧修改

**Compaction（`cloud_full_compaction.cpp`）：**

对启用异步发布的表，compaction中不再调用`get_delete_bitmap_update_lock()`（第363行），因为compaction与导入的delete bitmap计算已通过BE内存锁`rowset_update_lock`互斥。

**Schema Change（`cloud_schema_change_job.cpp`）：**

schema change的delete bitmap lock也需要评估。如果schema change与导入的互斥可以通过别的机制保证，则同样可以移除。但schema change的场景较为特殊，初期可暂不处理，保持现有锁机制。

---

## C. Tablet级互斥机制

### C.1 现有BE内存锁（rowset_update_lock）

`rowset_update_lock`是定义在`CloudTablet`中的per-tablet `std::mutex`（文件：`be/src/cloud/cloud_tablet.h`，第467行）：

```cpp
mutable std::mutex _rowset_update_lock;
```

当前已被以下操作使用：

1. **导入的delete bitmap计算**（`cloud_engine_calc_delete_bitmap_task.cpp`，第162行）：
   ```cpp
   std::unique_lock wrlock(tablet->get_rowset_update_lock());
   ```
   持锁范围：整个delete bitmap计算过程。

2. **Publish version（本地apply rowset）**（`engine_publish_version_task.cpp`，第475行）：
   ```cpp
   std::unique_lock<std::mutex> rowset_update_lock(_tablet->get_rowset_update_lock(), std::defer_lock);
   if (_tablet->enable_unique_key_merge_on_write()) {
       rowset_update_lock.lock();
   }
   ```

3. **Compaction（非cloud场景）**（`compaction.cpp`，第1164行和第1332行）：
   ```cpp
   std::lock_guard rwlock(tablet()->get_rowset_update_lock());
   ```

4. **Full compaction（非cloud场景）**（`full_compaction.cpp`，第155行和第171行）：
   ```cpp
   std::lock_guard rowset_update_lock(tablet()->get_rowset_update_lock());
   ```

### C.2 新增MS Tablet级别锁

对启用异步发布的表，新增**tablet粒度**的MS分布式锁，替代现有的表级别锁。

**KV Key格式**：
```
Key:   0x01 "meta" ${instance_id} "delete_bitmap_tablet_lock" ${table_id} ${tablet_id}
Value: DeleteBitmapUpdateLockPB (复用现有value，包含 lock_id, expiration, initiators)
```

**与现有表级别锁对比**：

| 属性 | 表级别锁 (delete_bitmap_lock) | Tablet级别锁 (delete_bitmap_tablet_lock) |
|------|------|------|
| Key infix | `"delete_bitmap_lock"` | `"delete_bitmap_tablet_lock"` |
| 粒度 | table_id + partition_id(-1) | table_id + tablet_id |
| 使用场景 | 导入commit阶段（一阶段提交） | publish阶段BE的convert_tmp_rowset（异步发布） |
| 互斥对象 | 同一表所有导入和compaction | 同一tablet的导入publish和compaction |
| 适用表 | 未启用异步发布的表 | 启用异步发布的表 |

**RPC**：复用现有`GetDeleteBitmapUpdateLockRequest`/`RemoveDeleteBitmapUpdateLockRequest`，新增字段：
- `tablet_level_lock = 20`（bool）：设为true表示请求tablet级别锁
- `lock_tablet_ids = 21`（repeated int64）：要加锁的tablet ID列表

**使用场景**：
1. Publish阶段：BE在convert_tmp_rowset前获取tablet锁，convert + local apply完成后释放
2. Compaction：compaction更新delete bitmap前获取tablet锁，完成后释放
3. 大多数情况下同一BE上的rowset_update_lock已够互斥，MS tablet锁主要处理**跨BE**的竞争（如tablet迁移后原BE上的compaction和新BE上的publish并发）

### C.3 双层互斥机制

在新的异步发布方案中：

**同一BE上的互斥：**
- 导入A的tablet T的delete bitmap计算持有`T.rowset_update_lock`
- 同一tablet T的compaction也需要持有`T.rowset_update_lock`
- 两者自然互斥，无需MS级别的锁

**不同BE上的互斥：**
- 通常情况下，同一tablet的导入和compaction在同一BE上执行（cloud模式下tablet有primary BE归属），通过上述内存锁互斥。
- 在极端情况下（如BE扩缩容、tablet迁移），可能出现同一tablet的导入在BE-A、compaction在BE-B的情况。此时通过**MS侧`convert_tmp_rowset`的FDB事务冲突检测**来保证正确性：
  - `convert_tmp_rowset`在MS中是一个FDB事务，该事务会读写`meta_rowset`（rowset元数据）和`tablet_stats`等KV
  - 如果导入的`convert_tmp_rowset`和compaction的`finish_tablet_job`并发操作同一tablet的rowset元数据，FDB的乐观并发控制会检测到冲突并让其中一个事务失败
  - 失败的操作会重试

### C.3 Cloud compaction的额外考虑

Cloud模式下的compaction（`CloudFullCompaction`）当前在`_cloud_full_compaction_update_delete_bitmap()`中：
1. 先不持锁计算大部分delete bitmap
2. 获取MS表级锁后sync rowsets，补算新增rowset的delete bitmap
3. 调用`update_delete_bitmap`更新到MS

在异步发布下，步骤2中的MS表级锁被移除。compaction需要改为：
1. 先不持锁计算大部分delete bitmap
2. 获取tablet本地`rowset_update_lock`，sync rowsets，补算新增rowset的delete bitmap
3. 持有`rowset_update_lock`的情况下，调用`update_delete_bitmap`更新到MS
4. 释放`rowset_update_lock`

这样通过本地内存锁保证compaction更新delete bitmap期间，同tablet的导入publish不会并发进行。

---

## D. 移除Pending Delete Bitmap

### D.1 当前Pending Delete Bitmap的完整流程

Pending delete bitmap用于在事务失败时清理已写入MS的delete bitmap KV，防止残留的无效delete bitmap影响数据正确性。

**KV格式：**
```
Key:   0x01 "meta" ${instance_id} "delete_bitmap_pending" ${tablet_id}
Value: PendingDeleteBitmapPB (包含 delete_bitmap_keys 列表)
```

**写入时机（`update_delete_bitmap` RPC，`meta_service.cpp`第4068行起）：**

当`lock_id > 0`（导入）或`lock_id == -2`（schema change）时，会将本次写入的delete bitmap key列表序列化为`PendingDeleteBitmapPB`，写入到pending KV：

```cpp
// lock_id > 0 : load
// lock_id = -1 : compaction
// lock_id = -2 : schema change
// lock_id = -3 : compaction update delete bitmap without lock
if (request->lock_id() > 0 || request->lock_id() == -2) {
    PendingDeleteBitmapPB delete_bitmap_keys;
    // ... 填充 delete_bitmap_keys
    std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
    txn->put(pending_key, pending_val);
}
```

对于显式事务（explicit txn，多sub txn），后续sub txn会累积前序sub txn的pending keys。

**清理时机一：`commit_txn`中的`process_mow_when_commit_txn()`（第1341行起）：**

事务成功commit时，在验证锁仍属于当前事务后，删除所有相关tablet的pending KV：

```cpp
for (auto tablet_id : table_id_tablet_ids[table_id]) {
    std::string pending_key = meta_pending_delete_bitmap_key({instance_id, tablet_id});
    txn->remove(pending_key);
}
```

**清理时机二：下一次`update_delete_bitmap`调用时（第4004行起）：**

新的delete bitmap更新开始时，先调用`remove_pending_delete_bitmap()`读取并删除上一个失败事务残留的pending delete bitmap（即根据`PendingDeleteBitmapPB`中记录的key列表，删除对应的delete bitmap KV）：

```cpp
static bool remove_pending_delete_bitmap(...) {
    // 读取 pending KV
    // 解析 PendingDeleteBitmapPB
    // 根据其中记录的 delete_bitmap_keys，逐个删除残留的 delete bitmap
    for (auto& delete_bitmap_key : pending_info.delete_bitmap_keys()) {
        std::string end_key = delete_bitmap_key;
        encode_int64(INT64_MAX, &end_key);
        txn->remove(delete_bitmap_key, end_key);
    }
}
```

**清理时机三：lazy commit路径中的`convert_tmp_rowsets()`（`txn_lazy_committer.cpp`第486行起）：**

当配置了`txn_lazy_commit_defer_deleting_pending_delete_bitmaps`时，pending delete bitmap的清理延迟到lazy commit的`convert_tmp_rowsets`阶段。在`make_committed_txn_visible()`中也会处理锁的释放。

### D.2 为什么异步发布不需要Pending Delete Bitmap

在新的异步发布方案中：

1. **Commit阶段不写delete bitmap**：commit仅更新partition version和TxnInfoPB，不涉及delete bitmap的任何操作
2. **Publish阶段事务必然成功**：一旦commit成功，所有tablet的publish（含delete bitmap计算和写入）必然会最终完成。即使中间失败也会重试直到成功
3. **每个tablet独立转正**：每个tablet完成delete bitmap更新和rowset转正后，该操作已经持久化。不存在"事务整体失败需回滚delete bitmap"的场景
4. **不存在残留delete bitmap的问题**：delete bitmap要么写成功（与rowset转正在同一事务或相邻步骤），要么没写（会重试），不会出现"delete bitmap写了但事务失败"的中间状态

### D.3 移除范围

**MS侧 `update_delete_bitmap`（`meta_service.cpp`）：**

对启用异步发布的表（可通过请求参数中的标志判断），跳过：
- `remove_pending_delete_bitmap()`调用（第4009行）
- `PendingDeleteBitmapPB`的构造和写入（第4068-4105行）

```cpp
// 修改后
if (!is_2pc_enabled) {
    // 原有 pending delete bitmap 逻辑
    if (!is_explicit_txn || is_first_sub_txn) {
        if (!remove_pending_delete_bitmap(code, msg, ss, txn, instance_id, tablet_id)) {
            return;
        }
    }
}
// ...
if (!is_2pc_enabled && (request->lock_id() > 0 || request->lock_id() == -2)) {
    // 原有写入 pending KV 逻辑
}
```

**MS侧 `commit_txn` / `process_mow_when_commit_txn()`（`meta_service_txn.cpp`）：**

对启用异步发布的表：
- 跳过锁验证（因为没有获取锁）
- 跳过pending delete bitmap删除（因为没有写入pending KV）

新方案的commit_txn中对异步发布的表，不调用`process_mow_when_commit_txn()`和`process_mow_when_commit_txn_deferred()`。

**MS侧 `txn_lazy_committer.cpp`：**

对启用异步发布的表，`convert_tmp_rowsets()`中不再执行pending delete bitmap清理（第486-493行），`make_committed_txn_visible()`中不再执行锁释放（第564行起）。但实际上，启用异步发布的表根本不会走lazy commit路径（见E节），因此这里自然不需要修改。

**BE侧：**

BE侧不直接操作pending delete bitmap KV（这些操作都在MS RPC中完成），无需修改。

### D.4 兼容性保障

必须确保只对启用异步发布的表移除pending逻辑。判断方式：
- 在`update_delete_bitmap`请求中增加一个标志字段（如`is_mow_async_publish`），由BE在调用时设置
- 或者通过MS侧查询tablet/table的属性判断（但会引入额外KV读取，不推荐）

推荐在请求中传递标志，MS根据标志走不同代码路径。

---

## E. 移除大事务/Lazy Commit路径

### E.1 当前Lazy Commit机制

大事务/lazy commit机制用于处理单次FDB事务中KV操作量过大（超过FDB 10MB事务限制）的场景。

**触发路径（`meta_service_txn.cpp`第3298行起）：**

```cpp
bool enable_txn_lazy_commit_feature =
    (request->has_is_2pc() && !request->is_2pc() &&
     request->has_enable_txn_lazy_commit() &&
     request->enable_txn_lazy_commit() &&
     config::enable_cloud_txn_lazy_commit);

while ((!enable_txn_lazy_commit_feature ||
        (tmp_rowsets_meta.size() <= config::txn_lazy_commit_rowsets_thresold))) {
    // 先尝试 commit_txn_immediately
    // 如果因TXN_BYTES_TOO_LARGE失败且启用了lazy commit，则跳出while循环
    break;
}
// 走 commit_txn_eventually
commit_txn_eventually(request, response, code, msg, instance_id, db_id, tmp_rowsets_meta, stats);
```

**`commit_txn_eventually()`（第2184行起）的核心流程：**

1. 在一个FDB事务中：更新TxnInfoPB状态为`TXN_STATUS_COMMITTED`，设置partition version（带pending_txn_id）
2. 提交TxnLazyCommitTask到`TxnLazyCommitter`线程池
3. `TxnLazyCommitTask::commit()`将tmp rowsets分批转正（`convert_tmp_rowsets`），每批在独立FDB事务中完成
4. 最后通过`make_committed_txn_visible()`将事务设为`TXN_STATUS_VISIBLE`

**`TxnLazyCommitter`（`txn_lazy_committer.h`）：**

- 管理一个worker线程池和parallel_commit线程池
- 维护`running_tasks_`映射表
- 通过`submit()`提交任务，`wait()`等待完成

### E.2 为什么异步发布不需要Lazy Commit

新方案中：
- **Commit阶段**仅更新partition version和TxnInfoPB，KV操作量为O(partition数)，远远不会超过FDB事务限制
- **Publish阶段**中每个tablet独立`convert_tmp_rowset`，每次操作的KV量仅与该tablet相关
- **最终publish**仅更新visible version，操作量为O(1)

因此不存在单次FDB事务过大的问题，不需要lazy commit分批提交机制。

### E.3 代码分叉点

**MS侧 `commit_txn`（`meta_service_txn.cpp`）：**

在`commit_txn`的主函数中，对启用异步发布的表走新的commit路径，直接跳过`enable_txn_lazy_commit_feature`的检查和`commit_txn_eventually()`调用。

**FE侧：**

FE在构造`CommitTxnRequest`时，对启用异步发布的表：
- 不设置`enable_txn_lazy_commit`标志
- 或者MS侧在新的commit路径中根本不检查此标志

**`TxnLazyCommitter`：**

`TxnLazyCommitter`本身不需要修改，它仍然为未启用异步发布的表服务。只是启用异步发布的表的事务不会再被submit到`TxnLazyCommitter`。

---

## F. 兼容性

### F.1 混合表场景

同一集群中可能同时存在：
- 启用异步发布的MOW表（新表）
- 未启用异步发布的MOW表（旧表）
- 非MOW表

所有清理工作仅针对启用异步发布的表，不影响其他表。

### F.2 判断依据

推荐通过以下机制判断是否启用异步发布：

1. **表属性**：建表时通过`PROPERTIES`设置`enable_mow_async_publish = true`，持久化在`TabletMetaCloudPB`或`OlapTableProperty`中
2. **事务属性**：在`TxnInfoPB`中记录该事务涉及的表是否启用异步发布
3. **请求标志**：FE/BE在构造MS RPC请求时，根据表属性设置请求中的标志位

### F.3 路径分叉示意

```
FE commitTransaction:
  ├── 表A (异步发布): 新路径
  │   ├── 不获取 delete bitmap update lock
  │   ├── 不下发 calc delete bitmap task
  │   ├── 调用 MS commit_txn (新路径: 仅更新 partition version + TxnInfoPB)
  │   └── 异步publish (由FE Publish Daemon驱动)
  │
  └── 表B (传统路径): 原有路径
      ├── 获取 delete bitmap update lock
      ├── 下发 calc delete bitmap task
      ├── 调用 MS commit_txn (原有路径: 验证锁, 清理pending, 转正rowset)
      └── 同步完成
```

### F.4 升级兼容

- 升级期间，新代码需要能正确处理旧表的原有流程
- 不存在强制迁移：旧表继续使用原有机制，新建表才可选择启用异步发布
- 如果需要将存量表迁移到异步发布，需要额外的migration模块（不在本模块范围内）

---

## G. 关键文件和修改点

### G.1 FE侧

| 文件 | 修改内容 |
|------|----------|
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CloudGlobalTransactionMgr.java` | `commitTransactionWithoutLock()`：对启用异步发布的表跳过`getDeleteBitmapUpdateLock()`、`getCalcDeleteBitmapInfo()`和`removeDeleteBitmapUpdateLock()` |
| 同上 | `commitTransactionWithSubTxns()`：同样的分叉逻辑 |

### G.2 MS侧

| 文件 | 修改内容 |
|------|----------|
| `cloud/src/meta-service/meta_service_txn.cpp` | `commit_txn`主函数：对启用异步发布的表走新路径，不进入`commit_txn_eventually()`，不调用`process_mow_when_commit_txn()` |
| 同上 | `process_mow_when_commit_txn()`/`process_mow_when_commit_txn_deferred()`：对启用异步发布的表跳过锁验证和pending删除 |
| `cloud/src/meta-service/meta_service.cpp` | `update_delete_bitmap()`：对启用异步发布的表跳过`remove_pending_delete_bitmap()`和pending KV写入 |
| `cloud/src/meta-service/meta_service.cpp` | `get_delete_bitmap_update_lock()` / `remove_delete_bitmap_update_lock()`：无需修改，未启用异步发布的表仍使用 |
| `cloud/src/meta-service/meta_service_job.cpp` | compaction job的`finish_tablet_job()`和`abort_tablet_job()`中锁释放逻辑：对启用异步发布的表跳过 |
| `cloud/src/meta-service/txn_lazy_committer.cpp` | 无需修改。启用异步发布的事务不会进入lazy commit路径 |

### G.3 BE侧

| 文件 | 修改内容 |
|------|----------|
| `be/src/cloud/cloud_full_compaction.cpp` | `_cloud_full_compaction_update_delete_bitmap()`：对启用异步发布的表，不调用`get_delete_bitmap_update_lock()`，改为使用`rowset_update_lock` |
| `be/src/cloud/cloud_meta_mgr.cpp` | `get_delete_bitmap_update_lock()`和`remove_delete_bitmap_update_lock()`：无需修改，由调用方决定是否调用 |
| `be/src/cloud/cloud_engine_calc_delete_bitmap_task.cpp` | 已使用`rowset_update_lock`，无需修改 |
| `be/src/storage/task/engine_publish_version_task.cpp` | 已使用`rowset_update_lock`，无需修改 |
| `be/src/cloud/cloud_schema_change_job.cpp` | 初期可暂不修改schema change的锁逻辑 |

### G.4 Proto文件

| 文件 | 修改内容 |
|------|----------|
| `gensrc/proto/cloud.proto` | 如需在RPC请求中传递异步发布标志（如`UpdateDeleteBitmapRequest`增加`is_mow_async_publish`字段），在此文件修改 |

---

## H. 单元测试要点

### H.1 MS表级锁移除

1. **启用异步发布的表不获取锁**：
   - 模拟启用异步发布的表提交事务，验证不调用`get_delete_bitmap_update_lock` RPC
   - 验证commit_txn中不执行`process_mow_when_commit_txn()`的锁验证逻辑

2. **未启用异步发布的表正常获取锁**：
   - 验证传统MOW表的lock获取/释放流程不受影响
   - 验证导入-compaction互斥仍然正常工作

3. **混合场景**：
   - 同一集群中同时提交启用和未启用异步发布的表的事务
   - 验证各自走正确的代码路径

### H.2 Pending Delete Bitmap移除

1. **启用异步发布的表不写入pending KV**：
   - 调用`update_delete_bitmap`时传入异步发布标志
   - 验证FDB中不存在该tablet的pending delete bitmap KV

2. **启用异步发布的表commit不清理pending**：
   - 验证commit_txn中不执行pending KV删除

3. **未启用的表pending逻辑正常**：
   - 验证传统表的pending delete bitmap写入、commit时清理、下次update时清理旧残留等流程正常

### H.3 Lazy Commit移除

1. **启用异步发布的表不走lazy commit**：
   - 构造大事务（大量tablet），验证不进入`commit_txn_eventually()`
   - 验证TxnLazyCommitter不接收这些事务

2. **未启用的表lazy commit正常**：
   - 验证传统大事务仍能走`commit_txn_eventually()`路径

### H.4 Tablet级互斥

1. **同BE上导入与compaction互斥**：
   - 在同一个tablet上并发执行delete bitmap计算和compaction
   - 验证通过`rowset_update_lock`正确串行化
   - 验证不出现数据不一致

2. **不同BE上的冲突检测**：
   - 模拟两个BE并发操作同一tablet的rowset元数据
   - 验证FDB事务冲突被正确检测
   - 验证失败方能正确重试

### H.5 回归测试

确保以下现有回归测试在混合模式下仍然通过：
- `test_cloud_mow_delete_bitmap_lock_case.groovy`
- `test_delete_bitmap_lock_with_restart.groovy`
- `test_cloud_pending_delete_bitmaps_removed_by_other_txn.groovy`
- `test_cloud_mow_stream_load_with_txn_conflict.groovy`
- `test_cloud_load_compaction_on_different_be.groovy`
