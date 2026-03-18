# Module 6: FE重启/切主恢复

## 概述

在Cloud MOW异步发布方案中，FE Master在内存中维护一个committed txns set（`CommittedTxnManager`，参见Module 4），用于跟踪所有已commit但尚未publish的事务。当FE重启或发生切主（Leader切换）时，该内存数据丢失，需要从MS（Meta Service）中恢复。

恢复的信息来源是MS中FDB存储的`TxnInfoPB`。其中`status == TXN_STATUS_COMMITTED`且`mow_async_publish == true`的事务即为需要恢复的历史committed事务。`TxnInfoPB`中包含了publish所需的全部信息：
- `committed_partition_ids` / `committed_versions`：每个partition的commit version
- `involved_tablets`：tablet和所在BE的映射
- `load_schema_param`：导入参数（TOlapTableSchemaParam的Thrift序列化bytes，包含部分列更新模式等）

关键特性：**partition commit version本身存储在FDB中，不依赖FE内存**。因此新导入的commit在恢复完成前可以正常进行（MS直接从FDB读取partition commit version并递增），只是publish需要等恢复完成后由`CloudPublishDaemon`处理。

本模块依赖：
- Module 0（Proto/KV Schema）
- Module 4（FE侧Commit阶段 + Committed Txns管理）

---

## A. 需要恢复的信息

### A.1 Committed Txns Set的内容

每个committed事务在`CommittedTxnManager`中以`CommittedTxnEntry`的形式存在（参见Module 4中的定义），包含以下信息：

| 字段 | 来源 | 说明 |
|------|------|------|
| `txnId` | `TxnInfoPB.txn_id` | 事务ID |
| `dbId` | `TxnInfoPB.db_id` | 数据库ID |
| `tableId` | `TxnInfoPB.table_ids[0]` | 表ID（异步发布限定单表事务） |
| `partitionCommitVersions` | `TxnInfoPB.committed_partition_ids` + `committed_versions` | 每个partition的commit version，Map<partitionId, commitVersion> |
| `tabletCommitInfos` | `TxnInfoPB.involved_tablets` | 每个tablet的ID和所在BE信息，用于publish阶段下发CalcDeleteBitmapTask |
| `txnCommitAttachment` | `TxnInfoPB.commit_attachment` | 导入附件信息（routine load进度等） |
| `commitTimeMs` | `TxnInfoPB.commit_time` | commit完成的时间戳 |

### A.2 数据来源：MS中的TxnInfoPB

TxnInfoPB在FDB中的存储格式：

```
Key:   0x01 "txn" ${instance_id} "txn_info" ${db_id} ${txn_id}
Value: TxnInfoPB (protobuf序列化)
```

筛选条件：
- `TxnInfoPB.status == TXN_STATUS_COMMITTED`
- `TxnInfoPB.mow_async_publish == true`

不满足这两个条件的COMMITTED事务属于lazy commit等其他机制，不由本模块恢复。

### A.3 恢复前后的信息对应关系

```
TxnInfoPB (FDB中)                    CommittedTxnEntry (FE内存中)
─────────────────                     ───────────────────────────
txn_id                           -->  txnId
db_id                            -->  dbId
table_ids[0]                     -->  tableId
committed_partition_ids +             partitionCommitVersions
  committed_versions             -->    (Map<Long, Long>)
involved_tablets                 -->  tabletCommitInfos
  (TxnTabletInfoPB列表)                (List<TabletCommitInfo>)
commit_attachment                -->  txnCommitAttachment
commit_time                      -->  commitTimeMs
```

注意：恢复出的`CommittedTxnEntry`的`publishLatch`字段为一个新的`CountDownLatch(1)`，但不会有导入线程在等待它（原始导入线程在FE重启后已经断开）。该latch仅在publish完成后被countDown，用于内部状态清理。

---

## B. 恢复流程设计

### B.1 触发时机

恢复在FE切主/重启后触发。具体位置是在`transferToMaster()`方法执行过程中。当前`transferToMaster()`的关键流程如下（参见`Env.java`第1666行）：

```
transferToMaster()
  ├── 停止replayer线程
  ├── 打开editlog
  ├── replay journal
  ├── postProcessAfterMetadataReplayed()
  ├── startMasterOnlyDaemonThreads()    <-- CloudPublishDaemon在此启动
  │     └── CloudEnv.startMasterOnlyDaemonThreads()
  │           └── super.startMasterOnlyDaemonThreads()
  ├── startNonMasterDaemonThreads()
  ├── isReady = true
  └── 完成
```

恢复应在`startMasterOnlyDaemonThreads()`中、`CloudPublishDaemon`启动之前执行。或者更合理的方式是：`CloudPublishDaemon`启动后自行执行恢复，恢复完成前不处理publish任务。

推荐方案：**在`CloudPublishDaemon`内部管理恢复状态**。

### B.2 恢复线程模型

```
CloudPublishDaemon 启动
  │
  ├── recoveryCompleted = false
  │
  ├── 首次 runAfterCatalogReady() 调用
  │     │
  │     ├── 检测到 recoveryCompleted == false
  │     │
  │     ├── 启动恢复流程 recoverCommittedTxns()
  │     │     ├── 获取所有 db 列表
  │     │     ├── 按 db 分批扫描 COMMITTED 事务
  │     │     │     ├── DB-1: scanCommittedTxns(dbId=1, startTxnId=0, limit=1000)
  │     │     │     │        -> 返回 TxnInfoPB 列表
  │     │     │     │        -> 筛选 mow_async_publish == true
  │     │     │     │        -> 构建 CommittedTxnEntry 加入 CommittedTxnManager
  │     │     │     │        -> 如果返回 limit 条，继续扫描
  │     │     │     ├── DB-2: 同上
  │     │     │     └── ...
  │     │     └── 设置 recoveryCompleted = true
  │     │
  │     └── 恢复期间不执行 schedulePublish()
  │
  ├── 后续 runAfterCatalogReady() 调用
  │     ├── recoveryCompleted == true
  │     └── 正常执行 schedulePublish()
  │
  └── ...
```

### B.3 按DB分批扫描

扫描按db分批进行，原因：
1. `txn_info_key`的编码格式为`0x01 "txn" ${instance_id} "txn_info" ${db_id} ${txn_id}`，db_id在txn_id之前，天然支持按db做前缀扫描
2. 每个db内按txn_id分页，防止单次RPC返回数据量过大
3. 不同db之间天然隔离，可以并行恢复（优化选项）

伪代码：

```java
private void recoverCommittedTxns() {
    LOG.info("start recovering committed txns from MS");
    long startTime = System.currentTimeMillis();
    int totalRecovered = 0;

    // 获取所有 db
    List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();

    for (long dbId : dbIds) {
        int dbRecovered = 0;
        long startTxnId = 0;  // 从 txnId=0 开始扫描

        while (true) {
            // 调用 MS 扫描 COMMITTED 状态的事务
            ScanTxnsByStatusResponse response = scanCommittedTxnsFromMS(
                    dbId, startTxnId, RECOVERY_SCAN_BATCH_SIZE);

            if (response.getTxnInfosList().isEmpty()) {
                break;  // 该 db 没有更多 COMMITTED 事务
            }

            for (TxnInfoPB txnInfo : response.getTxnInfosList()) {
                // 只恢复异步发布的事务
                if (!txnInfo.hasTwoPhaseCommit() || !txnInfo.getTwoPhaseCommit()) {
                    continue;
                }

                CommittedTxnEntry entry = buildEntryFromTxnInfo(txnInfo);
                committedTxnManager.addCommittedTxn(entry);
                dbRecovered++;
                totalRecovered++;
            }

            // 如果返回的数量小于 limit，说明已经扫描完毕
            if (response.getTxnInfosCount() < RECOVERY_SCAN_BATCH_SIZE) {
                break;
            }

            // 更新 startTxnId 为最后一个事务的 txnId + 1，继续扫描
            startTxnId = response.getTxnInfos(response.getTxnInfosCount() - 1).getTxnId() + 1;
        }

        if (dbRecovered > 0) {
            LOG.info("recovered {} committed txns for db {}", dbRecovered, dbId);
        }
    }

    long costMs = System.currentTimeMillis() - startTime;
    LOG.info("finished recovering committed txns, total={}, cost={}ms", totalRecovered, costMs);
}
```

### B.4 配置项

```java
// fe.conf 新增配置

@ConfField(mutable = true, description = {
    "FE重启/切主后，从MS恢复committed事务时每批扫描的事务数量。",
    "较大的值可以加快恢复速度，但会增加单次RPC的压力。默认 1000"})
public static int cloud_recovery_scan_batch_size = 1000;

@ConfField(mutable = true, description = {
    "FE重启/切主后，从MS恢复committed事务的超时时间，单位秒。",
    "超时后FE仍然可以正常工作（新导入可以commit），但历史committed事务的publish会延迟。默认 300s"})
public static int cloud_recovery_timeout_seconds = 300;
```

### B.5 恢复完成通知

恢复完成后需要通知`CloudPublishDaemon`开始正常的publish调度：

```java
public class CloudPublishDaemon extends MasterDaemon {

    // 恢复是否已完成
    private volatile boolean recoveryCompleted = false;

    @Override
    protected void runAfterCatalogReady() {
        if (!recoveryCompleted) {
            try {
                recoverCommittedTxns();
                recoveryCompleted = true;
                LOG.info("cloud publish daemon recovery completed, start normal scheduling");
            } catch (Exception e) {
                LOG.warn("cloud publish daemon recovery failed, will retry next round", e);
                // 下一个调度周期会重试恢复
            }
            return;
        }

        // 正常调度 publish
        try {
            schedulePublish();
        } catch (Throwable t) {
            LOG.error("errors in cloud publish daemon", t);
        }
    }
}
```

### B.6 恢复流程的完整时序

```
                FE重启/切主
                    │
                    v
            transferToMaster()
                    │
                    ├── replay journal
                    ├── ...
                    ├── startMasterOnlyDaemonThreads()
                    │     └── CloudPublishDaemon.start()
                    │           │
                    │           ├── 第1次 runAfterCatalogReady()
                    │           │     ├── recoveryCompleted == false
                    │           │     ├── recoverCommittedTxns()
                    │           │     │     ├── 扫描 DB-1 的 COMMITTED txns
                    │           │     │     ├── 扫描 DB-2 的 COMMITTED txns
                    │           │     │     └── ...
                    │           │     ├── recoveryCompleted = true
                    │           │     └── return (不执行 schedulePublish)
                    │           │
                    │           ├── 第2次 runAfterCatalogReady()
                    │           │     ├── recoveryCompleted == true
                    │           │     └── schedulePublish()  <-- 开始正常 publish
                    │           │
                    │           └── ...
                    │
                    ├── isReady = true
                    └── 完成
```

---

## C. MS侧扫描API

### C.1 现有接口分析

当前MS提供的事务查询接口：

| RPC | 功能 | 参数 | 限制 |
|-----|------|------|------|
| `get_txn` | 按txnId或label查询单个事务 | `db_id, txn_id` 或 `db_id, label` | 只能查单个事务 |
| `get_txn_id` | 按label和状态查询txnId | `db_id, label, txn_status[]` | 只返回txnId，需要label |
| `get_prepare_txn_by_coordinator` | 按coordinator查询PREPARED事务 | `ip, id, start_time` | 只查PREPARED状态，只按coordinator过滤 |
| `check_txn_conflict` | 检查事务冲突 | `db_id, table_ids, end_txn_id` | 按table过滤，不按状态过滤 |

以上接口均**不能满足**"按db扫描所有COMMITTED状态事务"的需求。需要新增一个扫描接口。

### C.2 新增RPC：scan_txns_by_status

#### Proto定义

```protobuf
// 按状态扫描事务请求
message ScanTxnsByStatusRequest {
    optional string cloud_unique_id = 1;  // For auth
    optional int64 db_id = 2;             // 数据库ID
    optional TxnStatusPB status = 3;      // 目标状态（如 TXN_STATUS_COMMITTED）
    optional int64 start_txn_id = 4;      // 起始txnId（用于分页，扫描 >= start_txn_id 的事务）
    optional int32 limit = 5;             // 每批返回的最大数量
    optional string request_ip = 6;       // 请求来源IP
}

// 按状态扫描事务响应
message ScanTxnsByStatusResponse {
    optional MetaServiceResponseStatus status = 1;
    repeated TxnInfoPB txn_infos = 2;     // 匹配的事务信息列表（按txnId升序）
}
```

#### RPC注册

```protobuf
service MetaService {
    // ... 现有 RPC ...

    // 按状态扫描事务（用于FE重启恢复）
    rpc scan_txns_by_status(ScanTxnsByStatusRequest) returns (ScanTxnsByStatusResponse);
}
```

### C.3 MS侧实现

#### 实现逻辑

由于`txn_info_key`的编码格式为`0x01 "txn" ${instance_id} "txn_info" ${db_id} ${txn_id}`，可以利用FDB的range scan能力，按`(instance_id, db_id)`作为前缀进行范围扫描：

```cpp
void MetaServiceImpl::scan_txns_by_status(
        ::google::protobuf::RpcController* controller,
        const ScanTxnsByStatusRequest* request,
        ScanTxnsByStatusResponse* response,
        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(scan_txns_by_status, get);

    int64_t db_id = request->db_id();
    TxnStatusPB target_status = request->status();
    int64_t start_txn_id = request->has_start_txn_id() ? request->start_txn_id() : 0;
    int32_t limit = request->has_limit() ? request->limit() : 1000;

    // 限制单次扫描返回的最大数量，防止响应过大
    limit = std::min(limit, (int32_t)10000);

    std::string cloud_unique_id = request->cloud_unique_id();
    instance_id = get_instance_id(resource_mgr_, cloud_unique_id);

    RPC_RATE_LIMIT(scan_txns_by_status)
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = fmt::format("failed to create txn, err={}", err);
        return;
    }

    // 构造扫描范围：从 (instance_id, db_id, start_txn_id) 开始
    std::string begin_key = txn_info_key({instance_id, db_id, start_txn_id});
    // 到 (instance_id, db_id, INT64_MAX) 结束
    std::string end_key = txn_info_key({instance_id, db_id, INT64_MAX});

    // 由于需要过滤状态，实际扫描的行数可能大于limit
    // 设置一个更大的scan limit，防止扫描太多无关事务
    int scan_limit = limit * 10;  // 预估大部分事务不是COMMITTED状态

    std::unique_ptr<RangeGetIterator> it;
    err = txn->get(begin_key, end_key, &it, false, scan_limit);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to scan txn_info, db_id={}, err={}", db_id, err);
        return;
    }

    int count = 0;
    while (it->has_next() && count < limit) {
        auto [k, v] = it->next();
        TxnInfoPB txn_info;
        if (!txn_info.ParseFromArray(v.data(), v.size())) {
            LOG(WARNING) << "failed to parse TxnInfoPB, key=" << hex(k);
            continue;
        }

        if (txn_info.status() == target_status) {
            *response->add_txn_infos() = std::move(txn_info);
            count++;
        }
    }

    // 如果 iterator 还有更多数据但已达到 limit，
    // 调用方可以用最后一个 txn_id + 1 作为 start_txn_id 继续扫描
}
```

#### 性能考虑

1. **扫描量估算**：txn_info_key按db_id分区，每个db内按txn_id顺序存储。COMMITTED状态的事务通常只是所有事务中的极小比例（绝大多数事务处于VISIBLE或ABORTED状态），因此实际需要扫描并过滤的KV数量可能较多。

2. **优化策略**：如果COMMITTED事务过于稀疏，可以考虑一次scan读取更多条目（但不超过FDB单次范围读取的5MB限制），在MS端过滤后返回。`scan_limit = limit * 10`的策略可以根据实际情况调整。

3. **反复扫描无需担心**：通常COMMITTED状态是短暂的中间态（正常情况下很快就会变为VISIBLE），只有在FE异常退出时才会积累。因此恢复时需要扫描的事务数量通常不大。

### C.4 FE侧调用

在`MetaServiceProxy`中新增方法：

```java
// MetaServiceProxy.java
public Cloud.ScanTxnsByStatusResponse scanTxnsByStatus(
        Cloud.ScanTxnsByStatusRequest request) throws RpcException {
    return executeWithMetrics("scanTxnsByStatus",
            (client) -> client.scanTxnsByStatus(request));
}
```

FE恢复时的调用：

```java
private ScanTxnsByStatusResponse scanCommittedTxnsFromMS(long dbId, long startTxnId, int limit)
        throws RpcException {
    ScanTxnsByStatusRequest request = ScanTxnsByStatusRequest.newBuilder()
            .setCloudUniqueId(Config.cloud_unique_id)
            .setDbId(dbId)
            .setStatus(TxnStatusPB.TXN_STATUS_COMMITTED)
            .setStartTxnId(startTxnId)
            .setLimit(limit)
            .setRequestIp(FrontendOptions.getLocalHostAddressCached())
            .build();

    return MetaServiceProxy.getInstance().scanTxnsByStatus(request);
}
```

### C.5 替代方案：复用现有接口（不推荐）

理论上可以通过以下方式"模拟"扫描：
1. 使用`get_current_max_txn_id`获取当前最大txnId
2. 逐个调用`get_txn`查询每个txnId

但这种方式效率极低（N个事务需要N次RPC），且无法按状态过滤，不推荐。

另一个思路是扩展`get_txn_id`接口（当前按label查询），增加按状态批量查询的能力。但`get_txn_id`只返回txnId不返回TxnInfoPB的完整内容，还需要额外调用`get_txn`获取详细信息，也不够高效。

因此，**新增`scan_txns_by_status`RPC是最优方案**。

---

## D. 恢复期间的行为

### D.1 新导入的Commit：正常进行

恢复期间，新的导入请求可以正常执行commit阶段。原因：

1. **Partition commit version存储在FDB中**：commit阶段MS直接从FDB读取`partition_commit_version_key`并递增，不依赖FE内存中的任何信息。
2. **BeginTxn正常**：`begin_txn`完全在MS完成，不依赖FE的committed txns set。
3. **CommitTxn正常**：`commit_txn`在MS中执行partition commit version递增和TxnInfoPB更新，这些操作不依赖FE内存状态。
4. **新commit的事务加入committed txns set**：commit完成后，FE将新事务的信息加入`CommittedTxnManager`，这个操作不需要恢复完成。新旧事务在同一个`CommittedTxnManager`中共存。

```
时间线:
  ├── FE重启
  ├── CloudPublishDaemon开始恢复
  │     ├── 扫描DB-1...
  │     │     │
  │     │     ├── 新导入A commit成功  <-- 正常！
  │     │     │     └── 加入 CommittedTxnManager
  │     │     │
  │     │     ├── 新导入B commit成功  <-- 正常！
  │     │     │     └── 加入 CommittedTxnManager
  │     │     │
  │     │     └── 扫描DB-1完成
  │     ├── 扫描DB-2...
  │     └── 恢复完成
  │
  ├── CloudPublishDaemon开始正常publish
  │     ├── 处理恢复出的历史事务
  │     ├── 处理新导入A的publish
  │     └── 处理新导入B的publish
  └── ...
```

### D.2 新导入的Publish：需要等恢复完成

新导入的commit可以正常进行，但其publish需要等恢复完成后才能开始处理。原因：

1. **版本连续性**：publish需要保证同一partition上版本的连续性（version N必须在version N-1完成后才能开始）。恢复前FE不知道历史committed事务的信息，无法正确检查版本依赖。
2. **CloudPublishDaemon调度**：恢复完成前，`CloudPublishDaemon`不执行`schedulePublish()`，所有publish操作（包括新commit事务的publish）都被挂起。

对用户的影响：
- 新导入的commit立即返回成功
- 新导入线程在`awaitPublish()`处等待
- 恢复完成后`CloudPublishDaemon`开始正常调度，新旧事务的publish按版本顺序推进
- 如果恢复耗时较长且超过了导入的publish等待超时，导入返回`PUBLISH_TIMEOUT`，但commit已成功，后台会继续publish

### D.3 查询：正常进行

查询操作完全不受恢复过程影响。原因：
- 查询使用的是**visible version**（`partition_version_key`），而非commit version
- Visible version在FDB中，由MS管理，不依赖FE内存
- 恢复过程不修改visible version
- 已经visible的数据继续可见，未publish的数据（commit version > visible version）对查询不可见

### D.4 行为对照表

| 操作 | 恢复期间是否可用 | 说明 |
|------|:---:|------|
| 新导入 begin_txn | 可用 | MS独立处理 |
| 新导入 commit_txn | 可用 | partition commit version在FDB中 |
| 新导入 publish | 等待恢复完成 | 需要CloudPublishDaemon调度 |
| 新导入等待publish返回 | 阻塞中 | awaitPublish等待latch |
| 查询 | 可用 | 使用visible version，不受影响 |
| SHOW TRANSACTION | 可用 | 直接调用MS get_txn |
| Compaction | 可用 | 不依赖committed txns set |

---

## E. 重复Publish的处理

### E.1 问题场景

FE切主时，旧Master可能有正在执行的publish任务。当新Master恢复committed txns set后，也会尝试publish同一事务。这导致同一事务的某些tablet可能被两个FE同时publish。

```
旧Master                           新Master
  │                                    │
  ├── 正在publish txn-100               │
  │     ├── tablet-1 APPLYING           │
  │     └── tablet-2 CALC_BITMAP_SENT   │
  │                                    │
  ├── 失去Master角色                    ├── 成为Master
  │   （但可能还有残余线程在执行）         │
  │                                    ├── 恢复committed txns
  │                                    │     └── 恢复出 txn-100
  │                                    │
  │     ├── tablet-1 convert完成?       ├── 开始publish txn-100
  │     └── tablet-2 还在计算?          │     ├── tablet-1 从PENDING开始
  │                                    │     └── tablet-2 从PENDING开始
```

### E.2 幂等性保证

整个publish流水线中的每个步骤都必须支持幂等操作：

| 步骤 | 操作 | 幂等性保证 |
|------|------|-----------|
| CalcDeleteBitmapTask | BE计算delete bitmap并写入MS | `update_delete_bitmap` RPC本身是幂等的：相同的bitmap key写入相同的值，FDB事务机制保证不会重复写入 |
| convert_tmp_rowset | MS将tmp rowset转正为formal rowset | **幂等设计**（参见Module 2）：如果formal rowset已存在且rowset_id匹配，直接返回成功 |
| BE本地apply | BE加载转正后的rowset到本地元数据 | BE端通过检查max_version是否已更新来判断是否已apply，已apply则跳过 |
| 轻量级publish | MS更新visible version | **幂等设计**（参见Module 3）：如果visible version已经 >= commit version，直接返回成功 |

### E.3 具体场景分析

**场景1：旧Master的convert_tmp_rowset已完成，新Master再次执行**

```
旧Master: convert_tmp_rowset(txn=100, tablet=1, version=5)  --> 成功
新Master: convert_tmp_rowset(txn=100, tablet=1, version=5)  --> 检测到已转正，返回成功
```

MS在`convert_tmp_rowset`实现中检查：
- 如果formal rowset key已存在，比对rowset_id
- rowset_id匹配则返回成功（幂等）
- rowset_id不匹配则返回版本冲突错误（异常情况）

**场景2：旧Master的CalcDeleteBitmapTask还在BE执行，新Master也下发了同一任务**

```
旧Master下发: CalcDeleteBitmapTask(txn=100, tablet=1) --> BE-1执行中
新Master下发: CalcDeleteBitmapTask(txn=100, tablet=1) --> BE-1或其他BE执行
```

两个任务都会执行delete bitmap计算。由于delete bitmap的计算结果是确定性的（基于相同的输入rowset），写入MS的结果一致，不会产生数据错误。`update_delete_bitmap`写入相同的bitmap数据，FDB事务会保证最终一致。

**场景3：旧Master正在执行轻量级publish，新Master也尝试**

```
旧Master: lightweightPublish(txn=100) --> visible version 4 -> 5，返回成功
新Master: lightweightPublish(txn=100) --> 发现 visible version 已经 >= 5，返回成功
```

MS在轻量级publish实现中检查：
- 读取当前visible version
- 如果已经 >= 目标version，说明已经publish过，直接返回成功
- 如果 < 目标version，执行version递增

### E.4 旧Master残余线程的清理

旧Master失去Master角色后，`CloudPublishDaemon`（继承`MasterDaemon`）会自动停止调度。但已经提交到线程池中正在执行的任务不会被立即中断。这些残余任务可能会：

1. 继续执行并完成（幂等性保证不会出错）
2. 执行失败（旧Master的MetaServiceProxy可能已断开）

两种情况都不会影响新Master的正确性。新Master从PENDING状态重新开始push所有tablet，幂等性保证已完成的步骤不会重复执行，未完成的步骤会被重新执行。

### E.5 TxnInfoPB.published_tablet_ids的作用

`TxnInfoPB`中的`published_tablet_ids`字段（Module 0定义）记录了已完成转正的tablet列表。这个字段的主要作用：

1. **断点续做**：新Master恢复后，可以通过检查`published_tablet_ids`跳过已完成的tablet，减少不必要的重复工作
2. **进度追踪**：提供publish进度的可观测性

但由于整个流水线的幂等性，即使不使用`published_tablet_ids`，直接从PENDING重新开始也是正确的——已完成的步骤会被幂等跳过。`published_tablet_ids`是一个**优化**而非**必需**。

---

## F. 恢复性能优化

### F.1 按DB分批扫描

- 每批扫描`cloud_recovery_scan_batch_size`（默认1000）条事务
- 在MS端通过range scan一次读取，避免逐条RPC
- 按txn_id升序返回，支持分页续扫

### F.2 并行恢复（可选优化）

如果db数量较多，可以并行扫描多个db：

```java
private void recoverCommittedTxns() {
    List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();

    // 使用并行流或线程池并行扫描
    ExecutorService recoveryPool = Executors.newFixedThreadPool(
            Math.min(dbIds.size(), Config.cloud_recovery_parallel_threads));

    List<Future<Integer>> futures = new ArrayList<>();
    for (long dbId : dbIds) {
        futures.add(recoveryPool.submit(() -> recoverDbCommittedTxns(dbId)));
    }

    int total = 0;
    for (Future<Integer> f : futures) {
        total += f.get();
    }

    recoveryPool.shutdown();
    LOG.info("recovery completed, total={} committed txns", total);
}
```

初期可以不做并行优化，串行扫描即可。当db数量或committed事务数量非常大时再考虑并行化。

### F.3 恢复期间新commit事务不受影响

如B.1节所述，恢复期间新导入的commit正常进行，新commit的事务直接加入`CommittedTxnManager`，不需要等恢复完成。恢复的是**历史committed事务**。

这意味着恢复耗时不会阻塞新导入的commit操作，只会延迟新导入的publish（因为CloudPublishDaemon在恢复完成前不执行publish调度）。

### F.4 恢复耗时预估

| 因素 | 典型值 | 说明 |
|------|--------|------|
| 每次扫描RPC延迟 | 5-20ms | MS range scan，取决于返回数据量 |
| 每批扫描数量 | 1000 | 可配置 |
| COMMITTED事务总数 | 0-100（正常），100-10000（极端） | 正常情况下很少，FE异常退出时可能积累 |
| DB数量 | 1-100 | 每个db独立扫描 |
| 总恢复耗时 | **<1s（正常），数秒（极端）** | N/batch_size * rpc_latency * db_count |

正常情况下（committed事务数 < 100），恢复在1秒内完成。即使极端情况下积累了1万个committed事务，按1000/批、20ms/批计算，10批 * 20ms = 200ms也能完成。加上多个db的开销，总时间通常在数秒以内。

### F.5 恢复超时处理

如果恢复在`cloud_recovery_timeout_seconds`内未完成：

```java
private void recoverCommittedTxns() {
    long deadline = System.currentTimeMillis() + Config.cloud_recovery_timeout_seconds * 1000L;
    // ...
    for (long dbId : dbIds) {
        if (System.currentTimeMillis() > deadline) {
            LOG.warn("recovery timeout after {}s, recovered {} txns so far, "
                    + "remaining dbs will be recovered in next round",
                    Config.cloud_recovery_timeout_seconds, totalRecovered);
            // 不设置 recoveryCompleted = true，下一轮继续恢复
            return;
        }
        // 扫描该db...
    }
    // 所有db扫描完成
    recoveryCompleted = true;
}
```

超时不是致命错误：
- 已恢复的事务可以正常publish
- 下一轮调度时继续恢复未完成的db
- 新导入的commit不受影响

---

## G. 关键文件和修改点

### G.1 新增Proto定义

| 文件 | 修改内容 |
|------|---------|
| `gensrc/proto/cloud.proto` | 新增`ScanTxnsByStatusRequest`和`ScanTxnsByStatusResponse` message；在`MetaService` service中注册`scan_txns_by_status` RPC |

### G.2 MS侧新增

| 文件 | 修改内容 |
|------|---------|
| `cloud/src/meta-service/meta_service_txn.cpp` | 实现`MetaServiceImpl::scan_txns_by_status()`方法：按db_id做range scan，过滤指定状态的事务 |
| `cloud/src/meta-service/meta_service.h` | 声明`scan_txns_by_status`方法 |

### G.3 FE侧修改

| 文件 | 修改内容 |
|------|---------|
| `fe/fe-core/src/main/java/org/apache/doris/cloud/rpc/MetaServiceProxy.java` | 新增`scanTxnsByStatus()`方法 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CloudPublishDaemon.java` | 新增恢复逻辑：`recoverCommittedTxns()`、`scanCommittedTxnsFromMS()`、`buildEntryFromTxnInfo()`方法；新增`recoveryCompleted`标志位；修改`runAfterCatalogReady()`在恢复完成前不执行publish调度 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/transaction/CommittedTxnManager.java` | 无需修改，`addCommittedTxn()`同时服务于正常commit和恢复 |
| `fe/fe-core/src/main/java/org/apache/doris/cloud/catalog/CloudEnv.java` | 可能需要在`startMasterOnlyDaemonThreads()`中确保`CloudPublishDaemon`的启动时机正确 |
| `fe/fe-common/src/main/java/org/apache/doris/common/Config.java` | 新增`cloud_recovery_scan_batch_size`和`cloud_recovery_timeout_seconds`配置项 |

### G.4 文件修改总览

```
gensrc/proto/cloud.proto
  ├── + message ScanTxnsByStatusRequest { ... }
  ├── + message ScanTxnsByStatusResponse { ... }
  └── + service MetaService { rpc scan_txns_by_status(...) }

cloud/src/meta-service/meta_service_txn.cpp
  └── + MetaServiceImpl::scan_txns_by_status()

cloud/src/meta-service/meta_service.h
  └── + scan_txns_by_status() 声明

fe/.../cloud/rpc/MetaServiceProxy.java
  └── + scanTxnsByStatus()

fe/.../cloud/transaction/CloudPublishDaemon.java
  ├── + recoveryCompleted 标志
  ├── + recoverCommittedTxns()
  ├── + scanCommittedTxnsFromMS()
  ├── + buildEntryFromTxnInfo()
  └── ~ runAfterCatalogReady() (增加恢复检查)

fe/.../common/Config.java
  ├── + cloud_recovery_scan_batch_size
  └── + cloud_recovery_timeout_seconds
```

---

## H. 单元测试要点

### H.1 MS侧scan_txns_by_status测试

| 测试用例 | 验证点 |
|---------|--------|
| `testScanCommittedTxns` | 创建多个不同状态的事务（PREPARED、COMMITTED、VISIBLE、ABORTED），扫描COMMITTED状态，验证只返回COMMITTED事务 |
| `testScanWithPagination` | 创建100个COMMITTED事务，limit=10分页扫描，验证分页结果完整且按txnId升序 |
| `testScanEmptyDb` | 对空db执行扫描，验证返回空列表不报错 |
| `testScanNoMatchingStatus` | db中只有VISIBLE状态事务，扫描COMMITTED状态，验证返回空列表 |
| `testScanStartTxnId` | 设置start_txn_id跳过前面的事务，验证只返回 >= start_txn_id的事务 |
| `testScanLimitEnforcement` | 验证返回数量不超过limit |
| `testScanMultipleDbs` | 不同db中各有COMMITTED事务，验证按db隔离扫描 |

### H.2 FE侧恢复流程测试

| 测试用例 | 验证点 |
|---------|--------|
| `testRecoveryBasic` | Mock MS返回若干COMMITTED事务，验证恢复后CommittedTxnManager中包含这些事务 |
| `testRecoveryOnlyTwoPhaseCommit` | Mock MS返回普通COMMITTED和mow_async_publish=true的事务，验证只恢复后者 |
| `testRecoveryWithPagination` | Mock MS返回大量事务（超过单批limit），验证分批扫描直到所有事务恢复完毕 |
| `testRecoveryMultipleDbs` | Mock多个db各有COMMITTED事务，验证所有db的事务都被恢复 |
| `testRecoveryEmptyResult` | Mock MS返回空结果（无COMMITTED事务），验证恢复正常完成，recoveryCompleted=true |
| `testRecoveryEntryFields` | 验证从TxnInfoPB构建的CommittedTxnEntry字段正确：txnId、dbId、tableId、partitionCommitVersions、tabletCommitInfos等 |

### H.3 恢复期间行为测试

| 测试用例 | 验证点 |
|---------|--------|
| `testNewCommitDuringRecovery` | 恢复进行中时新导入commit，验证commit成功且新事务加入CommittedTxnManager |
| `testPublishBlockedDuringRecovery` | 恢复进行中时验证schedulePublish()不被调用 |
| `testPublishStartsAfterRecovery` | 恢复完成后验证schedulePublish()正常执行，且包含恢复出的事务 |
| `testRecoveryTimeout` | Mock恢复超时（设置很小的timeout），验证部分恢复的事务已在CommittedTxnManager中，下一轮继续恢复 |

### H.4 幂等性和重复publish测试

| 测试用例 | 验证点 |
|---------|--------|
| `testIdempotentConvertTmpRowset` | 同一tablet的convert_tmp_rowset被调用两次，验证第二次返回成功且无副作用 |
| `testIdempotentLightweightPublish` | 同一事务的lightweight publish被调用两次，验证第二次返回成功 |
| `testDuplicateRecoveryNoError` | 连续执行两次恢复流程（模拟重复触发），验证CommittedTxnManager中不出现重复entry |
| `testOldMasterResidualTask` | 模拟旧Master的残余publish任务与新Master的publish任务并发执行，验证最终结果正确 |

### H.5 集成测试

| 测试用例 | 验证点 |
|---------|--------|
| `testFullRecoveryAndPublish` | 端到端测试：创建事务 -> commit -> 模拟FE重启 -> 恢复 -> publish完成 -> 数据可见 |
| `testRecoveryWithOngoingImport` | 恢复进行中同时有新导入，恢复完成后新旧事务都正确publish |
| `testLeaderSwitchRecovery` | 模拟Leader切换：旧Leader正在publish -> 新Leader接管 -> 恢复 -> 继续publish完成 |

### H.6 Mock策略

测试时需要Mock的外部依赖：

| 依赖 | Mock方式 | 说明 |
|------|---------|------|
| `MetaServiceProxy.scanTxnsByStatus()` | MockUp注入 | 返回预设的TxnInfoPB列表 |
| `Env.getCurrentInternalCatalog().getDbIds()` | MockUp注入 | 返回预设的db列表 |
| `CommittedTxnManager` | 使用真实实现 | 内存数据结构，不需要Mock |
| `CloudPublishDaemon` | 部分Mock | 只Mock MS RPC调用，保留恢复逻辑 |
