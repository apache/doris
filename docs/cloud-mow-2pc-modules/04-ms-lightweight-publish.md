# Module 3: MS侧轻量级Publish

## 1. 概述

在两阶段提交方案中，当所有tablet的per-tablet工作（delete bitmap计算 + rowset转正 + tablet stats更新）全部完成后，FE需要执行最终的"轻量级publish"来使事务对查询可见。这个操作在MS侧执行，本质上只需要：

- 更新每个涉及partition的visible version（+1）
- 将TxnInfoPB的状态从 `TXN_STATUS_COMMITTED` 更新为 `TXN_STATUS_VISIBLE`
- 删除 `txn_running_key`
- 创建 `CommitTxnLogPB`（供recycler使用）
- 创建 `recycle_txn_key`（供recycler回收过期txn label使用）

这是一个 **O(1)** 操作（相对于tablet数量），因为所有per-tablet相关的重量级工作已在之前的步骤中独立完成。

## 2. 当前commit_txn最终阶段分析

### 2.1 当前commit_txn的整体流程

当前 `commit_txn` RPC 的入口函数位于 `MetaServiceImpl::commit_txn()`（`meta_service_txn.cpp:3257`），根据事务大小和配置，它会走两条路径：

- **commit_txn_immediately()**：一个大的FDB事务中完成所有工作
- **commit_txn_eventually()**：先标记事务为 COMMITTED，然后通过 `TxnLazyCommitter` 异步完成剩余工作

在 `commit_txn_immediately()` 中，一个FDB事务内完成以下所有操作：

1. 读取 TxnInfoPB，验证事务状态
2. 获取所有tablet的 TabletIndexPB
3. 获取所有partition的当前 visible version
4. 如果存在 `last_pending_txn_id`（前序lazy commit未完成），等待其完成后重试
5. 为每个rowset分配版本号（partition version + 1）
6. 处理MOW锁（`process_mow_when_commit_txn`）：验证delete bitmap lock未过期，删除pending delete bitmap key
7. 将tmp rowset写为正式rowset（`meta_rowset_key`）
8. **更新每个partition的visible version**
9. 更新table version
10. **更新TxnInfoPB：status = TXN_STATUS_VISIBLE，设置commit_time/finish_time**
11. 更新tablet stats
12. 删除tmp rowset key
13. **删除txn_running_key**
14. **创建recycle_txn_key 或写入CommitTxnLogPB**（取决于versioned_write是否启用）
15. 提交FDB事务

其中加粗的步骤4、8、10、13、14就是"最终阶段"——即轻量级publish需要做的核心工作。

### 2.2 更新visible version的流程

在 `commit_txn_immediately()` 中（`meta_service_txn.cpp:1793-1831`）：

```cpp
// Save versions
for (auto& [partition_id, version] : versions) {
    int64_t new_version = version + 1;
    VersionPB version_pb;
    version_pb.set_version(new_version);
    version_pb.set_update_time_ms(version_update_time_ms);
    // 序列化后写入
    auto version_key = partition_version_key({instance_id, db_id, table_id, partition_id});
    txn->put(version_key, ver_val);

    // versioned write模式下额外写入versioned key
    if (is_versioned_write) {
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        versioned_put(txn.get(), partition_version_key, ver_val);
    }

    // 将版本信息填入response
    response->add_table_ids(table_id);
    response->add_partition_ids(partition_id);
    response->add_versions(new_version);
}
```

在新方案中，partition的commit_version在Module 1（MS Commit阶段）就已经分配好了。轻量级publish只需将visible version更新为commit_version。

### 2.3 CommitTxnLogPB的创建和意义

`CommitTxnLogPB` 定义在 `cloud.proto:748`：

```protobuf
message CommitTxnLogPB {
    repeated int64 table_ids = 1;
    map<int64, int64> partition_version_map = 2;   // partition_id -> new_version
    map<int64, int64> tablet_to_partition_map = 3;  // tablet_id -> partition_id
    optional int64 txn_id = 4;
    optional RecycleTxnPB recycle_txn = 5;
    optional int64 db_id = 6;
}
```

**用途**：`CommitTxnLogPB` 是recycler回收数据的关键依据。它记录了：
- 哪些table受本次事务影响（`table_ids`）
- 每个partition的新版本号（`partition_version_map`）
- tablet到partition的映射（`tablet_to_partition_map`）
- 事务回收信息（`recycle_txn`，包含创建时间和label）

在 **versioned_write** 模式下，CommitTxnLogPB被嵌入到 `OperationLogPB` 中通过 `versioned::blob_put` 写入FDB（`meta_service_txn.cpp:1974-1984`）：

```cpp
commit_txn_log.mutable_recycle_txn()->Swap(&recycle_pb);
std::string log_key = versioned::log_key({instance_id});
OperationLogPB operation_log;
operation_log.mutable_commit_txn()->Swap(&commit_txn_log);
versioned::blob_put(txn.get(), log_key, operation_log);
```

在非versioned_write模式下，不写CommitTxnLogPB（recycler通过recycle_txn_key回收）。

### 2.4 txn_running_key的作用和删除

`txn_running_key` 在事务begin时创建，存储 `TxnRunningPB`（包含关联的table_ids和超时时间）。它有两个核心作用：

1. **事务冲突检测**：通过scan running key range来发现同一DB下正在运行的事务，检测label冲突和超时
2. **事务存活标记**：running key存在表明事务尚未完成；删除running key是事务完结的信号

在commit最终阶段删除（`meta_service_txn.cpp:1966-1968`）：

```cpp
const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
txn->remove(running_key);
```

### 2.5 recycle_txn_key的作用

`recycle_txn_key` 存储 `RecycleTxnPB`（creation_time + label），供recycler在回收过期事务label时使用。recycler扫描recycle_txn_key range，找到过期的事务记录并清理对应的TxnInfoPB和label index。

在非versioned_write模式下，recycle_txn_key在commit时显式写入（`meta_service_txn.cpp:1986-1996`）：

```cpp
std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
RecycleTxnPB recycle_pb;
recycle_pb.set_creation_time(commit_time);
recycle_pb.set_label(txn_info.label());
txn->put(recycle_key, recycle_val);
```

在versioned_write模式下，`RecycleTxnPB` 嵌入到 `CommitTxnLogPB.recycle_txn` 字段中，随 OperationLogPB 一起写入，不单独创建recycle_txn_key。

## 3. 轻量级Publish API设计

### 3.1 API选择：复用commit_txn RPC

**建议**：复用现有的 `commit_txn` RPC，通过 `CommitTxnRequest` 中新增的标志字段来区分是常规commit还是两阶段提交的轻量级publish。

**理由**：
- 避免新增RPC带来的proto和service注册开销
- 复用现有的鉴权、限流、instance_id解析等基础设施
- `CommitTxnResponse` 的结构（partition_ids + versions + table_stats）完全适用于轻量级publish的响应需求
- 与当前`is_2pc`/`is_txn_load`等通过flag区分路径的模式一致

### 3.2 Proto变更

在 `CommitTxnRequest` 中新增字段：

```protobuf
message CommitTxnRequest {
    // ... 现有字段 ...
    optional bool is_txn_load = 9;
    repeated SubTxnInfo sub_txn_infos = 10;
    optional bool enable_txn_lazy_commit = 11;
    optional string request_ip = 12;

    // 新增：两阶段提交轻量级publish标志
    optional bool is_2pc_lightweight_publish = 13;
    // 新增：每个partition期望的commit_version（由FE在commit阶段记录）
    map<int64, int64> partition_commit_versions = 14;
}
```

**说明**：
- `is_2pc_lightweight_publish`：标识本次请求是两阶段提交的轻量级publish，而非常规commit
- `partition_commit_versions`：FE在commit阶段记录的每个partition的commit_version，在此处传入用于校验一致性。key=partition_id, value=commit_version

### 3.3 入口路由

在 `MetaServiceImpl::commit_txn()` 中增加路由逻辑：

```cpp
void MetaServiceImpl::commit_txn(...) {
    // ... 现有的预处理 ...

    if (request->has_is_txn_load() && request->is_txn_load()) {
        commit_txn_with_sub_txn(...);
        return;
    }

    // 新增：两阶段提交轻量级publish路径
    if (request->has_is_2pc_lightweight_publish() && request->is_2pc_lightweight_publish()) {
        commit_txn_2pc_lightweight_publish(request, response, code, msg,
                                            instance_id, db_id, stats);
        return;
    }

    // ... 现有的immediately/eventually路径 ...
}
```

### 3.4 核心实现：commit_txn_2pc_lightweight_publish()

在一个FDB事务中完成以下步骤：

```
步骤1: 读取并验证TxnInfoPB
步骤2: 验证partition commit_version一致性
步骤3: 更新每个partition的visible version = commit_version
步骤4: 更新TxnInfoPB: status=VISIBLE, 清空involved_tablets相关信息
步骤5: 删除txn_running_key
步骤6: 创建CommitTxnLogPB
步骤7: 创建recycle_txn_key（或嵌入OperationLogPB）
步骤8: 更新table version
步骤9: 提交FDB事务
```

**伪代码**：

```cpp
void MetaServiceImpl::commit_txn_2pc_lightweight_publish(
        const CommitTxnRequest* request, CommitTxnResponse* response,
        MetaServiceCode& code, std::string& msg,
        const std::string& instance_id, int64_t db_id, KVStats& stats) {
    int64_t txn_id = request->txn_id();
    bool is_versioned_write = is_version_write_enabled(instance_id);
    bool is_versioned_read = is_version_read_enabled(instance_id);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) { /* 错误处理 */ return; }
    if (is_versioned_write) {
        txn->enable_get_versionstamp();
    }

    // ========== 步骤1: 读取并验证TxnInfoPB ==========
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    std::string info_val;
    err = txn->get(info_key, &info_val);
    // ... 错误处理 ...

    TxnInfoPB txn_info;
    txn_info.ParseFromString(info_val);

    // 幂等性：如果已经VISIBLE，直接返回成功
    if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
        code = MetaServiceCode::OK;
        response->mutable_txn_info()->CopyFrom(txn_info);
        return;
    }

    // 必须是COMMITTED状态（两阶段commit阶段完成后的状态）
    if (txn_info.status() != TxnStatusPB::TXN_STATUS_COMMITTED) {
        code = MetaServiceCode::TXN_INVALID_STATUS;
        msg = fmt::format("txn {} status is {}, expected COMMITTED",
                          txn_id, TxnStatusPB_Name(txn_info.status()));
        return;
    }

    // ========== 步骤2: 验证partition commit_version一致性 ==========
    // 从request中获取FE传入的partition_commit_versions
    const auto& expected_versions = request->partition_commit_versions();
    if (expected_versions.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "partition_commit_versions is empty";
        return;
    }

    // 批量读取当前partition versions
    // 注意：此处需要知道每个partition对应的table_id来构造key
    // 可以从TxnInfoPB中获取table_ids，或在request中携带必要信息

    // ========== 步骤3: 更新每个partition的visible version ==========
    CommitTxnLogPB commit_txn_log;
    commit_txn_log.set_txn_id(txn_id);
    commit_txn_log.set_db_id(db_id);

    int64_t version_update_time_ms = /* current time ms */;
    response->set_version_update_time_ms(version_update_time_ms);

    for (const auto& [partition_id, commit_version] : expected_versions) {
        // 读取当前partition version
        auto ver_key = partition_version_key({instance_id, db_id, table_id, partition_id});
        std::string ver_val;
        err = txn->get(ver_key, &ver_val);
        VersionPB current_version_pb;
        current_version_pb.ParseFromString(ver_val);

        // 验证：当前visible version + 1 应该等于commit_version
        // 或者：当前version含有pending_txn_ids且第一个是本txn_id
        // （具体验证逻辑取决于commit阶段的实现方式）

        // 更新visible version
        VersionPB new_version_pb;
        new_version_pb.set_version(commit_version);
        new_version_pb.set_update_time_ms(version_update_time_ms);
        new_version_pb.clear_pending_txn_ids();  // 清除pending标记
        txn->put(ver_key, new_version_pb.SerializeAsString());

        // versioned write
        if (is_versioned_write) {
            std::string versioned_key =
                    versioned::partition_version_key({instance_id, partition_id});
            versioned_put(txn.get(), versioned_key, new_ver_val);
        }

        commit_txn_log.mutable_partition_version_map()->insert({partition_id, commit_version});

        // 填充response
        response->add_table_ids(table_id);
        response->add_partition_ids(partition_id);
        response->add_versions(commit_version);
    }

    // ========== 步骤4: 更新TxnInfoPB ==========
    txn_info.set_status(TxnStatusPB::TXN_STATUS_VISIBLE);
    uint64_t finish_time = /* current time ms */;
    txn_info.set_finish_time(finish_time);
    // 可选：清空不再需要的字段以减小存储
    txn->put(info_key, txn_info.SerializeAsString());

    // ========== 步骤5: 删除txn_running_key ==========
    const std::string running_key = txn_running_key({instance_id, db_id, txn_id});
    txn->remove(running_key);

    // ========== 步骤6 & 7: CommitTxnLogPB + recycle ==========
    RecycleTxnPB recycle_pb;
    recycle_pb.set_creation_time(finish_time);
    recycle_pb.set_label(txn_info.label());

    if (is_versioned_write) {
        commit_txn_log.mutable_recycle_txn()->Swap(&recycle_pb);
        std::string log_key = versioned::log_key({instance_id});
        OperationLogPB operation_log;
        operation_log.mutable_commit_txn()->Swap(&commit_txn_log);
        versioned::blob_put(txn.get(), log_key, operation_log);
    } else {
        std::string recycle_key = recycle_txn_key({instance_id, db_id, txn_id});
        txn->put(recycle_key, recycle_pb.SerializeAsString());
    }

    // ========== 步骤8: 更新table version ==========
    // 从involved table_ids中去重后，更新每个table的version
    std::set<int64_t> table_ids_set;
    for (const auto& [partition_id, _] : expected_versions) {
        // 获取partition对应的table_id（从request或TxnInfoPB中）
        table_ids_set.insert(table_id);
    }
    std::map<int64_t, int64_t> table_version_map;
    for (int64_t table_id : table_ids_set) {
        update_table_version(txn.get(), instance_id, db_id, table_id);
        commit_txn_log.add_table_ids(table_id);
        // 获取table version用于response（逻辑同现有代码）
    }

    // ========== 步骤9: 提交FDB事务 ==========
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit, txn_id={} err={}", txn_id, err);
        return;
    }

    // 填充response的table_stats
    for (const auto& [table_id, table_version] : table_version_map) {
        TableStatsPB* stats_pb = response->add_table_stats();
        stats_pb->set_table_id(table_id);
        stats_pb->set_table_version(table_version);
    }
    response->mutable_txn_info()->CopyFrom(txn_info);
}
```

### 3.5 不做的事情

轻量级publish明确 **不执行** 以下操作（这些都已在per-tablet阶段完成）：

| 操作 | 说明 | 完成阶段 |
|------|------|----------|
| `scan_tmp_rowset` | 扫描临时rowset | 不需要，per-tablet阶段已转正 |
| 写入正式rowset（`meta_rowset_key`） | tmp rowset转为formal | Module 2 (convert_tmp_rowset) |
| `process_mow_when_commit_txn` | MOW锁检查+pending delete bitmap删除 | Module 2 或 BE侧 |
| 更新tablet stats | 每个tablet的行数/大小统计 | Module 2 (convert_tmp_rowset) |
| 删除tmp rowset key | 临时rowset清理 | Module 2 (convert_tmp_rowset) |
| `TxnLazyCommitter` 提交 | lazy commit机制 | 不需要，新方案不走此路径 |

## 4. 和当前大事务/lazy commit的关系

### 4.1 当前lazy commit机制

当前 `commit_txn_eventually()` 的工作方式：

1. 将TxnInfoPB状态设为 `TXN_STATUS_COMMITTED`（不是VISIBLE）
2. 在VersionPB中记录 `pending_txn_ids`（表示该partition有未完成的事务）
3. 提交这个轻量FDB事务（只修改了txn_info和partition version）
4. 提交到 `TxnLazyCommitter` 异步执行真正的工作：
   - 按partition分组，每个partition独立提交：扫描tmp rowset → 转正rowset → 更新stats → 删除tmp key
   - 所有partition完成后，`make_committed_txn_visible()`：设TxnInfoPB=VISIBLE + 删除running_key + 创建recycle_txn_key

### 4.2 新方案下的区别

启用两阶段提交的表 **不再需要** lazy commit机制，原因：

1. **per-tablet工作由FE Publish Daemon调度**：不再由MS的TxnLazyCommitter在后台推进，而是由FE主动调度每个tablet的工作
2. **`TXN_STATUS_COMMITTED` 语义变更**：在新方案中，COMMITTED表示两阶段commit阶段已完成（partition commit_version已分配），而非lazy commit中"已标记但rowset未转正"的含义
3. **没有pending_txn_ids**：新方案中partition version在commit阶段直接分配commit_version（通过Module 1），不使用VersionPB的`pending_txn_ids`机制
4. **没有FDB大事务问题**：因为per-tablet转正是独立的FDB事务，最终publish也是轻量级事务，不存在"事务太大需要fallback到lazy commit"的场景

### 4.3 如何区分

在 `commit_txn()` 入口处，通过 `request->is_2pc_lightweight_publish()` 标志区分：

```cpp
if (request->has_is_2pc_lightweight_publish() && request->is_2pc_lightweight_publish()) {
    // 新路径：两阶段提交的轻量级publish
    commit_txn_2pc_lightweight_publish(...);
    return;
}
```

此外，表级别可以通过表属性 `enable_2pc_commit`（或类似名称）来控制是否启用两阶段提交。FE在发起请求时根据表属性设置相应的request标志。

## 5. Response设计

### 5.1 复用现有CommitTxnResponse

```protobuf
message CommitTxnResponse {
    optional MetaServiceResponseStatus status = 1;
    optional TxnInfoPB txn_info = 2;
    repeated int64 table_ids = 3;
    repeated int64 partition_ids = 4;
    repeated int64 versions = 5;       // 每个partition的新visible version
    repeated TableStatsPB table_stats = 6;
    optional int64 version_update_time_ms = 7;
}
```

现有Response结构完全满足需求，无需修改。

### 5.2 返回内容

- `txn_info`：更新后的TxnInfoPB（status=VISIBLE）
- `table_ids` / `partition_ids` / `versions`：三元组，每个partition对应的table_id和新的visible version
- `table_stats`：每个table的table_version（用于FE判断元数据是否需要刷新）
- `version_update_time_ms`：版本更新时间戳

### 5.3 FE使用方式

FE收到成功响应后：
1. 根据返回的partition visible version通知BE更新本地元数据
2. 根据table_version判断是否需要触发元数据同步
3. 将事务从committed txns集合中移除
4. 更新事务状态为VISIBLE

## 6. 错误处理

### 6.1 幂等性保证

轻量级publish支持安全重试。关键的幂等性设计：

| 场景 | 处理方式 |
|------|---------|
| TxnInfoPB已经是VISIBLE | 直接返回OK，携带当前txn_info |
| FDB事务提交后FE未收到响应 | FE重试，命中VISIBLE幂等检查 |
| 部分partition已更新，FDB事务失败 | FDB事务原子性保证回滚，安全重试 |

```cpp
// 幂等性检查
if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
    code = MetaServiceCode::OK;
    response->mutable_txn_info()->CopyFrom(txn_info);
    // 注意：此时response中的versions信息可能需要重新读取并填充
    return;
}
```

### 6.2 FDB事务冲突（TXN_CONFLICT）

FDB事务冲突可能发生在：
- 并发的publish请求修改同一partition version（理论上不应发生，因为FE有调度控制）
- 并发的compaction修改table version

处理方式：返回 `KV_TXN_CONFLICT` 错误码，FE侧重试。因为操作是幂等的，重试安全。

### 6.3 状态不一致检测

```
错误场景                          | 错误码                  | 处理方式
TxnInfoPB不存在                   | TXN_ID_NOT_FOUND       | FE侧标记事务异常
TxnInfoPB状态为ABORTED            | TXN_ALREADY_ABORTED    | FE侧标记事务失败
TxnInfoPB状态为PREPARED           | TXN_INVALID_STATUS     | 说明commit阶段未完成，不应发生
partition version与预期不一致      | UNDEFINED_ERR          | 需要排查，可能有并发问题
```

### 6.4 版本一致性验证

在步骤2中，需要验证每个partition的当前状态是否与预期一致。具体验证方式取决于Module 1（commit阶段）写入partition version的方式：

- **方案A**：如果commit阶段使用 `pending_txn_ids` 机制，则轻量级publish需验证pending_txn_ids包含当前txn_id，然后清除pending并更新version
- **方案B**（推荐）：如果commit阶段直接写入commit_version但不更新visible version，则轻量级publish只需要验证当前visible version + 1 == commit_version，然后更新visible version

无论哪种方案，如果验证失败，应返回错误而非静默继续。

### 6.5 超时处理

轻量级publish作为一个FDB事务，受FDB事务超时限制（默认5秒）。但因为只涉及少量KV操作（与partition数量成正比，而非tablet数量），正常情况下不会超时。

如果确实超时，FE安全重试即可。

## 7. 关键文件和修改点

### 7.1 Proto文件

| 文件 | 修改内容 |
|------|---------|
| `gensrc/proto/cloud.proto` | `CommitTxnRequest` 增加 `is_2pc_lightweight_publish` 和 `partition_commit_versions` 字段 |

### 7.2 MS核心实现

| 文件 | 修改内容 |
|------|---------|
| `cloud/src/meta-service/meta_service.h` | 声明 `commit_txn_2pc_lightweight_publish()` 成员函数 |
| `cloud/src/meta-service/meta_service_txn.cpp` | (1) `commit_txn()` 入口增加路由分支 (2) 实现 `commit_txn_2pc_lightweight_publish()` |

### 7.3 不需要修改的文件

| 文件 | 原因 |
|------|------|
| `cloud/src/meta-service/txn_lazy_committer.h/cpp` | 新方案不走lazy commit路径，不需要修改 |
| `cloud/src/recycler/recycler.cpp` | CommitTxnLogPB和recycle_txn_key的格式不变，recycler无需修改 |

### 7.4 修改清单

**`gensrc/proto/cloud.proto`**：

```protobuf
message CommitTxnRequest {
    // ... 现有字段 (1-12) ...
    optional bool is_2pc_lightweight_publish = 13;
    map<int64, int64> partition_commit_versions = 14;  // partition_id -> commit_version
}
```

**`cloud/src/meta-service/meta_service.h`**：

```cpp
class MetaServiceImpl {
    // ...现有成员函数...

    // 新增：两阶段提交轻量级publish
    void commit_txn_2pc_lightweight_publish(
            const CommitTxnRequest* request, CommitTxnResponse* response,
            MetaServiceCode& code, std::string& msg,
            const std::string& instance_id, int64_t db_id, KVStats& stats);
};
```

**`cloud/src/meta-service/meta_service_txn.cpp`**：

在 `commit_txn()` 入口增加路由（约第3285行之后）：

```cpp
// 两阶段提交轻量级publish
if (request->has_is_2pc_lightweight_publish() && request->is_2pc_lightweight_publish()) {
    commit_txn_2pc_lightweight_publish(request, response, code, msg,
                                        instance_id, db_id, stats);
    return;
}
```

新增 `commit_txn_2pc_lightweight_publish()` 函数实现（约500行代码）。

## 8. 单元测试要点

测试文件建议添加在 `cloud/test/txn_lazy_commit_test.cpp` 或新建 `cloud/test/txn_2pc_lightweight_publish_test.cpp`。

### 8.1 基本功能测试

| 测试用例 | 验证内容 |
|---------|---------|
| `LightweightPublishBasicTest` | 正常流程：begin_txn → prepare_rowset → commit(2pc) → per-tablet工作（模拟）→ lightweight publish → 验证partition visible version、TxnInfoPB状态、running key已删除、recycle_txn_key已创建 |
| `LightweightPublishMultiPartitionTest` | 多partition场景：验证所有partition的visible version正确更新，response中包含所有partition的版本信息 |
| `LightweightPublishVersionedWriteTest` | versioned_write模式下的测试：验证OperationLogPB正确写入，CommitTxnLogPB包含正确的partition_version_map和recycle_txn信息 |

### 8.2 幂等性测试

| 测试用例 | 验证内容 |
|---------|---------|
| `LightweightPublishIdempotentTest` | 连续两次调用lightweight publish，第二次应返回OK（非错误） |
| `LightweightPublishAfterVisibleTest` | TxnInfoPB已经是VISIBLE状态时调用，应返回OK并携带txn_info |

### 8.3 错误处理测试

| 测试用例 | 验证内容 |
|---------|---------|
| `LightweightPublishWrongStatusTest` | TxnInfoPB状态不是COMMITTED（例如PREPARED），应返回TXN_INVALID_STATUS |
| `LightweightPublishAbortedTxnTest` | TxnInfoPB状态为ABORTED，应返回TXN_ALREADY_ABORTED |
| `LightweightPublishTxnNotFoundTest` | txn_id不存在，应返回TXN_ID_NOT_FOUND |
| `LightweightPublishVersionMismatchTest` | request中的partition_commit_versions与实际不一致，应返回错误 |
| `LightweightPublishEmptyVersionsTest` | partition_commit_versions为空，应返回INVALID_ARGUMENT |

### 8.4 并发和冲突测试

| 测试用例 | 验证内容 |
|---------|---------|
| `LightweightPublishConflictTest` | 使用SyncPoint模拟FDB事务冲突，验证返回KV_TXN_CONFLICT，且数据未被部分修改 |
| `LightweightPublishConcurrentTest` | 两个线程同时对同一txn执行lightweight publish，验证只有一个成功执行，另一个命中幂等检查 |

### 8.5 Recycler集成测试

| 测试用例 | 验证内容 |
|---------|---------|
| `LightweightPublishRecyclerTest` | lightweight publish完成后，验证recycler能正确扫描到recycle_txn_key（或CommitTxnLogPB），并能正常回收过期的txn label |

### 8.6 测试辅助工具

可以利用现有测试基础设施：
- 使用 `MemTxnKv` 作为内存FDB替代
- 使用 `TEST_SYNC_POINT_CALLBACK` 进行并发控制和故障注入
- 复用 `TxnLazyCommitTest` 中的 helper 函数（`create_tablet`、`begin_txn`、`prepare_rowset` 等）

## 9. 性能分析

### 9.1 FDB事务大小

轻量级publish的FDB事务涉及的KV操作量：

| 操作 | 数量 | 说明 |
|------|------|------|
| Get TxnInfoPB | 1 | 读取事务信息 |
| Get partition version | N_partition | 读取当前版本 |
| Put partition version | N_partition | 更新visible version |
| Put TxnInfoPB | 1 | 更新状态为VISIBLE |
| Remove running_key | 1 | 删除运行标记 |
| Put recycle_txn_key | 1 | 回收信息 |
| Put/Get table version | N_table | 更新表版本 |

总计：约 `2 * N_partition + N_table + 4` 个KV操作。对于典型场景（10-100个partition），这远小于FDB的10MB事务限制，不会遇到 `TXN_BYTES_TOO_LARGE` 问题。

### 9.2 与当前方案的对比

| 指标 | 当前commit_txn_immediately | 轻量级publish |
|------|---------------------------|---------------|
| KV操作数 | O(N_tablet) | O(N_partition) |
| FDB事务大小 | 可能超过10MB限制 | 极小，不会超限 |
| 涉及的数据 | rowset meta + stats + version + lock | 仅version + txn_info |
| 耗时 | 数百ms到数秒 | 数ms到数十ms |
