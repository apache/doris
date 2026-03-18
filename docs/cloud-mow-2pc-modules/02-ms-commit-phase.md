# Module 1: MS侧Commit阶段API

## 1. 当前commit_txn流程分析

### 1.1 入口函数 `commit_txn()`

当前 `MetaServiceImpl::commit_txn()` 的入口逻辑位于 `cloud/src/meta-service/meta_service_txn.cpp:3257`，整体流程为：

```
commit_txn()
  ├── 1. 参数校验、获取instance_id
  ├── 2. get_txn_db_id()                              // 通过txn_index_key读取db_id
  ├── 3. 分支判断:
  │     ├── is_txn_load → commit_txn_with_sub_txn()   // 事务load（多子事务）
  │     └── 普通导入 →
  │           ├── scan_tmp_rowset()                    // 扫描tmp rowset元数据
  │           ├── 判断是否走lazy commit路径
  │           ├── commit_txn_immediately()             // 一阶段立即提交
  │           └── commit_txn_eventually()              // lazy commit路径
```

### 1.2 `commit_txn_immediately()` 详细步骤

位于 `meta_service_txn.cpp:1520`，在一个 `do...while(true)` 循环中执行（用于处理 pending txn 重试）：

| 步骤 | 操作 | 代码位置 | 新方案归属 |
|------|------|----------|-----------|
| S1 | 创建FDB事务 | 1533-1541 | **Commit阶段**（保留） |
| S2 | 读取TxnInfoPB（通过 `txn_info_key`） | 1555-1580 | **Commit阶段**（保留） |
| S3 | 校验事务状态（ABORTED/VISIBLE/2PC检查） | 1582-1617 | **Commit阶段**（保留） |
| S4 | 获取tablet index（`get_tablet_indexes`）：从 `meta_tablet_idx_key` 批量读取每个tablet的 table_id/index_id/partition_id | 1621-1644 | **Publish阶段移除**：commit阶段不需要tablet index，因为FE会在request中直接提供partition列表 |
| S5 | 构建 partition_indexes 映射（partition_id → {db_id, table_id}） | 1646-1652 | **Commit阶段**（保留，但数据来源改变） |
| S6 | 读取每个partition的当前version（`get_partition_versions`），检查pending_txn_ids | 1654-1692 | **Commit阶段**（保留，但读取的是partition_commit_version而非visible_version） |
| S7 | 若存在 pending txn，提交lazy commit任务等待其完成，然后重试 | 1675-1692 | **不再需要**：两阶段提交不使用lazy commit |
| S8 | 为每个tmp rowset分配version（partition_version + 1），设置 start_version/end_version | 1709-1746 | **Publish阶段**（per-tablet转正时分配） |
| S9 | `process_mow_when_commit_txn()`：检查delete bitmap update lock，移除lock和pending delete bitmap key | 1753-1757 | **Publish阶段**（per-tablet转正时处理） |
| S10 | 写入formal rowset meta（`meta_rowset_key`），从tmp转为正式 | 1759-1791 | **Publish阶段**（per-tablet转正RPC） |
| S11 | 写入partition version（version+1，写入 `partition_version_key`） | 1793-1831 | **拆分**：commit阶段写partition_commit_version+1；publish阶段写partition_visible_version+1 |
| S12 | 读取/更新table version（`table_version_key`，atomic_add） | 1834-1873 | **Publish阶段** |
| S13 | 更新TxnInfoPB：status=VISIBLE, commit_time, finish_time, commit_attachment | 1877-1907 | **Commit阶段**（但status设为COMMITTED而非VISIBLE） |
| S14 | 更新tablet stats（data_size, num_rows, num_rowsets等） | 1931-1958 | **Publish阶段**（per-tablet转正时更新） |
| S15 | 删除tmp rowset meta key | 1960-1964 | **Publish阶段**（per-tablet转正时删除） |
| S16 | 删除txn_running_key | 1966-1968 | **Commit阶段**（保留） |
| S17 | 写入recycle txn key | 1970-1997 | **Publish阶段** |
| S18 | 处理routine load / streaming job进度 | 1999-2012 | **Commit阶段**（保留，通过commit_attachment传递） |
| S19 | FDB事务commit | 2019-2033 | **Commit阶段** |
| S20 | 构建response：table_stats, partition_ids, versions | 2035-2070 | **Commit阶段**（返回commit version） |

### 1.3 `commit_txn_eventually()`（lazy commit）参考

位于 `meta_service_txn.cpp:2184`。与 `commit_txn_immediately` 的关键区别：

- TxnInfoPB的status设为 `TXN_STATUS_COMMITTED` 而非 `TXN_STATUS_VISIBLE`
- partition version中写入 `pending_txn_ids`（包含当前txn_id），但不直接+1
- **不写formal rowset meta**（不执行S10的rowset转正）
- **不更新tablet stats**（不执行S14）
- **不删除tmp rowset**（不执行S15）
- 仍然处理MOW锁（S9），但可以选择deferred模式
- FDB事务commit后，提交一个lazy commit task异步完成剩余工作

这与新的两阶段提交方案有相似之处，但新方案更加彻底：commit阶段连MOW锁检查都不做，也不需要scan tmp rowset。

### 1.4 `precommit_txn()` 参考

位于 `meta_service_txn.cpp:378`。precommit的逻辑非常轻量：

1. 读取 `txn_info_key` 获取 TxnInfoPB
2. 校验状态（不能是ABORTED/VISIBLE/PRECOMMITTED）
3. 设置 status = TXN_STATUS_PRECOMMITTED，设置 precommit_time
4. 更新 commit_attachment
5. 更新 txn_running_key（刷新timeout_time）
6. FDB事务commit

新方案的commit阶段与precommit类似的简洁程度，但额外需要读取和更新partition commit version。

### 1.5 新方案中步骤归属总结

**Commit阶段保留的操作**（在FE表锁内，需要极快完成）：
- 读取TxnInfoPB，校验事务状态
- 读取涉及partition的当前commit version
- 为每个partition的commit version +1
- 更新TxnInfoPB（status=COMMITTED, committed_version, involved partitions/tablets, commit_attachment）
- 删除txn_running_key
- 处理routine load / streaming job进度（通过commit_attachment）
- 返回partition commit version给FE

**移到Publish阶段的操作**：
- scan tmp rowset（FE在commit前已经从BE获取了tablet commit info）
- 获取tablet index（FE已有此信息）
- 为rowset分配version
- process MOW lock
- 写formal rowset meta
- 更新partition visible version
- 更新table version
- 更新tablet stats
- 删除tmp rowset meta
- 写recycle txn key

---

## 2. 新的Commit阶段API设计

### 2.1 API方案：复用现有 `commit_txn` RPC

**不新增RPC**，而是通过 `CommitTxnRequest` 中新增字段来区分两阶段提交模式。原因：

1. 减少proto变更和RPC注册工作量
2. 复用现有的认证、限流、错误处理框架
3. FE侧调用代码可以在同一个函数内通过条件分支处理
4. 便于灰度切换：同一个RPC根据表属性决定走哪个分支

### 2.2 Proto变更

```protobuf
message CommitTxnRequest {
    // ... 现有字段 ...
    optional bool is_2pc = 4;
    // ... 现有字段 ...

    // ==== 新增字段：两阶段提交 ====
    // 是否启用两阶段提交模式
    optional bool is_cloud_mow_2pc = 20;
    // 涉及的partition信息：partition_id -> {db_id, table_id}
    // FE已有此信息，直接传递给MS，避免MS再去读tablet index
    repeated PartitionInfo involved_partitions = 21;
}

// 新增message
message PartitionInfo {
    optional int64 partition_id = 1;
    optional int64 db_id = 2;
    optional int64 table_id = 3;
}

message CommitTxnResponse {
    // ... 现有字段 ...
    // 现有字段已包含 partition_ids 和 versions，可复用
    // repeated int64 partition_ids = 4;
    // repeated int64 versions = 5;
}
```

对 `TxnInfoPB` 的扩展（记录两阶段提交所需的持久化信息）：

```protobuf
message TxnInfoPB {
    // ... 现有字段 ...

    // ==== 新增字段：两阶段提交 ====
    // 标识此事务使用两阶段提交模式
    optional bool is_cloud_mow_2pc = 30;
    // 每个partition的committed version（commit阶段分配的版本号）
    // Map<partition_id, committed_version>
    map<int64, int64> partition_committed_versions = 31;
    // 涉及的tablet列表（来自FE的tablet commit info）
    repeated int64 involved_tablet_ids = 32;
}
```

### 2.3 Partition Commit Version的KV设计

**方案：新增独立的 partition_commit_version KV**，与现有 `partition_version_key`（语义上等同于 visible version）分离。

原因：
- 现有 `partition_version_key` 存储的是 visible version，查询时依赖此值来判断数据可见性
- 如果复用同一个key同时承载commit version和visible version，会导致：查询路径需要区分两种语义、并发控制逻辑复杂化
- 分离KV后，查询路径不受影响，commit阶段操作独立的key space

```
// 新增 KV
Key:   "partition" ${instance_id} "commit_version" ${db_id} ${table_id} ${partition_id}
Value: VersionPB { version: <commit_version> }
```

对应的key定义（在 `keys.h` 中）：

```cpp
// 0:instance_id  1:db_id  2:tbl_id  3:partition_id
using PartitionCommitVersionKeyInfo = BasicKeyInfo<__LINE__, std::tuple<std::string, int64_t, int64_t, int64_t>>;
```

### 2.4 Commit阶段的具体步骤

新增函数 `commit_txn_2pc()`，在 `commit_txn()` 入口处根据 `request->is_cloud_mow_2pc()` 标志分流。

```cpp
void MetaServiceImpl::commit_txn_2pc(
        const CommitTxnRequest* request, CommitTxnResponse* response,
        MetaServiceCode& code, std::string& msg,
        const std::string& instance_id, int64_t db_id, KVStats& stats);
```

**完整步骤：**

```
commit_txn_2pc()
│
├── Step 1. 创建FDB事务
│
├── Step 2. 读取TxnInfoPB (txn_info_key)
│     └── 校验状态：必须是 TXN_STATUS_PREPARED 或 TXN_STATUS_PRECOMMITTED
│         - ABORTED → 返回 TXN_ALREADY_ABORTED
│         - VISIBLE → 返回 TXN_ALREADY_VISIBLE
│         - COMMITTED → 返回 OK（幂等，直接返回已有的committed versions）
│
├── Step 3. 校验事务超时
│     └── (prepare_time + timeout_ms) < current_time → 返回错误
│
├── Step 4. 从request中提取involved_partitions信息
│     └── 构建 partition_indexes: Map<partition_id, {db_id, table_id}>
│
├── Step 5. 批量读取每个partition的当前commit version
│     └── batch_get(partition_commit_version_key)
│         - key存在：version = VersionPB.version
│         - key不存在：version = 对应partition的visible_version（首次使用，需要从partition_version_key读取fallback值）
│
├── Step 6. 为每个partition的commit version +1
│     └── new_commit_version = current_commit_version + 1
│
├── Step 7. 写入新的partition commit version
│     └── 对每个partition: put(partition_commit_version_key, VersionPB{version: new_commit_version})
│
├── Step 8. 更新TxnInfoPB
│     ├── status = TXN_STATUS_COMMITTED
│     ├── commit_time = now
│     ├── is_cloud_mow_2pc = true
│     ├── partition_committed_versions = {partition_id: new_commit_version, ...}
│     ├── involved_tablet_ids = request中的tablet信息
│     └── commit_attachment（如果有）
│     └── put(txn_info_key, updated TxnInfoPB)
│
├── Step 9. 删除txn_running_key
│     └── remove(txn_running_key)
│
├── Step 10. 处理routine load / streaming job进度
│     └── 与现有逻辑相同，通过commit_attachment处理
│
├── Step 11. FDB事务commit
│     └── txn->commit()
│         - TXN_CONFLICT → 重试（回到Step 1）
│         - TXN_OK → 继续
│
└── Step 12. 构建response
      ├── partition_ids: [p1, p2, ...]
      ├── versions: [v1, v2, ...]  // commit versions
      ├── table_ids: [t1, t2, ...]
      └── txn_info: updated TxnInfoPB
```

### 2.5 此阶段明确不做的事情

| 操作 | 原因 |
|------|------|
| **scan tmp rowset** | FE在commit前已经从BE收到了tablet commit info，不需要MS去扫描。tmp rowset在publish阶段per-tablet转正时处理 |
| **读取tablet index** | FE在request中直接提供partition信息（involved_partitions），不需要从tablet_id反查 |
| **为rowset分配version** | rowset的version在publish阶段per-tablet转正时分配（基于commit version） |
| **写formal rowset meta** | 在publish阶段per-tablet转正RPC中完成 |
| **处理MOW delete bitmap lock** | 两阶段提交不再使用MS分布式表锁，delete bitmap计算改为tablet级别在publish阶段进行 |
| **删除pending delete bitmap key** | 与上同理 |
| **更新partition visible version** | 在publish阶段轻量级MS commit中完成 |
| **更新table version** | 在publish阶段轻量级MS commit中完成 |
| **更新tablet stats** | 在publish阶段per-tablet转正时更新 |
| **删除tmp rowset meta** | 在publish阶段per-tablet转正时删除 |
| **写recycle txn key** | 在publish阶段轻量级MS commit中完成 |

---

## 3. Partition Commit Version管理

### 3.1 概念区分

引入两阶段提交后，每个partition存在两个version：

| 概念 | KV Key | 语义 | 更新时机 |
|------|--------|------|---------|
| **commit version** | `partition_commit_version_key` | 最后一次commit分配的版本号。用于commit阶段版本分配 | Commit阶段（快速，持FE表锁） |
| **visible version** | `partition_version_key`（现有） | 数据对查询可见的版本号。用于查询可见性判断 | Publish阶段轻量级commit（异步） |

正常状态下：`commit_version >= visible_version`。差值代表已commit但尚未publish的事务数。

### 3.2 读写逻辑

**写入（commit阶段）：**

```cpp
// 读取当前commit version
std::string commit_ver_key = partition_commit_version_key(
    {instance_id, db_id, table_id, partition_id});
std::string commit_ver_val;
err = txn->get(commit_ver_key, &commit_ver_val);

int64_t current_commit_version;
if (err == TxnErrorCode::TXN_OK) {
    VersionPB version_pb;
    version_pb.ParseFromString(commit_ver_val);
    current_commit_version = version_pb.version();
} else if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
    // 首次使用两阶段提交，fallback到visible version
    std::string vis_ver_key = partition_version_key(
        {instance_id, db_id, table_id, partition_id});
    std::string vis_ver_val;
    err = txn->get(vis_ver_key, &vis_ver_val);
    if (err == TxnErrorCode::TXN_OK) {
        VersionPB version_pb;
        version_pb.ParseFromString(vis_ver_val);
        current_commit_version = version_pb.version();
    } else {
        current_commit_version = 1;  // 初始version
    }
}

// 分配新version
int64_t new_commit_version = current_commit_version + 1;

// 写入
VersionPB new_version_pb;
new_version_pb.set_version(new_commit_version);
new_version_pb.set_update_time_ms(current_time_ms);
txn->put(commit_ver_key, new_version_pb.SerializeAsString());
```

**读取（publish阶段 - 在其他module中，此处仅说明关系）：**
- per-tablet转正时，使用TxnInfoPB中记录的 `partition_committed_versions` 来确定rowset的version
- 轻量级publish完成后，更新 `partition_version_key`（visible version）推进到commit version

### 3.3 并发控制

**多个导入同时commit同一partition的场景：**

FDB事务提供了乐观并发控制。具体来说：

1. **FE侧**：同一张表的commit操作被FE表级commit锁（`Table.commitLock`）串行化。因此在同一时刻，同一张表只有一个commit请求到达MS。不同表的commit请求完全独立，无并发冲突。

2. **MS侧FDB事务层面**：即使FE侧（理论上）有并发commit请求到达MS（例如多FE master切换的极端情况），FDB的read-write conflict range机制会保证：
   - 两个FDB事务如果读取了同一个 `partition_commit_version_key` 并尝试写入，其中一个会收到 `TXN_CONFLICT` 错误
   - 收到冲突的事务需要重试（回到Step 1重新创建FDB事务、重新读取最新version）

3. **不同partition之间**：不同partition的commit version互相独立，不存在冲突。

**Commit和Publish的并发：**

- Commit阶段读写 `partition_commit_version_key`
- Publish阶段读写 `partition_version_key`（visible version）
- 两者操作不同的key，不会在FDB层面冲突
- 语义约束由FE保证：visible version只能推进到已commit的version

---

## 4. 事务状态转换

### 4.1 状态机

```
                    begin_txn
                       │
                       v
              TXN_STATUS_PREPARED
                    /        \
                   /          \
           (两阶段commit)    (abort)
                 /              \
                v                v
       TXN_STATUS_COMMITTED   TXN_STATUS_ABORTED
                |
                | (publish完成)
                v
        TXN_STATUS_VISIBLE
```

### 4.2 PREPARED -> COMMITTED 转换

**前置条件：**
- 当前状态必须是 `TXN_STATUS_PREPARED` 或 `TXN_STATUS_PRECOMMITTED`（支持2PC场景）
- 事务未超时：`prepare_time + timeout_ms > current_time`
- request中包含 `is_cloud_mow_2pc = true`

**原子性保证：**

以下操作在一个FDB事务中原子完成：
1. 读取TxnInfoPB，校验状态
2. 读取所有涉及partition的commit version
3. 为每个partition的commit version +1
4. 更新TxnInfoPB为COMMITTED状态
5. 删除txn_running_key

如果FDB事务commit失败（`TXN_CONFLICT`），所有操作都不生效，可以安全重试。

**幂等性：**

如果事务已经处于COMMITTED状态（例如FE重试），直接返回OK，并从TxnInfoPB中读取已有的 `partition_committed_versions` 返回给FE。

### 4.3 与现有 COMMITTED 状态（lazy commit）的区分

当前代码中 `TXN_STATUS_COMMITTED` 仅用于 `commit_txn_eventually()`（lazy commit）路径。在该路径下：
- COMMITTED表示"partition version已带上pending_txn_ids，但rowset尚未转正、version尚未推进"
- lazy commit task异步完成剩余工作后，状态变为VISIBLE

新方案中，对于启用两阶段提交的表：
- COMMITTED表示"partition commit version已推进，TxnInfoPB已记录committed versions和involved tablets，但rowset尚未转正、visible version尚未推进"
- publish阶段完成后，状态变为VISIBLE

**区分方式：** 通过 `TxnInfoPB.is_cloud_mow_2pc` 字段区分。

- `is_cloud_mow_2pc = true` + `status = COMMITTED`：两阶段提交的commit完成状态
- `is_cloud_mow_2pc` 未设置 + `status = COMMITTED`：lazy commit的中间状态

这两种COMMITTED状态的后续处理完全不同：
- lazy commit的COMMITTED由 `TxnLazyCommitTask` 推进到VISIBLE
- 两阶段提交的COMMITTED由FE publish daemon推进到VISIBLE

**重要约束**：启用两阶段提交的表不再使用lazy commit机制。FE在构建CommitTxnRequest时，对于两阶段提交的表，不设置 `enable_txn_lazy_commit`，且设置 `is_cloud_mow_2pc = true`。

---

## 5. 错误处理

### 5.1 Commit阶段失败

由于commit阶段的所有操作在一个FDB事务中原子执行，失败时所有写入都不生效。事务可以安全地：

| 错误类型 | 处理方式 |
|---------|---------|
| `TXN_CONFLICT`（FDB事务冲突） | 重试整个commit阶段（重新创建FDB事务，重新读取最新的commit version）。重试次数由FDB client配置控制 |
| `TXN_KEY_NOT_FOUND`（txn_info_key不存在） | 返回 `TXN_ID_NOT_FOUND` |
| `TXN_ALREADY_ABORTED` | 返回错误，FE报告导入失败 |
| 事务超时 | 返回错误，FE可选择abort事务 |
| 网络错误 / MS不可用 | FE重试commit请求。由于幂等性，重试安全 |
| `TXN_BYTES_TOO_LARGE` | 理论上不会发生，因为commit阶段写入量极小（仅TxnInfoPB + 若干partition version + 删除running key）。如果发生，返回错误 |

### 5.2 FDB事务冲突的重试策略

```cpp
void MetaServiceImpl::commit_txn_2pc(...) {
    // 重试逻辑：do-while循环
    do {
        std::unique_ptr<Transaction> txn;
        err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) { /* 返回错误 */ return; }

        // ... 读取、校验、写入 ...

        err = txn->commit();
        if (err == TxnErrorCode::TXN_CONFLICT) {
            // FDB事务冲突，重试
            LOG(INFO) << "commit_txn_2pc conflict, retrying, txn_id=" << txn_id;
            continue;
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            // 返回错误
            return;
        }
        break;  // 成功
    } while (true);
}
```

### 5.3 Commit成功后、Publish前的异常

Commit成功后事务处于COMMITTED状态。如果此时FE crash：
- FE重启后，从MS加载COMMITTED状态的事务（通过 `GetTxn` 或扫描），恢复到内存的committed txns set中
- Publish daemon继续推进这些事务
- 详见 Module 6（FE Recovery）

---

## 6. 关键文件和修改点

### 6.1 Proto文件

| 文件 | 修改内容 |
|------|---------|
| `gensrc/proto/cloud.proto` | 1. `CommitTxnRequest` 新增 `is_cloud_mow_2pc`、`involved_partitions` 字段<br>2. 新增 `PartitionInfo` message<br>3. `TxnInfoPB` 新增 `is_cloud_mow_2pc`、`partition_committed_versions`、`involved_tablet_ids` 字段 |

### 6.2 MS侧C++文件

| 文件 | 修改内容 |
|------|---------|
| `cloud/src/meta-store/keys.h` | 新增 `PartitionCommitVersionKeyInfo` 类型定义 |
| `cloud/src/meta-store/keys.cpp` | 新增 `partition_commit_version_key()` 编解码函数 |
| `cloud/src/meta-service/meta_service.h` | 在 `MetaServiceImpl` 类中声明 `commit_txn_2pc()` 私有方法 |
| `cloud/src/meta-service/meta_service_txn.cpp` | 1. `commit_txn()` 入口新增分支：当 `is_cloud_mow_2pc = true` 时调用 `commit_txn_2pc()`<br>2. 实现 `commit_txn_2pc()` 函数 |

### 6.3 具体代码修改点

**`commit_txn()` 入口修改**（`meta_service_txn.cpp:3257`）：

```cpp
void MetaServiceImpl::commit_txn(...) {
    // ... 现有参数校验、获取instance_id、get_txn_db_id ...

    if (request->has_is_txn_load() && request->is_txn_load()) {
        commit_txn_with_sub_txn(...);
        return;
    }

    // ==== 新增分支 ====
    if (request->has_is_cloud_mow_2pc() && request->is_cloud_mow_2pc()) {
        commit_txn_2pc(request, response, code, msg, instance_id, db_id, stats);
        return;
    }

    // ... 现有逻辑：scan_tmp_rowset, commit_txn_immediately/eventually ...
}
```

**`commit_txn_2pc()` 函数签名**：

```cpp
void MetaServiceImpl::commit_txn_2pc(
        const CommitTxnRequest* request, CommitTxnResponse* response,
        MetaServiceCode& code, std::string& msg,
        const std::string& instance_id, int64_t db_id, KVStats& stats) {
    std::stringstream ss;
    int64_t txn_id = request->txn_id();

    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) { /* 错误处理 */ return; }

        DORIS_CLOUD_DEFER {
            if (txn == nullptr) return;
            stats.get_bytes += txn->get_bytes();
            stats.put_bytes += txn->put_bytes();
            stats.del_bytes += txn->delete_bytes();
            stats.get_counter += txn->num_get_keys();
            stats.put_counter += txn->num_put_keys();
            stats.del_counter += txn->num_del_keys();
        };

        // Step 2: 读取TxnInfoPB
        const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
        std::string info_val;
        err = txn->get(info_key, &info_val);
        // ... 错误处理 ...

        TxnInfoPB txn_info;
        txn_info.ParseFromString(info_val);

        // Step 2: 状态校验
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_ABORTED) {
            code = MetaServiceCode::TXN_ALREADY_ABORTED;
            return;
        }
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_VISIBLE) {
            code = MetaServiceCode::OK; // 幂等
            response->mutable_txn_info()->CopyFrom(txn_info);
            return;
        }
        if (txn_info.status() == TxnStatusPB::TXN_STATUS_COMMITTED) {
            // 已经commit过（幂等），从TxnInfoPB中恢复committed versions
            code = MetaServiceCode::OK;
            for (auto& [pid, ver] : txn_info.partition_committed_versions()) {
                response->add_partition_ids(pid);
                response->add_versions(ver);
            }
            response->mutable_txn_info()->CopyFrom(txn_info);
            return;
        }
        if (txn_info.status() != TxnStatusPB::TXN_STATUS_PREPARED &&
            txn_info.status() != TxnStatusPB::TXN_STATUS_PRECOMMITTED) {
            code = MetaServiceCode::TXN_INVALID_STATUS;
            return;
        }

        // Step 3: 超时检查
        auto now = duration_cast<milliseconds>(
            system_clock::now().time_since_epoch()).count();
        if ((txn_info.prepare_time() + txn_info.timeout_ms()) < now) {
            code = MetaServiceCode::UNDEFINED_ERR;
            msg = fmt::format("txn is expired, txn_id={}", txn_id);
            return;
        }

        // Step 4: 从request中提取partition信息
        // ... 构建partition_indexes ...

        // Step 5-6: 读取commit version并+1
        // ... batch_get partition_commit_version_keys ...
        // ... fallback到partition_version_key（visible version）...

        // Step 7: 写入新的commit version
        // ... put partition_commit_version_keys ...

        // Step 8: 更新TxnInfoPB
        txn_info.set_status(TxnStatusPB::TXN_STATUS_COMMITTED);
        txn_info.set_commit_time(now);
        txn_info.set_is_cloud_mow_2pc(true);
        // ... 设置partition_committed_versions, involved_tablet_ids ...
        txn->put(info_key, txn_info.SerializeAsString());

        // Step 9: 删除running key
        txn->remove(txn_running_key({instance_id, db_id, txn_id}));

        // Step 10: routine load / streaming job进度
        // ... 与现有逻辑相同 ...

        // Step 11: FDB commit
        err = txn->commit();
        if (err == TxnErrorCode::TXN_CONFLICT) {
            continue;  // 重试
        }
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::COMMIT>(err);
            return;
        }

        // Step 12: 构建response
        // ... 填充partition_ids, versions, table_ids, txn_info ...
        break;
    } while (true);
}
```

### 6.4 `partition_commit_version_key` 编码方案

在 `keys.h` 中，新key需要编码为与现有 `partition_version_key` 不同的前缀，以避免key空间冲突。

```cpp
// keys.h 新增
//                                                      0:instance_id  1:db_id  2:tbl_id  3:partition_id
using PartitionCommitVersionKeyInfo = BasicKeyInfo<__LINE__, std::tuple<std::string, int64_t, int64_t, int64_t>>;

// keys.h 新增函数声明
void partition_commit_version_key(const PartitionCommitVersionKeyInfo& in, std::string* out);
static inline std::string partition_commit_version_key(const PartitionCommitVersionKeyInfo& in) {
    std::string s; partition_commit_version_key(in, &s); return s;
}
```

在 `keys.cpp` 中，编码格式参考 `partition_version_key`，但使用不同的key tag（例如 `"commit_version"` 而非 `"version"`）。

---

## 7. 单元测试要点

### 7.1 测试文件

新增测试文件：`cloud/test/commit_txn_2pc_test.cpp`

或在现有 `txn_lazy_commit_test.cpp` 中新增 test fixture / test case。

### 7.2 测试用例

**基本功能测试：**

| 测试用例 | 验证内容 |
|---------|---------|
| `CommitTxn2PC_Basic` | 创建事务 → begin_txn → prepare_rowset → commit_txn(is_cloud_mow_2pc=true) → 验证返回的partition versions正确、TxnInfoPB状态为COMMITTED |
| `CommitTxn2PC_MultiPartition` | 事务涉及多个partition，验证每个partition的commit version独立+1 |
| `CommitTxn2PC_Idempotent` | 同一事务两次调用commit_txn_2pc，第二次应返回OK并返回相同的committed versions |

**状态校验测试：**

| 测试用例 | 验证内容 |
|---------|---------|
| `CommitTxn2PC_AlreadyAborted` | 事务已abort，调用commit应返回 `TXN_ALREADY_ABORTED` |
| `CommitTxn2PC_AlreadyVisible` | 事务已visible，调用commit应返回OK |
| `CommitTxn2PC_Expired` | 事务已超时，调用commit应返回错误 |
| `CommitTxn2PC_InvalidStatus` | 事务状态非PREPARED/PRECOMMITTED，应返回错误 |

**并发测试：**

| 测试用例 | 验证内容 |
|---------|---------|
| `CommitTxn2PC_ConcurrentSamePartition` | 两个事务（不同txn_id）几乎同时commit同一partition。通过SyncPoint控制时序，验证：一个成功（version=N+1），另一个FDB冲突后重试成功（version=N+2） |
| `CommitTxn2PC_ConcurrentDiffPartition` | 两个事务commit不同partition，应该都成功且互不干扰 |

**Version正确性测试：**

| 测试用例 | 验证内容 |
|---------|---------|
| `CommitTxn2PC_FirstCommit` | partition首次使用两阶段提交（无commit_version key），验证fallback到visible version |
| `CommitTxn2PC_SequentialCommits` | 同一partition连续3次commit（不同事务），验证commit version依次递增：v+1, v+2, v+3 |
| `CommitTxn2PC_CommitVersionIndependent` | 验证commit version和visible version互相独立：commit使commit_version推进，但visible_version不变 |

**KV层面验证：**

| 测试用例 | 验证内容 |
|---------|---------|
| `CommitTxn2PC_KVContents` | commit后直接读取FDB，验证：partition_commit_version_key写入正确、txn_info_key内容正确（status=COMMITTED, partition_committed_versions, involved_tablet_ids）、txn_running_key已删除 |
| `CommitTxn2PC_NoTmpRowsetScan` | 验证commit_txn_2pc不读取tmp rowset相关的key（通过SyncPoint或mock验证） |
| `CommitTxn2PC_NoMowLockProcessing` | 验证commit_txn_2pc不读取/写入delete_bitmap_update_lock相关的key |

### 7.3 测试辅助

测试使用 `MemTxnKv`（内存KV存储）或FDB作为后端，参考 `txn_lazy_commit_test.cpp` 的测试框架：

```cpp
// 测试初始化
static std::shared_ptr<TxnKv> txn_kv;
// ... 初始化MetaServiceImpl ...

// 辅助函数
void create_and_begin_txn(int64_t txn_id, int64_t db_id, ...);
void prepare_rowset(int64_t txn_id, int64_t tablet_id, ...);
void verify_txn_info(int64_t txn_id, TxnStatusPB expected_status, ...);
void verify_partition_commit_version(int64_t partition_id, int64_t expected_version);
```
