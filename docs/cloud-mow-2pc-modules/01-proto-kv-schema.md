# Module 0: Proto & KV Schema 变更

## 1. 概述

本模块定义 Cloud MOW 异步发布(Async Publish)方案中所有基础的 Proto 消息和 KV Schema 变更。作为基础模块，所有其他模块（MS Commit、MS Convert Rowset、MS Lightweight Publish、FE/BE 改造）均依赖本模块。

### 1.1 核心设计目标

- 引入 partition commit version，与现有 partition visible version 分离
- 扩展 TxnInfoPB 以持久化异步发布所需的额外信息
- 定义 ConvertTmpRowset 新 RPC 的 Request/Response
- 适配 CommitTxnRequest/Response 以支持异步发布 commit 和轻量级 publish
- 定义表属性以控制是否启用异步发布
- 分析 TxnStatusPB 的语义变更

---

## 2. 新增 partition_commit_version KV

### 2.1 背景

当前系统中 partition 的版本管理只有一个 `partition_version_key`，存储 `VersionPB`，代表该 partition 的**可见版本**（visible version）。异步发布需要引入一个新的版本概念——**commit version**：

- **commit version**：在 Commit 阶段递增，表示事务已被逻辑提交（数据已确认写入，但 delete bitmap 尚未计算完成，rowset 尚未转正）
- **visible version**：在 Publish 阶段（轻量级 commit）递增，表示数据已对用户可见

关系：`visible_version <= commit_version`。所有版本号在 `[visible_version + 1, commit_version]` 范围内的事务处于"已提交但未发布"状态。

### 2.2 KV Key 格式

参考现有 `partition_version_key` 的设计：

```
// 现有 (visible version):
// 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id} -> VersionPB

// 新增 (commit version):
// 0x01 "version" ${instance_id} "partition_commit" ${db_id} ${tbl_id} ${partition_id} -> VersionPB
```

### 2.3 KeyInfo 定义 (keys.h)

```cpp
// 新增 infix 常量 (keys.cpp)
static const char* PARTITION_COMMIT_VERSION_KEY_INFIX = "partition_commit";

// 新增 KeyInfo 类型 (keys.h)
// 与现有 PartitionVersionKeyInfo 相同的元组结构:
//                                                      0:instance_id  1:db_id  2:tbl_id  3:partition_id
using PartitionCommitVersionKeyInfo = BasicKeyInfo<__LINE__, std::tuple<std::string, int64_t, int64_t, int64_t>>;
```

### 2.4 Key 编解码函数 (keys.h / keys.cpp)

```cpp
// keys.h 声明
void partition_commit_version_key(const PartitionCommitVersionKeyInfo& in, std::string* out);
static inline std::string partition_commit_version_key(const PartitionCommitVersionKeyInfo& in) {
    std::string s;
    partition_commit_version_key(in, &s);
    return s;
}
bool decode_partition_commit_version_key(std::string_view* in, int64_t* db_id,
                                         int64_t* tbl_id, int64_t* partition_id);

// keys.cpp 实现
void partition_commit_version_key(const PartitionCommitVersionKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                                 // 0x01 "version" ${instance_id}
    encode_bytes(PARTITION_COMMIT_VERSION_KEY_INFIX, out);  // "partition_commit"
    encode_int64(std::get<1>(in), out);                     // db_id
    encode_int64(std::get<2>(in), out);                     // tbl_id
    encode_int64(std::get<3>(in), out);                     // partition_id
}
```

### 2.5 Value 定义

复用现有 `VersionPB`：

```protobuf
// 已有定义，无需修改
message VersionPB {
    optional int64 version = 1;
    optional int64 update_time_ms = 2;
    repeated int64 pending_txn_ids = 3;
}
```

复用理由：
- `version` 字段存储 commit version 值
- `update_time_ms` 可用于记录最后一次 commit 的时间
- `pending_txn_ids` 在 commit version 场景下暂不使用，但保留兼容性

### 2.6 与现有 partition_version_key 的关系

| 属性 | partition_version_key (visible) | partition_commit_version_key (commit) |
|------|------|------|
| 递增时机 | Publish 阶段（轻量级 commit） | Commit 阶段 |
| 语义 | 数据对用户可见的最高版本 | 数据已逻辑提交的最高版本 |
| 用途 | 查询读取、compaction 参考 | Commit 阶段分配版本号 |
| 更新者 | MS 轻量级 publish 逻辑 | MS commit_txn 逻辑 |

### 2.7 兼容性考虑

- **新建表**：同时创建 `partition_version_key` 和 `partition_commit_version_key`，初始值均为 1
- **未启用异步发布的表**：不创建 `partition_commit_version_key`，行为与现有完全一致
- **已有表**：不支持升级为异步发布（仅限新表），避免版本迁移问题

### 2.8 单元测试要点

- 测试 `partition_commit_version_key` 的编码/解码正确性
- 测试 commit version 和 visible version 的独立递增
- 测试 key 的字典序排列：同一 partition 的 commit version key 和 visible version key 应在不同的 key range 中（因为 infix 不同，"partition" vs "partition_commit"）
- 测试初始化时两个 version 的一致性

---

## 3. TxnInfoPB 扩展

### 3.1 现有字段分析

当前 `TxnInfoPB` 定义（字段编号 1-22）：

```protobuf
message TxnInfoPB {
    optional int64 db_id = 1;
    repeated int64 table_ids = 2;
    optional int64 txn_id = 3;
    optional string label = 4;
    optional UniqueIdPB request_id = 5;
    optional TxnCoordinatorPB coordinator = 6;
    optional LoadJobSourceTypePB load_job_source_type = 7;
    optional int64 timeout_ms = 8;
    optional int64 precommit_timeout_ms = 9;
    optional int64 prepare_time = 10;
    optional int64 precommit_time = 11;
    optional int64 commit_time = 12;
    optional int64 finish_time = 13;
    optional string reason = 14;
    optional TxnStatusPB status = 15;
    optional TxnCommitAttachmentPB commit_attachment = 16;
    optional int64 listener_id = 17;
    repeated int64 sub_txn_ids = 18;
    optional bool versioned_write = 19;
    optional bool versioned_read = 20;
    optional bool defer_deleting_pending_delete_bitmaps = 21;
    optional string load_cluster_id = 22;
}
```

可复用字段：
- `commit_time` (12)：可复用为异步发布 commit 的 commit 时间
- `finish_time` (13)：可复用为轻量级 publish 完成时间
- `table_ids` (2)：已有，可继续使用
- `status` (15)：COMMITTED 状态的语义需要适配（见第 7 节）

### 3.2 新增字段

从字段编号 **30** 开始（预留 23-29 给未来的通用扩展），使用 30-39 区间作为异步发布专用字段：

```protobuf
message TxnInfoPB {
    // ... 现有字段 1-22 保持不变 ...

    // ========== 异步发布专用字段（30-39）==========

    // 标记本事务是否使用异步发布模式
    optional bool mow_async_publish = 30;

    // Commit 阶段分配的版本信息：每个 partition 的 commit version
    // partition_ids 和 committed_versions 一一对应
    repeated int64 committed_partition_ids = 31;
    repeated int64 committed_versions = 32;

    // 参与本事务的 tablet 信息（tablet -> BE 的映射）
    // 用于 publish 阶段 FE 向 BE 下发 delete bitmap 计算任务
    repeated TxnTabletInfoPB involved_tablets = 33;

    // 导入参数：TOlapTableSchemaParam 的 Thrift 序列化 bytes
    // coordinator（FE或BE）在 commit 时将 TOlapTableSchemaParam 序列化后写入
    // publish 阶段 BE 计算 delete bitmap 时从此字段反序列化恢复导入上下文
    // 整个导入只需存储一份，不需要 per-tablet 存储
    optional bytes load_schema_param = 34;

    // Publish 阶段每个 tablet 的完成状态追踪
    // 记录哪些 tablet 已完成转正（用于断点续做）
    repeated int64 published_tablet_ids = 35;
}
```

### 3.3 新增辅助 Message 定义

#### 3.3.1 TxnTabletInfoPB

```protobuf
// 事务中参与的 tablet 信息
// 字段设计参考现有 TabletIndexPB，增加 BE 节点信息
message TxnTabletInfoPB {
    optional int64 tablet_id = 1;
    optional int64 table_id = 2;
    optional int64 index_id = 3;
    optional int64 partition_id = 4;
    // 执行导入的 BE 节点标识（cloud_unique_id），
    // publish 阶段需要向该 BE 下发 delete bitmap 计算任务
    optional string be_cloud_unique_id = 5;
    // BE 的 host:port，用于 FE 向 BE 发送 RPC
    optional string be_endpoint = 6;
}
```

#### 3.3.2 导入参数存储方案（load_schema_param）

导入参数直接复用现有的 Thrift 类型 `TOlapTableSchemaParam`（定义在 `gensrc/thrift/Descriptors.thrift`）：

```thrift
struct TOlapTableSchemaParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version
    4: required list<TSlotDescriptor> slot_descs
    5: required TTupleDescriptor tuple_desc
    6: required list<TOlapTableIndexSchema> indexes
    7: optional bool is_dynamic_schema // deprecated
    8: optional bool is_partial_update // deprecated, use unique_key_update_mode
    9: optional list<string> partial_update_input_columns
    10: optional bool is_strict_mode = false
    11: optional string auto_increment_column
    12: optional i32 auto_increment_column_unique_id = -1
    13: optional Types.TInvertedIndexFileStorageFormat inverted_index_file_storage_format
    14: optional Types.TUniqueKeyUpdateMode unique_key_update_mode
    15: optional i32 sequence_map_col_unique_id = -1
    16: optional TPartialUpdateNewRowPolicy partial_update_new_key_policy
}
```

**复用理由**：
- 已包含完整的表 schema 信息和部分列更新参数
- coordinator（FE或BE）在构建导入计划时已经生成了 `TOlapTableSchemaParam`，直接序列化传递
- 避免自定义 proto message 导致的信息遗漏和重复定义

**跨类型序列化**：TxnInfoPB 是 Protobuf，TOlapTableSchemaParam 是 Thrift，不能直接嵌套。在 TxnInfoPB 中使用 `bytes load_schema_param` 字段存储 Thrift 序列化的二进制数据。

**序列化（Commit 阶段）**：

```java
// Java (FE) 示例
TOlapTableSchemaParam schemaParam = buildSchemaParam(table, ...);
TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
byte[] bytes = serializer.serialize(schemaParam);
commitTxnRequest.setLoadSchemaParam(ByteString.copyFrom(bytes));
```

```cpp
// C++ (BE) 示例
TOlapTableSchemaParam schema_param = ...;
ThriftSerializer ser(false, 4096);
uint8_t* buf = nullptr; uint32_t len = 0;
ser.serialize(&schema_param, &buf, &len);
request.set_load_schema_param(buf, len);
```

**反序列化（Publish 阶段，BE cache miss 时）**：

```cpp
// 从 TxnInfoPB 获取 bytes 并反序列化
const std::string& param_bytes = txn_info.load_schema_param();
TOlapTableSchemaParam schema_param;
ThriftDeserializer deser;
Status st = deser.deserialize(&schema_param,
    (const uint8_t*)param_bytes.data(), param_bytes.size());
```

### 3.4 兼容性考虑

- 所有新增字段均使用 `optional`/`repeated`，老版本代码解析时自动忽略
- 新增字段编号从 30 开始，与现有字段（1-22）无冲突
- 未启用异步发布的事务不设置这些字段，行为完全不变
- `TxnTabletInfoPB` 是新增独立 message，不影响任何现有 message
- `load_schema_param` 使用 `bytes` 类型，对 Protobuf 层面完全透明，不引入 Thrift 依赖

### 3.5 单元测试要点

- 测试 TxnInfoPB 序列化/反序列化的向前兼容性（老数据反序列化后新字段为空）
- 测试 `committed_partition_ids` 和 `committed_versions` 的一一对应关系
- 测试 `involved_tablets` 的增删操作
- 测试 `published_tablet_ids` 的幂等追加（用于断点续做验证）
- 测试 `load_schema_param` 中 TOlapTableSchemaParam 的 Thrift 序列化/反序列化正确性
- 测试 `load_schema_param` 为空时的兼容处理（非部分列更新场景）
- 测试较大的 TOlapTableSchemaParam（大量列定义）的序列化 bytes 能正确存储和读取

---

## 4. 新 RPC 定义：ConvertTmpRowset

### 4.1 背景

当前的 tmp rowset 转正发生在 `commit_txn` 的 MS 端逻辑中（一次性处理所有 tablet）。异步发布方案中，转正改为 per-tablet 粒度：BE/FE 在每个 tablet 完成 delete bitmap 计算后，单独调用 MS 将该 tablet 的 tmp rowset 转正。

转正操作包括：
1. 读取 tmp rowset meta（key: `0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${tablet_id}`）
2. 写入 formal rowset meta（key: `0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}`），设置版本号
3. 删除 tmp rowset key
4. 更新 tablet stats（num_rowsets, num_rows, data_size 等）

### 4.2 Request 定义

```protobuf
message ConvertTmpRowsetRequest {
    optional string cloud_unique_id = 1;  // For auth
    optional int64 txn_id = 2;            // 事务 ID
    optional int64 tablet_id = 3;         // 要转正的 tablet ID
    optional int64 version = 4;           // 目标版本号（即 commit version）
    optional int64 db_id = 5;             // 数据库 ID
    optional int64 table_id = 6;          // 表 ID
    optional int64 index_id = 7;          // index ID（用于 schema 查找）
    optional int64 partition_id = 8;      // partition ID（用于构造 stats key 等）
    optional string request_ip = 9;       // 请求来源 IP（用于审计/调试）
    // sub_txn_id 用于事务 load 场景，同一 tablet 可能有多个 sub txn 的 tmp rowset
    repeated int64 sub_txn_ids = 10;
}
```

### 4.3 Response 定义

```protobuf
message ConvertTmpRowsetResponse {
    optional MetaServiceResponseStatus status = 1;
    // 返回转正后的 rowset meta，BE 可以用于本地 apply
    optional doris.RowsetMetaCloudPB rowset_meta = 2;
    // 返回更新后的 tablet stats
    optional TabletStatsPB stats = 3;
}
```

### 4.4 RPC 注册

在 `MetaService` service 中注册新 RPC：

```protobuf
service MetaService {
    // ... 现有 RPC ...

    // 异步发布：per-tablet tmp rowset 转正
    rpc convert_tmp_rowset(ConvertTmpRowsetRequest) returns (ConvertTmpRowsetResponse);
}
```

### 4.5 与现有 rowset 操作的关系

| RPC | 用途 | 场景 |
|-----|------|------|
| `prepare_rowset` | 写入 tmp rowset meta | 导入阶段（不变） |
| `commit_rowset` | 现有的 rowset 转正 | 一阶段提交（不变） |
| `update_tmp_rowset` | 更新已有的 tmp rowset meta | 导入阶段（不变） |
| **`convert_tmp_rowset`** | **per-tablet 转正** | **异步发布 Publish 阶段（新增）** |

### 4.6 ConvertTmpRowset 的幂等性设计

此 RPC 必须幂等，以支持失败重试和断点续做：
- 如果 formal rowset 已存在（key `0x01 "meta" ... "rowset" ${tablet_id} ${version}` 已有值），检查其 rowset_id 是否匹配：
  - 匹配：说明已经转正过，直接返回成功
  - 不匹配：返回错误（版本冲突）
- 如果 tmp rowset 不存在且 formal rowset 已存在且匹配：说明已经完成，返回成功

### 4.7 单元测试要点

- 测试正常转正流程：tmp rowset 被正确读取、formal rowset 被写入、tmp rowset 被删除、stats 被更新
- 测试幂等性：重复调用返回成功，不产生重复数据
- 测试 tmp rowset 不存在的错误处理
- 测试版本号冲突的错误处理
- 测试事务 load 场景（多个 sub_txn_ids）的正确转正
- 测试 stats 更新的正确性（num_rowsets、num_rows、data_size 等增量更新）

---

## 5. CommitTxnRequest/Response 修改

### 5.1 概述

`CommitTxnRequest` 在异步发布方案中承担两个不同阶段的角色：
1. **Commit 阶段**：快速提交，只更新 partition commit version 和 TxnInfoPB
2. **Publish 阶段（轻量级 commit）**：所有 tablet 转正完成后，更新 partition visible version 和 TxnInfoPB 状态

通过新增字段区分这两种用途。

### 5.2 CommitTxnRequest 新增字段

```protobuf
message CommitTxnRequest {
    // ... 现有字段 1-12 保持不变 ...
    optional string cloud_unique_id = 1;
    optional int64 db_id = 2;
    optional int64 txn_id = 3;
    optional bool is_2pc = 4;
    optional TxnCommitAttachmentPB commit_attachment = 5;
    repeated int64 mow_table_ids = 6;
    repeated int64 base_tablet_ids = 7;
    // 8 is not used (skipped in original definition)
    optional bool is_txn_load = 9;
    repeated SubTxnInfo sub_txn_infos = 10;
    optional bool enable_txn_lazy_commit = 11;
    optional string request_ip = 12;

    // ========== 异步发布新增字段（20-29）==========

    // 标记这是一个异步发布的 commit 请求
    // true: Commit 阶段，只更新 commit version + TxnInfoPB
    optional bool enable_mow_async_publish = 20;

    // 参与本事务的 tablet 信息（Commit 阶段传入）
    // MS 会将其持久化到 TxnInfoPB.involved_tablets
    repeated TxnTabletInfoPB involved_tablets = 21;

    // 导入参数（Commit 阶段传入）：TOlapTableSchemaParam 的 Thrift 序列化 bytes
    // MS 会将其持久化到 TxnInfoPB.load_schema_param
    optional bytes load_schema_param = 22;

    // 标记这是一个轻量级 publish 请求
    // true: Publish 阶段，只更新 visible version + TxnInfoPB 状态
    optional bool is_lightweight_publish = 23;
}
```

### 5.3 CommitTxnResponse 新增字段

```protobuf
message CommitTxnResponse {
    // ... 现有字段 1-7 保持不变 ...
    optional MetaServiceResponseStatus status = 1;
    optional TxnInfoPB txn_info = 2;
    repeated int64 table_ids = 3;
    repeated int64 partition_ids = 4;
    repeated int64 versions = 5;
    repeated TableStatsPB table_stats = 6;
    optional int64 version_update_time_ms = 7;

    // ========== 异步发布新增字段（10-19）==========

    // Commit 阶段返回：每个 partition 的 commit version
    // 与 partition_ids 一一对应
    repeated int64 commit_versions = 10;
}
```

### 5.4 两个阶段的请求/响应对比

#### Commit 阶段请求

```
CommitTxnRequest {
    cloud_unique_id = "..."
    db_id = 123
    txn_id = 456
    enable_mow_async_publish = true
    involved_tablets = [
        { tablet_id: 1001, table_id: 100, partition_id: 10, be_cloud_unique_id: "be1", be_endpoint: "10.0.0.1:8040" },
        { tablet_id: 1002, table_id: 100, partition_id: 10, be_cloud_unique_id: "be2", be_endpoint: "10.0.0.2:8040" },
    ]
    load_schema_param = <TOlapTableSchemaParam 的 Thrift 序列化 bytes>
    mow_table_ids = [100]
}
```

MS 处理逻辑：
1. 为每个涉及的 partition 读取 `partition_commit_version_key`，version + 1
2. 写入更新后的 `partition_commit_version_key`
3. 更新 TxnInfoPB：设置 `mow_async_publish = true`，`committed_partition_ids/committed_versions`，`involved_tablets`，`load_schema_param`，`status = TXN_STATUS_COMMITTED`
4. 返回各 partition 的 commit version

Commit 阶段响应：

```
CommitTxnResponse {
    status = { code: OK }
    txn_info = { ... status: TXN_STATUS_COMMITTED, mow_async_publish: true ... }
    partition_ids = [10]
    commit_versions = [5]
}
```

#### Publish 阶段（轻量级 commit）请求

```
CommitTxnRequest {
    cloud_unique_id = "..."
    db_id = 123
    txn_id = 456
    is_lightweight_publish = true
}
```

MS 处理逻辑：
1. 读取 TxnInfoPB，验证 `status == TXN_STATUS_COMMITTED` 且 `mow_async_publish == true`
2. 对每个 partition，将 `partition_version_key` (visible version) 更新为 commit version
3. 更新 TxnInfoPB：`status = TXN_STATUS_VISIBLE`，`finish_time = now()`
4. 清理相关临时数据（如 TxnRunning key）

### 5.5 与现有字段的兼容性

- `is_2pc` (字段 4)：现有的 FE 两阶段提交协议标志（begin_txn → precommit_txn → commit_txn with is_2pc=true），与本方案的 `enable_mow_async_publish` 语义不同
- `enable_txn_lazy_commit` (字段 11)：lazy commit 机制。启用异步发布的表不使用 lazy commit
- 新增字段编号从 20 开始，避免与现有字段（1-12）冲突，也预留了 13-19 的空间

### 5.6 单元测试要点

- 测试 Commit 阶段：commit version 正确递增，visible version 不变
- 测试 Publish 阶段：visible version 正确更新为 commit version
- 测试 Publish 阶段的前置检查：事务必须是 COMMITTED + mow_async_publish
- 测试 `enable_mow_async_publish` 和 `is_lightweight_publish` 不能同时为 true
- 测试轻量级 publish 的幂等性（重复请求不产生错误）
- 测试与 lazy commit 的互斥：同时设置 `enable_mow_async_publish` 和 `enable_txn_lazy_commit` 应报错

---

## 6. 表属性

### 6.1 控制方式

通过建表时的 `PROPERTIES` 设置 `enable_mow_async_publish` 属性来控制是否启用异步发布：

```sql
CREATE TABLE example_mow_table (
    k1 INT,
    v1 STRING,
    v2 INT
) UNIQUE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 4
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "enable_mow_async_publish" = "true"
);
```

### 6.2 约束条件

- 仅在 **Cloud 模式**下允许设置
- 仅对 **MOW 表**（`enable_unique_key_merge_on_write = true`）生效
- 仅限**新表**，不支持已有表的动态启用/禁用
- 非 Cloud 模式或非 MOW 表设置此属性应抛出 `AnalysisException`

### 6.3 持久化方式

表属性持久化在 FE 的 `OlapTable` 元数据中（`TableProperty`），不需要在 proto 层面新增字段。FE 在 `commit_txn` 时通过 `CommitTxnRequest.enable_mow_async_publish` 告知 MS。

MS 端通过 TxnInfoPB 中的 `mow_async_publish` 字段持久化此信息，后续处理不依赖表属性读取。

### 6.4 FE 端变更（概要）

以下变更的详细设计在 Module 4 (05-fe-commit-phase.md) 中描述，此处仅列出与 Proto/Schema 相关的部分：

- `PropertyAnalyzer` 新增 `PROPERTIES_ENABLE_TWO_PHASE_COMMIT` 常量
- `OlapTable` / `TableProperty` 持久化该属性
- 建表时校验 Cloud + MOW 的约束

### 6.5 单元测试要点

- 测试 Cloud 模式 MOW 表可以成功设置 `enable_mow_async_publish = true`
- 测试非 Cloud 模式设置该属性报错
- 测试非 MOW 表设置该属性报错
- 测试属性值只接受 `true` / `false`

---

## 7. TxnStatusPB 语义分析

### 7.1 现有状态定义

```protobuf
enum TxnStatusPB {
    TXN_STATUS_UNKNOWN      = 0;
    TXN_STATUS_PREPARED     = 1;
    TXN_STATUS_COMMITTED    = 2;
    TXN_STATUS_VISIBLE      = 3;
    TXN_STATUS_ABORTED      = 4;
    TXN_STATUS_PRECOMMITTED = 5;
}
```

### 7.2 `TXN_STATUS_COMMITTED` 的语义变更

**当前语义**：
- 一阶段提交场景：事务的 rowset 已转正，partition version 已更新，但尚未确认数据一致性（主要用于 lazy commit 场景）
- Lazy commit 场景：commit_txn 快速返回后标记为 COMMITTED，后台异步完成最终一致性确认

**异步发布下的新语义**：
- `TXN_STATUS_COMMITTED`：事务已完成异步发布 Commit（partition commit version 已递增，TxnInfoPB 已更新），但尚未 Publish（delete bitmap 尚未计算，rowset 尚未转正，visible version 尚未更新）

### 7.3 不新增状态值的理由

经分析，**不需要新增** TxnStatusPB 值，理由如下：

1. **区分依据充分**：通过 `TxnInfoPB.mow_async_publish` 字段即可区分同一 COMMITTED 状态下的不同含义
2. **状态流转相同**：两种方案的状态流转路径一致（PREPARED → COMMITTED → VISIBLE），只是 COMMITTED 到 VISIBLE 之间的工作内容不同
3. **向后兼容**：不增加 enum 值避免老版本代码遇到未知状态的解析问题
4. **代码简洁**：已有的状态机逻辑可以复用，通过 `mow_async_publish` 做分支即可

### 7.4 状态流转对比

```
一阶段提交（现有）:
  PREPARED ──commit_txn──> COMMITTED/VISIBLE
                           （rowset转正+version更新在同一步完成）

Lazy commit（现有）:
  PREPARED ──commit_txn(lazy)──> COMMITTED ──async──> VISIBLE
                                 （快速返回）     （后台确认）

异步发布（新方案）:
  PREPARED ──commit_txn(2pc)──> COMMITTED ──publish──> VISIBLE
            （commit version+1）  （delete bitmap + rowset转正 + visible version+1）
```

### 7.5 如何区分 COMMITTED 的含义

```
if (txn_info.status() == TXN_STATUS_COMMITTED) {
    if (txn_info.has_mow_async_publish() && txn_info.mow_async_publish()) {
        // 异步发布的 COMMITTED：需要 publish
    } else if (txn_info.has_defer_deleting_pending_delete_bitmaps() &&
               txn_info.defer_deleting_pending_delete_bitmaps()) {
        // Lazy commit 的 COMMITTED：需要完成最终确认
    } else {
        // 标准一阶段提交的 COMMITTED（不常见）
    }
}
```

### 7.6 单元测试要点

- 测试异步发布事务的状态流转：PREPARED → COMMITTED → VISIBLE
- 测试 COMMITTED 状态下 `mow_async_publish` 为 true 时的行为
- 测试非异步发布事务的 COMMITTED 状态行为不变
- 测试 COMMITTED 状态下的事务恢复逻辑（FE 重启后能正确识别需要 publish 的事务）
- 测试 abort 操作在 COMMITTED 状态下的处理（异步发布的 COMMITTED 事务是否允许 abort，以及 abort 时需要回滚哪些操作）

---

## 8. MS Tablet 级别锁

### 8.1 背景

当前系统使用**表级别**的 delete bitmap update lock（`meta_delete_bitmap_update_lock_key`），用于导入和 compaction 之间的互斥。异步发布方案中，publish 阶段 BE 需要 per-tablet 地计算 delete bitmap 并转正 rowset，需要与该 tablet 上的 compaction 互斥。表级别锁粒度太粗，因此引入 **tablet 级别锁**。

> **注意**：现有表级别锁的所有代码（`meta_delete_bitmap_update_lock_key`、`GetDeleteBitmapUpdateLockRequest/Response`、`RemoveDeleteBitmapUpdateLockRequest/Response`）**全部保留不动**，未启用异步发布的表继续使用表级别锁。

### 8.2 Tablet 级别锁 KV 定义

```
// 新增 tablet 级别 delete bitmap update lock:
Key:  0x01 "meta" ${instance_id} "delete_bitmap_tablet_lock" ${table_id} ${tablet_id}
Value: DeleteBitmapUpdateLockPB (复用现有，包含 lock_id, expiration, initiators)
```

```cpp
// keys.h
using MetaDeleteBitmapTabletLockInfo = BasicKeyInfo<__LINE__, std::tuple<std::string, int64_t, int64_t>>;
void meta_delete_bitmap_tablet_lock_key(const MetaDeleteBitmapTabletLockInfo& in, std::string* out);

// keys.cpp
static const char* META_KEY_INFIX_DELETE_BITMAP_TABLET_LOCK = "delete_bitmap_tablet_lock";
void meta_delete_bitmap_tablet_lock_key(const MetaDeleteBitmapTabletLockInfo& in, std::string* out) {
    encode_prefix(in, out);
    encode_bytes(META_KEY_INFIX_DELETE_BITMAP_TABLET_LOCK, out);
    encode_int64(std::get<1>(in), out);  // table_id
    encode_int64(std::get<2>(in), out);  // tablet_id
}
```

### 8.3 RPC 扩展

复用现有 RPC，通过新增字段区分表级别锁和 tablet 级别锁：

```protobuf
message GetDeleteBitmapUpdateLockRequest {
    // ... 现有字段 1-10 保持不变 ...

    // Tablet 级别锁新增字段
    optional bool tablet_level_lock = 20;
    repeated int64 lock_tablet_ids = 21;
}

message RemoveDeleteBitmapUpdateLockRequest {
    // ... 现有字段 1-6 保持不变 ...

    // Tablet 级别锁新增字段
    optional bool tablet_level_lock = 20;
    repeated int64 unlock_tablet_ids = 21;
}
```

### 8.4 使用场景

- **Publish 阶段**：BE 在 convert_tmp_rowset 前获取 tablet 锁，convert + local apply 完成后释放
- **Compaction**：对启用异步发布的表，compaction 获取 tablet 锁（替代表级别锁）
- 大多数情况下同一 BE 上的 `rowset_update_lock` 已提供互斥，MS tablet 锁主要处理跨 BE 场景

### 8.5 单元测试要点

- 测试 `meta_delete_bitmap_tablet_lock_key` 的编码/解码正确性
- 测试同一 tablet 的 publish 和 compaction 互斥
- 测试不同 tablet 的操作可以并发
- 测试锁过期和幂等获取
- 测试 `tablet_level_lock=false` 时行为与现有表级别锁完全一致

---

## 9. 老代码保留原则

> **本功能通过表属性 `enable_mow_async_publish` 控制开启，只在新建的表上启用。所有现有代码完整保留，两条路径互不干扰。**

### 保留清单

- **Proto 字段**：TxnInfoPB (1-22)、CommitTxnRequest (1-12)、CommitTxnResponse (1-7)、GetDeleteBitmapUpdateLockRequest (1-10)、RemoveDeleteBitmapUpdateLockRequest (1-6)、TxnStatusPB 枚举值——全部保留不动
- **KV Schema**：partition_version_key、meta_delete_bitmap_update_lock_key、所有其他现有 KV——格式和语义不变
- **RPC**：commit_txn、get/remove_delete_bitmap_update_lock——通过新增字段区分场景，现有字段语义不变

---

## 10. 完整 Proto 变更汇总

以下是所有需要在 `cloud.proto` 中进行的变更：

### 8.1 新增 Message

```protobuf
// ======== 新增 Message ========

// 事务中参与的 tablet 信息
message TxnTabletInfoPB {
    optional int64 tablet_id = 1;
    optional int64 table_id = 2;
    optional int64 index_id = 3;
    optional int64 partition_id = 4;
    optional string be_cloud_unique_id = 5;
    optional string be_endpoint = 6;
}

// Per-tablet tmp rowset 转正请求
message ConvertTmpRowsetRequest {
    optional string cloud_unique_id = 1;
    optional int64 txn_id = 2;
    optional int64 tablet_id = 3;
    optional int64 version = 4;
    optional int64 db_id = 5;
    optional int64 table_id = 6;
    optional int64 index_id = 7;
    optional int64 partition_id = 8;
    optional string request_ip = 9;
    repeated int64 sub_txn_ids = 10;
}

// Per-tablet tmp rowset 转正响应
message ConvertTmpRowsetResponse {
    optional MetaServiceResponseStatus status = 1;
    optional doris.RowsetMetaCloudPB rowset_meta = 2;
    optional TabletStatsPB stats = 3;
}
```

### 8.2 修改现有 Message

```protobuf
// ======== 修改 TxnInfoPB ========
message TxnInfoPB {
    // ... 现有字段 1-22 保持不变 ...

    // 新增字段 (30-39)
    optional bool mow_async_publish = 30;
    repeated int64 committed_partition_ids = 31;
    repeated int64 committed_versions = 32;
    repeated TxnTabletInfoPB involved_tablets = 33;
    optional bytes load_schema_param = 34;  // TOlapTableSchemaParam Thrift 序列化 bytes
    repeated int64 published_tablet_ids = 35;
}

// ======== 修改 CommitTxnRequest ========
message CommitTxnRequest {
    // ... 现有字段 1-12 保持不变 ...

    // 新增字段 (20-29)
    optional bool enable_mow_async_publish = 20;
    repeated TxnTabletInfoPB involved_tablets = 21;
    optional bytes load_schema_param = 22;  // TOlapTableSchemaParam Thrift 序列化 bytes
    optional bool is_lightweight_publish = 23;
}

// ======== 修改 CommitTxnResponse ========
message CommitTxnResponse {
    // ... 现有字段 1-7 保持不变 ...

    // 新增字段 (10-19)
    repeated int64 commit_versions = 10;
}
```

### 8.3 修改 Service 定义

```protobuf
service MetaService {
    // ... 现有 RPC 保持不变 ...

    // 新增
    rpc convert_tmp_rowset(ConvertTmpRowsetRequest) returns (ConvertTmpRowsetResponse);
}
```

### 8.4 不需要修改的部分

- `TxnStatusPB`：不新增状态值（见第 7 节分析）
- `VersionPB`：直接复用，不修改
- `RowsetMetaCloudPB`：不修改
- `UpdateDeleteBitmapRequest/Response`：不修改（直接复用现有 RPC）
- `GetDeleteBitmapUpdateLockRequest/Response`：扩展以支持 tablet 级别锁（新增字段 20-21）
- `RemoveDeleteBitmapUpdateLockRequest/Response`：扩展以支持 tablet 级别锁（新增字段 20-21）

---

## 11. 完整 KV Schema 变更汇总

### 11.1 新增 KV

```
// 新增 partition commit version
// Key 格式:
0x01 "version" ${instance_id} "partition_commit" ${db_id} ${tbl_id} ${partition_id}
// Value: VersionPB (复用)

// Key encoding (keys.h):
using PartitionCommitVersionKeyInfo = BasicKeyInfo<__LINE__, std::tuple<std::string, int64_t, int64_t, int64_t>>;

// Key function (keys.cpp):
void partition_commit_version_key(const PartitionCommitVersionKeyInfo& in, std::string* out);

// -------

// 新增 tablet 级别 delete bitmap update lock
// Key 格式:
0x01 "meta" ${instance_id} "delete_bitmap_tablet_lock" ${table_id} ${tablet_id}
// Value: DeleteBitmapUpdateLockPB (复用)

// Key encoding (keys.h):
using MetaDeleteBitmapTabletLockInfo = BasicKeyInfo<__LINE__, std::tuple<std::string, int64_t, int64_t>>;

// Key function (keys.cpp):
void meta_delete_bitmap_tablet_lock_key(const MetaDeleteBitmapTabletLockInfo& in, std::string* out);
```

### 11.2 现有 KV 不修改

以下 KV 的 key 格式和 value 结构均不变化：

| KV | 说明 |
|----|------|
| `partition_version_key` → `VersionPB` | visible version，不变 |
| `txn_info_key` → `TxnInfoPB` | value 的 proto 增加了字段，但 key 不变 |
| `meta_rowset_tmp_key` → `RowsetMetaCloudPB` | tmp rowset，不变 |
| `meta_rowset_key` → `RowsetMetaCloudPB` | formal rowset，不变 |
| `stats_tablet_key` → `TabletStatsPB` | tablet stats，不变 |
| `meta_delete_bitmap_key` → roaringbitmap | delete bitmap，不变 |
| `meta_delete_bitmap_update_lock_key` → `DeleteBitmapUpdateLockPB` | delete bitmap lock，不变 |

---

## 12. 文件变更清单

| 文件 | 变更类型 | 变更内容 |
|------|---------|---------|
| `gensrc/proto/cloud.proto` | 修改 | 新增 `TxnTabletInfoPB`、`ConvertTmpRowsetRequest`、`ConvertTmpRowsetResponse`；扩展 `TxnInfoPB`（新增 `load_schema_param` bytes 字段等）、`CommitTxnRequest`、`CommitTxnResponse`、`GetDeleteBitmapUpdateLockRequest`、`RemoveDeleteBitmapUpdateLockRequest`；在 `MetaService` 中注册 `convert_tmp_rowset` RPC |
| `cloud/src/meta-store/keys.h` | 修改 | 新增 `PartitionCommitVersionKeyInfo`、`MetaDeleteBitmapTabletLockInfo` 类型定义和对应函数声明 |
| `cloud/src/meta-store/keys.cpp` | 修改 | 新增 `PARTITION_COMMIT_VERSION_KEY_INFIX` 和 `META_KEY_INFIX_DELETE_BITMAP_TABLET_LOCK` 常量和编解码实现 |

---

## 13. 与其他模块的接口约定

| 下游模块 | 依赖本模块的内容 |
|---------|----------------|
| Module 1 (MS Commit 阶段) | `PartitionCommitVersionKeyInfo`、`CommitTxnRequest` 新增字段、`TxnInfoPB` 新增字段 |
| Module 2 (MS Convert Rowset) | `ConvertTmpRowsetRequest/Response`、`convert_tmp_rowset` RPC |
| Module 3 (MS Lightweight Publish) | `CommitTxnRequest.is_lightweight_publish`、`CommitTxnResponse.commit_versions` |
| Module 4 (FE Commit 阶段) | `CommitTxnRequest/Response` 新增字段、表属性定义 |
| Module 5 (FE Publish Daemon) | `TxnInfoPB.involved_tablets`、`TxnInfoPB.published_tablet_ids` |
| Module 6 (FE Recovery) | `TxnInfoPB.mow_async_publish`、`TxnInfoPB.committed_versions`、`TxnStatusPB` 语义 |
| Module 7 (BE CalcBitmap) | `TxnInfoPB.load_schema_param`（反序列化为 `TOlapTableSchemaParam`）、`ConvertTmpRowsetRequest/Response`、tablet 级别锁 RPC |
| Module 8 (Cleanup) | `TxnInfoPB.mow_async_publish`（决定是否跳过 lazy commit / pending delete bitmap 逻辑）、tablet 级别锁 KV/RPC |
