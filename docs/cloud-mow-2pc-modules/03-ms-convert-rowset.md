# Module 2: MS侧Per-Tablet Rowset转正（新RPC）

## 1. 概述

在异步发布方案中，publish阶段每个tablet独立完成以下流水线：

```
BE calc delete bitmap
  → BE调用MS update_delete_bitmap (现有RPC)
  → BE/FE调用MS convert_tmp_rowset (新RPC，本模块)
  → BE本地apply rowset到tablet元数据
```

本模块设计 `convert_tmp_rowset` 新RPC，将指定tablet的tmp rowset meta转正为正式的formal rowset meta。当前方案中这个转正操作是在 `commit_txn` 的FDB事务中统一对所有tablet一起做的，现在改为每个tablet独立调用，从而将最终publish操作降为O(1)。

---

## 2. 当前tmp rowset转正流程分析

### 2.1 tmp rowset的写入（commit_rowset RPC）

BE在数据写入完成后，通过 `MetaServiceImpl::commit_rowset()` RPC将rowset meta写入MS的FDB中。

核心代码位于 `cloud/src/meta-service/meta_service.cpp:2695`：

```cpp
void MetaServiceImpl::commit_rowset(::google::protobuf::RpcController* controller,
                                    const CreateRowsetRequest* request,
                                    CreateRowsetResponse* response,
                                    ::google::protobuf::Closure* done) {
    // ...
    auto tmp_rs_key = meta_rowset_tmp_key({instance_id, rowset_meta.txn_id(), tablet_id});
    // ...
    // 幂等性检查：如果tmp_rs_key已存在且rowset_id相同，直接返回OK
    // 删除recycle_rowset_key，写入tmp_rowset_key
    txn->remove(recycle_rs_key);
    auto tmp_rs_val = rowset_meta.SerializeAsString();
    txn->put(tmp_rs_key, tmp_rs_val);
    // ...
    err = txn->commit();
}
```

**tmp rowset KV 格式**：

- **Key**: `meta_rowset_tmp_key({instance_id, txn_id, tablet_id})`
  - 编码格式: `0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${tablet_id}`
  - KeyInfo 定义 (keys.h:173): `MetaRowsetTmpKeyInfo = BasicKeyInfo<..., std::tuple<std::string, int64_t, int64_t>>`
    - 元素0: `instance_id` (string)
    - 元素1: `txn_id` (int64)
    - 元素2: `tablet_id` (int64)

- **Value**: `RowsetMetaCloudPB` 序列化后的二进制
  - 核心字段包括：`tablet_id`, `partition_id`, `txn_id`, `rowset_id_v2`, `num_rows`, `total_disk_size`, `data_disk_size`, `index_disk_size`, `num_segments` 等
  - **注意**：此时 `start_version` 和 `end_version` 尚未设置（或为0），因为版本号在 commit_txn 时才分配

### 2.2 scan_tmp_rowset()

在 `commit_txn` 流程中，首先通过 `scan_tmp_rowset()` 函数（`meta_service_txn.cpp:1102`）读取该事务的所有tmp rowset：

```cpp
void scan_tmp_rowset(
        const std::string& instance_id, int64_t txn_id, std::shared_ptr<TxnKv> txn_kv,
        MetaServiceCode& code, std::string& msg,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* tmp_rowsets_meta,
        KVStats* stats) {
    // Range scan: [meta_rowset_tmp_key(instance_id, txn_id, 0),
    //              meta_rowset_tmp_key(instance_id, txn_id+1, 0))
    MetaRowsetTmpKeyInfo rs_tmp_key_info0 {instance_id, txn_id, 0};
    MetaRowsetTmpKeyInfo rs_tmp_key_info1 {instance_id, txn_id + 1, 0};
    // ...
    // 使用独立的只读FDB事务进行range scan
    // 返回所有 (tmp_key_string, RowsetMetaCloudPB) 对
}
```

关键点：
- 使用txn_id作为scan的范围，一次获取该事务下所有tablet的tmp rowset
- 对于新RPC，我们只需要单个tablet的tmp rowset，可以用精确的point get替代range scan

### 2.3 commit_txn_immediately() 中的转正流程

位于 `meta_service_txn.cpp:1520-2074`，在一个FDB事务中完成所有操作。与rowset转正直接相关的步骤如下：

**步骤1：分配版本号并构建rowset列表** (行1709-1746)

```cpp
for (auto& [_, i] : tmp_rowsets_meta) {
    int64_t tablet_id = i.tablet_id();
    int64_t partition_id = i.partition_id();
    // 更新rowset版本号为 partition visible version + 1
    int64_t new_version = versions[partition_id] + 1;
    i.set_start_version(new_version);
    i.set_end_version(new_version);
    i.set_visible_ts_ms(rowsets_visible_ts_ms);
    // 累加tablet stats
    auto& stats = tablet_stats[tablet_id];
    stats.data_size += i.total_disk_size();
    stats.num_rows += i.num_rows();
    ++stats.num_rowsets;
    stats.num_segs += i.num_segments();
    stats.index_size += i.index_disk_size();
    stats.segment_size += i.data_disk_size();
    // 记录 (tablet_id, version) -> rowset_meta 的映射
    rowsets.emplace_back(std::make_tuple(tablet_id, i.end_version()), i);
}
```

**步骤2：写入formal rowset KV** (行1759-1791)

```cpp
// Save rowset meta
for (auto& i : rowsets) {
    auto [tablet_id, version] = i.first;
    // formal rowset key: 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}
    std::string rowset_key = meta_rowset_key({instance_id, tablet_id, version});
    std::string val;
    i.second.SerializeToString(&val);
    txn->put(rowset_key, val);

    // 如果启用了versioned_write，还要写入versioned rowset key
    if (is_versioned_write) {
        // versioned rowset key: 0x03 "meta" ${instance_id} "rowset_load" ${tablet_id} ${version} ${timestamp}
        std::string versioned_rowset_key =
                versioned::meta_rowset_load_key({instance_id, tablet_id, version});
        RowsetMetaCloudPB copied_rowset_meta(i.second);
        versioned::document_put(txn.get(), versioned_rowset_key, std::move(copied_rowset_meta));
    }
}
```

**formal rowset KV 格式**：

- **Key**: `meta_rowset_key({instance_id, tablet_id, version})`
  - 编码格式: `0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}`
  - KeyInfo 定义 (keys.h:170): `MetaRowsetKeyInfo = BasicKeyInfo<..., std::tuple<std::string, int64_t, int64_t>>`
    - 元素0: `instance_id` (string)
    - 元素1: `tablet_id` (int64)
    - 元素2: `version` (int64)

- **Value**: `RowsetMetaCloudPB` 序列化后的二进制（与tmp rowset的value相同，但version字段已被设置）

**versioned rowset KV 格式**（仅当启用versioned_write时）：

- **Key**: `versioned::meta_rowset_load_key({instance_id, tablet_id, version})`
  - 编码格式: `0x03 "meta" ${instance_id} "rowset_load" ${tablet_id} ${version} ${timestamp}`
  - timestamp由FDB versionstamp自动附加

- **Value**: `RowsetMetaCloudPB` 通过 `versioned::document_put` 写入（包含document header）

**步骤3：更新tablet stats** (行1932-1958)

```cpp
for (auto& [tablet_id, stats] : tablet_stats) {
    auto& tablet_idx = tablet_ids[tablet_id];
    StatsTabletKeyInfo info {instance_id, tablet_idx.table_id(), tablet_idx.index_id(),
                             tablet_idx.partition_id(), tablet_id};
    update_tablet_stats(info, stats, txn, code, msg);

    // versioned_write场景下还要更新versioned tablet stats
    if (is_versioned_write) {
        TabletStatsPB stats_pb = existing_versioned_stats[tablet_id];
        merge_tablet_stats(stats_pb, stats);
        std::string stats_key = versioned::tablet_load_stats_key({instance_id, tablet_id});
        versioned::document_put(txn.get(), stats_key, std::move(stats_pb));
    }
}
```

**tablet stats KV 格式和更新逻辑**：

`update_tablet_stats()` 函数（`meta_service_txn.cpp:1228`）有两种模式：

1. **split_tablet_stats 模式**（`config::split_tablet_stats = true`）：
   - 各维度分拆为独立的key，使用 `atomic_add` 原子增量更新：
     - `stats_tablet_data_size_key` → atomic_add(data_size)
     - `stats_tablet_num_rows_key` → atomic_add(num_rows)
     - `stats_tablet_num_rowsets_key` → atomic_add(num_rowsets)
     - `stats_tablet_num_segs_key` → atomic_add(num_segs)
     - `stats_tablet_index_size_key` → atomic_add(index_size)
     - `stats_tablet_segment_size_key` → atomic_add(segment_size)
   - Key格式统一: `0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "<suffix>"`

2. **非split模式**（`config::split_tablet_stats = false`）：
   - 单个key `stats_tablet_key` → `TabletStatsPB`
   - 先读取现有 `TabletStatsPB`，字段累加后整体写回
   - Key格式: `0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id}`

**versioned tablet stats KV 格式**：
- Key: `versioned::tablet_load_stats_key({instance_id, tablet_id})`
  - 编码格式: `0x03 "stats" ${instance_id} "tablet_load" ${tablet_id} ${timestamp}`
- Value: `TabletStatsPB`（先读取已有的，merge后写入）

**步骤4：删除tmp rowset KV** (行1960-1964)

```cpp
// Remove tmp rowset meta
for (auto& [k, _] : tmp_rowsets_meta) {
    txn->remove(k);
}
```

**步骤5：提交FDB事务** (行2021)

以上所有操作（写formal rowset、更新stats、删除tmp rowset，以及更新partition version、TxnInfoPB等）都在同一个FDB事务中提交。

### 2.4 当前方案的问题

1. **FDB事务大小**：所有tablet的rowset meta + stats更新 + partition version更新 + TxnInfoPB更新，全在一个FDB事务中。当tablet数量很大时，FDB事务可能超过10MB限制（`TXN_BYTES_TOO_LARGE`）
2. **原子性范围过大**：任何一个tablet的操作失败都会导致整个事务回滚
3. **不支持per-tablet并发**：所有tablet必须串行处理

---

## 3. 新RPC设计：ConvertTmpRowset

### 3.1 Proto定义

Proto定义已在 Module 0 (`01-proto-kv-schema.md`) 中定义，此处重述关键部分：

```protobuf
// Request
message ConvertTmpRowsetRequest {
    optional string cloud_unique_id = 1;  // 用于鉴权
    optional int64 txn_id = 2;            // 事务 ID
    optional int64 tablet_id = 3;         // 要转正的 tablet ID
    optional int64 version = 4;           // commit version（由commit阶段分配）
    optional int64 db_id = 5;             // 数据库 ID
    optional int64 table_id = 6;          // 表 ID
    optional int64 index_id = 7;          // index ID（用于构造 StatsTabletKeyInfo）
    optional int64 partition_id = 8;      // partition ID（用于构造 StatsTabletKeyInfo）
    optional string request_ip = 9;       // 调试用
    repeated int64 sub_txn_ids = 10;      // sub txn 场景（事务 load）
}

// Response
message ConvertTmpRowsetResponse {
    optional MetaServiceResponseStatus status = 1;
    // 返回转正后的 rowset meta（version已设置），BE可用于本地apply
    optional doris.RowsetMetaCloudPB rowset_meta = 2;
    // 返回更新后的 tablet stats
    optional TabletStatsPB stats = 3;
}

// Service注册
service MetaService {
    // ...
    rpc convert_tmp_rowset(ConvertTmpRowsetRequest) returns (ConvertTmpRowsetResponse);
}
```

### 3.2 Request字段说明

| 字段 | 必需 | 来源 | 说明 |
|------|:----:|------|------|
| `cloud_unique_id` | 是 | BE配置 | 用于鉴权和获取instance_id |
| `txn_id` | 是 | commit阶段 | 与tablet_id一起定位tmp rowset key |
| `tablet_id` | 是 | commit阶段 | 要转正的tablet |
| `version` | 是 | commit阶段 | 即commit version，写入rowset meta的start_version/end_version |
| `db_id` | 是 | commit阶段 | 目前未直接使用，预留 |
| `table_id` | 是 | commit阶段 | 用于构造 StatsTabletKeyInfo |
| `index_id` | 是 | commit阶段 | 用于构造 StatsTabletKeyInfo |
| `partition_id` | 是 | commit阶段 | 用于构造 StatsTabletKeyInfo |
| `sub_txn_ids` | 否 | commit阶段 | 事务load场景下一个tablet可能有多个sub txn的tmp rowset |

### 3.3 实现步骤（单个FDB事务）

以下所有操作在同一个FDB事务中完成：

```
步骤1: 鉴权 + 参数校验
步骤2: 幂等性检查（检查formal rowset是否已存在）
步骤3: 读取tmp rowset KV
步骤4: 设置version字段
步骤5: 写入formal rowset KV
步骤6: 如果是versioned_write，写入versioned::meta_rowset_load_key
步骤7: 更新tablet stats（含versioned tablet stats）
步骤8: 删除tmp rowset KV
步骤9: 提交FDB事务
步骤10: 构建Response返回
```

### 3.4 详细伪代码

```cpp
void MetaServiceImpl::convert_tmp_rowset(
        ::google::protobuf::RpcController* controller,
        const ConvertTmpRowsetRequest* request,
        ConvertTmpRowsetResponse* response,
        ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(convert_tmp_rowset, get, put, del);

    // ===== 步骤1: 鉴权 + 参数校验 =====
    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    int64_t txn_id = request->txn_id();
    int64_t tablet_id = request->tablet_id();
    int64_t version = request->version();
    if (txn_id <= 0 || tablet_id <= 0 || version <= 0) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "invalid txn_id/tablet_id/version";
        return;
    }

    RPC_RATE_LIMIT(convert_tmp_rowset)

    bool is_versioned_write = is_version_write_enabled(instance_id);
    bool is_versioned_read = is_version_read_enabled(instance_id);

    // 创建FDB事务
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create txn";
        return;
    }
    if (is_versioned_write) {
        txn->enable_get_versionstamp();
    }

    // ===== 步骤2: 幂等性检查 =====
    // 检查formal rowset是否已存在
    std::string formal_rowset_key = meta_rowset_key({instance_id, tablet_id, version});
    std::string formal_rowset_val;
    err = txn->get(formal_rowset_key, &formal_rowset_val);
    if (err == TxnErrorCode::TXN_OK) {
        // formal rowset已存在 -> 可能是重试
        RowsetMetaCloudPB existing_rs_meta;
        if (!existing_rs_meta.ParseFromString(formal_rowset_val)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = "failed to parse existing formal rowset meta";
            return;
        }
        // 校验rowset_id是否匹配（同一个txn同一个tablet应该产生相同的rowset）
        // 读取tmp rowset来比较rowset_id
        auto tmp_rs_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
        std::string tmp_rs_val;
        err = txn->get(tmp_rs_key, &tmp_rs_val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            // tmp已删除 + formal已存在 -> 完全重复的请求，直接返回成功
            response->mutable_rowset_meta()->CopyFrom(existing_rs_meta);
            code = MetaServiceCode::OK;
            return;
        }
        if (err == TxnErrorCode::TXN_OK) {
            RowsetMetaCloudPB tmp_rs_meta;
            if (!tmp_rs_meta.ParseFromString(tmp_rs_val)) {
                code = MetaServiceCode::PROTOBUF_PARSE_ERR;
                msg = "failed to parse tmp rowset meta";
                return;
            }
            if (existing_rs_meta.rowset_id_v2() == tmp_rs_meta.rowset_id_v2()) {
                // rowset_id匹配，说明是重试（之前FDB事务可能部分完成——
                // 实际不可能，因为FDB事务原子性，但作为防御性编程）
                // 返回成功，但仍需清理tmp rowset（下面统一处理）
                // 注意：如果formal已存在且tmp也存在，说明上一次写入formal成功但
                // 删除tmp失败——这在FDB原子事务中不可能发生。
                // 所以这种情况理论上不存在，但为安全起见直接返回成功
                response->mutable_rowset_meta()->CopyFrom(existing_rs_meta);
                code = MetaServiceCode::OK;
                return;
            } else {
                // rowset_id不匹配 -> version冲突
                code = MetaServiceCode::ALREADY_EXISTED;
                msg = fmt::format("version {} already occupied by different rowset, "
                                  "existing={}, current={}",
                                  version, existing_rs_meta.rowset_id_v2(),
                                  tmp_rs_meta.rowset_id_v2());
                return;
            }
        }
        // 其他get错误
        code = cast_as<ErrCategory::READ>(err);
        msg = "failed to check tmp rowset";
        return;
    }
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        msg = "failed to check formal rowset existence";
        return;
    }
    // formal rowset 不存在，继续正常流程

    // ===== 步骤3: 读取tmp rowset KV =====
    auto tmp_rs_key = meta_rowset_tmp_key({instance_id, txn_id, tablet_id});
    std::string tmp_rs_val;
    err = txn->get(tmp_rs_key, &tmp_rs_val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("tmp rowset not found, txn_id={}, tablet_id={}", txn_id, tablet_id);
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to get tmp rowset, err={}", err);
        return;
    }

    RowsetMetaCloudPB rs_meta;
    if (!rs_meta.ParseFromString(tmp_rs_val)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse tmp rowset meta";
        return;
    }

    // 检查tmp rowset是否已被标记为recycled
    if (rs_meta.has_is_recycled() && rs_meta.is_recycled()) {
        code = MetaServiceCode::TXN_ALREADY_ABORTED;
        msg = "rowset has been marked as recycled";
        return;
    }

    // 校验tablet_id一致性
    if (rs_meta.tablet_id() != tablet_id) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("tablet_id mismatch: request={}, rowset_meta={}",
                          tablet_id, rs_meta.tablet_id());
        return;
    }

    // ===== 步骤4: 设置version字段 =====
    rs_meta.set_start_version(version);
    rs_meta.set_end_version(version);
    int64_t visible_ts_ms = duration_cast<milliseconds>(
            system_clock::now().time_since_epoch()).count();
    rs_meta.set_visible_ts_ms(visible_ts_ms);

    // ===== 步骤5: 写入formal rowset KV =====
    std::string formal_val;
    if (!rs_meta.SerializeToString(&formal_val)) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = "failed to serialize rowset meta";
        return;
    }
    txn->put(formal_rowset_key, formal_val);
    LOG(INFO) << "convert_tmp_rowset: put formal rowset_key=" << hex(formal_rowset_key)
              << " txn_id=" << txn_id << " tablet_id=" << tablet_id
              << " version=" << version;

    // ===== 步骤6: 写入versioned rowset（如果启用） =====
    if (is_versioned_write) {
        std::string versioned_rowset_key =
                versioned::meta_rowset_load_key({instance_id, tablet_id, version});
        RowsetMetaCloudPB copied_rs_meta(rs_meta);
        if (!versioned::document_put(txn.get(), versioned_rowset_key,
                                     std::move(copied_rs_meta))) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to put versioned rowset meta";
            return;
        }
        LOG(INFO) << "convert_tmp_rowset: put versioned rowset key="
                  << hex(versioned_rowset_key) << " txn_id=" << txn_id;
    }

    // ===== 步骤7: 更新tablet stats =====
    TabletStats stats;
    stats.data_size = rs_meta.total_disk_size();
    stats.num_rows = rs_meta.num_rows();
    stats.num_rowsets = 1;
    stats.num_segs = rs_meta.num_segments();
    stats.index_size = rs_meta.index_disk_size();
    stats.segment_size = rs_meta.data_disk_size();

    StatsTabletKeyInfo stats_info {instance_id, request->table_id(), request->index_id(),
                                   request->partition_id(), tablet_id};
    update_tablet_stats(stats_info, stats, txn, code, msg);
    if (code != MetaServiceCode::OK) return;

    // versioned tablet stats
    if (is_versioned_write) {
        // 先读取现有的versioned stats
        CloneChainReader meta_reader(instance_id, resource_mgr_.get());
        std::unordered_map<int64_t, TabletIndexPB> tablet_indexes;
        TabletIndexPB idx;
        idx.set_table_id(request->table_id());
        idx.set_index_id(request->index_id());
        idx.set_partition_id(request->partition_id());
        idx.set_tablet_id(tablet_id);
        tablet_indexes[tablet_id] = idx;

        std::unordered_map<int64_t, TabletStatsPB> existing_versioned_stats;
        internal_get_load_tablet_stats_batch(code, msg, meta_reader, txn.get(),
                                             instance_id, tablet_indexes,
                                             &existing_versioned_stats);
        if (code != MetaServiceCode::OK) {
            LOG(WARNING) << "failed to get versioned tablet stats, tablet_id=" << tablet_id;
            return;
        }

        TabletStatsPB stats_pb = existing_versioned_stats[tablet_id];
        merge_tablet_stats(stats_pb, stats);
        std::string versioned_stats_key =
                versioned::tablet_load_stats_key({instance_id, tablet_id});
        if (!versioned::document_put(txn.get(), versioned_stats_key,
                                     std::move(stats_pb))) {
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            msg = "failed to put versioned tablet stats";
            return;
        }
    }

    // ===== 步骤8: 删除tmp rowset KV =====
    txn->remove(tmp_rs_key);
    LOG(INFO) << "convert_tmp_rowset: remove tmp_rs_key=" << hex(tmp_rs_key)
              << " txn_id=" << txn_id;

    // ===== 步骤9: 提交FDB事务 =====
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::COMMIT>(err);
        msg = fmt::format("failed to commit fdb txn, err={}", err);
        return;
    }

    // ===== 步骤10: 构建Response =====
    response->mutable_rowset_meta()->CopyFrom(rs_meta);
    // tablet stats 可选返回（如果调用方需要）
}
```

### 3.5 sub_txn 场景

当请求中 `sub_txn_ids` 不为空时（事务load场景），一个tablet可能有多个sub txn各自写了一个tmp rowset。此时需要遍历所有sub_txn_ids：

```cpp
if (request->sub_txn_ids_size() > 0) {
    // 对每个sub_txn_id读取tmp rowset并转正
    for (int64_t sub_txn_id : request->sub_txn_ids()) {
        auto sub_tmp_key = meta_rowset_tmp_key({instance_id, sub_txn_id, tablet_id});
        // 读取、设version、写formal、删tmp...
        // 每个sub_txn的rowset可能version不同（同一partition内递增）
    }
}
```

注意：sub_txn场景需要从request传入每个sub_txn对应的version。当前proto设计中version是单个字段，如果需要支持sub_txn，考虑改为：
- 方案A: `repeated int64 sub_txn_ids` + `repeated int64 sub_txn_versions` 一一对应
- 方案B: 每个sub_txn单独调用一次convert_tmp_rowset

当前设计采用方案B（每个sub_txn独立调用），保持接口简洁。如果后续性能需要，可以扩展为批量接口。

---

## 4. 幂等性设计

### 4.1 核心原则

`convert_tmp_rowset` 必须是幂等的，因为：
1. 网络超时后调用方会重试
2. FE/BE可能在收到响应前崩溃，重启后会重新发起请求
3. publish阶段的断点续做机制依赖幂等性

### 4.2 幂等性检查逻辑

在写入formal rowset之前，先检查formal rowset key是否已存在：

```
情况1: formal rowset 不存在 + tmp rowset 存在
  → 正常转正流程

情况2: formal rowset 已存在 + tmp rowset 不存在
  → 上一次调用已完整成功（formal写入 + tmp删除 都完成了）
  → 直接返回 OK + 已有的formal rowset meta

情况3: formal rowset 已存在 + tmp rowset 存在（理论上不应该发生）
  → FDB事务的原子性保证了formal写入和tmp删除同时成功或失败
  → 但为防御性编程，比较rowset_id：
    - 相同: 返回 OK（认为是之前的成功结果）
    - 不同: 返回 ALREADY_EXISTED 错误（version冲突）

情况4: formal rowset 不存在 + tmp rowset 不存在
  → tmp可能已被recycler清理（事务已abort），或者txn_id/tablet_id参数错误
  → 返回错误 INVALID_ARGUMENT
```

### 4.3 并发重试安全性

FDB的乐观并发控制保证了即使多个相同请求并发到达，也只有一个能成功提交FDB事务。其他请求会收到 `TXN_CONFLICT` 错误，重试后会走到幂等性检查路径。

### 4.4 不需要检查txn状态

与现有 `commit_rowset` RPC不同，`convert_tmp_rowset` **不需要检查TxnInfoPB状态**，原因：
- `convert_tmp_rowset` 被调用时，事务已经处于 `TXN_STATUS_COMMITTED` 状态
- 调用方（FE publish线程）已经确认事务commit成功
- 即使事务被并发abort，tmp rowset会被recycler标记为recycled，步骤3中的 `is_recycled` 检查会拦截

---

## 5. 和现有 update_delete_bitmap RPC 的关系

### 5.1 调用顺序

对于每个tablet，publish阶段的调用顺序是严格的：

```
1. BE 计算 delete bitmap
2. BE 调用 MS update_delete_bitmap() 写入 delete bitmap KV
3. BE/FE 调用 MS convert_tmp_rowset() 转正 rowset
4. BE 本地 apply rowset 到 tablet 元数据
```

**顺序约束的理由**：
- delete bitmap 必须先于 rowset 转正写入MS，因为查询侧一旦看到formal rowset（通过version），就需要同时有对应的delete bitmap来保证正确的MVCC语义
- 在异步发布方案中，虽然查询通过visible version过滤（不会看到version > visible_version的rowset），但为了防御性设计和简化推理，仍然保持 "先delete bitmap，后formal rowset" 的顺序

### 5.2 两个RPC的独立性

- `update_delete_bitmap` 和 `convert_tmp_rowset` 是两个独立的RPC，在不同的FDB事务中执行
- 它们之间没有FDB层面的事务性保证
- 这是有意的设计：将大的原子操作拆分为多个小的独立操作

### 5.3 异常场景处理

| 场景 | 状态 | 处理 |
|------|------|------|
| update_delete_bitmap 成功，convert_tmp_rowset 成功 | 正常 | 继续本地apply |
| update_delete_bitmap 成功，convert_tmp_rowset 失败 | delete bitmap已写入，但rowset未转正 | 重试convert_tmp_rowset。delete bitmap的写入是幂等的，即使多写一些也不影响正确性（只会多标记一些row为deleted） |
| update_delete_bitmap 失败 | delete bitmap未写入 | 重试update_delete_bitmap。不调用convert_tmp_rowset |
| convert_tmp_rowset 成功，但BE崩溃未做本地apply | formal rowset已在MS，但BE本地tablet不知道 | BE重启后，FE重新下发任务。convert_tmp_rowset幂等返回成功，BE继续本地apply |

### 5.4 对recycler的影响

delete bitmap一旦写入MS但rowset未转正的中间状态下：
- delete bitmap不会被recycler清理（因为事务仍在COMMITTED状态，未VISIBLE也未ABORTED）
- 如果最终事务被abort，recycler会清理这些orphaned delete bitmap

---

## 6. 和最终轻量级Publish的关系

### 6.1 convert_tmp_rowset完成后的状态

当一个tablet的 `convert_tmp_rowset` 成功后：
- **MS侧**：该tablet的formal rowset KV已存在（`meta_rowset_key`），version字段已设置为commit version。tablet stats已更新。tmp rowset KV已删除
- **MS侧**：partition visible version **尚未更新**，仍为旧值
- **BE侧**（apply后）：该tablet的本地元数据已包含该rowset，max_version已提升

### 6.2 查询可见性

查询通过 `partition visible version` 来过滤rowset：
- 只有 `version <= visible_version` 的rowset对查询可见
- 由于visible version尚未更新，这些刚转正的formal rowset不会对查询可见
- 直到最终的轻量级publish更新visible version后，这些rowset才变为可见

因此，即使formal rowset已经在MS中，查询的正确性不受影响。

### 6.3 Recycler注意事项

Recycler在清理rowset时需要考虑这个中间状态：

**风险**：recycler扫描某tablet的formal rowsets时，可能发现某些rowset的version > current visible version。如果recycler将这些rowset视为"无效"并清理，会导致数据丢失。

**保护措施**：
1. **TxnInfoPB状态保护**：事务处于 `TXN_STATUS_COMMITTED` 状态，recycler不应清理COMMITTED状态事务的相关数据
2. **recycle_txn_key尚未创建**：在异步发布方案中，`recycle_txn_key` 是在轻量级publish阶段才创建的。在那之前，recycler的事务回收流程不会触及这些rowset
3. **formal rowset的version保护**：recycler清理过期rowset时，通常是根据compaction后不再需要的旧version来判断。新写入的version > visible_version的rowset不会被判定为过期
4. **建议**：recycler在处理formal rowset时，增加对 `version > visible_version` 的检查，跳过这些尚未publish的rowset

### 6.4 轻量级publish的前置条件

只有当事务涉及的**所有tablet**都完成了 convert_tmp_rowset 后，FE才会调用轻量级publish。轻量级publish的操作仅包括：
- 更新每个partition的 visible version = commit version
- 更新 TxnInfoPB: status = TXN_STATUS_VISIBLE
- 删除 txn_running_key
- 创建 recycle_txn_key / CommitTxnLogPB

---

## 7. 错误处理

### 7.1 FDB事务冲突（TXN_CONFLICT）

FDB使用乐观并发控制，如果两个事务读写了相同的key范围，后提交的事务会收到 `TXN_CONFLICT` 错误。

**可能的冲突场景**：
- 同一个tablet的 `convert_tmp_rowset` 被并发调用（例如FE重试）
- compaction任务更新了同一个tablet的stats key

**处理**：调用方重试。由于幂等性设计，重试是安全的。建议调用方使用指数退避重试策略。

### 7.2 tmp rowset不存在（TXN_KEY_NOT_FOUND）

**可能原因**：
1. 该tablet在此事务中没有写入数据（没有调用过commit_rowset）
2. 事务已经被abort，recycler已经清理了tmp rowset（标记为recycled或直接删除）
3. 之前的 `convert_tmp_rowset` 已经成功（tmp已删除，formal已写入）—— 此情况在幂等性检查中处理
4. 调用参数 `txn_id` 或 `tablet_id` 错误

**处理**：返回错误码 `INVALID_ARGUMENT`，附带详细的错误信息。调用方根据上下文判断是否需要重试。

### 7.3 tmp rowset已被标记为recycled

`RowsetMetaCloudPB` 中有 `is_recycled` 字段。如果recycler已经标记该rowset为recycled，说明事务已经被abort。

**处理**：返回 `TXN_ALREADY_ABORTED`。调用方应停止该事务的publish流程。

### 7.4 序列化/反序列化错误

**处理**：返回 `PROTOBUF_PARSE_ERR` 或 `PROTOBUF_SERIALIZE_ERR`。这通常表示数据损坏，不应重试。

### 7.5 FDB事务大小超限（TXN_BYTES_TOO_LARGE）

由于 `convert_tmp_rowset` 只处理单个tablet的单个rowset，FDB事务大小通常很小（远小于10MB限制），不太可能触发此错误。

**处理**：返回错误。如果确实发生，需要检查rowset meta是否异常大。

### 7.6 并发安全性总结

| 并发场景 | 安全性 | 机制 |
|---------|--------|------|
| 同一tablet的重复调用 | 安全 | 幂等性检查 + FDB乐观并发控制 |
| 不同tablet的并发调用 | 安全 | 操作不同的key，FDB事务不冲突 |
| convert与compaction并发 | 安全 | stats使用atomic_add（split模式）或不同的key |
| convert与recycler并发 | 安全 | is_recycled检查 + TxnInfoPB状态保护 |
| convert与查询并发 | 安全 | visible version未更新，查询看不到新rowset |

---

## 8. 关键文件和修改点

### 8.1 需要新增的代码

| 文件 | 说明 |
|------|------|
| `cloud/src/meta-service/meta_service_txn.cpp` | 新增 `MetaServiceImpl::convert_tmp_rowset()` 方法实现 |
| `cloud/src/meta-service/meta_service.h` | 声明 `convert_tmp_rowset()` 方法 |

### 8.2 需要修改的文件

| 文件 | 修改内容 |
|------|---------|
| `gensrc/proto/cloud.proto` | 新增 `ConvertTmpRowsetRequest`、`ConvertTmpRowsetResponse`，在 `MetaService` service 中注册 `convert_tmp_rowset` RPC（已在Module 0中定义） |
| `be/src/cloud/cloud_meta_mgr.h` | 新增 `convert_tmp_rowset()` 方法声明 |
| `be/src/cloud/cloud_meta_mgr.cpp` | 新增 `convert_tmp_rowset()` 方法实现：构建Request，调用MS RPC，处理Response |

### 8.3 涉及但不修改的文件（需理解）

| 文件 | 说明 |
|------|------|
| `cloud/src/meta-store/keys.h` | `MetaRowsetTmpKeyInfo`、`MetaRowsetKeyInfo`、`StatsTabletKeyInfo` 的定义 |
| `cloud/src/meta-store/keys.cpp` | `meta_rowset_tmp_key()`、`meta_rowset_key()`、`stats_tablet_key()` 等key编码函数 |
| `cloud/src/meta-service/meta_service_tablet_stats.h` | `TabletStats` 结构体、`update_tablet_stats()`、`merge_tablet_stats()`、`internal_get_load_tablet_stats_batch()` 等函数 |
| `cloud/src/meta-service/meta_service.cpp` | 现有 `commit_rowset()` RPC 实现（参考tmp rowset写入逻辑） |
| `cloud/src/meta-store/versioned_value.h` | `versioned::document_put()` 等versioned写入函数 |
| `cloud/src/recycler/recycler.cpp` | 后续需要审视recycler对 version > visible_version 的 formal rowset 的处理 |

### 8.4 参考的现有代码模式

| 模式 | 参考位置 | 说明 |
|------|---------|------|
| RPC方法注册 | `meta_service.h` 中的 `commit_rowset` 声明 | 同样的RPC声明方式 |
| RPC前处理宏 | `RPC_PREPROCESS(commit_rowset, get, put, del)` | 自动创建txn、处理鉴权 |
| 限流宏 | `RPC_RATE_LIMIT(commit_rowset)` | 复用相同的限流基础设施 |
| Stats更新 | `commit_txn_immediately()` 行1932-1958 | 完整的tablet stats更新逻辑 |
| Versioned写入 | `commit_txn_immediately()` 行1775-1790 | versioned rowset + versioned stats的写入方式 |

---

## 9. 单元测试要点

### 9.1 正常转正流程

| 测试用例 | 验证点 |
|---------|--------|
| `test_convert_basic` | 给定txn_id和tablet_id，tmp rowset正确读取，formal rowset正确写入（version已设置），tmp rowset被删除 |
| `test_convert_version_set` | 转正后的formal rowset meta中 start_version == end_version == request.version |
| `test_convert_visible_ts` | 转正后的formal rowset meta中 visible_ts_ms 为非零的合理时间戳 |
| `test_convert_stats_updated` | 转正后tablet stats中 num_rowsets/num_rows/data_size/num_segments 正确增加 |
| `test_convert_stats_split_mode` | 在 split_tablet_stats=true 时，各维度的split key使用atomic_add正确更新 |
| `test_convert_response_fields` | Response中 rowset_meta 包含完整的转正后rowset meta |

### 9.2 幂等性测试

| 测试用例 | 验证点 |
|---------|--------|
| `test_convert_idempotent_full` | 第二次调用时formal已存在+tmp已删除，返回OK + 正确的rowset_meta |
| `test_convert_idempotent_version_conflict` | formal rowset已存在但rowset_id不同，返回 ALREADY_EXISTED |
| `test_convert_concurrent` | 两个线程同时对同一tablet调用convert，一个成功一个冲突重试后成功 |

### 9.3 错误处理测试

| 测试用例 | 验证点 |
|---------|--------|
| `test_convert_tmp_not_found` | tmp rowset不存在（且formal也不存在），返回 INVALID_ARGUMENT |
| `test_convert_tmp_recycled` | tmp rowset的 is_recycled=true，返回 TXN_ALREADY_ABORTED |
| `test_convert_invalid_params` | txn_id=0 或 tablet_id=0 或 version=0，返回 INVALID_ARGUMENT |
| `test_convert_fdb_conflict` | 模拟FDB事务冲突，验证调用方重试后成功 |
| `test_convert_invalid_instance` | cloud_unique_id无法解析为instance_id，返回 INVALID_ARGUMENT |

### 9.4 Versioned写入测试

| 测试用例 | 验证点 |
|---------|--------|
| `test_convert_versioned_write` | 启用versioned_write时，versioned::meta_rowset_load_key正确写入 |
| `test_convert_versioned_stats` | 启用versioned_write时，versioned tablet stats正确写入（先读取已有值再merge） |
| `test_convert_non_versioned` | 未启用versioned_write时，不写入versioned key |

### 9.5 端到端集成测试

| 测试用例 | 验证点 |
|---------|--------|
| `test_convert_after_delete_bitmap` | 先调用update_delete_bitmap再调用convert_tmp_rowset，两者均成功 |
| `test_convert_then_lightweight_publish` | convert完成后调用轻量级publish，visible version正确更新，rowset对查询可见 |
| `test_convert_multi_tablets` | 同一事务的多个tablet分别独立调用convert，全部成功后调用轻量级publish |
| `test_convert_query_isolation` | convert完成但publish未完成时，查询不应看到新写入的数据 |
