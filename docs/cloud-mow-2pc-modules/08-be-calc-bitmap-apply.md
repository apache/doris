# Module 7: BE侧CalcDeleteBitmapTask改造 + 本地Apply

## 概述

本模块设计BE侧`CalcDeleteBitmapTask`的改造方案，使其在两阶段提交模式下，除了原有的delete bitmap计算和写入MS之外，还要额外完成：调用MS `convert_tmp_rowset` RPC转正该tablet的rowset meta，以及将rowset应用到BE本地tablet元数据（更新`max_version`），最后上报成功给FE。

本模块依赖：
- Module 0（Proto/KV Schema）：`TxnInfoPB`扩展、`ConvertTmpRowsetRequest/Response`定义
- Module 2（MS侧Per-Tablet Rowset转正）：`convert_tmp_rowset` RPC的MS端实现

---

## A. 当前CalcDeleteBitmapTask流程分析

### A.1 总体架构

当前的CalcDeleteBitmapTask处理涉及以下核心文件和类：

- `be/src/cloud/cloud_engine_calc_delete_bitmap_task.h/.cpp`：任务入口
- `be/src/cloud/cloud_txn_delete_bitmap_cache.h/.cpp`：事务delete bitmap缓存
- `be/src/cloud/cloud_tablet.h/.cpp`：tablet操作（sync_rowsets、save_delete_bitmap等）
- `be/src/cloud/cloud_meta_mgr.h/.cpp`：MS RPC封装
- `be/src/storage/tablet/base_tablet.cpp`：`update_delete_bitmap`核心计算逻辑
- `be/src/agent/task_worker_pool.cpp`：`calc_delete_bitmap_callback`任务回调

FE通过Agent Task机制下发`TCalcDeleteBitmapRequest`到BE，BE通过`TaskWorkerPool`派发到`calc_delete_bitmap_callback`处理。

### A.2 任务入口：CloudEngineCalcDeleteBitmapTask::execute()

位于 `be/src/cloud/cloud_engine_calc_delete_bitmap_task.cpp`（第67行）。

主要流程：
1. 从`TCalcDeleteBitmapRequest`中解析transaction_id和每个partition的tablet列表
2. 使用`calc_tablet_delete_bitmap_task_thread_pool`创建并发token
3. 遍历每个partition的每个tablet，创建`CloudTabletCalcDeleteBitmapTask`
4. 如果请求中携带compaction stats（`base_compaction_cnts`等），设置到task中
5. 如果请求中携带tablet_states，设置到task中
6. 通过thread pool token并发提交所有tablet task
7. `token->wait()`等待所有task完成
8. 汇总error_tablet_ids和succ_tablet_ids返回

关键点：**当前所有tablet并发执行，主线程阻塞等待全部完成后返回结果给FE**。

### A.3 单tablet处理：CloudTabletCalcDeleteBitmapTask::handle()

位于 `be/src/cloud/cloud_engine_calc_delete_bitmap_task.cpp`（第147行）。

完整流程如下：

**Step 1: 获取tablet对象**
```cpp
auto base_tablet = DORIS_TRY(_engine.get_tablet(_tablet_id));
auto tablet = std::dynamic_pointer_cast<CloudTablet>(base_tablet);
```

**Step 2: 获取rowset_update_lock**
```cpp
std::unique_lock wrlock(tablet->get_rowset_update_lock());
```
此锁（`_rowset_update_lock`，定义在`cloud_tablet.h`第467行）用于：
- 避免同一`(txn_id, tablet_id)`的多个CalcDeleteBitmapTask并发执行（PR #50417引入的问题）
- 当前仅在`CloudTabletCalcDeleteBitmapTask::handle()`中使用

**Step 3: 获取当前max_version并判断是否需要sync_rowsets**

`should_sync_rowsets()`（第167行）判断逻辑：
```
需要sync的条件（满足任一）：
1. _version != max_version + 1  （版本不连续，说明有其他事务的rowset尚未同步到本地）
2. _ms_base_compaction_cnt == -1  （FE未传递compaction stats）
3. 其他BE上有compaction完成（base/cumu compaction cnt或cumulative_point更大）
4. 其他BE上有SC完成（tablet_state变化）
```

如果需要sync，调用`tablet->sync_rowsets()`从MS同步最新的rowset列表到本地。sync_rowsets内部调用`_engine.meta_mgr().sync_tablet_rowsets_unlocked()`从MS拉取rowset meta并更新本地`_rs_version_map`和`_max_version`。

**Step 4: 版本连续性检查**
```cpp
max_version = tablet->max_version_unlocked();
if (_version != max_version + 1) {
    return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR, false>("version not continuous");
}
```
如果sync_rowsets之后版本仍不连续，返回`DELETE_BITMAP_LOCK_ERROR`。FE收到此错误后会重试。

**当前方案中的含义**：`_version`是FE分配的publish version，`max_version`是该tablet当前的visible version（由MS commit_txn更新后sync_rowsets获取）。只有当前一个事务的commit_txn完成（MS更新了partition visible version和rowset meta）并且BE通过sync_rowsets拉取到之后，`max_version`才会等于`_version - 1`。

**Step 5: 检查空rowset并调用_handle_rowset**

对于非sub_txn场景：
```cpp
if (_engine.txn_delete_bitmap_cache().is_empty_rowset(_transaction_id, _tablet_id)) {
    return Status::OK();  // 空rowset跳过
}
status = _handle_rowset(tablet, _version);
```

对于sub_txn场景（事务load），遍历每个sub_txn_id，依次调用`_handle_rowset`，并维护`invisible_rowsets`和`tablet_delete_bitmap`。

### A.4 实际计算：_handle_rowset()

位于 `be/src/cloud/cloud_engine_calc_delete_bitmap_task.cpp`（第296行）。

**Step 1: 从txn_delete_bitmap_cache获取事务信息**
```cpp
Status status = _engine.txn_delete_bitmap_cache().get_tablet_txn_info(
        transaction_id, _tablet_id, &rowset, &delete_bitmap, &rowset_ids,
        &txn_expiration, &partial_update_info, &publish_status, &previous_publish_info);
```

从缓存获取：
- `rowset`：导入阶段写入的rowset对象
- `delete_bitmap`：commit阶段预计算的delete bitmap
- `rowset_ids`：commit阶段记录的已有rowset id集合
- `partial_update_info`：部分列更新参数
- `publish_status`：上次publish状态（INIT/PREPARE/SUCCEED）
- `previous_publish_info`：上次publish的版本和compaction信息

**Cache miss处理**：如果LRU cache中delete bitmap被淘汰（`get_delete_bitmap`返回NOT_FOUND），但txn_map中仍有记录：
- `delete_bitmap`被重置为空的`DeleteBitmap`
- `publish_status`被重置为`PublishStatus::INIT`
- 这意味着后续需要**全量重算**delete bitmap（因为rowset_ids为空，所有现有rowset都被视为新增）

**Step 2: 设置rowset版本号**
```cpp
rowset->set_version(Version(version, version));
```

**Step 3: 检查是否可以跳过计算**

如果`publish_status == SUCCEED`且版本和compaction stats完全匹配上次计算结果，说明这是一次重试，且上次已经成功计算过。此时仍需要调用`save_delete_bitmap_to_ms`将delete bitmap写入MS（因为MS中的pending delete bitmap可能已被其他事务替换）。

**Step 4: 实际计算delete bitmap**

调用`CloudTablet::update_delete_bitmap(tablet, &txn_info, ...)`（定义在`base_tablet.cpp`第1394行），核心逻辑：
1. 加载rowset的segment数据
2. 获取当前tablet的所有rowset id（`get_all_rs_id_unlocked`）
3. 对比commit阶段记录的rowset_ids，找出新增和删除的rowset（`_rowset_ids_difference`）
4. 对新增rowset计算delete bitmap（处理键冲突）
5. 对于部分列更新，还需要生成transient rowset writer处理冲突
6. 最终调用`tablet->save_delete_bitmap()`保存结果

**Step 5: save_delete_bitmap()内部流程**

位于`cloud_tablet.cpp`第1076行：
1. 调用`txn_delete_bitmap_cache().update_tablet_txn_info(..., PublishStatus::PREPARE)`更新缓存状态
2. 如果是部分列更新且有新行，调用`meta_mgr().update_tmp_rowset()`更新tmp rowset meta
3. 调用`save_delete_bitmap_to_ms()`将delete bitmap写入MS（通过`meta_mgr().update_delete_bitmap()` RPC）
4. 调用`txn_delete_bitmap_cache().update_tablet_txn_info(..., PublishStatus::SUCCEED)`更新缓存状态为成功

### A.5 txn_delete_bitmap_cache的结构

位于`be/src/cloud/cloud_txn_delete_bitmap_cache.h/.cpp`。

**Key结构**：
```cpp
struct TxnKey {
    TTransactionId txn_id;
    int64_t tablet_id;
};
```

**Value结构**（分两层存储）：
1. `_txn_map: map<TxnKey, TxnVal>`：存储rowset、过期时间、partial_update_info、publish_status、publish_info
2. LRU Cache（继承自`LRUCachePolicy`）：以`"{txn_id}/{tablet_id}"`为key，存储`DeleteBitmapCacheValue`（delete_bitmap + rowset_ids）

**写入时机**：
- 导入commit阶段：`CloudRowsetBuilder::set_txn_related_delete_bitmap()`调用`set_tablet_txn_info()`写入cache
- 空rowset：`mark_empty_rowset()`只记录轻量级标记

**读取时机**：
- Publish阶段：`CloudTabletCalcDeleteBitmapTask::_handle_rowset()`调用`get_tablet_txn_info()`读取

**过期清理**：后台线程定期调用`remove_expired_tablet_txn_info()`清理过期条目

### A.6 任务回调：calc_delete_bitmap_callback()

位于`be/src/agent/task_worker_pool.cpp`（第2357行）：
1. 创建`CloudEngineCalcDeleteBitmapTask`并执行
2. 构建`TFinishTaskRequest`，包含成功/失败状态和error_tablet_ids
3. 调用`finish_task()`通过RPC回报给FE
4. 调用`remove_task_info()`清理本地任务记录

---

## B. 新的CalcDeleteBitmapTask流程

### B.1 总体变化

当前方案和新方案的对比：

| 维度 | 当前方案 | 新方案（两阶段提交） |
|------|---------|-------------------|
| 下发方式 | FE在表锁内同步下发 | FE后台线程（CloudPublishDaemon）异步下发 |
| BE计算完后 | 直接返回结果给FE | 还需调用MS RPC和本地apply，然后返回 |
| 版本检查依赖 | 等MS commit_txn完成，sync_rowsets获取visible version | 等同tablet前一事务本地apply完成，max_version提升 |
| delete bitmap写MS | 同当前 | 同当前 |
| rowset转正 | FE在commit_txn中一次性完成所有tablet | BE per-tablet调用MS convert_tmp_rowset |
| 本地apply | 由BE的sync_rowsets从MS拉取 | BE在MS转正成功后直接本地apply |
| cache miss | 导入参数从cache获取，miss则失败 | 导入参数可从MS的TxnInfoPB获取 |

### B.2 新的handle()流程（伪代码）

```cpp
Status CloudTabletCalcDeleteBitmapTask::handle() const {
    // === Step 1: 获取tablet对象（同当前） ===
    auto tablet = get_tablet(_tablet_id);

    // === Step 2: 获取rowset_update_lock（同当前） ===
    // 此锁同时互斥delete bitmap计算和compaction
    std::unique_lock wrlock(tablet->get_rowset_update_lock());

    // === Step 3: 版本检查 + sync_rowsets（同当前，但语义变化） ===
    // 新方案中：max_version由前一事务的本地apply更新，不依赖MS visible version
    int64_t max_version = tablet->max_version_unlocked();
    if (should_sync_rowsets()) {
        tablet->sync_rowsets();
        max_version = tablet->max_version_unlocked();
    }
    if (_version != max_version + 1) {
        return Status::Error<ErrorCode::DELETE_BITMAP_LOCK_ERROR>("version not continuous");
    }

    // === Step 4: 计算delete bitmap（同当前） ===
    // 从cache或TxnInfoPB获取导入参数
    // 计算并写入MS：update_delete_bitmap RPC
    if (_sub_txn_ids.empty()) {
        status = _handle_rowset(tablet, _version);
    } else {
        // sub_txn处理逻辑同当前
    }
    RETURN_IF_ERROR(status);

    // === Step 5: 【新增】调用MS convert_tmp_rowset ===
    ConvertTmpRowsetRequest req;
    req.set_txn_id(_transaction_id);
    req.set_tablet_id(_tablet_id);
    req.set_version(_version);
    // ... 设置db_id, table_id, partition_id等
    for (auto sub_txn_id : _sub_txn_ids) {
        req.add_sub_txn_ids(sub_txn_id);
    }
    ConvertTmpRowsetResponse resp;
    RETURN_IF_ERROR(_engine.meta_mgr().convert_tmp_rowset(req, &resp));

    // === Step 6: 【新增】MS转正成功后，本地apply rowset ===
    // 将rowset加入tablet本地元数据，更新max_version
    {
        std::unique_lock meta_wlock(tablet->get_header_lock());
        std::vector<RowsetSharedPtr> to_add = {rowset};
        tablet->add_rowsets(std::move(to_add), /*version_overlap=*/false, meta_wlock);
    }
    // 此时 tablet->max_version_unlocked() == _version

    return Status::OK();
}
```

### B.3 新增步骤详解

#### B.3.1 调用MS convert_tmp_rowset

此步骤在delete bitmap成功写入MS之后执行。调用`CloudMetaMgr::convert_tmp_rowset()` RPC（新增方法），请求MS将该tablet的tmp rowset转正为formal rowset。

MS端处理逻辑（参见Module 2）：
1. 读取tmp rowset meta（key: `meta_rowset_tmp_key(txn_id, tablet_id)`）
2. 写入formal rowset meta（key: `meta_rowset_key(tablet_id, version)`），设置版本号
3. 删除tmp rowset key
4. 更新tablet stats
5. 此操作幂等：如果formal rowset已存在且rowset_id匹配，直接返回成功

#### B.3.2 本地Apply Rowset

MS转正成功后，BE将rowset应用到tablet本地元数据。这个操作的核心是：
- 将rowset加入`_rs_version_map`
- 更新`_timestamped_version_tracker`
- 更新`_max_version`
- 更新`_tablet_meta`

具体实现复用CloudTablet已有的`add_rowsets()`方法（`cloud_tablet.cpp`第352行），该方法内部调用`_add_rowsets_directly()`（第1699行），其关键操作：
```cpp
_rs_version_map.emplace(rs->version(), rs);
_timestamped_version_tracker.add_version(rs->version());
_max_version = std::max(rs->end_version(), _max_version);
_tablet_meta->add_rowsets_unchecked(rowsets);
```

### B.4 对_handle_rowset()的影响

`_handle_rowset()`内部逻辑**基本不变**，仍然负责：
1. 从cache获取事务信息
2. 计算delete bitmap
3. 调用`CloudTablet::update_delete_bitmap()` / `save_delete_bitmap()`写入MS

唯一的差异在于cache miss时的处理方式（见E节）。

### B.5 两阶段提交标志传递

FE在`TCalcDeleteBitmapRequest`中需要新增一个标志位，标识当前请求是两阶段提交模式：

```thrift
struct TCalcDeleteBitmapRequest {
    // ... 现有字段 ...
    // 新增：是否为两阶段提交模式
    optional bool enable_two_phase_commit;
    // 新增：tablet的元信息（db_id, table_id, partition_id等），用于构造ConvertTmpRowsetRequest
    optional map<i64, TTabletMetaInfo> tablet_meta_infos;
}
```

`CloudTabletCalcDeleteBitmapTask`根据此标志决定是否执行新增的Step 5和Step 6。

---

## C. BE本地Apply机制

### C.1 转正成功后如何将rowset加入tablet元数据

BE调用MS `convert_tmp_rowset`成功后，需要将该rowset加入本地tablet的内存元数据。具体操作：

```cpp
// 1. 获取meta写锁
std::unique_lock meta_wlock(tablet->get_header_lock());

// 2. 设置rowset的版本号（此时rowset已在Step 4中设置过version）
// rowset->set_version(Version(_version, _version)); // 已在_handle_rowset中完成

// 3. 调用add_rowsets，version_overlap=false（因为版本连续性已保证）
std::vector<RowsetSharedPtr> to_add = {rowset};
tablet->add_rowsets(std::move(to_add), /*version_overlap=*/false, meta_wlock);
```

`add_rowsets()`方法（`cloud_tablet.cpp`第352行）当`version_overlap=false`时，直接调用`_add_rowsets_directly()`，此方法的行为：
- 将rowset加入`_rs_version_map`（版本到rowset的映射）
- 将版本加入`_timestamped_version_tracker`（版本追踪器）
- 更新`_max_version = max(rs->end_version(), _max_version)`
- 调用`_tablet_meta->add_rowsets_unchecked(rowsets)`更新tablet meta

对于sub_txn场景（事务load），同一tablet可能有多个sub txn产生多个rowset，需要按顺序逐一apply：

```cpp
for (size_t i = 0; i < sub_txn_rowsets.size(); ++i) {
    std::unique_lock meta_wlock(tablet->get_header_lock());
    std::vector<RowsetSharedPtr> to_add = {sub_txn_rowsets[i]};
    tablet->add_rowsets(std::move(to_add), /*version_overlap=*/false, meta_wlock);
}
```

### C.2 max_version如何更新

`_max_version`是`CloudTablet`的成员变量（`cloud_tablet.h`第458行），类型为`int64_t`。更新发生在`_add_rowsets_directly()`中：

```cpp
_max_version = std::max(rs->end_version(), _max_version);
```

在两阶段提交中，apply完成后：
- apply前：`_max_version == _version - 1`（前一事务已完成apply或初始状态）
- apply后：`_max_version == _version`

这个更新是在`_meta_lock`写锁保护下完成的（调用`add_rowsets`时需传入`std::unique_lock<std::shared_mutex>`），因此与其他读取`max_version`的操作是线程安全的。

注意：`max_version_unlocked()`方法（`cloud_tablet.h`第170行）直接返回`_max_version`，不需要持锁。但在两阶段提交的流程中，所有对`_max_version`的读取都在`rowset_update_lock`保护范围内，因此不会有并发问题。

### C.3 和查询的交互

**查询读取路径**：查询通过FE分配的visible version读取数据。在两阶段提交方案中：

- `visible version`由MS的`partition_version_key`控制，在最终轻量级publish（所有tablet都complete后）时才更新
- 查询在`CloudTablet::capture_rs_readers()`中使用的是FE传来的`spec_version`，而不是BE本地的`max_version`
- 查询在读取前调用`sync_rowsets()`时，`sync_rowsets`从MS获取的rowset列表只包含formal rowset（已转正的），且只到visible version为止

因此：
- **BE本地apply后，查询不会立即看到这些rowset**：因为partition visible version尚未更新，FE不会分配高于visible version的版本给查询
- **只有轻量级publish完成后**，FE才会获取到新的visible version，此后的查询才能看到这些rowset
- 如果查询到达BE时，BE已经本地apply了该rowset，但FE分配的spec_version低于该rowset的version，`capture_rs_readers`中的版本过滤会排除该rowset

这保证了**快照隔离**语义不被破坏。

### C.4 和compaction的交互

**compaction读取路径**：compaction需要读取tablet上的rowset列表来选择输入。在当前实现中：

- Compaction在选择输入rowset前会调用`sync_rowsets()`从MS同步
- Compaction使用`_rs_version_map`中的rowset
- Compaction计算delete bitmap时通过`get_delete_bitmap_update_lock` RPC获取MS级别的锁
- Compaction完成后通过MS RPC提交结果

在两阶段提交方案中的交互：

1. **MS转正完成后**，该rowset在MS中已是formal rowset，compaction通过sync_rowsets可以看到它
2. **BE本地apply完成后**，该rowset也在`_rs_version_map`中，compaction在选择输入时可以使用它
3. **`rowset_update_lock`互斥**：delete bitmap计算和compaction之间通过此锁互斥。具体来说：
   - `CalcDeleteBitmapTask::handle()`持有`rowset_update_lock`期间，compaction的delete bitmap计算被阻塞
   - 在新方案中，`rowset_update_lock`的持有范围**扩展到包含MS convert_tmp_rowset和本地apply**，确保在整个过程中compaction不会干扰
4. **MS转正成功后才更新BE本地元数据**的设计保证了：
   - 如果BE先本地apply然后MS转正，可能出现BE本地有rowset但MS没有的中间状态，此时compaction的sync_rowsets看不到该rowset，导致不一致
   - 先MS转正后本地apply，保证compaction通过sync_rowsets看到的rowset和BE本地看到的rowset一致

---

## D. Version检查和版本连续性

### D.1 `_version != max_version + 1`检查在新方案中的行为

当前方案和新方案中，此检查的语义有本质区别：

**当前方案**：
- `_version`是FE分配的publish version
- `max_version`来源于MS visible version（通过sync_rowsets获取）
- 检查通过的前提：前一个事务的commit_txn已完成（MS更新了visible version和formal rowset）
- 等待链：前一事务的FE commit_txn完成 -> MS更新visible version -> BE sync_rowsets获取新version -> 检查通过

**新方案（两阶段提交）**：
- `_version`是FE分配的commit version
- `max_version`由前一事务在同一BE上的本地apply更新
- 检查通过的前提：前一事务的同tablet已完成本地apply
- 等待链：前一事务的同tablet完成CalcDeleteBitmap + MS convert_tmp_rowset + 本地apply -> max_version提升 -> 检查通过

### D.2 版本连续性的保证机制

**FE侧**（Module 5 CloudPublishDaemon）：
- 维护`tabletAppliedVersions`数据结构，记录每个tablet最近完成apply的commit version
- 通过`isVersionReady()`检查：只有同tablet前序版本（commitVersion - 1）已完成APPLIED，才下发当前事务的CalcDeleteBitmapTask
- 这保证了FE不会过早下发任务

**BE侧**（本模块）：
- `should_sync_rowsets()`和`_version != max_version + 1`检查是最终的安全保障
- 即使FE的版本检查通过，如果前一事务的本地apply尚未完成（由于并发或时序问题），BE端的版本检查仍会返回`DELETE_BITMAP_LOCK_ERROR`
- FE收到此错误后等待下一轮调度重试

**关键时序**：
```
事务A (version=N):
  [CalcBitmap + WriteMS + ConvertRowset + LocalApply]
  完成LocalApply后: tablet.max_version = N
                                              |
                                              v
事务B (version=N+1):                   should_sync_rowsets() -> false (version连续)
  [CalcBitmap + WriteMS + ConvertRowset + LocalApply]
  完成LocalApply后: tablet.max_version = N+1
```

### D.3 和当前方案的对比

| 维度 | 当前方案 | 新方案 |
|------|---------|--------|
| 版本来源 | MS visible version (通过sync_rowsets) | BE本地max_version (通过前事务apply) |
| sync_rowsets的必要性 | 必须，否则看不到新version | 仅在compaction stats变化等场景需要 |
| 版本更新延迟 | 需要MS commit_txn完成 + sync_rowsets网络延迟 | 仅需前事务在本BE完成apply（本地操作） |
| 跨BE影响 | MS commit_txn是全局操作 | 只影响执行该tablet计算的BE |
| 吞吐优势 | 受MS commit_txn锁和网络延迟限制 | tablet级流水线，前事务apply完立即开始 |

### D.4 sync_rowsets在新方案中的角色

在新方案中，`sync_rowsets()`仍然是必要的，但其主要作用变化为：

1. **获取compaction产生的新rowset**：其他BE上的compaction可能产生了新的rowset（base/cumu compaction），本BE需要通过sync_rowsets拉取，以保证delete bitmap计算的正确性
2. **获取schema change的结果**：其他BE上的schema change可能改变了tablet状态
3. **不再是版本检查的必要条件**：前一事务的rowset通过本地apply已经在BE上可见，不需要sync_rowsets来获取

`should_sync_rowsets()`的判断逻辑在新方案中基本不变，但**版本不连续（`_version != max_version + 1`）触发sync_rowsets的场景更少**——只在以下情况发生：
- BE重启后，本地max_version落后较多
- 该tablet被切换到了新的BE（新BE上没有历史数据）

---

## E. Cache miss和全量重算

### E.1 Cache miss的场景

BE的`txn_delete_bitmap_cache`是一个内存中的LRU cache，在以下情况下会发生cache miss：

1. **BE重启**：内存中的cache全部丢失
2. **LRU淘汰**：高并发导入时，cache容量不足，旧条目被淘汰
3. **txn过期清理**：后台清理线程清理了过期的txn条目
4. **tablet被切换到其他BE**：原始BE上有cache，新BE上没有

### E.2 当前方案的cache miss处理

当前`get_tablet_txn_info()`的处理（`cloud_txn_delete_bitmap_cache.cpp`第57行）：

1. 如果`_txn_map`中找不到条目（txn_id + tablet_id），返回`NOT_FOUND`错误
2. 如果`_txn_map`中有条目但LRU cache中的delete bitmap被淘汰：
   - `delete_bitmap`重置为空的`DeleteBitmap`
   - `publish_status`重置为`PublishStatus::INIT`
   - 返回`OK`，后续`_handle_rowset`会全量重算

全量重算的原理：
- `rowset_ids`为空（cache miss），所以`_rowset_ids_difference()`会认为所有现有rowset都是新增的
- `update_delete_bitmap()`会对所有现有rowset重新计算delete bitmap
- 这保证了正确性，但性能开销较大

**当前方案的限制**：如果`_txn_map`也没有条目（BE重启），则返回`NOT_FOUND`错误，任务失败。这依赖FE重试——但如果所有BE都重启了，cache完全丢失，当前方案可能需要特殊处理。

### E.3 新方案的cache miss处理

在两阶段提交方案中，cache miss的处理得到了增强：

**关键改进：导入参数已持久化到MS的TxnInfoPB中**

在Commit阶段（Module 4），FE将导入参数（partial_update_info等）通过`TxnLoadInfoPB`写入`TxnInfoPB`，持久化到MS。因此：

```
当前方案:  导入参数只存在于BE的txn_delete_bitmap_cache内存中
新方案:    导入参数持久化在MS的TxnInfoPB.load_info中
```

**新的cache miss处理流程**：

```cpp
Status status = _engine.txn_delete_bitmap_cache().get_tablet_txn_info(
        transaction_id, _tablet_id, &rowset, &delete_bitmap, &rowset_ids,
        &txn_expiration, &partial_update_info, &publish_status, &previous_publish_info);

if (status.is<ErrorCode::NOT_FOUND>()) {
    // Cache完全miss（txn_map也没有），需要从MS获取导入参数
    // 1. 从MS获取TxnInfoPB
    TxnInfoPB txn_info;
    RETURN_IF_ERROR(_engine.meta_mgr().get_txn_info(_transaction_id, &txn_info));

    // 2. 从TxnInfoPB.load_info构造partial_update_info
    partial_update_info = build_partial_update_info(txn_info.load_info());

    // 3. 从MS获取tmp rowset meta，构造rowset对象
    RowsetMetaCloudPB rowset_meta_pb;
    RETURN_IF_ERROR(_engine.meta_mgr().get_tmp_rowset(_transaction_id, _tablet_id, &rowset_meta_pb));
    rowset = build_rowset_from_meta(rowset_meta_pb);

    // 4. 初始化空的delete_bitmap和rowset_ids（触发全量重算）
    delete_bitmap = std::make_shared<DeleteBitmap>(_tablet_id);
    rowset_ids.clear();  // 空，会触发全量重算
    publish_status = std::make_shared<PublishStatus>(PublishStatus::INIT);
}
```

### E.4 全量重算delete bitmap的逻辑

当cache miss（`rowset_ids`为空）时，`update_delete_bitmap()`的行为：

```
1. get_all_rs_id_unlocked() 获取当前tablet的所有rowset_id -> cur_rowset_ids
2. _rowset_ids_difference(cur_rowset_ids, /*txn_info.rowset_ids=*/empty)
   -> rowset_ids_to_add = cur_rowset_ids (全部都是新增)
   -> rowset_ids_to_del = empty
3. get_rowset_by_ids(&rowset_ids_to_add) 获取所有rowset
4. 对所有rowset计算delete bitmap (全量)
```

### E.5 性能影响和优化建议

**性能影响**：
- 全量重算的代价与tablet的rowset数量和数据量成正比
- 对于大表（数百个rowset），全量重算可能耗时数秒到数十秒
- 在正常运行场景下（无BE重启），cache命中率高，不会触发全量重算

**优化建议**：

1. **增大cache容量**：通过配置`delete_bitmap_agg_cache_capacity`增大LRU cache容量，降低淘汰概率
2. **持久化delete bitmap到本地文件cache**：将delete bitmap序列化到本地磁盘，BE重启后可恢复，避免全量重算
3. **增量重算优化**：即使cache miss，如果能从MS获取到上次计算的delete bitmap（已写入MS），只需要增量计算新增rowset的部分。具体做法：
   - 从MS读取该tablet的delete bitmap（通过`sync_tablet_delete_bitmap`）
   - 将读取到的delete bitmap作为基础，只对新增rowset计算增量
4. **预热cache**：在FE下发CalcDeleteBitmapTask之前，先发一个预热请求让BE加载导入参数到cache
5. **BE之间cache迁移**：当tablet被调度到新BE时，可以考虑从原BE传输cache数据（但复杂度较高，优先级低）

---

## F. 错误处理

### F.1 MS convert_tmp_rowset RPC失败

**失败原因**：
- MS不可达（网络问题）
- MS内部错误（KV事务冲突、存储故障等）
- tmp rowset不存在（可能已被其他流程清理）

**处理方式**：
```cpp
Status st = _engine.meta_mgr().convert_tmp_rowset(req, &resp);
if (!st.ok()) {
    LOG(WARNING) << "convert_tmp_rowset failed, txn_id=" << _transaction_id
                 << ", tablet_id=" << _tablet_id << ", status=" << st;
    return st;  // 返回失败，FE会重试整个tablet的publish流程
}
```

- BE将错误返回给FE（通过`TFinishTaskRequest`的task_status）
- FE的CloudPublishDaemon将tablet状态置为FAILED -> PENDING，下一轮调度时重试
- `convert_tmp_rowset`是幂等的（MS端实现），重试安全

**特殊错误码**：
- 如果MS返回"rowset already converted"（formal rowset已存在且rowset_id匹配），说明之前的调用实际上成功了（只是响应丢失），可以继续后续步骤
- 如果MS返回版本冲突（formal rowset已存在但rowset_id不匹配），说明有严重错误，需要上报并停止处理

### F.2 MS update_delete_bitmap RPC失败

与当前方案处理方式相同：
- 返回失败，FE重试
- 重试时如果`publish_status == SUCCEED`且版本匹配，跳过重新计算，直接重新写入MS

### F.3 sync_rowsets失败

与当前方案处理方式相同：
```cpp
auto sync_st = tablet->sync_rowsets();
if (!sync_st.ok()) {
    LOG(WARNING) << "failed to sync rowsets, tablet_id=" << _tablet_id;
    return sync_st;  // 返回失败，FE重试
}
```

### F.4 tablet不在当前BE上

存算分离架构下，delete bitmap计算不依赖特定BE。如果原始BE不可达：
- FE的CloudPublishDaemon检测到BE不存活后，选择其他存活BE下发任务（参见Module 5的`selectBeForCalcBitmap`）
- 新BE执行时：
  1. `_engine.get_tablet(_tablet_id)`会通过`CloudMetaMgr::get_tablet_meta()`从MS获取tablet meta
  2. `sync_rowsets()`从MS同步所有rowset到本地
  3. cache miss -> 从MS的TxnInfoPB获取导入参数 -> 全量重算delete bitmap
  4. 正常执行后续流程（MS convert_tmp_rowset + 本地apply）
- 性能代价：全量sync_rowsets + 全量重算delete bitmap，但保证了正确性和可用性

### F.5 本地apply失败

本地apply（add_rowsets）是纯内存操作，正常情况下不会失败。但如果发生内存不足或内部错误：
- 返回失败，FE重试
- 重试时：由于`convert_tmp_rowset`是幂等的，MS端已转正完成，BE只需重做本地apply
- 如果BE重启后重试：从MS sync_rowsets获取formal rowset（已转正），`_max_version`会正确更新

### F.6 部分tablet成功、部分失败

`CloudEngineCalcDeleteBitmapTask::execute()`并发处理所有tablet。在新方案中：
- 成功的tablet已完成MS转正和本地apply，结果已持久化到MS
- 失败的tablet通过`error_tablet_ids`上报给FE
- FE只重试失败的tablet（状态回退为PENDING），不影响已成功的tablet
- 这比当前方案更高效——当前方案中如果部分tablet失败，整个事务的commit_txn可能需要重试

### F.7 BE在convert_tmp_rowset成功后、本地apply之前崩溃

这是一个关键的故障场景：
- MS端rowset已转正（持久化完成）
- BE本地还没有apply（内存状态丢失）

恢复方式：
1. FE下一轮调度时，发现该tablet状态仍非APPLIED，重新下发CalcDeleteBitmapTask
2. BE重试时：
   - sync_rowsets()会从MS拉取已转正的formal rowset，`_max_version`更新为`_version`
   - 版本检查：`_version != max_version + 1`变为`_version == _version`，即`_version != _version + 1`？
   - 不对，此时BE的`_max_version`已经通过sync_rowsets更新为`_version`（因为formal rowset已存在），所以实际的`max_version == _version`
   - 这意味着`_version == max_version`而非`max_version + 1`，版本检查会失败
3. **正确处理**：需要在handle()开始时增加检查——如果sync_rowsets后`max_version >= _version`，说明该tablet的rowset已经转正并被同步到本地，可以直接返回成功，跳过所有后续步骤

```cpp
// 在sync_rowsets之后，版本检查之前增加
max_version = tablet->max_version_unlocked();
if (max_version >= _version) {
    // rowset已转正且已同步到本地（可能是之前的部分成功或BE重启恢复）
    LOG(INFO) << "tablet already has version " << _version
              << ", skip calc delete bitmap, tablet_id=" << _tablet_id;
    return Status::OK();
}
```

---

## G. 关键文件和修改点

### G.1 修改文件

| 文件路径 | 修改内容 |
|---------|---------|
| `be/src/cloud/cloud_engine_calc_delete_bitmap_task.h` | `CloudTabletCalcDeleteBitmapTask`新增成员：`_enable_two_phase_commit`标志、tablet元信息（db_id, table_id, partition_id）。新增方法：`_convert_tmp_rowset()`、`_apply_rowset_locally()` |
| `be/src/cloud/cloud_engine_calc_delete_bitmap_task.cpp` | `handle()`方法增加两阶段提交逻辑分支：在delete bitmap计算完成后调用MS convert_tmp_rowset + 本地apply。增加已转正rowset的检测逻辑（处理部分成功重试场景） |
| `be/src/cloud/cloud_meta_mgr.h` | 新增`convert_tmp_rowset()`方法声明、新增`get_txn_info()`方法声明（用于cache miss时获取TxnInfoPB） |
| `be/src/cloud/cloud_meta_mgr.cpp` | 实现`convert_tmp_rowset()` RPC调用、实现`get_txn_info()` RPC调用 |
| `be/src/cloud/cloud_txn_delete_bitmap_cache.h/.cpp` | 优化cache miss处理：当txn_map也miss时，新增从MS恢复导入参数的接口支持 |
| `be/src/cloud/cloud_tablet.h` | 可能需要新增public方法用于本地apply（如果现有`add_rowsets`接口不完全满足） |
| `be/src/agent/task_worker_pool.cpp` | `calc_delete_bitmap_callback()`可能需要适配新的请求字段 |

### G.2 新增文件

无需新增文件，所有改动均在现有文件上进行。

### G.3 涉及的Thrift定义文件

| 文件 | 修改内容 |
|------|---------|
| `gensrc/thrift/AgentService.thrift` 或相关文件 | `TCalcDeleteBitmapRequest`新增`enable_two_phase_commit`字段和`tablet_meta_infos`字段 |

### G.4 涉及的Proto定义（来自Module 0）

| Message | 用途 |
|---------|------|
| `ConvertTmpRowsetRequest` | 调用MS convert_tmp_rowset的请求 |
| `ConvertTmpRowsetResponse` | MS返回转正后的rowset meta和stats |
| `TxnInfoPB.load_info (TxnLoadInfoPB)` | cache miss时从MS获取导入参数 |

### G.5 核心代码变更示意

#### cloud_engine_calc_delete_bitmap_task.h 变更

```cpp
class CloudTabletCalcDeleteBitmapTask {
public:
    // 现有构造函数参数不变
    CloudTabletCalcDeleteBitmapTask(CloudStorageEngine& engine, int64_t tablet_id,
                                    int64_t transaction_id, int64_t version,
                                    const std::vector<int64_t>& sub_txn_ids);

    // 新增：设置两阶段提交模式
    void set_two_phase_commit(bool enable) { _enable_two_phase_commit = enable; }

    // 新增：设置tablet元信息（用于构造ConvertTmpRowsetRequest）
    void set_tablet_meta_info(int64_t db_id, int64_t table_id,
                               int64_t index_id, int64_t partition_id);

    // ... 现有方法不变 ...

private:
    // 新增：调用MS convert_tmp_rowset
    Status _convert_tmp_rowset(std::shared_ptr<CloudTablet> tablet,
                                RowsetSharedPtr rowset) const;

    // 新增：本地apply rowset
    Status _apply_rowset_locally(std::shared_ptr<CloudTablet> tablet,
                                  RowsetSharedPtr rowset) const;

    // 新增：从MS获取导入参数（cache miss时使用）
    Status _recover_txn_info_from_ms(int64_t transaction_id, int64_t tablet_id,
                                      RowsetSharedPtr* rowset,
                                      DeleteBitmapPtr* delete_bitmap,
                                      RowsetIdUnorderedSet* rowset_ids,
                                      std::shared_ptr<PartialUpdateInfo>* partial_update_info) const;

    // ... 现有成员不变 ...

    // 新增成员
    bool _enable_two_phase_commit {false};
    int64_t _db_id {-1};
    int64_t _table_id {-1};
    int64_t _index_id {-1};
    int64_t _partition_id {-1};
};
```

#### handle()方法核心变更

```cpp
Status CloudTabletCalcDeleteBitmapTask::handle() const {
    // ... Step 1-3 同当前（获取tablet、加锁、sync_rowsets、版本检查）...

    // 【新增】检查rowset是否已转正（处理重试场景）
    max_version = tablet->max_version_unlocked();
    if (max_version >= _version) {
        LOG(INFO) << "rowset already applied, tablet_id=" << _tablet_id
                  << ", max_version=" << max_version << ", request_version=" << _version;
        return Status::OK();
    }

    // ... Step 4 同当前（计算delete bitmap + 写MS）...

    // 【新增】两阶段提交的额外步骤
    if (_enable_two_phase_commit) {
        // Step 5: 调用MS convert_tmp_rowset
        RETURN_IF_ERROR(_convert_tmp_rowset(tablet, rowset));

        // Step 6: 本地apply rowset
        RETURN_IF_ERROR(_apply_rowset_locally(tablet, rowset));
    }

    return Status::OK();
}
```

---

## H. 单元测试要点

### H.1 基本功能测试

1. **正常两阶段提交流程**：
   - Mock一个完整的CalcDeleteBitmapTask请求（`enable_two_phase_commit=true`）
   - 验证delete bitmap计算正确
   - 验证MS `update_delete_bitmap` RPC被调用
   - 验证MS `convert_tmp_rowset` RPC被调用，请求参数正确（txn_id, tablet_id, version等）
   - 验证本地apply后`tablet->max_version_unlocked()`等于目标version
   - 验证rowset出现在`tablet->rowset_map()`中

2. **非两阶段提交不执行新步骤**：
   - 请求中`enable_two_phase_commit=false`或未设置
   - 验证不调用`convert_tmp_rowset`和本地apply
   - 验证行为与当前完全一致

3. **sub_txn场景**：
   - 同一tablet有多个sub_txn
   - 验证每个sub_txn的rowset都被正确apply
   - 验证最终`max_version`等于最大的version

4. **空rowset跳过**：
   - 验证空rowset（`is_empty_rowset`返回true）在两阶段提交模式下也被正确跳过
   - 但convert_tmp_rowset和本地apply也需要跳过（无rowset需要转正）

### H.2 版本连续性测试

5. **正常版本连续**：
   - 前一事务已apply（`max_version = N-1`），当前事务`_version = N`
   - 验证版本检查通过，正常执行

6. **版本不连续返回DELETE_BITMAP_LOCK_ERROR**：
   - `max_version = N-2`，`_version = N`
   - 验证返回`DELETE_BITMAP_LOCK_ERROR`

7. **已转正rowset的检测**：
   - sync_rowsets后`max_version >= _version`（之前部分成功的重试场景）
   - 验证直接返回OK，不执行任何计算

### H.3 Cache miss测试

8. **LRU cache miss但txn_map有记录**：
   - delete bitmap被LRU淘汰，但txn_map仍有rowset等信息
   - 验证全量重算delete bitmap
   - 验证最终结果正确（delete bitmap正确计算并写入MS）

9. **完全cache miss（txn_map也无记录）**：
   - 模拟BE重启场景，cache全部丢失
   - 验证从MS获取TxnInfoPB和tmp rowset meta
   - 验证导入参数正确构造
   - 验证全量重算delete bitmap
   - 验证后续convert_tmp_rowset和本地apply正常执行

10. **切换到新BE**：
    - 新BE上无任何cache
    - 验证能够从MS获取所有必要信息
    - 验证sync_rowsets正确拉取历史数据
    - 验证全量重算并成功完成

### H.4 错误处理测试

11. **convert_tmp_rowset RPC失败**：
    - Mock MS RPC返回错误
    - 验证任务返回失败
    - 验证delete bitmap已写入MS但rowset未转正
    - 重试时：验证幂等性——delete bitmap不重复写入（`publish_status == SUCCEED`时跳过计算）

12. **convert_tmp_rowset幂等性**：
    - 第一次调用成功但BE未收到响应
    - 第二次调用时MS返回"already converted"
    - 验证BE正确处理，继续本地apply

13. **本地apply后BE崩溃并重启**：
    - MS已转正，BE本地apply未完成
    - BE重启后sync_rowsets拉取到formal rowset
    - 验证`max_version >= _version`检测生效，直接返回成功

14. **sync_rowsets失败**：
    - Mock sync_rowsets返回错误
    - 验证任务返回失败，可重试

### H.5 并发测试

15. **rowset_update_lock互斥**：
    - 同时提交两个CalcDeleteBitmapTask到同一tablet
    - 验证第二个task等待第一个完成后才执行
    - 验证最终结果正确

16. **CalcDeleteBitmapTask和compaction并发**：
    - 在CalcDeleteBitmapTask持有`rowset_update_lock`期间，触发compaction
    - 验证compaction被正确阻塞或序列化
    - 验证两者完成后数据一致

17. **多tablet并发执行**：
    - 同一事务的多个tablet并发执行
    - 验证各tablet独立完成，互不影响
    - 验证error_tablet_ids和succ_tablet_ids正确汇总

### H.6 性能和回归测试

18. **性能基准**：
    - 测量两阶段提交模式下单tablet处理时间（包含MS RPC和本地apply）
    - 对比当前方案的处理时间
    - 新增的开销应主要是convert_tmp_rowset RPC（一次网络往返）

19. **非两阶段提交表无回归**：
    - 对未启用两阶段提交的表执行完整的CalcDeleteBitmapTask
    - 验证行为和性能与改动前完全一致

20. **大规模tablet测试**：
    - 单个事务涉及数百个tablet
    - 验证并发执行的稳定性和正确性
    - 验证线程池不会耗尽
