# Event-Driven Warmup 同步进度观测方案

## 一、背景

Event-driven warmup 是存算分离架构下的数据缓存预热功能。当源集群发生数据写入时，系统自动将新写入的数据预热到目标集群的本地 File Cache 中。

当前系统存在以下观测痛点：

1. **无法区分表**：现有 bvar 指标均为全局累加器，多个表的数据混在一起，无法知道某张表的预热进度
2. **无法区分 Job**：多个 event-driven Job 的统计数据无法区分
3. **累加器的永久损失问题**：event-driven warmup 是尽力而为（best-effort），如果过程中源端或目标端 BE 挂掉或重启，正在传输中的数据会丢失且不会重试。使用累加计数器（`bvar::Adder`），这些丢失会永久体现在指标中——`failed` 计数只增不减，`finished` 与 `submitted` 的差距永远存在，无法反映系统是否已恢复正常

**核心解决思路**：

- 在 BE 侧使用 `bvar::MultiDimension` + `bvar::Window` 完成 per-table 的窗口统计。窗口指标只展示最近一段时间内的活动，过去的错误自然过期消失
- 源集群 BE 统计 **requested**（发送了多少数据去预热），目标集群 BE 统计 **finished**（实际完成了多少）
- FE 从两侧收集，计算 `gap = requested - finished` 差值，反映同步积压。gap > 0 表示有数据尚未完成预热。窗口化后 gap 自然收敛，不会永久积累

---

## 二、现有相关指标

### 2.1 源集群 BE 指标（`cloud_warm_up_manager.cpp`）

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `file_cache_event_driven_warm_up_requested_segment_num` | Adder\<uint64_t\> | 请求预热的 segment 数量 |
| `file_cache_event_driven_warm_up_requested_segment_size` | Adder\<uint64_t\> | 请求预热的 segment 总大小 |
| `file_cache_event_driven_warm_up_requested_index_num` | Adder\<uint64_t\> | 请求预热的索引文件数量 |
| `file_cache_event_driven_warm_up_requested_index_size` | Adder\<uint64_t\> | 请求预热的索引文件总大小 |
| `file_cache_event_driven_warm_up_skipped_rowset_num` | Adder\<uint64_t\> | 被跳过的 rowset 数 |
| `file_cache_warm_up_rowset_last_call_unix_ts` | Status\<int64_t\> | 最近一次 warm_up_rowset 调用时间戳 |
| `file_cache_warm_up_rowset_wait_for_compaction_latency` | LatencyRecorder | 等待 compaction 完成的延迟 |

### 2.2 目标集群 BE 指标（`cloud_internal_service.cpp`）

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `file_cache_event_driven_warm_up_submitted_segment_num` | Adder\<uint64_t\> | 提交下载的 segment 数量 |
| `file_cache_event_driven_warm_up_submitted_segment_size` | Adder\<uint64_t\> | 提交下载的 segment 总大小 |
| `file_cache_event_driven_warm_up_finished_segment_num` | Adder\<uint64_t\> | 完成下载的 segment 数量 |
| `file_cache_event_driven_warm_up_finished_segment_size` | Adder\<uint64_t\> | 完成下载的 segment 总大小 |
| `file_cache_event_driven_warm_up_failed_segment_num` | Adder\<uint64_t\> | 下载失败的 segment 数量 |
| `file_cache_event_driven_warm_up_failed_segment_size` | Adder\<uint64_t\> | 下载失败的 segment 总大小 |
| `file_cache_event_driven_warm_up_submitted_index_num` | Adder\<uint64_t\> | 提交下载的索引文件数量 |
| `file_cache_event_driven_warm_up_submitted_index_size` | Adder\<uint64_t\> | 提交下载的索引文件总大小 |
| `file_cache_event_driven_warm_up_finished_index_num` | Adder\<uint64_t\> | 完成下载的索引文件数量 |
| `file_cache_event_driven_warm_up_finished_index_size` | Adder\<uint64_t\> | 完成下载的索引文件总大小 |
| `file_cache_event_driven_warm_up_failed_index_num` | Adder\<uint64_t\> | 下载失败的索引文件数量 |
| `file_cache_event_driven_warm_up_failed_index_size` | Adder\<uint64_t\> | 下载失败的索引文件总大小 |
| `file_cache_warm_up_rowset_last_handle_unix_ts` | Status\<int64_t\> | 最近一次处理 warmup 请求时间戳 |
| `file_cache_warm_up_rowset_last_finish_unix_ts` | Status\<int64_t\> | 最近一次完成 warmup 时间戳 |
| `file_cache_warm_up_rowset_latency` | LatencyRecorder | warmup 全流程延迟 |
| `file_cache_warm_up_rowset_request_to_handle_latency` | LatencyRecorder | 请求到处理的延迟 |
| `file_cache_warm_up_rowset_handle_to_finish_latency` | LatencyRecorder | 处理到完成的延迟 |

### 2.3 CloudTablet 触发指标（`cloud_tablet.cpp`）

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `file_cache_warm_up_rowset_triggered_by_event_driven_num` | Adder\<uint64_t\> | 被 event-driven 触发的 rowset 数量 |
| `file_cache_warm_up_rowset_complete_num` | Adder\<uint64_t\> | 完成的 warmup rowset 数量 |

### 2.4 FE 侧 Prometheus 指标（`CloudMetrics.java`）

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `file_cache_warm_up_job_exec_count` | LongCounterMetric | 按 cluster 维度的 Job 执行次数 |
| `file_cache_warm_up_job_requested_tablets` | LongCounterMetric | 按 cluster 维度的请求 tablet 数 |
| `file_cache_warm_up_job_finished_tablets` | LongCounterMetric | 按 cluster 维度的完成 tablet 数 |

### 2.5 现有指标的不足

| 问题 | 说明 |
|------|------|
| **全部为全局累加器** | 不区分表和 Job |
| **BE crash 后永久损失** | 累加器只增不减，历史错误永久可见 |
| **无法观测同步缺口** | 没有将源端 requested 与目标端 finished 对比 |

---

## 三、方案设计

### 3.1 整体架构

```
 ┌─────────────────────────────────────┐     ┌─────────────────────────────────────┐
 │           源集群 BE                  │     │           目标集群 BE                │
 │                                     │     │                                     │
 │  commit_rowset                      │     │  warm_up_rowset RPC handler          │
 │    → _warm_up_rowset(table_id)      │     │    → submit_download_task()          │
 │      → _do_warm_up_rowset ──── RPC ─────────→→ download_done callback           │
 │                                     │     │                                     │
 │  ┌──────────────────────────┐       │     │  ┌──────────────────────────┐       │
 │  │ requested_segment_num  (5m) │       │     │  │ finish_segment_num  (5m) │       │
 │  │ requested_segment_size (5m) │       │     │  │ finish_segment_size (5m) │       │
 │  │ requested_index_num    (5m) │       │     │  │ finish_index_num    (5m) │       │
 │  │ requested_index_size   (5m) │       │     │  │ finish_index_size   (5m) │       │
 │  │            ... (30m, 2h) │       │     │  │ fail_segment_num   (5m)  │       │
 │  │ last_trigger_ts         │       │     │  │            ... (30m, 2h) │       │
 │  │ last_trigger_ts         │       │     │  │            ... (30m, 2h) │       │
 │  └──────────┬───────────────┘       │     │  │ last_finish_ts           │       │
 │             │ HTTP API              │     │  └──────────┬───────────────┘       │
 └─────────────┼───────────────────────┘     └─────────────┼───────────────────────┘
               │  /api/warmup_event_                        │
               │  driven_stats                              │
               └──────────────┬────────────────────────────┘
                              │ CacheHotspotManager
                              │ .progressCollectDaemon
                              │ (MasterDaemon, 每 N 秒)
                              ▼
                 ┌──────────────────────────────┐
                 │            FE                │
                 │                              │
                 │  1. GET /api/warmup_event_   │
                 │     driven_stats from BE     │
                 │  2. 一次性采集所有 cluster   │
                 │     → clusterStats          │
                 │  3. 按 Job matchedTableIds   │
                 │     聚合 → jobWarmUpStatsMap  │
                 │  4. SHOW WARM UP JOB 展示    │
                 └──────────────────────────────┘
```

### 3.2 核心公式

```
同步缺口 = requested - finished  （同一窗口内，同一张表）

对于某个 Job 在窗口 W 内：
  JobSyncGap_seg_num(W)   = Σ(requested_segment_num[table, W]) - Σ(finish_segment_num[table, W])
  JobSyncGap_seg_size(W)  = Σ(requested_segment_size[table, W]) - Σ(finish_segment_size[table, W])
  JobSyncGap_idx_num(W)   = Σ(requested_index_num[table, W])    - Σ(finish_index_num[table, W])
  JobSyncGap_idx_size(W)  = Σ(requested_index_size[table, W])   - Σ(finish_index_size[table, W])

其中 Σ 遍历该 Job 的所有 matchedTableIds，聚合所有源 BE 和目标 BE。
```

> **gap > 0**：有 segment 尚未完成下载（在传输中、排队中、或失败丢失）
> **gap ≈ 0**：系统跟得上，同步正常
> **窗口化后**：crash 造成的 gap 在窗口过期后自动消失

### 3.3 为什么窗口指标能解决永久损失问题

```
BE crash 场景:
──────────────────────────────────────────────────────────
时刻              累加器 gap         窗口 5min gap
──────────────────────────────────────────────────────────
t=0:00            0                  0
t=0:03  crash → 50 个 segment 丢失
                  50 (永久存在)      50  ← 窗口内正确反映
t=0:05                               0   ← 窗口轮转，旧值过期
t=0:07  BE 重启
t=0:10                               0   ← 正常
t=0:15                               0   ← 正常
一周后            50 (仍然存在)      0   ← 窗口反映当前正常
```

---

## 四、BE 侧：MBvarWindowedAdder

### 4.1 核心问题

`bvar::Window` 需要绑定到一个已有的 `bvar::Adder` 指针才能构造，而 `bvar::MultiDimension` 内部为每个维度值动态创建 Adder 实例。需要封装一个 `MBvarWindowedAdder` 类，在维度值首次出现时同时创建 Adder 和 Window 实例。

### 4.2 MBvarWindowedAdder 定义

```cpp
// be/src/util/bvar_windowed_adder.h

#pragma once

#include <bvar/bvar.h>
#include <bvar/multi_dimension.h>
#include <bvar/window.h>
#include <bthread/mutex.h>

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

/**
 * @brief 多维度窗口化累加器。
 *
 * 为每个维度值组合（如 (job_id, table_id)）自动创建：
 *   - 一个 bvar::Adder（通过 MultiDimension 管理的累积计数器）
 *   - 多个 bvar::Window（不同窗口大小的滑动窗口视图）
 *
 * 窗口在维度值首次写入时延迟创建。
 *
 * @example
 *   MBvarWindowedAdder requested_seg_num(
 *       "warmup_ed_requested_segment_num",      // 指标名前缀
 *       {"job_id", "table_id"},               // 维度名
 *       {300, 1800, 7200}                     // 窗口大小（秒）
 *   );
 *
 *   requested_seg_num.put({"13419", "12345"}, 1);
 *   // 自动暴露:
 *   //   warmup_ed_requested_segment_num_total{job_id="13419",table_id="12345"}  (累计)
 *   //   warmup_ed_requested_segment_num_300s_13419,12345                        (5min 窗口)
 *   //   warmup_ed_requested_segment_num_1800s_13419,12345                       (30min 窗口)
 *   //   warmup_ed_requested_segment_num_7200s_13419,12345                       (2h 窗口)
 */
class MBvarWindowedAdder {
public:
    MBvarWindowedAdder(const std::string& name,
                       const std::initializer_list<std::string>& dim_names,
                       std::vector<int> window_seconds)
            : name_(name),
              window_seconds_(std::move(window_seconds)),
              md_total_(name + "_total", std::list<std::string>(dim_names)) {}

    void put(const std::initializer_list<std::string>& dim_values, int64_t value) {
        auto* adder = md_total_.get_stats(std::list<std::string>(dim_values));
        if (!adder) return;
        *adder << value;
        ensure_windows(dim_values, adder);
    }

    /** 获取指定维度和窗口索引的当前窗口值。window_idx 对应构造时的 window_seconds 下标。 */
    int64_t get_window_value(const std::initializer_list<std::string>& dim_values,
                             size_t window_idx) {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        auto it = dims_.find(make_key(dim_values));
        if (it == dims_.end() || window_idx >= it->second.windows.size()) {
            return 0;
        }
        return it->second.windows[window_idx]->get_value();
    }

    /** 获取所有已出现的维度值列表。 */
    std::vector<std::string> list_dimensions() const {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        std::vector<std::string> result;
        for (auto& [key, _] : dims_) {
            result.push_back(key);
        }
        return result;
    }

private:
    struct DimEntry {
        bvar::Adder<int64_t>* adder; // 由 MultiDimension 拥有
        std::vector<std::unique_ptr<bvar::Window<bvar::Adder<int64_t>>>> windows;
    };

    void ensure_windows(const std::initializer_list<std::string>& dim_values,
                        bvar::Adder<int64_t>* adder) {
        std::string key = make_key(dim_values);
        {
            std::lock_guard<bthread::Mutex> lock(mutex_);
            if (dims_.count(key)) return;
            DimEntry entry;
            entry.adder = adder;
            for (int ws : window_seconds_) {
                std::string wname = name_ + "_" + std::to_string(ws) + "s_" + key;
                entry.windows.emplace_back(
                        std::make_unique<bvar::Window<bvar::Adder<int64_t>>>(
                                wname, adder, ws));
            }
            dims_[key] = std::move(entry);
        }
    }

    static std::string make_key(const std::initializer_list<std::string>& dim_values) {
        std::string result;
        for (auto& v : dim_values) {
            if (!result.empty()) result += ",";
            result += v;
        }
        return result;
    }

    std::string name_;
    std::vector<int> window_seconds_;
    bvar::MultiDimension<bvar::Adder<int64_t>> md_total_;
    mutable bthread::Mutex mutex_;
    std::map<std::string, DimEntry> dims_;
};
```

### 4.3 源集群 BE — requested 指标定义

```cpp
// be/src/cloud/cloud_warm_up_manager.cpp

static constexpr int WINDOW_5M  = 300;
static constexpr int WINDOW_30M = 1800;
static constexpr int WINDOW_2H  = 7200;

// ---- requested 指标（per (job_id, table_id), 3 窗口）----

MBvarWindowedAdder g_warmup_ed_requested_segment_num(
    "warmup_ed_requested_segment_num", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_requested_segment_size(
    "warmup_ed_requested_segment_size", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_requested_index_num(
    "warmup_ed_requested_index_num", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_requested_index_size(
    "warmup_ed_requested_index_size", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

// ---- 瞬态指标 ----

bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_trigger_ts(
    "warmup_ed_last_trigger_ts", {"job_id", "table_id"});
```

### 4.4 目标集群 BE — finished / failed 指标定义

```cpp
// be/src/cloud/cloud_internal_service.cpp

// ---- finished 指标（per (job_id, table_id), 3 窗口）----

MBvarWindowedAdder g_warmup_ed_finish_segment_num(
    "warmup_ed_finish_segment_num", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_finish_segment_size(
    "warmup_ed_finish_segment_size", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_finish_index_num(
    "warmup_ed_finish_index_num", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_finish_index_size(
    "warmup_ed_finish_index_size", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

// ---- failed 指标（per (job_id, table_id), 3 窗口）----

MBvarWindowedAdder g_warmup_ed_fail_segment_num(
    "warmup_ed_fail_segment_num", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_fail_segment_size(
    "warmup_ed_fail_segment_size", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_fail_index_num(
    "warmup_ed_fail_index_num", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_fail_index_size(
    "warmup_ed_fail_index_size", {"job_id", "table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

// ---- 瞬态指标 ----

bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_finish_ts(
    "warmup_ed_last_finish_ts", {"job_id", "table_id"});
```

### 4.5 BE 获取 job_id 和 table_id

指标维度为 `(job_id, table_id)`。源 BE 和目标 BE 均需获取 job_id：

**源 BE**：`CloudWarmUpManager` 维护一个 `(table_id, dst_cluster) → job_id` 的内存映射。FE 在创建/更新 event-driven Job 时通过 BE 的 `update_warmup_job_mapping` RPC 推送映射关系。当 `_warm_up_rowset` 发送预热请求时，根据 table_id 和目标 replica 所属 cluster 查表得到 job_id。

**目标 BE**：job_id 通过 `PWarmUpRowsetRequest` 的新增字段 `job_id` 传递。源 BE 在构造 RPC 请求时填入。

**table_id**：目标 BE 从本地 CloudTablet 获取，无需通过 RPC 传递：

```cpp
// cloud_internal_service.cpp — warm_up_rowset handler
for (auto& rs_meta_pb : request->rowset_metas()) {
    int64_t tablet_id = rs_meta.tablet_id();
    auto tablet_res = _engine.tablet_mgr().get_tablet(tablet_id);
    if (!tablet_res.ok()) continue;
    int64_t table_id = tablet_res.value()->tablet_meta()->table_id();
    std::string tid = std::to_string(table_id);
    std::string jid = std::to_string(request->job_id());  // ★ 从 RPC 请求获取
    // ... 后续 per-(job, table) 统计 ...
}
```

### 4.6 埋点位置

#### 4.6.1 源集群 BE — requested（`_warm_up_rowset`）

```cpp
void CloudWarmUpManager::_warm_up_rowset(RowsetMeta& rs_meta, int64_t table_id,
                                         int64_t sync_wait_timeout_ms) {
    bool cache_hit = false;
    auto replicas = get_replica_info(rs_meta.tablet_id(), table_id, false, cache_hit);
    if (replicas.empty()) {
        g_file_cache_event_driven_warm_up_skipped_rowset_num << 1;
        return;
    }

    // ★ 查找 (table_id, dst_cluster) → job_id 映射
    //    对每组目标 replica，确定 job_id 后记录 per-(job, table) requested 统计
    std::string tid = std::to_string(table_id);
    for (auto& replica_group : group_replicas_by_cluster(replicas)) {
        std::string jid = lookup_job_id(table_id, replica_group.cluster_name);
        if (jid.empty()) jid = "0";  // 无匹配 job 时用 0

        g_warmup_ed_requested_segment_num.put({jid, tid}, rs_meta.num_segments());
        int64_t total_seg_size = 0;
        for (int64_t i = 0; i < rs_meta.num_segments(); i++) {
            total_seg_size += rs_meta.segment_file_size(cast_set<int>(i));
        }
        g_warmup_ed_requested_segment_size.put({jid, tid}, total_seg_size);

        int64_t idx_num = 0, idx_size = 0;
        auto schema_ptr = rs_meta.tablet_schema();
        if (schema_ptr && (schema_ptr->has_inverted_index() || schema_ptr->has_ann_index())) {
            for (int64_t i = 0; i < rs_meta.num_segments(); i++) {
                for (const auto& info :
                     rs_meta.inverted_index_file_info(cast_set<int>(i)).index_info()) {
                    idx_num++;
                    if (info.index_file_size() != -1) idx_size += info.index_file_size();
                }
            }
        }
        g_warmup_ed_requested_index_num.put({jid, tid}, idx_num);
        g_warmup_ed_requested_index_size.put({jid, tid}, idx_size);

        // 更新最近触发时间戳
        auto* ts = g_warmup_ed_last_trigger_ts.get_stats({jid, tid});
        if (ts) {
            ts->set_value(std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());
        }
    }

    Status st = _do_warm_up_rowset(rs_meta, replicas, sync_wait_timeout_ms,
                                   !cache_hit);
    // ...
}
```

#### 4.6.2 目标集群 BE — finished / failed（`cloud_internal_service.cpp`）

在 `warm_up_rowset` RPC handler 中，从 RPC 请求获取 `job_id`，从本地 CloudTablet 获取 `table_id`，在下载完成/失败回调中记录：

```cpp
void CloudInternalServiceImpl::warm_up_rowset(...) {
    // ★ 从 RPC 请求获取 job_id
    std::string jid = std::to_string(request->job_id());

    for (auto& rs_meta_pb : request->rowset_metas()) {
        int64_t tablet_id = rs_meta.tablet_id();
        auto tablet_res = _engine.tablet_mgr().get_tablet(tablet_id);
        if (!tablet_res.ok()) continue;

        // ★ 从本地 tablet 获取 table_id
        int64_t table_id = tablet_res.value()->tablet_meta()->table_id();
        std::string tid = std::to_string(table_id);

        // ... 现有的处理逻辑 ...

        for (int64_t segment_id = 0; segment_id < rs_meta.num_segments(); segment_id++) {
            auto segment_size = rs_meta.segment_file_size(segment_id);
            // ... 现有的 submit_download_task 调用 ...

            // ★ 修改回调：加入 per-(job, table) finished/failed 统计
            auto done_cb = [jid, tid, segment_size](Status st) {
                if (st.ok()) {
                    g_file_cache_event_driven_warm_up_finished_segment_num << 1;
                    g_file_cache_event_driven_warm_up_finished_segment_size << segment_size;
                    // ★ 新增 per-(job, table) 统计
                    if (!tid.empty() && tid != "0") {
                        g_warmup_ed_finish_segment_num.put({jid, tid}, 1);
                        g_warmup_ed_finish_segment_size.put({jid, tid}, segment_size);
                    }
                    // ★ 更新最近完成时间戳（在回调中更新，非提交时）
                    auto* ts = g_warmup_ed_last_finish_ts.get_stats({jid, tid});
                    if (ts) {
                        ts->set_value(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch()).count());
                    }
                } else {
                    g_file_cache_event_driven_warm_up_failed_segment_num << 1;
                    g_file_cache_event_driven_warm_up_failed_segment_size << segment_size;
                    // ★ 新增 per-(job, table) 统计
                    if (!tid.empty() && tid != "0") {
                        g_warmup_ed_fail_segment_num.put({jid, tid}, 1);
                        g_warmup_ed_fail_segment_size.put({jid, tid}, segment_size);
                    }
                }
            };
        }

        // 索引文件回调同理，加入 per-(job, table) finished/failed 统计
        // ...
    }
}
```

### 4.7 BE HTTP API 暴露

参照现有 `tablets_info_action.cpp` 模式，在 BE 新增 HTTP Action `/api/warmup_event_driven_stats`，将 per-table 窗口统计以 JSON 格式暴露。

**用户和 FE 均可通过此 API 直接查询 BE 上的 warmup 统计。**

#### 4.7.1 BE 侧实现

每个 BE 统一输出全量数据（requested + finish + fail），不区分自身是源还是目标角色。源 BE 的 finish/fail 字段为 0，目标 BE 的 requested 字段为 0，这是自然结果。维度为 `(job_id, table_id)`，JSON 按 job_id 分组输出。

```cpp
// be/src/http/action/warmup_stats_action.h
class WarmUpStatsAction : public HttpHandler {
public:
    void handle(HttpRequest* req) override;
};

// be/src/http/action/warmup_stats_action.cpp

// 辅助函数：填充一组 MBvarWindowedAdder 到 JSON 对象
static void fill_windowed(EasyJson& parent, const std::string& key,
                          MBvarWindowedAdder& num_adder, MBvarWindowedAdder& size_adder,
                          const std::string& dim_key) {
    EasyJson obj = parent.Set(key, EasyJson::kObject);
    EasyJson num = obj.Set("num", EasyJson::kObject);
    // dim_key 格式为 "job_id,table_id"，需要拆分回 {job_id, table_id}
    auto dims = split_dim_key(dim_key);
    num["5m"]  = num_adder.get_window_value(dims, 0);
    num["30m"] = num_adder.get_window_value(dims, 1);
    num["2h"]  = num_adder.get_window_value(dims, 2);
    EasyJson size = obj.Set("size", EasyJson::kObject);
    size["5m"]  = size_adder.get_window_value(dims, 0);
    size["30m"] = size_adder.get_window_value(dims, 1);
    size["2h"]  = size_adder.get_window_value(dims, 2);
}

void WarmUpStatsAction::handle(HttpRequest* req) {
    auto& engine = ExecEnv::GetInstance()->storage_engine().to_cloud();

    // 收集所有出现过的维度 key "job_id,table_id"
    std::set<std::string> all_keys;
    for (auto& k : g_warmup_ed_requested_segment_num.list_dimensions()) all_keys.insert(k);
    for (auto& k : g_warmup_ed_finish_segment_num.list_dimensions()) all_keys.insert(k);
    for (auto& k : g_warmup_ed_fail_segment_num.list_dimensions()) all_keys.insert(k);

    // 按 job_id 分组
    std::map<std::string, std::vector<std::string>> job_to_keys;
    for (auto& key : all_keys) {
        auto pos = key.find(',');
        std::string jid = (pos != std::string::npos) ? key.substr(0, pos) : "0";
        job_to_keys[jid].push_back(key);
    }

    EasyJson result;
    result["code"] = 0;
    EasyJson jobs = result.Set("data", EasyJson::kArray);

    for (auto& [jid, keys] : job_to_keys) {
        EasyJson job_entry = jobs.PushBack(EasyJson::kObject);
        job_entry["job_id"] = std::stoll(jid);
        EasyJson tables = job_entry.Set("tables", EasyJson::kArray);

        for (auto& dim_key : keys) {
            auto pos = dim_key.find(',');
            std::string tid = (pos != std::string::npos) ? dim_key.substr(pos + 1) : dim_key;

            EasyJson entry = tables.PushBack(EasyJson::kObject);
            entry["table_id"] = std::stoll(tid);

            // requested: { seg: { num: {5m,30m,2h}, size: {5m,30m,2h} }, idx: { ... } }
            EasyJson req_obj = entry.Set("requested", EasyJson::kObject);
            fill_windowed(req_obj, "seg",
                          g_warmup_ed_requested_segment_num, g_warmup_ed_requested_segment_size, dim_key);
            fill_windowed(req_obj, "idx",
                          g_warmup_ed_requested_index_num, g_warmup_ed_requested_index_size, dim_key);

            // finish
            EasyJson fin_obj = entry.Set("finish", EasyJson::kObject);
            fill_windowed(fin_obj, "seg",
                          g_warmup_ed_finish_segment_num, g_warmup_ed_finish_segment_size, dim_key);
            fill_windowed(fin_obj, "idx",
                          g_warmup_ed_finish_index_num, g_warmup_ed_finish_index_size, dim_key);

            // fail
            EasyJson fail_obj = entry.Set("fail", EasyJson::kObject);
            fill_windowed(fail_obj, "seg",
                          g_warmup_ed_fail_segment_num, g_warmup_ed_fail_segment_size, dim_key);
            fill_windowed(fail_obj, "idx",
                          g_warmup_ed_fail_index_num, g_warmup_ed_fail_index_size, dim_key);

            // 瞬态
            auto dims = split_dim_key(dim_key);
            auto* trigger_ts = g_warmup_ed_last_trigger_ts.get_stats(dims);
            entry["last_trigger_ts"] = trigger_ts ? trigger_ts->get_value() : 0;
            auto* finish_ts = g_warmup_ed_last_finish_ts.get_stats(dims);
            entry["last_finish_ts"] = finish_ts ? finish_ts->get_value() : 0;
        }
    }
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
    HttpChannel::send_reply(req, result.ToString());
}
```

注册（参照 `http_service.cpp` 中 `tablets_info_action` 的注册方式）：

```cpp
// be/src/service/http_service.cpp
WarmUpStatsAction* warmup_stats_action = _pool.add(new WarmUpStatsAction());
_ev_http_server->register_handler(HttpMethod::GET, "/api/warmup_event_driven_stats", warmup_stats_action);
```

#### 4.7.2 API 输出示例

```bash
$ curl http://be_host:http_port/api/warmup_event_driven_stats
```

```json
{
  "code": 0,
  "data": [
    {
      "job_id": 13419,
      "tables": [
        {
          "table_id": 12345,
          "requested": {
            "seg": {
              "num":  {"5m": 5234, "30m": 28456, "2h": 112000},
              "size": {"5m": 4500000000, "30m": 24500000000, "2h": 98000000000}
            },
            "idx": {
              "num":  {"5m": 1200, "30m": 6500, "2h": 25000},
              "size": {"5m": 500000000, "30m": 2700000000, "2h": 10000000000}
            }
          },
          "finish": {
            "seg": {
              "num":  {"5m": 5210, "30m": 28300, "2h": 111200},
              "size": {"5m": 4480000000, "30m": 24300000000, "2h": 97500000000}
            },
            "idx": {
              "num":  {"5m": 1190, "30m": 6400, "2h": 24800},
              "size": {"5m": 495000000, "30m": 2650000000, "2h": 9800000000}
            }
          },
          "fail": {
            "seg": {
              "num":  {"5m": 0, "30m": 2, "2h": 12},
              "size": {"5m": 0, "30m": 2500000, "2h": 15000000}
            },
            "idx": {
              "num":  {"5m": 0, "30m": 1, "2h": 5},
              "size": {"5m": 0, "30m": 500000, "2h": 3000000}
            }
          },
          "last_trigger_ts": 1714000000000,
          "last_finish_ts": 1714000003000
        },
        {
          "table_id": 67890,
          ...
        }
      ]
    },
    {
      "job_id": 13420,
      "tables": [...]
    }
  ]
}
```

> **每个 BE 输出全量数据，不区分自身角色。** 源 BE 的 `finish`/`fail` 字段自然为 0，目标 BE 的 `requested` 字段自然为 0。
> JSON 按 job_id 分组，每个 job 内按 table_id 列出。
> 层级结构 `job_id → table_id → {requested|finish|fail}.{seg|idx}.{num|size}.{5m|30m|2h}` 减少字段名长度，同时保持语义清晰。
> 用户可直接 curl 此 API 查看 BE 上的实时 warmup 统计。

---

## 五、FE 侧：CacheHotspotManager 中周期采集与聚合

在 `CacheHotspotManager` 中新增一个 `MasterDaemon`，参照现有 `jobDaemon` 和 `tableFilterRefreshDaemon` 的模式，周期性调用 BE 的 `/api/warmup_event_driven_stats` HTTP API，采集并聚合 per-Job 的同步进度。

### 5.1 整体流程

```
CacheHotspotManager (已有类)
  │
  ├─ jobDaemon              (已有) 管理 warmup job 生命周期
  ├─ tableFilterRefreshDaemon (已有) 刷新 ON TABLES 匹配
  │
  └─ progressCollectDaemon  (新增) 周期采集 warmup 进度
       │
       ├─ 每 N 秒执行一次（可配置）
       │
       ├─ 一次性收集所有 event-driven Job 涉及的 cluster 并集
       │   allClusters = union(所有 job 的 srcCluster, dstCluster)
       │
       ├─ 枚举所有 (cluster, BE) 对
       │   → 并发提交 HTTP 请求（CompletionService）
       │   → 尽可能保证各 BE 采集时间窗口一致
       │   → 等待所有请求完成
       │   → mergeStats() 写入 clusterStats[cluster][jobId][tableId]
       │
       └─ 所有数据采集完毕后，一次性按 job.matchedTableIds 聚合
           → 对每个 Job：从 srcCluster 取 requested，从 dstCluster 取 finished
           → 存入 per-job 的 JobWarmUpStats 内存
           → SHOW WARM UP JOB 时直接读取
```

### 5.2 采集逻辑

参照现有 `NodeAction.getBeConfigNames()` 中 FE 调用 BE HTTP API 的标准模式：

```java
// 已有模式：NodeAction.java:205
String url = "http://" + NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHttpPort())
        + "/api/show_config";
String result = HttpUtils.doGet(url, null);
```

在 `CacheHotspotManager` 中新增：

```java
// CacheHotspotManager.java

// 新增：per-job 聚合后的同步统计
private final ConcurrentHashMap<Long, JobWarmUpStats> jobWarmUpStatsMap = new ConcurrentHashMap<>();

// 新增：per-cluster per-(job, table) 原始采集数据（每轮采集重建）
// key: clusterName → (jobId → (tableId → TableWarmUpWindowedStats))
// 不区分 src/dst，同一个 cluster 只采集一次
private volatile Map<String, Map<Long, Map<Long, TableWarmUpWindowedStats>>> clusterStats = Map.of();

// 新增：持久化线程池（避免每轮采集创建/销毁线程）
private final ExecutorService warmupStatsExecutor =
        Executors.newFixedThreadPool(16, new ThreadFactoryBuilder()
                .setNameFormat("warmup-stats-collector-%d").setDaemon(true).build());

// 新增 daemon
private MasterDaemon progressCollectDaemon;
private boolean startProgressCollectDaemon = false;

/**
 * 初始化 progressCollectDaemon。
 * 参照 startJobDaemon() 的模式。
 */
public void startProgressCollectDaemon() {
    if (startProgressCollectDaemon) return;
    startProgressCollectDaemon = true;
    progressCollectDaemon = new MasterDaemon(
            "warmup-progress-collect-daemon",
            Config.warmup_progress_collect_interval_sec) { // 新增配置项，默认 30s
        @Override
        protected void runAfterCatalogReady() {
            collectAndAggregate();
        }
    };
    progressCollectDaemon.start();
}

/**
 * 周期性采集并聚合。
 *
 * 核心思路：
 * 1. 先一次性收集所有涉及的 cluster，枚举所有需要请求的 BE
 * 2. 并发地对所有 BE 发起 HTTP 请求，尽可能保证采集时间相近
 * 3. 等待所有请求完成后，一次性聚合计算
 * 不按 job 逐个遍历采集，避免同一个 cluster 被重复请求。
 */
private void collectAndAggregate() {
    // 1. 收集所有 event-driven Job 涉及的 cluster 并集
    Set<String> allClusters = new HashSet<>();
    for (var job : runnableCloudWarmUpJobs.values()) {
        if (job.isEventDriven()) {
            allClusters.add(job.getSrcClusterName());
            allClusters.add(job.getDstClusterName());
        }
    }
    if (allClusters.isEmpty()) return;

    // 2. 枚举所有需要请求的 (cluster, BE) 对
    List<Pair<String, Backend>> allTargets = new ArrayList<>();
    for (String cluster : allClusters) {
        for (Backend be : getBackendsByCluster(cluster)) {
            allTargets.add(Pair.of(cluster, be));
        }
    }
    if (allTargets.isEmpty()) return;

    // 3. 并发请求所有 BE，尽可能保证采集时间窗口一致
    //    使用 CompletionService + 持久化线程池
    CompletionService<Pair<String, String>> completionService =
            new ExecutorCompletionService<>(warmupStatsExecutor);

    for (var target : allTargets) {
        String cluster = target.first;
        Backend be = target.second;
        completionService.submit(() -> {
            String url = "http://"
                + NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHttpPort())
                + "/api/warmup_event_driven_stats";
            String json = HttpUtils.doGet(url, null, 5000);
            return Pair.of(cluster, json);
        });
    }

    // 4. 等待所有请求完成，收集结果
    // 按 cluster → jobId → tableId 组织
    Map<String, Map<Long, Map<Long, TableWarmUpWindowedStats>>> newClusterStats = new HashMap<>();
    for (int i = 0; i < allTargets.size(); i++) {
        try {
            var future = completionService.take();
            var result = future.get(10, TimeUnit.SECONDS);
            String cluster = result.first;
            String json = result.second;
            var jobTableMap = newClusterStats.computeIfAbsent(cluster, k -> new HashMap<>());
            mergeStatsFromJson(jobTableMap, json);
        } catch (Exception e) {
            LOG.warn("Failed to collect warmup stats: {}", e.getMessage());
        }
    }
    this.clusterStats = newClusterStats;

    // 5. 所有数据采集完毕后，一次性按 Job 聚合
    for (var job : runnableCloudWarmUpJobs.values()) {
        if (!job.isEventDriven()) continue;
        JobWarmUpStats stats = aggregateStatsForJob(job);
        jobWarmUpStatsMap.put(job.getJobId(), stats);
    }
}
```

### 5.3 解析与聚合

```java
/**
 * 解析 BE 返回的 JSON（按 job_id 分组），merge 到 jobTableMap 中。
 * JSON 结构：data[].{job_id, tables[].{table_id, requested, finish, fail, ...}}
 * 全量解析所有字段（requested + finish + fail），不区分 src/dst。
 */
private void mergeStatsFromJson(
        Map<Long, Map<Long, TableWarmUpWindowedStats>> jobTableMap, String json) {
    JsonObject root = GsonUtils.GSON.fromJson(json, JsonObject.class);
    JsonArray data = root.getAsJsonArray("data");
    for (JsonElement jobElem : data) {
        JsonObject jobObj = jobElem.getAsJsonObject();
        long jobId = jobObj.get("job_id").getAsLong();
        var tableMap = jobTableMap.computeIfAbsent(jobId, k -> new HashMap<>());

        JsonArray tables = jobObj.getAsJsonArray("tables");
        if (tables == null) continue;
        for (JsonElement tableElem : tables) {
            JsonObject obj = tableElem.getAsJsonObject();
            long tableId = obj.get("table_id").getAsLong();
            TableWarmUpWindowedStats stats = TableWarmUpWindowedStats.fromJson(obj);
            tableMap.compute(tableId, (tid, existing) -> {
                if (existing == null) return stats;
                existing.merge(stats);
                return existing;
            });
        }
    }
}

/**
 * 按 Job 聚合：遍历 matchedTableIds，从 srcCluster 取 requested，
 * 从 dstCluster 取 finished，累加为一个 Job 级别汇总。
 *
 * 使用 job_id 维度直接从 clusterStats 中取出该 job 对应的数据，
 * 不会混入其他 job 的同表数据。
 */
private JobWarmUpStats aggregateStatsForJob(CloudWarmUpJob job) {
    JobWarmUpStats result = new JobWarmUpStats();
    long jobId = job.getJobId();
    String srcCluster = job.getSrcClusterName();
    String dstCluster = job.getDstClusterName();

    var srcJobMap = clusterStats.getOrDefault(srcCluster, Map.of())
            .getOrDefault(jobId, Map.of());
    var dstJobMap = clusterStats.getOrDefault(dstCluster, Map.of())
            .getOrDefault(jobId, Map.of());

    for (Long tableId : job.getCurrentTableIds()) {
        TableWarmUpWindowedStats srcStat = srcJobMap.get(tableId);
        TableWarmUpWindowedStats dstStat = dstJobMap.get(tableId);
        if (srcStat != null) result.mergeRequested(srcStat);
        if (dstStat != null) result.mergeFinished(dstStat);
    }
    result.computeGap();
    return result;
}

/**
 * 供 SHOW WARM UP JOB 调用。
 */
public JobWarmUpStats getJobWarmUpStats(long jobId) {
    return jobWarmUpStatsMap.get(jobId);
}
```

### 5.4 数据模型

```java
public class TableWarmUpWindowedStats {
    public long tableId;

    // requested（源集群 BE 有值，目标集群 BE 为 0）
    public long requestedSegmentNum5m, requestedSegmentNum30m, requestedSegmentNum2h;
    public long requestedSegmentSize5m, requestedSegmentSize30m, requestedSegmentSize2h;
    public long requestedIndexNum5m, requestedIndexNum30m, requestedIndexNum2h;
    public long requestedIndexSize5m, requestedIndexSize30m, requestedIndexSize2h;
    public long lastTriggerTs;

    // finished（目标集群 BE 有值，源集群 BE 为 0）
    public long finishSegmentNum5m, finishSegmentNum30m, finishSegmentNum2h;
    public long finishSegmentSize5m, finishSegmentSize30m, finishSegmentSize2h;
    public long finishIndexNum5m, finishIndexNum30m, finishIndexNum2h;
    public long finishIndexSize5m, finishIndexSize30m, finishIndexSize2h;
    public long lastFinishTs;

    // failed（目标集群 BE 有值，源集群 BE 为 0）
    public long failSegmentNum5m, failSegmentNum30m, failSegmentNum2h;
    public long failSegmentSize5m, failSegmentSize30m, failSegmentSize2h;
    public long failIndexNum5m, failIndexNum30m, failIndexNum2h;
    public long failIndexSize5m, failIndexSize30m, failIndexSize2h;

    /**
     * 从 BE JSON 响应解析。全量解析所有字段，不区分 src/dst。
     * JSON 层级结构：{requested|finish|fail}.{seg|idx}.{num|size}.{5m|30m|2h}
     */
    public static TableWarmUpWindowedStats fromJson(JsonObject obj) {
        TableWarmUpWindowedStats s = new TableWarmUpWindowedStats();
        s.tableId = obj.get("table_id").getAsLong();

        // requested.seg.num / requested.seg.size / requested.idx.num / requested.idx.size
        JsonObject req = obj.getAsJsonObject("requested");
        if (req != null) {
            s.requestedSegmentNum5m  = getWindow(req, "seg", "num", "5m");
            s.requestedSegmentNum30m = getWindow(req, "seg", "num", "30m");
            s.requestedSegmentNum2h  = getWindow(req, "seg", "num", "2h");
            s.requestedSegmentSize5m  = getWindow(req, "seg", "size", "5m");
            s.requestedSegmentSize30m = getWindow(req, "seg", "size", "30m");
            s.requestedSegmentSize2h  = getWindow(req, "seg", "size", "2h");
            s.requestedIndexNum5m  = getWindow(req, "idx", "num", "5m");
            s.requestedIndexNum30m = getWindow(req, "idx", "num", "30m");
            s.requestedIndexNum2h  = getWindow(req, "idx", "num", "2h");
            s.requestedIndexSize5m  = getWindow(req, "idx", "size", "5m");
            s.requestedIndexSize30m = getWindow(req, "idx", "size", "30m");
            s.requestedIndexSize2h  = getWindow(req, "idx", "size", "2h");
        }
        // finish / fail 同理...

        s.lastTriggerTs = obj.has("last_trigger_ts") ? obj.get("last_trigger_ts").getAsLong() : 0;
        s.lastFinishTs  = obj.has("last_finish_ts")  ? obj.get("last_finish_ts").getAsLong()  : 0;
        return s;
    }

    private static long getWindow(JsonObject parent, String type, String metric, String window) {
        JsonObject typeObj = parent.getAsJsonObject(type);
        if (typeObj == null) return 0;
        JsonObject metricObj = typeObj.getAsJsonObject(metric);
        if (metricObj == null) return 0;
        return metricObj.has(window) ? metricObj.get(window).getAsLong() : 0;
    }

    /** 聚合另一个 BE 的统计（同 cluster 多 BE 累加；时间戳取 max） */
    public void merge(TableWarmUpWindowedStats other) {
        // 所有 num/size 字段 +=
        // 时间戳字段取 max
        this.lastTriggerTs = Math.max(this.lastTriggerTs, other.lastTriggerTs);
        this.lastFinishTs  = Math.max(this.lastFinishTs,  other.lastFinishTs);
    }
}
```

### 5.5 Job 级别差值计算

```java
public class JobWarmUpStats {
    // 聚合后的 requested（跨所有匹配表、跨所有源 BE）
    public long requestedSegmentNum5m, requestedSegmentNum30m, requestedSegmentNum2h;
    public long requestedSegmentSize5m, requestedSegmentSize30m, requestedSegmentSize2h;
    public long requestedIndexNum5m, requestedIndexNum30m, requestedIndexNum2h;
    public long requestedIndexSize5m, requestedIndexSize30m, requestedIndexSize2h;
    public long lastTriggerTs;

    // 聚合后的 finished/failed（跨所有匹配表、跨所有目标 BE）
    public long finishSegmentNum5m, finishSegmentNum30m, finishSegmentNum2h;
    public long finishSegmentSize5m, finishSegmentSize30m, finishSegmentSize2h;
    public long finishIndexNum5m, finishIndexNum30m, finishIndexNum2h;
    public long finishIndexSize5m, finishIndexSize30m, finishIndexSize2h;
    public long failSegmentNum5m, failSegmentNum30m, failSegmentNum2h;
    public long failSegmentSize5m, failSegmentSize30m, failSegmentSize2h;
    public long failIndexNum5m, failIndexNum30m, failIndexNum2h;
    public long failIndexSize5m, failIndexSize30m, failIndexSize2h;
    public long lastFinishTs;

    // gap = requested - finished（正值表示积压，负值通常不出现）
    public long gapSegmentNum5m, gapSegmentNum30m, gapSegmentNum2h;
    public long gapSegmentSize5m, gapSegmentSize30m, gapSegmentSize2h;
    public long gapIndexNum5m, gapIndexNum30m, gapIndexNum2h;
    public long gapIndexSize5m, gapIndexSize30m, gapIndexSize2h;

    /** 计算 gap = requested - finished */
    public void computeGap() {
        gapSegmentNum5m  = requestedSegmentNum5m  - finishSegmentNum5m;
        gapSegmentNum30m = requestedSegmentNum30m - finishSegmentNum30m;
        gapSegmentNum2h  = requestedSegmentNum2h  - finishSegmentNum2h;
        // seg_size, idx_num, idx_size 同理
    }

    /** 聚合时间戳取 max */
    public void mergeRequested(TableWarmUpWindowedStats tableStat) {
        // 累加 requested 字段
        this.lastTriggerTs = Math.max(this.lastTriggerTs, tableStat.lastTriggerTs);
    }

    public void mergeFinished(TableWarmUpWindowedStats tableStat) {
        // 累加 finished/failed 字段
        this.lastFinishTs = Math.max(this.lastFinishTs, tableStat.lastFinishTs);
    }

    /** 序列化为 SyncStats JSON，用于 SHOW WARM UP JOB 输出 */
    public String toJsonString() { /* ... */ }
}
```

> **gap 的含义**：`gap = requested - finished`。正值表示有 segment 尚未完成下载（传输中或积压）。窗口化后 gap 自然收敛，BE crash 造成的 gap 在窗口过期后消失。负值理论上不出现，但 FE 采集时序差异可能导致瞬时负值。

---

## 六、SQL 展示

`SHOW WARM UP JOB` 增加 **SyncStats** 列，展示该 Job 聚合后的窗口统计摘要（JSON 格式）。一个 Job 对应一行，不管匹配了多少张表。

```
SHOW WARM UP JOB;
```

**新增列 `SyncStats`**：

```
+-------+---------+---------+-----------+-----------------------------------------------------------+
| JobId | Status  | Type    | SyncMode  | SyncStats                                                 |
+-------+---------+---------+-----------+-----------------------------------------------------------+
| 13418 | RUNNING | CLUSTER | EVENT..   |                                                           |
| 13419 | RUNNING | CLUSTER | EVENT..   | {"seg_num":{"requested_5m":15234,"finish_5m":15180,          |
|       |         |         |           |  "gap_5m":54,"fail_5m":3,"requested_30m":89234,...},          |
|       |         |         |           |  "seg_size":{"requested_5m":"12.5GB","finish_5m":"12.4GB",   |
|       |         |         |           |  "gap_5m":"100MB","fail_5m":"2.5MB",...},                  |
|       |         |         |           |  "idx_num":{...},"idx_size":{...},                         |
|       |         |         |           |  "last_trigger_ts":"10:30:25","last_finish_ts":"10:30:28"} |
+-------+---------+---------+-----------+-----------------------------------------------------------+
```

> 集群级别 Job（Job 13418，无 ON TABLES）的 SyncStats 为空。
> 表级别 Job（Job 13419）的 SyncStats 是该 Job 匹配的所有表聚合后的结果。

**SyncStats JSON 结构说明**：

```json
{
  "seg_num": {
    "requested_5m":   15234,       // 最近 5min 源端提交 segment 个数
    "finish_5m":   15180,       // 最近 5min 目标端完成 segment 个数
    "gap_5m":      54,          // requested_5m - finish_5m（正值=积压）
    "fail_5m":     3,           // 最近 5min 目标端失败 segment 个数
    "requested_30m":  89234,       // 最近 30min ...
    "finish_30m":  88900,
    "gap_30m":     334,
    "fail_30m":    12,
    "requested_2h":   345678,      // 最近 2h ...
    "finish_2h":   344000,
    "gap_2h":      1678,
    "fail_2h":     45
  },
  "seg_size": {
    // 同结构，值为人类可读大小字符串 "12.5GB"
    "requested_5m":   "12.5 GB",
    "finish_5m":   "12.4 GB",
    "gap_5m":      "100 MB",
    "fail_5m":     "2.5 MB",
    ...
  },
  "idx_num": {
    // 同结构，索引文件个数
    ...
  },
  "idx_size": {
    // 同结构，索引文件大小
    ...
  },
  "last_trigger_ts":     "10:30:25",   // 源端最近触发时间
  "last_finish_ts":      "10:30:28"    // 目标端最近完成时间
}
```

> **gap 的含义**：`gap = requested - finished`。正值表示有 segment 尚未完成下载（传输中或积压）。窗口化后 gap 自然收敛，BE crash 造成的 gap 在窗口过期后消失。

---

## 七、数据流总结

```
源 BE (commit_rowset)
  │  → g_warmup_ed_requested_segment_num.put({job_id, table_id}, N)
  │  → g_warmup_ed_requested_segment_size.put({job_id, table_id}, bytes)
  │  → g_warmup_ed_requested_index_num.put({job_id, table_id}, N)
  │  → g_warmup_ed_requested_index_size.put({job_id, table_id}, bytes)
  │  ★ bvar::Window 自动维护 5min/30min/2h 窗口
  │  ★ /api/warmup_event_driven_stats HTTP API 暴露全量 JSON（按 job_id 分组）
  │
  │  _do_warm_up_rowset → PWarmUpRowsetRequest(含 job_id) → RPC
  │                                                    │
────────────────────────────────────────────────────┘    │
                                                        ▼
目标 BE (warm_up_rowset RPC handler)
  │  → g_warmup_ed_finish_segment_num.put({job_id, table_id}, 1)     [下载完成回调]
  │  → g_warmup_ed_finish_segment_size.put({job_id, table_id}, bytes)
  │  → g_warmup_ed_fail_segment_num.put({job_id, table_id}, 1)       [下载失败回调]
  │  → g_warmup_ed_fail_segment_size.put({job_id, table_id}, bytes)
  │  ★ 索引文件同理
  │  ★ bvar::Window 自动维护 5min/30min/2h 窗口
  │  ★ /api/warmup_event_driven_stats HTTP API 暴露全量 JSON（按 job_id 分组）
  │
  ▼
FE (CacheHotspotManager.progressCollectDaemon)
  │  每 N 秒执行（MasterDaemon）
  │  ★ 使用持久化线程池 warmupStatsExecutor（不再每轮创建/销毁）
  │
  │  一次性收集所有 event-driven Job 涉及的 cluster 并集
  │  allClusters = union(所有 job 的 srcCluster, dstCluster)
  │
  │  枚举所有 (cluster, BE) 对，并发提交 HTTP 请求
  │  （CompletionService, 保证采集时间窗口一致）
  │    GET http://be_host:http_port/api/warmup_event_driven_stats
  │    → 全量解析 JSON（按 job_id, table_id 二维聚合）
  │    → 等待所有请求完成
  │    → mergeStatsFromJson 累加同 cluster 所有 BE（时间戳取 max）
  │    → 写入 clusterStats[cluster][jobId][tableId]
  │
  │  所有数据采集完毕后，一次性按 job 聚合：
  │    clusterStats[srcCluster][jobId][tableId] → requested
  │    clusterStats[dstCluster][jobId][tableId] → finished
  │    → JobWarmUpStats（gap = requested - finished）
  │    → 存入 jobWarmUpStatsMap[jobId]
  │
  │  SHOW WARM UP JOB → getJobWarmUpStats(jobId) → SyncStats JSON 列
  │
  ▼
用户
  ├─ curl BE: /api/warmup_event_driven_stats   ← 直接查看 BE per-(job, table) 原始数据
  └─ SHOW WARM UP JOB                          ← 查看 per-job 聚合后的 SyncStats
```

---

## 八、典型观测场景

以下场景均基于 `SHOW WARM UP JOB` 的 SyncStats JSON 列解读。

### 场景 1：系统正常运行，gap ≈ 0

```json
"seg_num": {"requested_5m": 15234, "finish_5m": 15200, "gap_5m": 34, "fail_5m": 0}
```
→ gap 很小，系统跟得上

### 场景 2：BE crash 后恢复

```
t=0:00  gap_5m=34      ← 正常微小延迟
t=0:03  BE crash
t=0:05  窗口轮转，旧值过期
t=0:07  gap_5m=0        ← 窗口内无活动（BE 重启中）
t=0:11  BE 重启完成
t=0:15  gap_5m=20       ← 恢复正常
```
→ 窗口指标自动恢复，无永久残留

### 场景 3：目标端处理能力不足

```json
"seg_num": {"requested_5m": 50000, "finish_5m": 12000, "gap_5m": 38000, "fail_5m": 50}
```
→ gap 持续增长，目标端跟不上。可能需要扩容或限速

### 场景 4：多 Job 对比

```
Job 13419: gap_5m=34,    fail_5m=0    ← 正常
Job 13420: gap_5m=38000, fail_5m=50   ← 异常，目标端跟不上
```
→ 不同 Job 独立跟踪（job_id 维度），精准定位哪个 (src, dst) 对有问题

---

## 九、性能考量

| 组件 | 开销 | 说明 |
|------|------|------|
| 源 BE `put()` | O(1) + 首次 O(W) | W=3 个 Window 实例，延迟创建 |
| 目标 BE `put()` | O(1) + 首次 O(W) | 在下载完成/失败回调中 |
| BE HTTP API `/api/warmup_event_driven_stats` | O(J×T) | J 为活跃 Job 数，T 为活跃表数 |
| `bvar::Window` 运行时 | 每窗口一个定时器 | bvar 框架管理，开销可忽略 |
| FE HTTP 采集 | O(B × J × T) | B = 涉及的 BE 数，J = Job 数，T = 每台 BE 的活跃表数 |
| FE `aggregateStatsForJob()` | O(K) | K = Job 匹配的表数 |
| 用户 curl BE API | O(J×T) | 直接查 BE，不经过 FE |

> **bvar 指标数量**：维度为 `(job_id, table_id)`。源 BE 4 个 MBvarWindowedAdder × J 个 Job × N 张表 × (1 cumulative + 3 windows) = 16JN 个 bvar。目标 BE 8 个 × J × N × 4 = 32JN。假设 J=3 个 Job、N=100 张表，共约 14400 个 bvar 指标，在常规负载范围内。实际 J 一般较小（1~5），主要变量是 N。

---

## 十、兼容性

| 变更点 | 兼容性 | 说明 |
|--------|--------|------|
| BE 新增 HTTP API `/api/warmup_event_driven_stats` | ✅ | 新端点，不影响已有 API |
| `MBvarWindowedAdder` | ✅ | 纯内存，不涉及持久化 |
| `PWarmUpRowsetRequest` 新增 `job_id` 字段 | ✅ | protobuf 可选字段，旧版 BE 忽略；缺失时按 0 处理 |
| 源 BE 新增 `update_warmup_job_mapping` RPC | ✅ | 新增 RPC，旧版 BE 不接收此调用 |
| `SHOW WARM UP JOB` 新增 SyncStats 列 | ✅ | 非 event-driven Job 新增列为空，不引入新 SQL 语法 |
| FE `CacheHotspotManager` 新增 `progressCollectDaemon` | ✅ | 参照现有 daemon 模式，不影响已有功能 |
| **滚动升级建议** | — | 先升级所有 BE（源+目标），再升级所有 FE |
