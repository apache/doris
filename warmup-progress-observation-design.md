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
- FE 从两侧收集，计算 `finished - requested` 差值，反映同步进度缺口。窗口化后差值自然收敛，不会永久积累

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
 │             │ bvar 自动暴露         │     │  └──────────┬───────────────┘       │
 └─────────────┼───────────────────────┘     └─────────────┼───────────────────────┘
               │                                           │
               └──────────────┬────────────────────────────┘
                              │ FE 周期性主动采集
                              ▼
                 ┌──────────────────────────────┐
                 │            FE                │
                 │                              │
                 │  1. 周期性从 BE 采集         │
                 │     per-table 窗口值         │
                 │  2. 按 Job 的 matchedTableIds│
                 │     聚合 requested / finished   │
                 │  3. 计算 finished - requested   │
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
 * 为每个维度值（如 table_id）自动创建：
 *   - 一个 bvar::Adder（通过 MultiDimension 管理的累积计数器）
 *   - 多个 bvar::Window（不同窗口大小的滑动窗口视图）
 *
 * 窗口在维度值首次写入时延迟创建。
 *
 * @example
 *   MBvarWindowedAdder requested_seg_num(
 *       "warmup_ed_requested_segment_num",      // 指标名前缀
 *       {"table_id"},                         // 维度名
 *       {300, 1800, 7200}                     // 窗口大小（秒）
 *   );
 *
 *   requested_seg_num.put({"12345"}, 1);
 *   // 自动暴露:
 *   //   warmup_ed_requested_segment_num_total{table_id="12345"}  (累计)
 *   //   warmup_ed_requested_segment_num_300s_12345               (5min 窗口)
 *   //   warmup_ed_requested_segment_num_1800s_12345              (30min 窗口)
 *   //   warmup_ed_requested_segment_num_7200s_12345              (2h 窗口)
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

// ---- requested 指标（per table_id, 3 窗口）----

MBvarWindowedAdder g_warmup_ed_requested_segment_num(
    "warmup_ed_requested_segment_num", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_requested_segment_size(
    "warmup_ed_requested_segment_size", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_requested_index_num(
    "warmup_ed_requested_index_num", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_requested_index_size(
    "warmup_ed_requested_index_size", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

// ---- 瞬态指标 ----

bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_trigger_ts(
    "warmup_ed_last_trigger_ts", {"table_id"});
```

### 4.4 目标集群 BE — finished / failed 指标定义

```cpp
// be/src/cloud/cloud_internal_service.cpp

// ---- finished 指标（per table_id, 3 窗口）----

MBvarWindowedAdder g_warmup_ed_finish_segment_num(
    "warmup_ed_finish_segment_num", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_finish_segment_size(
    "warmup_ed_finish_segment_size", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_finish_index_num(
    "warmup_ed_finish_index_num", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_finish_index_size(
    "warmup_ed_finish_index_size", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

// ---- failed 指标（per table_id, 3 窗口）----

MBvarWindowedAdder g_warmup_ed_fail_segment_num(
    "warmup_ed_fail_segment_num", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_fail_segment_size(
    "warmup_ed_fail_segment_size", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_fail_index_num(
    "warmup_ed_fail_index_num", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

MBvarWindowedAdder g_warmup_ed_fail_index_size(
    "warmup_ed_fail_index_size", {"table_id"}, {WINDOW_5M, WINDOW_30M, WINDOW_2H});

// ---- 瞬态指标 ----

bvar::MultiDimension<bvar::Status<int64_t>> g_warmup_ed_last_finish_ts(
    "warmup_ed_last_finish_ts", {"table_id"});
```

### 4.5 目标 BE 获取 table_id

目标 BE 在 `warm_up_rowset` RPC handler 中可从本地 CloudTablet 直接获取 `table_id`，无需修改 protobuf：

```cpp
// cloud_internal_service.cpp — warm_up_rowset handler
for (auto& rs_meta_pb : request->rowset_metas()) {
    int64_t tablet_id = rs_meta.tablet_id();
    auto tablet_res = _engine.tablet_mgr().get_tablet(tablet_id);
    if (!tablet_res.ok()) continue;
    // ★ 直接从本地 tablet 获取 table_id
    int64_t table_id = tablet_res.value()->tablet_meta()->table_id();
    std::string tid = std::to_string(table_id);
    // ... 后续 per-table 统计 ...
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

    // ★ 记录 per-table requested 统计
    std::string tid = std::to_string(table_id);
    g_warmup_ed_requested_segment_num.put({tid}, rs_meta.num_segments());
    int64_t total_seg_size = 0;
    for (int64_t i = 0; i < rs_meta.num_segments(); i++) {
        total_seg_size += rs_meta.segment_file_size(cast_set<int>(i));
    }
    g_warmup_ed_requested_segment_size.put({tid}, total_seg_size);

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
    g_warmup_ed_requested_index_num.put({tid}, idx_num);
    g_warmup_ed_requested_index_size.put({tid}, idx_size);

    // 更新最近触发时间戳
    auto* ts = g_warmup_ed_last_trigger_ts.get_stats({tid});
    if (ts) {
        ts->set_value(std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count());
    }

    Status st = _do_warm_up_rowset(rs_meta, replicas, sync_wait_timeout_ms,
                                   !cache_hit);
    // ...
}
```

#### 4.6.2 目标集群 BE — finished / failed（`cloud_internal_service.cpp`）

在 `warm_up_rowset` RPC handler 中，从本地 CloudTablet 获取 `table_id`，在下载完成/失败回调中记录：

```cpp
void CloudInternalServiceImpl::warm_up_rowset(...) {
    for (auto& rs_meta_pb : request->rowset_metas()) {
        int64_t tablet_id = rs_meta.tablet_id();
        auto tablet_res = _engine.tablet_mgr().get_tablet(tablet_id);
        if (!tablet_res.ok()) continue;

        // ★ 从本地 tablet 获取 table_id
        int64_t table_id = tablet_res.value()->tablet_meta()->table_id();
        std::string tid = std::to_string(table_id);

    for (auto& rs_meta_pb : request->rowset_metas()) {
        // ... 现有的处理逻辑 ...

        for (int64_t segment_id = 0; segment_id < rs_meta.num_segments(); segment_id++) {
            auto segment_size = rs_meta.segment_file_size(segment_id);
            // ... 现有的 submit_download_task 调用 ...

            // ★ 修改回调：加入 per-table finished/failed 统计
            auto done_cb = [tid, segment_size](Status st) {
                if (st.ok()) {
                    g_file_cache_event_driven_warm_up_finished_segment_num << 1;
                    g_file_cache_event_driven_warm_up_finished_segment_size << segment_size;
                    // ★ 新增 per-table 统计
                    if (!tid.empty() && tid != "0") {
                        g_warmup_ed_finish_segment_num.put({tid}, 1);
                        g_warmup_ed_finish_segment_size.put({tid}, segment_size);
                    }
                } else {
                    g_file_cache_event_driven_warm_up_failed_segment_num << 1;
                    g_file_cache_event_driven_warm_up_failed_segment_size << segment_size;
                    // ★ 新增 per-table 统计
                    if (!tid.empty() && tid != "0") {
                        g_warmup_ed_fail_segment_num.put({tid}, 1);
                        g_warmup_ed_fail_segment_size.put({tid}, segment_size);
                    }
                }
            };
        }

        // 索引文件回调同理，加入 per-table finished/failed 统计
        // ...

        // ★ 更新最近完成时间戳
        if (!tid.empty() && tid != "0") {
            auto* ts = g_warmup_ed_last_finish_ts.get_stats({tid});
            if (ts) {
                ts->set_value(std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count());
            }
        }
    }
}
```

### 4.7 BE 侧指标暴露

BE 的 `MBvarWindowedAdder` 通过 bvar 框架自动暴露指标，FE 可通过 BE 的 HTTP `/vars` 端点或其他 bvar 采集通道读取窗口值。源 BE 和目标 BE 各自暴露自己的指标：

- **源 BE** 暴露 `warmup_ed_requested_*` 和 `warmup_ed_last_trigger_ts`
- **目标 BE** 暴露 `warmup_ed_finish_*`、`warmup_ed_fail_*` 和 `warmup_ed_last_finish_ts`

FE 根据每个 event-driven Job 的 `srcClusterName` 和 `dstClusterName`，知道应该从哪些 BE 读取 requested 指标、从哪些 BE 读取 finished 指标。

---

## 五、FE 侧：按 Job 聚合 + 差值计算

FE 周期性主动从 BE 采集 per-table 窗口统计，按 Job 聚合后计算 `finished - requested` 差值。

**不做的事情**：不通过 heartbeat 携带统计数据、不做窗口计算（BE 已完成）。

### 5.1 数据模型

```java
public class TableWarmUpWindowedStats {
    public long tableId;

    // requested（源集群）
    public long requestedSegmentNum5m, requestedSegmentNum30m, requestedSegmentNum2h;
    public long requestedSegmentSize5m, requestedSegmentSize30m, requestedSegmentSize2h;
    public long requestedIndexNum5m, requestedIndexNum30m, requestedIndexNum2h;
    public long requestedIndexSize5m, requestedIndexSize30m, requestedIndexSize2h;
    public long lastTriggerTs;

    // finished（目标集群）
    public long finishSegmentNum5m, finishSegmentNum30m, finishSegmentNum2h;
    public long finishSegmentSize5m, finishSegmentSize30m, finishSegmentSize2h;
    public long finishIndexNum5m, finishIndexNum30m, finishIndexNum2h;
    public long finishIndexSize5m, finishIndexSize30m, finishIndexSize2h;
    public long lastFinishTs;

    // failed（目标集群）
    public long failSegmentNum5m, failSegmentNum30m, failSegmentNum2h;
    public long failSegmentSize5m, failSegmentSize30m, failSegmentSize2h;
    public long failIndexNum5m, failIndexNum30m, failIndexNum2h;
    public long failIndexSize5m, failIndexSize30m, failIndexSize2h;

    /** 聚合另一张表的统计 */
    public void merge(TableWarmUpWindowedStats other) {
        requestedSegmentNum5m  += other.requestedSegmentNum5m;
        // ... 所有字段同理 ...
    }
}
```

### 5.2 收集与聚合

```java
public class WarmUpProgressCollector {

    // srcCluster → tableId → requested 统计（聚合该集群所有源 BE）
    private final ConcurrentHashMap<String, ConcurrentHashMap<Long, TableWarmUpWindowedStats>>
            srcClusterStats = new ConcurrentHashMap<>();

    // dstCluster → tableId → finished/failed 统计（聚合该集群所有目标 BE）
    private final ConcurrentHashMap<String, ConcurrentHashMap<Long, TableWarmUpWindowedStats>>
            dstClusterStats = new ConcurrentHashMap<>();

    /**
     * 从源集群 BE 采集到 per-table requested 窗口值后调用。
     * 聚合同一集群不同 BE 上同一张表的统计。
     */
    public void mergeSrcStats(String srcCluster, long beId,
                              Map<Long, TableWarmUpWindowedStats> beStats) {
        var tableMap = srcClusterStats.computeIfAbsent(srcCluster, k -> new ConcurrentHashMap<>());
        for (var entry : beStats.entrySet()) {
            tableMap.compute(entry.getKey(), (tid, existing) -> {
                if (existing == null) return entry.getValue();
                existing.merge(entry.getValue());
                return existing;
            });
        }
    }

    /**
     * 从目标集群 BE 采集到 per-table finished 窗口值后调用。
     */
    public void mergeDstStats(String dstCluster, long beId,
                              Map<Long, TableWarmUpWindowedStats> beStats) {
        var tableMap = dstClusterStats.computeIfAbsent(dstCluster, k -> new ConcurrentHashMap<>());
        for (var entry : beStats.entrySet()) {
            tableMap.compute(entry.getKey(), (tid, existing) -> {
                if (existing == null) return entry.getValue();
                existing.merge(entry.getValue());
                return existing;
            });
        }
    }

    /**
     * 按 Job 聚合：遍历该 Job 的所有 matchedTableIds，
     * 从 srcCluster 取 requested，从 dstCluster 取 finished，
     * 累加为一个 Job 级别的汇总结果。
     */
    public JobWarmUpStats getStatsForJob(CloudWarmUpJob job) {
        JobWarmUpStats result = new JobWarmUpStats();
        String srcCluster = job.getSrcClusterName();
        String dstCluster = job.getDstClusterName();

        var srcTableMap = srcClusterStats.getOrDefault(srcCluster, Map.of());
        var dstTableMap = dstClusterStats.getOrDefault(dstCluster, Map.of());

        for (Long tableId : job.getCurrentTableIds()) {
            TableWarmUpWindowedStats srcStat = srcTableMap.get(tableId);
            TableWarmUpWindowedStats dstStat = dstTableMap.get(tableId);

            if (srcStat != null) result.mergeSubmit(srcStat);
            if (dstStat != null) result.mergeFinished(dstStat);
        }
        result.computeGap();
        return result;
    }
}
```

> FE 内部按 table_id 遍历聚合，但对外只输出一个 Job 级别的汇总 JSON。不提供 per-table 的 SQL 查询接口。

### 5.3 差值计算（Job 级别）

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

    // gap = finished - requested
    public long gapSegmentNum5m, gapSegmentNum30m, gapSegmentNum2h;
    public long gapSegmentSize5m, gapSegmentSize30m, gapSegmentSize2h;
    public long gapIndexNum5m, gapIndexNum30m, gapIndexNum2h;
    public long gapIndexSize5m, gapIndexSize30m, gapIndexSize2h;

    /** 累加一张表的 requested 统计 */
    public void mergeSubmit(TableWarmUpWindowedStats tableStat) { /* 累加 requested 字段 */ }

    /** 累加一张表的 finished/failed 统计 */
    public void mergeFinished(TableWarmUpWindowedStats tableStat) { /* 累加 finished/failed 字段 */ }

    /** 计算 gap */
    public void computeGap() {
        gapSegmentNum5m  = finishSegmentNum5m  - requestedSegmentNum5m;
        gapSegmentNum30m = finishSegmentNum30m - requestedSegmentNum30m;
        gapSegmentNum2h  = finishSegmentNum2h  - requestedSegmentNum2h;
        // seg_size, idx_num, idx_size 同理
    }

    /** 序列化为 SyncStats JSON，用于 SHOW WARM UP JOB 输出 */
    public String toJsonString() { /* ... */ }
}
```

> **gap 可以为负**：FE 采集时序差异可能导致 finished 略大于 requested。用户关注的是 gap 的数量级和趋势，不是绝对值。

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
|       |         |         |           |  "gap_5m":-54,"fail_5m":3,"requested_30m":89234,...},         |
|       |         |         |           |  "seg_size":{"requested_5m":"12.5GB","finish_5m":"12.4GB",   |
|       |         |         |           |  "gap_5m":"-100MB","fail_5m":"2.5MB",...},                 |
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
    "gap_5m":      -54,         // finish_5m - requested_5m
    "fail_5m":     3,           // 最近 5min 目标端失败 segment 个数
    "requested_30m":  89234,       // 最近 30min ...
    "finish_30m":  88900,
    "gap_30m":     -334,
    "fail_30m":    12,
    "requested_2h":   345678,      // 最近 2h ...
    "finish_2h":   344000,
    "gap_2h":      -1678,
    "fail_2h":     45
  },
  "seg_size": {
    // 同结构，值为人类可读大小字符串 "12.5GB"
    "requested_5m":   "12.5 GB",
    "finish_5m":   "12.4 GB",
    "gap_5m":      "-100 MB",
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

> **gap 的含义**：`gap = finish - requested`。负值表示有 segment 尚未完成下载（在传输中或丢失）。窗口化后 gap 自然收敛，BE crash 造成的 gap 在窗口过期后消失。

---

## 七、数据流总结

```
源 BE (commit_rowset)
  │  → g_warmup_ed_requested_segment_num.put({table_id}, N)
  │  → g_warmup_ed_requested_segment_size.put({table_id}, bytes)
  │  → g_warmup_ed_requested_index_num.put({table_id}, N)
  │  → g_warmup_ed_requested_index_size.put({table_id}, bytes)
  │  ★ bvar::Window 自动维护 5min/30min/2h 窗口
  │  ★ 指标通过 bvar 自动暴露
  │
  │  _do_warm_up_rowset → PWarmUpRowsetRequest → RPC
  │                                                    │
────────────────────────────────────────────────────┘    │
                                                        ▼
目标 BE (warm_up_rowset RPC handler)
  │  → g_warmup_ed_finish_segment_num.put({table_id}, 1)     [下载完成回调]
  │  → g_warmup_ed_finish_segment_size.put({table_id}, bytes)
  │  → g_warmup_ed_fail_segment_num.put({table_id}, 1)       [下载失败回调]
  │  → g_warmup_ed_fail_segment_size.put({table_id}, bytes)
  │  ★ 索引文件同理
  │  ★ bvar::Window 自动维护 5min/30min/2h 窗口
  │  ★ 指标通过 bvar 自动暴露
  │
  ▼
FE (WarmUpProgressCollector)
  │  周期性主动从 BE 采集 per-table 窗口值
  │  mergeSrcStats(srcCluster, ...) → srcClusterStats 聚合所有源 BE 的 requested
  │  mergeDstStats(dstCluster, ...) → dstClusterStats 聚合所有目标 BE 的 finished
  │
  │  SHOW WARM UP JOB → getStatsForJob(job)
  │    → 遍历该 Job 的所有 matchedTableIds
  │    → 对每张表取 src requested + dst finished，累加到 Job 级别汇总
  │    → gap = Σ finished - Σ requested
  │    → 输出为 SyncStats JSON 列
  │
  ▼
用户 → 每个 Job 一行，看到聚合后的 requested / finished / gap / failed 窗口统计
```

---

## 八、典型观测场景

以下场景均基于 `SHOW WARM UP JOB` 的 SyncStats JSON 列解读。

### 场景 1：系统正常运行，gap ≈ 0

```json
"seg_num": {"requested_5m": 15234, "finish_5m": 15200, "gap_5m": -34, "fail_5m": 0}
```
→ gap 很小，系统跟得上

### 场景 2：BE crash 后恢复

```
t=0:00  gap_5m=-50     ← 正常微小延迟
t=0:03  BE crash
t=0:05  窗口轮转，旧值过期
t=0:07  gap_5m=0        ← 窗口内无活动（BE 重启中）
t=0:11  BE 重启完成
t=0:15  gap_5m=-20      ← 恢复正常
```
→ 窗口指标自动恢复，无永久残留

### 场景 3：目标端处理能力不足

```json
"seg_num": {"requested_5m": 50000, "finish_5m": 12000, "gap_5m": -38000, "fail_5m": 50}
```
→ gap 持续增长，目标端跟不上。可能需要扩容或限速

### 场景 4：多 Job 对比

```
Job 13419: gap_5m=-34,   fail_5m=0    ← 正常
Job 13420: gap_5m=-38000, fail_5m=50   ← 异常，目标端跟不上
```
→ 不同 Job 可以对比，定位哪个 (src, dst) 对有问题

---

## 九、性能考量

| 组件 | 开销 | 说明 |
|------|------|------|
| 源 BE `put()` | O(1) + 首次 O(W) | W=3 个 Window 实例，延迟创建 |
| 目标 BE `put()` | O(1) + 首次 O(W) | 在下载完成/失败回调中 |
| BE `collect_*_stats()` | O(T) | T 为活跃表数，按 FE 采集周期调用 |
| `bvar::Window` 运行时 | 每窗口一个定时器 | bvar 框架管理，开销可忽略 |
| Heartbeat 传输 | 每表约 300 bytes | 100 张表 ≈ 30 KB |
| FE `onHeartbeat()` | O(T) | merge 操作 |
| FE `getStatsForJob()` | O(K) | K = Job 匹配的表数 |

> **bvar 指标数量**：源 BE 4 个 MBvarWindowedAdder × N 张表 × (1 cumulative + 3 windows) = 16N 个 bvar。目标 BE 8 个 × N × 4 = 32N。100 张表共约 4800 个 bvar 指标，在常规负载范围内。

---

## 十、兼容性

| 变更点 | 兼容性 | 说明 |
|--------|--------|------|
| `MBvarWindowedAdder` | ✅ | 纯内存，不涉及持久化 |
| `SHOW WARM UP JOB` 新增 SyncStats 列 | ✅ | 非 event-driven Job 新增列为空，不引入新 SQL 语法 |
| FE 周期性采集 | ✅ | 新增采集逻辑，不影响已有功能 |
| **滚动升级建议** | — | 先升级所有 BE（源+目标），再升级所有 FE |
