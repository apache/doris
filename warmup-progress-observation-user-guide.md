# Event-Driven Warmup 同步进度观测指南

本文档面向使用 Event-Driven Warmup 功能的用户和测试人员，说明如何观测预热任务的同步进度。

---

## 一、观测方式

有两种方式可以观测预热同步进度：

| 方式 | 命令/接口 | 适用场景 |
|------|----------|---------|
| SQL | `SHOW WARM UP JOB` | 查看所有 Job 的聚合进度，推荐日常使用 |
| HTTP API | `GET /api/warmup_event_driven_stats` | 直接查看单个 BE 节点的原始指标，用于排查问题 |

---

## 二、SHOW WARM UP JOB — SyncStats 列

### 2.1 查看方式

```sql
SHOW WARM UP JOB;
```

输出的 **SyncStats** 列为 JSON 格式，展示该 Job 的同步进度。每个 Job 一行，无论匹配了多少张表。

> 非 event-driven 类型的 Job，SyncStats 列为空。

### 2.2 SyncStats JSON 结构

SyncStats JSON 包含 4 大类指标和 2 个时间戳：

```json
{
  "seg_num": { ... },
  "seg_size": { ... },
  "idx_num": { ... },
  "idx_size": { ... },
  "last_trigger_ts": "10:30:25",
  "last_finish_ts": "10:30:28"
}
```

### 2.3 指标分类说明

| 指标类别 | 含义 | 值类型 |
|---------|------|--------|
| `seg_num` | Segment 文件数量 | 整数 |
| `seg_size` | Segment 文件大小 | 人类可读字符串（如 `"12.5 GB"`） |
| `idx_num` | 索引文件数量 | 整数 |
| `idx_size` | 索引文件大小 | 人类可读字符串（如 `"500 MB"`） |

每个类别下包含以下窗口指标（以 `seg_num` 为例）：

| 字段 | 含义 |
|------|------|
| `requested_5m` | 最近 5 分钟内，源集群提交的预热请求数 |
| `finish_5m` | 最近 5 分钟内，目标集群完成的预热数 |
| `gap_5m` | `requested_5m - finish_5m`，同步缺口 |
| `fail_5m` | 最近 5 分钟内，目标集群预热失败数 |
| `requested_30m` | 最近 30 分钟内，源集群提交的预热请求数 |
| `finish_30m` | 最近 30 分钟内，目标集群完成的预热数 |
| `gap_30m` | `requested_30m - finish_30m`，同步缺口 |
| `fail_30m` | 最近 30 分钟内，目标集群预热失败数 |
| `requested_1h` | 最近 1 小时内，源集群提交的预热请求数 |
| `finish_1h` | 最近 1 小时内，目标集群完成的预热数 |
| `gap_1h` | `requested_1h - finish_1h`，同步缺口 |
| `fail_1h` | 最近 1 小时内，目标集群预热失败数 |

### 2.4 时间戳字段

| 字段 | 含义 |
|------|------|
| `last_trigger_ts` | 源集群最近一次触发预热的时间 |
| `last_finish_ts` | 目标集群最近一次完成预热的时间 |

### 2.5 完整 SyncStats 示例

```json
{
  "seg_num": {
    "requested_5m": 15234,
    "finish_5m": 15200,
    "gap_5m": 34,
    "fail_5m": 0,
    "requested_30m": 89234,
    "finish_30m": 88900,
    "gap_30m": 334,
    "fail_30m": 12,
    "requested_1h": 345678,
    "finish_1h": 344000,
    "gap_1h": 1678,
    "fail_1h": 45
  },
  "seg_size": {
    "requested_5m": "12.5 GB",
    "finish_5m": "12.4 GB",
    "gap_5m": "100 MB",
    "fail_5m": "2.5 MB",
    "requested_30m": "72 GB",
    "finish_30m": "71.5 GB",
    "gap_30m": "500 MB",
    "fail_30m": "15 MB",
    "requested_1h": "280 GB",
    "finish_1h": "279 GB",
    "gap_1h": "1 GB",
    "fail_1h": "50 MB"
  },
  "idx_num": {
    "requested_5m": 1200,
    "finish_5m": 1190,
    "gap_5m": 10,
    "fail_5m": 0,
    "requested_30m": 6500,
    "finish_30m": 6400,
    "gap_30m": 100,
    "fail_30m": 1,
    "requested_1h": 25000,
    "finish_1h": 24800,
    "gap_1h": 200,
    "fail_1h": 5
  },
  "idx_size": {
    "requested_5m": "500 MB",
    "finish_5m": "495 MB",
    "gap_5m": "5 MB",
    "fail_5m": "0",
    "requested_30m": "2.7 GB",
    "finish_30m": "2.65 GB",
    "gap_30m": "50 MB",
    "fail_30m": "500 KB",
    "requested_1h": "10 GB",
    "finish_1h": "9.8 GB",
    "gap_1h": "200 MB",
    "fail_1h": "3 MB"
  },
  "last_trigger_ts": "10:30:25",
  "last_finish_ts": "10:30:28"
}
```

---

## 三、BE HTTP API — /api/warmup_event_driven_stats

### 3.1 请求方式

```bash
curl http://<be_host>:<be_http_port>/api/warmup_event_driven_stats
```

> 如果集群开启了 HTTP 认证（`enable_all_http_auth = true`），需要在请求头中带上 Auth-Token。

### 3.2 响应格式

API 按 `job_id` 列出每个 Job 在该 BE 上的原始窗口指标：

```json
{
  "code": 0,
  "data": [
    {
      "job_id": 13419,
      "requested": {
        "seg": {
          "num":  {"5m": 5234, "30m": 28456, "1h": 112000},
          "size": {"5m": 4500000000, "30m": 24500000000, "1h": 98000000000}
        },
        "idx": {
          "num":  {"5m": 1200, "30m": 6500, "1h": 25000},
          "size": {"5m": 500000000, "30m": 2700000000, "1h": 10000000000}
        }
      },
      "finish": {
        "seg": {
          "num":  {"5m": 5210, "30m": 28300, "1h": 111200},
          "size": {"5m": 4480000000, "30m": 24300000000, "1h": 97500000000}
        },
        "idx": {
          "num":  {"5m": 1190, "30m": 6400, "1h": 24800},
          "size": {"5m": 495000000, "30m": 2650000000, "1h": 9800000000}
        }
      },
      "fail": {
        "seg": {
          "num":  {"5m": 0, "30m": 2, "1h": 12},
          "size": {"5m": 0, "30m": 2500000, "1h": 15000000}
        },
        "idx": {
          "num":  {"5m": 0, "30m": 1, "1h": 5},
          "size": {"5m": 0, "30m": 500000, "1h": 3000000}
        }
      },
      "last_trigger_ts": 1714000000000,
      "last_finish_ts": 1714000003000
    }
  ]
}
```

### 3.3 字段说明

**顶层字段：**

| 字段 | 含义 |
|------|------|
| `code` | 状态码，0 表示成功 |
| `data` | Job 列表 |

**每个 Job 的字段：**

| 字段 | 含义 |
|------|------|
| `job_id` | 预热任务 ID |
| `requested` | 源集群提交的预热请求统计（目标集群 BE 上此字段为 0） |
| `finish` | 目标集群完成的预热统计（源集群 BE 上此字段为 0） |
| `fail` | 目标集群失败的预热统计（源集群 BE 上此字段为 0） |
| `last_trigger_ts` | 最近一次触发预热的 Unix 毫秒时间戳 |
| `last_finish_ts` | 最近一次完成预热的 Unix 毫秒时间戳 |

**`requested` / `finish` / `fail` 内部结构：**

```
{category}.{type}.{metric}.{window}
```

| 层级 | 可选值 | 说明 |
|------|--------|------|
| category | `requested`, `finish`, `fail` | 指标大类 |
| type | `seg`, `idx` | segment 文件 或 索引文件 |
| metric | `num`, `size` | 数量 或 字节大小 |
| window | `5m`, `30m`, `1h` | 时间窗口 |

> **注意**：HTTP API 返回的是单个 BE 的原始值，size 为字节数（整数）。`SHOW WARM UP JOB` 的 SyncStats 是所有 BE 聚合后的结果，size 为人类可读格式。

### 3.4 源集群 BE 与目标集群 BE 的区别

每个 BE 输出全量字段（requested + finish + fail），但：

| BE 角色 | requested 字段 | finish/fail 字段 |
|---------|---------------|-----------------|
| 源集群 BE | **有值**（负责发送预热请求） | 自然为 0 |
| 目标集群 BE | 自然为 0 | **有值**（负责接收并下载数据） |

---

## 四、关键指标解读

### 4.1 gap（同步缺口）

```
gap = requested - finished
```

| gap 值 | 含义 | 建议操作 |
|--------|------|---------|
| `gap ≈ 0` | 系统同步正常，目标端跟得上源端的写入速度 | 无需操作 |
| `gap > 0` 且持续增长 | 目标端处理能力不足，积压越来越大 | 检查目标集群负载或考虑扩容 |
| `gap > 0` 但稳定 | 存在固定延迟但不恶化 | 正常范围内的传输延迟 |
| `gap < 0`（罕见） | 采集时序差异导致的瞬时现象 | 通常可忽略 |

### 4.2 fail（预热失败）

| fail 值 | 含义 | 建议操作 |
|---------|------|---------|
| `fail = 0` | 无预热失败 | 正常 |
| `fail > 0` 但很小 | 少量失败，可能由网络抖动导致 | 关注趋势，通常会自动恢复 |
| `fail > 0` 且持续增长 | 系统持续出现预热失败 | 检查 BE 日志和网络连通性 |

### 4.3 窗口选择

| 窗口 | 适用场景 |
|------|---------|
| `5m` | 观测实时状态，判断当前是否正常 |
| `30m` | 观测近期趋势 |
| `1h` | 观测整体状况 |

### 4.4 时间戳

| 字段 | 用途 |
|------|------|
| `last_trigger_ts` | 确认源集群是否在持续触发预热。如果长时间不更新，说明源端无新写入或预热任务可能异常 |
| `last_finish_ts` | 确认目标集群是否在持续完成预热。如果 `last_trigger_ts` 持续更新但 `last_finish_ts` 不更新，说明目标端可能有问题 |

---

## 五、常见场景判断

### 场景 1：系统运行正常

```
seg_num:  requested_5m=15234  finish_5m=15200  gap_5m=34   fail_5m=0
```

gap 很小且 fail 为 0，表示目标集群跟得上源集群的写入速度。

### 场景 2：BE 重启后恢复

```
时间点            gap_5m    说明
────────────────────────────────
正常运行            34      正常延迟
BE crash                    进行中的数据丢失
5 分钟后             0      窗口轮转，旧数据过期
BE 重启完成后        20      恢复正常
```

窗口化指标的特性：历史异常在窗口过期后自动消失，无需人工清除。

### 场景 3：目标端处理能力不足

```
seg_num:  requested_5m=50000  finish_5m=12000  gap_5m=38000  fail_5m=50
```

gap 持续很大且 fail 不断增长 → 目标端跟不上，需检查目标集群负载或考虑扩容。

### 场景 4：多 Job 对比定位问题

```
Job 13419:  gap_5m=34,    fail_5m=0    → 正常
Job 13420:  gap_5m=38000, fail_5m=50   → 异常
```

每个 Job 独立跟踪，可精准定位是哪个源→目标集群对有问题。

### 场景 5：预热任务没有在工作

```
last_trigger_ts 长时间不更新，requested_5m = 0
```

源集群未触发预热 → 检查是否有新数据写入，预热 Job 是否为 RUNNING 状态。

### 场景 6：源端正常但目标端不响应

```
last_trigger_ts 持续更新，requested_5m > 0
last_finish_ts 不更新，finish_5m = 0, fail_5m = 0
```

数据已发送但目标端无响应 → 检查目标集群 BE 是否正常运行、网络连通性。

---

## 六、测试验证指南

以下内容面向测试人员，说明如何验证观测指标的正确性。

### 6.1 验证 SyncStats JSON 结构

执行 `SHOW WARM UP JOB` 后，检查 SyncStats 列（第 16 列，index 15）：

1. Event-driven Job 的 SyncStats 非空
2. 非 event-driven Job（如 CLUSTER 类型的 ONCE 任务）的 SyncStats 为空
3. JSON 包含所有顶层 key：`seg_num`, `seg_size`, `idx_num`, `idx_size`, `last_trigger_ts`, `last_finish_ts`
4. 每个指标类别下包含 12 个字段：`requested_5m/30m/1h`, `finish_5m/30m/1h`, `gap_5m/30m/1h`, `fail_5m/30m/1h`

### 6.2 验证指标数值正确性

基本流程：

1. 创建 event-driven 预热任务
2. 记录初始 bvar 累积指标基线
3. 执行 N 次 INSERT（每次产生 1 个 segment）
4. 等待 bvar 累积指标确认预热完成：源集群 `submitted` == 目标集群 `finished`
5. 执行 `SHOW WARM UP JOB` 检查 SyncStats

验证项：

| # | 检查内容 | 预期值 |
|---|---------|--------|
| 1 | `seg_num.requested_5m` | == 源集群 submitted 增量（即 N） |
| 2 | `seg_num.finish_5m` | == 目标集群 finished 增量（即 N） |
| 3 | `seg_num.gap_5m` | == 0（预热完成后无缺口） |
| 4 | `seg_num.fail_5m` | == 0（无失败） |
| 5 | `seg_size` 各字段 | 为人类可读字符串格式（如 `"2.6 KB"`） |
| 6 | `last_trigger_ts` | 非空 |
| 7 | `last_finish_ts` | 非空 |

### 6.3 验证 BE HTTP API

```bash
# 查看源集群 BE
curl http://<source_be_host>:<http_port>/api/warmup_event_driven_stats

# 查看目标集群 BE
curl http://<target_be_host>:<http_port>/api/warmup_event_driven_stats
```

验证项：

| # | 检查内容 | 预期 |
|---|---------|------|
| 1 | `code` | == 0 |
| 2 | `data` 数组包含对应 job_id | 存在 |
| 3 | 源集群 BE：`requested` 有值，`finish`/`fail` 为 0 | 符合角色 |
| 4 | 目标集群 BE：`finish` 有值，`requested` 为 0 | 符合角色 |
| 5 | size 字段为字节数（整数） | 非字符串格式 |

### 6.4 窗口过期验证

> 窗口指标的时间范围为 5 分钟 / 30 分钟 / 1 小时。验证窗口过期需要等待对应时间，仅建议在必要时执行。

1. 执行 INSERT 触发预热，等待完成
2. 记录 `seg_num.requested_5m` 的值
3. 等待 5 分钟以上，不做任何写入
4. 再次查看 `seg_num.requested_5m`，预期值回到 0（窗口内无活动）
5. `seg_num.requested_30m` 和 `seg_num.requested_1h` 在更长时间后也应归零

### 6.5 现有 bvar 累积指标参考

以下为现有的全局 bvar 累积指标，可用于辅助验证和对照：

**源集群 BE（`/brpc_metrics`）：**

| 指标名 | 说明 |
|--------|------|
| `file_cache_event_driven_warm_up_requested_segment_num` | 累计请求预热 segment 数 |
| `file_cache_event_driven_warm_up_requested_segment_size` | 累计请求预热 segment 大小 |
| `file_cache_event_driven_warm_up_requested_index_num` | 累计请求预热索引文件数 |
| `file_cache_event_driven_warm_up_requested_index_size` | 累计请求预热索引文件大小 |
| `file_cache_event_driven_warm_up_skipped_rowset_num` | 累计跳过的 rowset 数 |

**目标集群 BE（`/brpc_metrics`）：**

| 指标名 | 说明 |
|--------|------|
| `file_cache_event_driven_warm_up_submitted_segment_num` | 累计提交下载 segment 数 |
| `file_cache_event_driven_warm_up_finished_segment_num` | 累计完成下载 segment 数 |
| `file_cache_event_driven_warm_up_failed_segment_num` | 累计失败 segment 数 |
| `file_cache_event_driven_warm_up_submitted_index_num` | 累计提交下载索引文件数 |
| `file_cache_event_driven_warm_up_finished_index_num` | 累计完成下载索引文件数 |
| `file_cache_event_driven_warm_up_failed_index_num` | 累计失败索引文件数 |

> 以上累积指标为全局指标，不区分 Job 和表。新增的窗口指标按 `job_id` 维度独立统计。
