# FIX-PERF-04 — Design（惰性 manifest-cache 接回流式规划 + COUNT(*) 下推）

> 覆盖审计发现 **C17（流式路径绕过 manifest cache）+ C18（COUNT(*) 下推绕过 manifest cache）**。
> 用户 2026-07-18 拍板方向 = **惰性+缓存兼得**（把缓存规划改成边读边吐，两路径接回）。基线 HEAD `584ef3822d8`。行号信 grep。

## Problem

manifest cache（`meta.cache.iceberg.manifest.enable`，**默认 OFF**，opt-in）把解析过的 manifest 缓在 FE 内存里，让重复查询把远端 manifest 读变内存命中。**只接在同步物化路径** `planFileScanTask`（gate `isManifestCacheEnabled`）→ `planFileScanTaskWithManifestCache`。两条路径即使开了缓存也绕过它：

- **C17（主）**：文件数 ≥ `num_files_in_batch_mode`（默认 1024）的大表走**流式**规划（`streamSplits → scan.planFiles()`，SDK 裸扫、不物化整表任务列表 = OOM 保护），**不读缓存**。讽刺：大表恰是缓存最想加速的对象，命中率 0。
- **C18（次）**：`COUNT(*)` 下推 `planCountPushdown → scan.planFiles()`（只取第一个数据文件拼占位切片；行数其实来自 `getCountFromSnapshot` 快照摘要），也**不读缓存**，且 `ParallelIterable` 激进提交所有 manifest 读任务（虽只要第一个）。

## 复核确认的冲突（已解，见 progress）

审计说"legacy batch 模式走缓存"——**核 legacy `IcebergScanNode`（`6fef25709d3^`）确认，但补全另一半**：legacy `doStartSplit → planFileScanTask`（610）**确走缓存**；**但 legacy 走缓存时是一次性物化整表任务列表的**（`planFileScanTaskWithManifestCache` 建 `List<FileScanTask> tasks`），即 **legacy 开缓存的大表根本没 OOM 保护**（OOM 保护只在缓存关时的 `splitFiles` 惰性分支）。迁移故意让大表一律流式（OOM 安全）换掉了缓存收益。故审计的"缓存开就退回物化"（= 恢复 legacy）会**重引 OOM 风险**——已否决。

## Root Cause

缓存规划（`planFileScanTaskWithManifestCache`）**物化**整表任务列表（phase 2 攒 `List<FileScanTask>`），与流式的惰性 OOM 保护互斥。故大表要么走缓存（物化/OOM）要么走流式（惰性/无缓存）——二选一。**解法 = 让缓存规划本身惰性化**（边读 manifest 边吐任务、不攒全表），流式与 COUNT 都复用它 → 缓存与不 OOM 兼得。

## Design（连接器侧，零 fe-core 改动；核心 = 抽一个惰性缓存迭代器，三处复用）

### 1. 新建 `cacheBackedFileScanTasks(scan, session, table, filter, String statsQueryId)` → `CloseableIterable<FileScanTask>`（惰性）
从现 `planFileScanTaskWithManifestCache` 抽出，产出**整文件**（未切片）`BaseFileScanTask`：
- **保留 `snapshot==null` 守卫**（现 1751-1754）：空表返回空 `CloseableIterable`（空表 COUNT 会带 null 快照走到这里，丢守卫会 NPE）。
- **Phase 1（eager，有界）**：读所有匹配 **DELETE** manifest（经 cache）建 `DeleteFileIndex`。与现状/legacy/iceberg `planFiles` 一致——iceberg 的 `planFiles` 本就先读全部 delete manifest 建索引再吐 data 任务，故 eager delete 不比现流式差（现流式 `scan.planFiles()` 也 eager 读 delete）。delete manifest 通常远少于 data，且这是产任一 data 任务的正确性前提。
- **Phase 2（LAZY）**：返回一个 `CloseableIterable`，其迭代器**扁平映射**匹配 **DATA** manifest（`getMatchingManifest`），逐个经 cache 读、逐文件 `InclusiveMetricsEvaluator` + `ResidualEvaluator` 剪枝、`DeleteFileIndex.forDataFile` 挂 delete，产出存活 `BaseFileScanTask`——**不攒 list**。峰值内存 = delete 索引 + 当前一个 manifest 的文件 + 切片队列背压，**非整表**。
- **stats 线程安全（红队 MED）**：manifest cache 的命中/未命中计数器 `getManifestCacheValue(m, table, queryId)` 是**按 queryId 单线程假设**的（无锁 `stats.hits++`）。流式的 Phase 2 在引擎 pump 线程跑、Phase 1 在调用线程跑 → 两线程同 queryId 竞争。故 `statsQueryId` **可空**：流式传 **null** → 用**无 stats** 重载 `getManifestCacheValue(m, table)`（流式今天本就不报 cache stats，行为不变、消竞争）；COUNT/同步单线程 → 传 `session.getQueryId()` 用 stats 重载（不变）。

迭代器 `ManifestCacheFileScanTaskIterator`（内部类，`CloseableIterator<FileScanTask>`）：持 `CloseableIterator<ManifestFile> manifestIt` + 当前 `Iterator<DataFile>` + 当前 spec/residual + `deleteIndex`/evaluators + 缓冲 `next`；`advance()` 拉取下一个存活任务（必要时跨 manifest）；`close()` 关 `manifestIt`。三个消费方均须在 try-with-resources / SplitPlan / streaming source.close 内关闭它（单次关，`withNoopClose` 防同步路径二次关）。

### 2. 重构 `planFileScanTaskWithManifestCache`（同步物化路径，小表 < 阈值）
改成：物化 `cacheBackedFileScanTasks(...)` 成 `List` → `determineTargetFileSplitSize(list)`（**启发式**切片大小，小表要它给 BE 并行度）→ `SplitPlan(splitFiles(withNoopClose(list), size), size)`。**字节等价**（同任务、同大小）——现有同步 cache parity 测试必须继续绿（守门）。

### 3. `streamSplits`（C17）
`isManifestCacheEnabled()` 时：`whole = cacheBackedFileScanTasks(scan, session, table, filter, null)`（**null** = 无 stats，消 pump/调用线程竞争）；`tasks = TableScanUtil.splitFiles(whole, sliceSize)`（**固定** `file_split_size`/`max_file_split_size`，与现流式同款——大表本就固定大小，**切片大小不变**）→ 喂现 `IcebergStreamingSplitSource`。**唯一变化 = 大表现在命中/填充缓存，切片大小/流式/OOM 边界全不变**。
- **失败回退**：调 `cacheBackedFileScanTasks(...)`（含 eager phase-1）包 **`try/catch(Exception)`**（红队 HIGH：`IcebergManifestCache.loadManifestCacheValue` 把 IOException 重抛成 `RuntimeException`，`deleteManifests` 抛 `RuntimeIOException`，故须 `Exception` 非 `IOException`，镜像同步 `planFileScanTask:1731` 的 `catch (Exception)`）→ `manifestCache.recordFailure(queryId)` + 退回 `scan.planFiles()`（SDK 流式）。phase-2 惰性失败经 streaming source 的 `hasNext` 路由到引擎 setException（与现 SDK 流式失败同路径；且经 cache 读 manifest 失败 ⇔ SDK 读同一 manifest 也失败，无"cache 挂但 SDK 成"场景）。
- 缓存关：现 `scan.planFiles()` 不变。

### 4. `planCountPushdown`（C18）
`isManifestCacheEnabled()` 时：迭代 `cacheBackedFileScanTasks(scan, session, table, filter, session.getQueryId())`（COUNT 单线程 → 用 stats 重载）取**第一个**任务 buildRange（惰性 → 只读到出第一个文件的 manifest 即停，避免 `ParallelIterable` 全提交）。包 **`try/catch(Exception)`** → 退回 `scan.planFiles()`。需把 `filter` 从 `planScanInternal` 透传给 `planCountPushdown`（现未传；count 下推通常无 WHERE，filter 多为 alwaysTrue，透传保通用+正确）。行数仍来自 `realCount`（快照摘要），BE 不读文件——**首文件取自剪枝后存活文件，与 SDK 路径 count 值相同**（占位切片的 file path 因 SDK `ParallelIterable` 乱序而 OFF/ON 不同，属正常，测试勿断言 path 相等）。
- **scan-metrics**：cache 路径不调 `scan.planFiles()` → 不触发 `IcebergScanProfileReporter`；与**现有同步 cache 路径一致**（它也不调 planFiles），仅 count+cache 少一份 scan profile，可接受。
- 缓存关：现 `scan.planFiles()` 不变。

### 5. `streamingSplitEstimate` — **不改**
批量/流式**决策**（按 manifest 元数据计数，cheap）不变；只改流式**执行**读法。OOM 边界（阈值决策）原封不动。

## Parity 论证

- **同任务**：三路径都源自 `cacheBackedFileScanTasks`，其 phase-1/phase-2 剪枝逻辑 = 现 `planFileScanTaskWithManifestCache` 逐字搬（只是惰性化）→ 产出与现同步 cache 路径**逐任务相同**；现同步 cache 路径已声明与 `scan.planFiles()` legacy-parity。故惰性化**不引入新 parity 风险**（不发明新规划，只把既有缓存规划改惰性）。
- **切片大小**：小表同步 = 启发式（不变）；大表流式 = 固定（不变）；COUNT = 单切片无关。
- **失败语义**：eager phase-1 失败可回退 SDK（superset of 现流式，且 ≥ 现同步 cache 的回退）；phase-2 失败路由引擎（同现流式）。
- **delete 索引 eager**：= 现流式 `scan.planFiles()` 与现同步 cache 与 legacy，无回退。

## Implementation Plan（连接器侧，逐处）

1. `IcebergScanPlanProvider`：抽 `cacheBackedFileScanTasks(...)` + 内部类 `ManifestCacheFileScanTaskIterator`；`planFileScanTaskWithManifestCache` 改为物化它；`streamSplits`/`planCountPushdown` 加 cache 分支 + 回退；`planScanInternal` 给 `planCountPushdown` 透传 `filter`。
2. 无新缓存、无新字段、无 fe-core/thrift/BE 改动。

## Test Plan

- **流式 cache parity（新）** `IcebergScanPlanProviderTest`（仿 `planScanManifestCacheEnabledMatchesSdkPathAndConsumesCache`）：真表，`streamSplits` 缓存 OFF（SDK）vs ON（惰性 cache）→ 排序路径相等 + `cache.size()>0`（缓存被填/命中）。分区表 + WHERE 剪枝一例。**MUTATION**：流式仍走 SDK → cache 空 → 红。
- **COUNT cache parity（新）**：`planScan(countPushdown=true)` 缓存 OFF vs ON → **同 count（table_level_row_count）+ 均为单个合法 range（其 path 是表内某文件）**；**不断言 path 相等**（红队 MED：SDK `planFiles` 的 `ParallelIterable` 乱序令首文件 OFF/ON 不定）。+ `cache.size()>0`；惰性早停（多 manifest 表断言 `cache.size() < 总 data manifest 数`）。
- **空表 COUNT + cache（新，红队 MED）**：空表（null 快照）`planScan(countPushdown=true)` 缓存 ON → 0 range（守卫保住、不 NPE）。
- **惰性迭代器行为（新）**：drain 全量、close-before-iterate 不抛、exhausted `next()` 抛（镜像现 `streamSplits*` 测试）；空表/全剪空 → 0 任务。
- **回退（新）**：注入 phase-1 IOException（可用抛异常的 fake manifestCache 或坏 manifest）→ 流式/count 退回 SDK 且 `recordFailure` 记账。
- **同步 cache 路径不回归**：现 `planScanManifestCache*` 全绿（物化路径字节等价守门）。
- 全 iceberg 模块 UT 绿 + checkstyle 绿。

## Risk

- **中**：共用热路径（流式 + COUNT + 同步 cache 三者现共源一个惰性方法）。缓解：同步路径物化同一 iterable → 现同步 cache parity 测试守字节等价；新增流式/COUNT cache-parity（vs SDK）守新路径；惰性迭代器行为测试守 close/exhaust。
- 惰性迭代器手写（扁平映射 + close 语义），有实现坑（缓冲/推进/关闭）→ 测试覆盖。
- delete 索引 eager 内存 = 现状，未新增；如超大 MOR 表 delete 多则同现状受影响（非本任务回归）。
- **缓存填充变化（红队 LOW，本就是目的）**：大表流式现在会**填充** manifest cache（每读一个 data manifest 入缓存），改变 eviction 动态、增 FE 堆压力、可能挤掉小表条目。容量有界（100000）不会 OOM；这正是"让大表享受缓存"的题中之义，用户已 opt-in。
- 触发面窄（opt-in 缓存默认关）→ 影响面可控。
