# FIX-PERF-04 — Summary（惰性 manifest-cache 接回流式 + COUNT(*)）

> 权威设计见 [`FIX-PERF-04-manifest-cache-lazy-reconnect-design.md`](./FIX-PERF-04-manifest-cache-lazy-reconnect-design.md)。本文件只记落地结果。
> commit `2e5f393779c`（`[perf]`，iceberg 自包含）。基线 HEAD `584ef3822d8`。用户 2026-07-18 拍板方向 = 惰性+缓存兼得。

## Problem

`IcebergManifestCache`（`meta.cache.iceberg.manifest.enable`，**默认 OFF**，opt-in）只接在同步物化路径。两条路径即使开缓存也绕过它：
- **C17**：≥`num_files_in_batch_mode` 的大表走流式 `streamSplits → scan.planFiles()`（SDK 裸扫），而大表恰是缓存目标 → 命中率 0。
- **C18**：`COUNT(*)` 下推 `planCountPushdown → scan.planFiles()` 只为取一个占位文件（行数其实来自快照摘要），也不走缓存且 `ParallelIterable` 全提交。

## 复核（推翻审计"一行修复"）

核 legacy（`6fef25709d3^`）：batch 模式**确走缓存，但走缓存时一次性物化整表任务列表**（无 OOM 保护；OOM 保护只在缓存关时的惰性分支）。迁移用"一律流式（OOM 安全、无缓存）"换掉了它。审计建议的"缓存开→退回物化"会重引 legacy 就有的 OOM 风险 → 否决。

## Fix（连接器侧，零 fe-core 改动）

抽 `cacheBackedFileScanTasks()`——一个**惰性** `CloseableIterable<FileScanTask>`，三处复用：
- **Phase 1（eager）** 读全部 delete manifest（经 cache）建 `DeleteFileIndex`（产任一 data 任务的前提，且与 SDK `planFiles`/legacy 同样 eager，delete manifest 少）；
- **Phase 2（LAZY）** 扁平映射 data manifest，逐个经 cache 读、逐文件剪枝、产整文件 `BaseFileScanTask`，**不攒整表**（峰值 = delete 索引 + 一个 manifest 的文件 + 切片队列）。
- `planFileScanTaskWithManifestCache` 改为**物化**它（字节等价；小表保启发式切片大小）；`streamSplits` 缓存开时喂它给现流式 source（固定切片大小不变，大表现在命中缓存且仍 OOM 安全）；`planCountPushdown` 缓存开时迭代它**取第一个**（惰性早停，无 `ParallelIterable` 全提交）。
- **决策不改**（`streamingSplitEstimate`）。**失败回退** `catch (Exception)`（缓存把失败重抛成 `RuntimeException`）→ `recordFailure` + 退 SDK。**`statsQueryId` 可空**：流式传 null（无 stats 重载，消 pump/调用线程对同 queryId 计数器的竞争）；同步/COUNT 单线程用 stats。

## 红队（7 攻击核心 sound，采纳修正）

- **HIGH**：回退须 `catch (Exception)` 非 `IOException`（缓存重抛 RuntimeException）——已修。
- **MED**：流式两阶段跨线程 → 流式改用无 stats 重载消竞争——已修。
- **MED**：COUNT 对比测试不断言 path 相等（SDK `ParallelIterable` 乱序），改断言 count + 单切片——已采纳。
- **MED**：保留 `snapshot==null` 守卫 + 空表 COUNT 用例——已采纳。
- **LOW**：大表流式现在填充缓存（本就是目的、容量有界不 OOM）；COUNT+cache 少一份 scan profile（与现有 cache 路径一致）——已记录。

## Tests（`IcebergScanPlanProviderTest` +5）

- 流式 cache parity vs SDK + cache consumed；分区剪枝 parity；**跨多 data manifest 扁平映射**（3 append → 3 range）；COUNT cache count-parity（60）+ **惰性早停**（`cache.size() < 总 manifest 数`）；空表 null 快照 COUNT → 0 range（守卫不 NPE）。
- 同步 cache 路径既有测试继续绿（物化字节等价守门）。
- **回退路径未单测**：`IcebergManifestCache` 是 `final` 无法造抛异常 seam，与既有同步 `planFileScanTask` 回退同样未单测；流式/COUNT 的 `catch(Exception)+recordFailure` 逐字镜像它。

## Result

- 全 iceberg 模块 **962 pass / 0 fail / 0 error / 1 skip**（`install -am`），checkstyle 0 违规。
- 减负：开缓存的大表流式规划从"每查询裸读全部 manifest（0 命中）"→"命中缓存且惰性不 OOM"；`COUNT(*)` 从"全提交 manifest 读"→"命中缓存 + 首文件即停"。
- 修坑：`java.util.Iterator` 漏 import（编译）+ 同步空表路径 null session 下 `session.getQueryId()` NPE（改 null-safe）——均 CI 前本地捕获。
- 可复用：一个 `(delete 索引 eager + data manifest 惰性扁平映射)` 缓存迭代范式，供后续同类"物化 vs 惰性"权衡参考。
