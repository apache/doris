# FIX-PERF-11 — 大扫描 per-split 不变量重算 + payload 复制（C12 / C15a / C13-plan）设计

> 硬约束：性能修复 → 行为逐字节不变（同样的 range、同样的 thrift、同样的扫描结果）。只减 planning CPU + FE heap。
> 基线：分支 `catalog-spi-review-16`，行号已按 HEAD 重新 grep（PERF-01~08 后）。

---

## Problem

一个数据文件被 `TableScanUtil.splitFiles` 切成 **k 个 byte-slice**（k 可达几十），每个 slice 是一个 `FileScanTask`。规划时对**每个 slice** 都调一遍 `buildRange`（`IcebergScanPlanProvider.buildRange:1126`），重算一遍**对该文件恒定**的量：
- `partitionDataJson`（`IcebergPartitionUtils.getPartitionDataJson` = 日期/时间格式化 + Jackson `writeValueAsString`，最贵）；
- identity map + ordered `partitionValues`（`getIdentityPartitionInfoMap` + LinkedHashMap 重排）；
- delete 载体列表（`buildDeleteFiles` → 逐 delete `convertDelete`，含 bounds 解码）。

**10 万 slice 的 timestamp 分区 MOR 表 ≈ 0.5~2 秒额外 planning CPU + 大量垃圾**（审计 C12）。此外 k 个 slice 的 `IcebergScanRange` 各持一份这些量的独立实例 → FE heap 放大（C15a）。v3 时 `buildRangeForTask` 还**逐 slice** 调 `rewritableDeleteDescs()` + `rewritableDeleteSupply.put()`（同一文件 k 次幂等 put，C13 plan 侧）。

## Root Cause

`buildRange` 的输出只有 `start` / `length` / `selfSplitWeight`（= `task.length()` + Σdelete 大小）是 **per-slice** 的；其余全部是 **per-file 不变量**（由 `task.file()`/`task.deletes()`/scan 级 zone·spec 决定）。但没有任何 memo，逐 slice 全量重算。`task.file()` 对一个文件的全部 slice 返回**同一 `DataFile` 实例**（`SplitScanTask` 委派），且 `splitFiles` 把一个文件的 slice **连续**吐出——天然可按文件 memo。

## Design（连接器侧，单一 choke point）

`buildRangeForTask`（`:739`）是 eager 循环（`:681`）与流式 `IcebergStreamingSplitSource`（`:564`）**共用**的唯一入口。在它这里穿入一个 **1 条目「当前文件」缓存**（consecutive-file cache）：

```
// 伪码：per-file 不变量缓存，穿过 buildRangeForTask
class PerFileScratch {
    DataFile file;                 // 身份键（== 比较）
    Integer partitionSpecId;
    String partitionDataJson;
    Map<String,String> partitionValues;   // unmodifiable，跨 slice 共享同一实例
    List<IcebergScanRange.DeleteFile> deleteCarriers; // 跨 slice 共享同一实例
    String fileFormat; Long firstRowId; Long lastUpdatedSequenceNumber;
    String rawDataPath; String normalizedPath;
}
```

`buildRange(slice)`：若 `task.file() == scratch.file` → 复用上面全部 per-file 量，只用 slice 的 `start/length` 重算 `selfSplitWeight` 并组装 range；否则重算 per-file 量、刷新 scratch。**range 直接引用 scratch 里的 `partitionValues`/`deleteCarriers` 同一（不可变）实例** → 同文件 k 个 range 共享实例（C15a：省 heap/GC，`IcebergScanRange` 全 final + `unmodifiableMap/List`，只读，安全共享）。

- **eager 路径**：scratch 是 `planScanInternal` 里的一个局部，穿进循环。
- **流式路径**：scratch 是 `IcebergStreamingSplitSource` 的一个实例字段。**仍 1 条目 → O(1) 内存**，不破 PERF-04 的流式不物化/防 OOM 纪律。

**C13 plan 侧**：`buildRangeForTask` 里 `rewritableDeleteSupply.put()` 改为**仅当文件切换时**（即该文件第一个 slice）做一次——同一文件 k 次幂等 put 坍缩为 1 次；put 的 value（`rewritableDeleteDescs()`）也从 per-file 共享 deleteCarriers 派生一次。行为不变（幂等 put 相同 value）。

**为什么行为逐字节不变**：per-file 量只依赖 `task.file()`/`task.deletes()`/scan 级 zone·spec，对同文件全部 slice 恒等；1 条目缓存因 slice 连续而 100% 命中同文件；range 的 per-slice 字段（start/length/weight）仍逐 slice 真实计算。→ 产出的 range 列表与 thrift 逐字节一致。

## 明确不做（本次范围外，另议）

- **线路字节去重（C15b）**：`populateRangeParams`（`IcebergScanRange:330`）**逐 range** 把该文件完整 delete 列表 `toThrift()` + partition JSON 塞进每个 `TFileRangeDesc`——这是**线路协议**逐 range 自带一份的固有形状。真正减线路字节要改 thrift（params 级 delete-file 字典 + per-range 索引）+ **BE reader**，是协议演进、非回归修复（审计 C15 自述）。本次不碰。
- **fe-core 通用节点 per-split hoist（C14）**：`FileQueryScanNode`/`PluginDrivenScanNode` 逐 split 的 `LocationPath.of`、逐 split 新建 provider、造完即弃的 columns-from-path。属**框架层**（惠及所有连接器但须证 byte+cost 双不变），且量级更小（~几百 ms@10 万 split）。单列，另行决策。
- **跨文件分区元组 memo**：审计「可选」的 `(specId, partition tuple)` 二级 memo（不同文件共享分区值时再 dedup partition JSON）——需 `StructLikeWrapper` map、**无界内存**，破流式 OOM 安全。跳过；如需，仅限 eager 路径且加界。

## Implementation Plan（已落地）

一个连接器侧 `[perf]` commit（仅 `IcebergScanPlanProvider.java` + 测试）：
1. 新增私有静态类 `PerFileScratch`（10 个 per-file 字段，`file` 为身份键）。
2. 抽 `computePerFileInvariants(...)`（原 buildRange 前半段逐字不变，末尾写入 scratch）；`buildRange` 改为「`scratch.file == dataFile` ? 复用 : compute」+ per-slice `start/length/selfSplitWeight` + 组装。
3. `buildRangeForTask` 加 `scratch` 参；**动 buildRange 前**捕获 `firstSliceOfFile = scratch.file != dataFile`，`rewritableDeleteSupply.put` 仅在 `firstSliceOfFile` 时（覆盖含最后一个文件）。
4. eager `planScanInternal` 建局部 `PerFileScratch` 穿入；流式 `IcebergStreamingSplitSource` 加 `private final PerFileScratch scratch`（每 source = 每 scan、单线程 → O(1)）。
5. `planCountPushdown` 的 `buildRange` 传 `null` scratch（单 range、不缓存）。
6. 度量守门：`@VisibleForTesting int perFileInvariantComputeCount`（`computePerFileInvariants` 首行 `++`）。

## Risk Analysis

- **正确性关键**：per-file 量若误缓存会串味（错分区 JSON/错 delete = 错扫描结果/静默漏删）。缓解=键用 `task.file()` 身份 `==`、只在同文件复用、per-slice 字段照算；红队专攻 parity。
- **流式 OOM**：1 条目缓存 O(1)，不累积。
- **实例共享**：`IcebergScanRange` 全 final + unmodifiable，`populateRangeParams` 只读 → 共享安全（不会有 range 改动共享 list）。
- **v3 put-once**：幂等 put 相同 value，坍缩为 1 次不改 supply 内容。

## Test Plan

### Unit Tests
- **per-file 复用度量守门**：一个数据文件切多 slice（构造 ≥2 slice），断言 `getPartitionDataJson`/`getIdentityPartitionInfoMap`（或一个连接器内计数器）对该文件**恰算 1 次**，而非 k 次。
- **byte-parity**：memo 前后同一表 planScan 产出的 range 列表逐字段等价（path/original/start/length/partitionSpecId/partitionDataJson/partitionValues/deleteFiles/weight），尤其**多 slice 文件的每个 slice 的 start/length 各异、其余共享**。
- **实例共享**：同文件多 slice 的 range 的 `getPartitionValues()`/delete 载体是**同一实例**（`assertSame`）。
- **v3 put-once**：rewritableDeleteSupply 对一个文件只被 put 一次、且 value 与逐 slice 版一致。
- 流式路径同构：`IcebergStreamingSplitSource` 产出与 eager 逐字节一致（既有 parity 测试扩展）。

### E2E Tests
- 大 MOR 表 + 分区 + 多 slice 的 SELECT/COUNT 结果 parity 随 P6.6 切换阶段统一补（对齐既有约定）。

## 设计红队（3 独立对抗视角，byte-parity 专攻）

三视角（不变量真实性 / 键与顺序 / 实例共享+put-once）均判 `parity_preserved=true`，无法证伪。关键确认：不同逻辑文件**永不共享** `DataFile` 实例（SDK `ContentFileUtil.copy` + manifest 缓存 `dataFile.copy()` 逐条拷贝，后者带「防复用」注释），故身份键不会跨文件返回 stale；`splitFiles`（`FluentIterable.transformAndConcat`）连续吐同文件 slice，1 条目缓存 100% 命中，即便假设非连续也只降 perf 不破 correctness；`IcebergScanRange` 全 immutable、消费者只读、刷新是换引用不改旧对象 → 流式 look-ahead + 已发出 range 保持各自正确对象。**3 条落地纪律（已遵守）**：① `selfSplitWeight/start/length` 必须 per-slice（只 delete-size 和是 per-file，但 length 项 per-slice，故整权重 per-slice）；② put 必须在**文件第一个 slice（miss）用新文件路径**（否则漏最后一个文件 → v3 静默复活）；③ scratch 每 scan 新建、绝不 provider/static 字段。

## 度量收益

- **C12**：partition JSON / identity / delete 载体从 **per-slice(k)** → **per-file(1)**；10 万 slice ≈ 省 0.5~2s planning CPU + 大幅减垃圾。
- **C15a**：同文件 k 个 range 共享 `partitionValues`/delete 载体实例 → 省 FE heap。
- **C13(plan)**：v3 rewritable-delete supply put 从 per-slice → per-file。
