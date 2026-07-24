# FIX-PERF-11 — 大扫描 per-file 不变量 memo（C12 / C15a / C13-plan）小结

> 设计 + 红队详见 [`FIX-PERF-11-per-file-invariant-memo-design.md`](./FIX-PERF-11-per-file-invariant-memo-design.md)。
> 一个连接器侧 commit。硬约束达成：只减 planning CPU + FE heap，扫描 range/thrift/结果逐字节不变。

## Problem

一个数据文件被 `splitFiles` 切成 k 个 byte-slice，`buildRange` 逐 slice 重算**对整个文件恒定**的量（partition JSON 的 Jackson+时区格式化、identity/ordered 分区值、delete 载体转换）。10 万 slice 的分区 MOR 表 ≈ 多花 0.5~2 秒 planning CPU + 大量垃圾；k 个 range 各持独立副本放大 FE heap；v3 时还逐 slice 幂等 put 删除供给。

## Root Cause

`buildRange` 输出仅 `start/length/selfSplitWeight` 是 per-slice，其余全是 per-file 不变量（由 `task.file()`/`task.deletes()`/scan 级 zone·spec 决定），但无 memo。`task.file()` 对一个文件全部 slice 返回同一 `DataFile` 实例，且 slice 连续吐出。

## Fix（`10b7d29423f`）

在共用 choke point `buildRangeForTask` 穿入 **1 条目「当前文件」缓存** `PerFileScratch`：
- 抽 `computePerFileInvariants`（原逐 slice 计算逐字不变）；`buildRange` 改「`scratch.file == dataFile` ? 复用 : compute」+ per-slice `start/length/selfSplitWeight` + 组装。**range 直接引用 scratch 的不可变 `partitionValues`/`deleteCarriers` 实例** → 同文件 k range 共享（C15a）。
- eager `planScanInternal` 局部 scratch；流式 `IcebergStreamingSplitSource` 实例字段 scratch（每 scan 新建、单线程 → **O(1) 内存、不破流式防 OOM**）。
- **C13 plan**：`rewritableDeleteSupply.put` 改为**文件第一个 slice（miss）时用新文件路径**一次（覆盖含最后一个文件，杜绝 v3 静默复活）。
- `planCountPushdown` 传 `null` scratch（单 range 不缓存）。
- 度量守门 `@VisibleForTesting int perFileInvariantComputeCount`。

**逐字节不变**靠：per-file 量只依赖 `task.file()`/scan 不变量、对同文件恒等；不同逻辑文件永不共享 `DataFile` 实例（copy-per-entry）→ 身份键不 stale；slice 连续 → 1 条目 100% 命中；per-slice 字段照算。3 独立对抗视角均判 parity 保持、0 破坏性发现。

## Tests

`IcebergScanPlanProviderTest` +2：① 96MB 分区文件按 32MB 切 3 slice → `perFileInvariantComputeCount==1`、各 slice partition 值/JSON 相同而 start/length 连续平铺；② 两个切分文件 → count==2 且各带自己的分区值（无跨文件串味）。既有 103 scan-plan parity 测试 + 66 transaction（delete 供给）全绿。

## Result

- **C12**：partition JSON/identity/delete 载体 per-slice(k) → per-file(1)，10 万 slice 省 0.5~2s CPU + 减垃圾。
- **C15a**：同文件 k range 共享 `partitionValues`/delete 载体实例，省 FE heap。
- **C13(plan)**：v3 rewritable-delete supply put per-slice → per-file。
- **线路字节（C15b）+ fe-core 通用节点 hoist（C14）本次不做**（协议演进/框架层，另议）。
- 验证：iceberg `IcebergScanPlanProviderTest` 105/105 + `IcebergConnectorTransactionTest` 66/66 + 全模块 `1064 pass / 1 skip / 0 fail`，BUILD SUCCESS + 0 checkstyle。
