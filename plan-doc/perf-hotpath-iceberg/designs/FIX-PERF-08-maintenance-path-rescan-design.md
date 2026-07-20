# FIX-PERF-08 — 维护路径逐单位重扫 / 无去重（C19 + C21）设计

> 一个任务、两处独立修复（各自独立 commit）。同一「逐单位重扫、零去重」模式。
> 硬约束：**性能修复 → 行为不变**，只减远程读，不改任何可观测结果 / 异常 / 提交后表状态。
> 基线：分支 `catalog-spi-review-16`，行号已按 HEAD 重新 grep（PERF-07 后）。

---

## Problem

两条 `ALTER TABLE ... EXECUTE` 维护命令，各自把「读一次就够」的整表元数据扫描按「单位数」重复了 N 遍：

- **C19 `rewrite_data_files`**：分布式 compaction 规划把 G 个 bin-pack 组的 source 文件**逐组注册**，每次注册在连接器侧触发一遍**整表 `planFiles()` 扫描**（读 manifest-list + 全部 manifest）。G 个组 = **G 次整表扫描**；G 常见 50~200，分钟级。
- **C21 `expire_snapshots`**：过期前构建「delete 文件 → 内容类型」映射时，**逐 snapshot** 读该 snapshot 的**全部** delete manifest，**无 visited 去重**。相邻 snapshot 的 delete manifest 大量重叠（iceberg manifest 不可变、跨 snapshot 原样前推），同一 manifest 被远程读 S 次。S×M 串行远程读。

---

## Root Cause

- **C19**：`ConnectorRewriteDriver.run()` STEP3（fe-core）逐组调 SPI `registerRewriteSourceFiles`；iceberg 实现 `IcebergConnectorTransaction.registerRewriteSourceFiles`（:366）每次调用都跑一遍 `table.newScan().useSnapshot(startingSnapshotId).planFiles()` 把 raw path 反解回 `DataFile`。关键事实：**所有逐组调用 pin 的是同一个 `startingSnapshotId`**（一条共享事务、beginWrite 时捕获一次），且**组之间互斥**（bin-pack，每个 data file 只属于一个组）。因此 G 次「同快照、各自过滤子集」的扫描，本可合成一次「同快照、过滤并集」的扫描。SPI 契约本就写明 `registerRewriteSourceFiles` "Accumulates across calls" 且入参是 `Set<String>` —— 一次传并集与 G 次传分片，累积结果完全一致。
- **C21**：`IcebergExpireSnapshotsAction.buildDeleteFileContentMap`（:271）双层循环 `for snapshot → for deleteManifest → readDeleteManifest`，没有「这个 manifest 我读过了」的 visited 判断。iceberg 的 `ManifestFile` 不可变，读同一 manifest 得到的 `DeleteFile` 集合恒等，且 `putIfAbsent` 对重复 path 本就是 no-op —— 逐 snapshot 重读纯属浪费。

---

## Design

### C19 — union 一次注册（改 fe-core 通用驱动，保持 connector-agnostic）

`ConnectorRewriteDriver.run()` STEP3 由「逐组循环调用」改为「先并集、再一次调用」：

```java
// STEP 3: register the UNION of every group's source data files in a single call. The connector re-derives
// them from the table at the pinned OCC snapshot with ONE planFiles() scan; the previous per-group loop
// repeated that full-table scan once per group (PERF-08 / C19). Ordering is unchanged — still after
// runGroups (so the table + OCC snapshot are loaded) and before commit.
Set<String> sourceFilePaths = unionSourceFilePaths(groups);
connectorTx.registerRewriteSourceFiles(sourceFilePaths);
```

并集抽成一个包私有静态纯函数 `static Set<String> unionSourceFilePaths(List<ConnectorRewriteGroup> groups)`（唯一目的：让并集/去重逻辑可单测；见 Test Plan）。iceberg 连接器**零改动**。

**为什么行为不变**（逐条对齐红队关注点）：
1. 同快照：G 次调用与并集 1 次调用都用同一 `startingSnapshotId`（共享事务，恒定）→ 反解结果同源。
2. 累积集合一致：`registerRewriteSourceFiles` 内 `matched` 按 path 去重；组之间互斥 → `filesToDelete`（`List<DataFile>`）内容与逐组累积**逐元素一致**（仅可能顺序不同，`RewriteFiles.deleteFile` 是集合语义，顺序无关）。
3. fail-loud 一致：任一 path 在 pin 快照缺失，逐组版在第 k 组抛、并集版在唯一一次调用抛，均 `DorisConnectorException`；STEP2 的 `catch` 都会 `rollback` 整条事务，部分注册无副作用。
4. 结果行不受影响：STEP5 的四列计数来自 `ConnectorRewriteGroup` 元数据（组级 `getDataFileCount/getTotalSizeBytes`）与 commit 后的 added-count，**不读** `filesToDelete.size()`。
5. 空输入：空组不贡献并集；全空 → 并集空 → `registerRewriteSourceFiles` 早退（与逐组全空等价）。

**约束核对**：`ConnectorRewriteDriver` 在 fe-core，但本改动 (a) 不引入 source-specific 分支（仍只搬运 `Set<String>` 中性路径，无 iceberg 类型）；(b) 不把连接器逻辑/属性解析/缓存搬进 fe-core —— 只是把**已有** driver 对**已有** SPI 的调用方式从 G 次收敛为 1 次，是行为收敛而非能力上移。符合「fe-core 只出不进 + 禁 scaffolding 搬迁」两律与 connector-agnostic 纪律。

### C21 — 按 manifest path 去重（连接器侧，纯 iceberg 局部）

`buildDeleteFileContentMap` 内层加 visited set：

```java
Set<String> visitedDeleteManifests = new HashSet<>();
for (Snapshot snapshot : icebergTable.snapshots()) {
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(icebergTable.io());
    if (deleteManifests == null || deleteManifests.isEmpty()) {
        continue;
    }
    for (ManifestFile manifest : deleteManifests) {
        if (!visitedDeleteManifests.add(manifest.path())) {
            // Immutable manifests are carried across adjacent snapshots unchanged; reading one again yields
            // the identical DeleteFile set (putIfAbsent already made the re-read a no-op). Skip the remote read.
            continue;
        }
        try (CloseableIterable<DeleteFile> deleteFiles = ManifestFiles.readDeleteManifest(
                manifest, icebergTable.io(), icebergTable.specs())) {
            for (DeleteFile deleteFile : deleteFiles) {
                deleteFileContentByPath.putIfAbsent(deleteFile.location(), deleteFile.content());
            }
        }
    }
}
```

**为什么行为不变**：manifest 不可变 → 读每个 distinct manifest 一次即覆盖当前代码看到的全部 delete 文件（当前代码读的是同一批 distinct manifest 的带重复超集）；`putIfAbsent` first-win 因 `deleteFile.content()` 对给定 location 恒定而与遍历顺序无关 → **结果 map 逐字节一致**，只是远程读从 O(S×M) 降到 O(distinct M)。

**度量守门**：新增 `@VisibleForTesting` 计数器记录本次 `readDeleteManifest` 实际调用次数，测试断言「S 个 snapshot 共享同一 delete manifest 时，读次数 == distinct manifest 数」而非 S×M。

---

## Implementation Plan

两个独立 commit：

1. **commit A（C19，fe-core）**：`ConnectorRewriteDriver` STEP3 循环 → 并集一次调用 + 抽 `unionSourceFilePaths` 静态纯函数；新增 `ConnectorRewriteDriverTest` 用例断言并集/去重/空组。**须两段验**（动了 fe-core：iceberg 反应堆不含 fe-core）。
2. **commit B（C21，iceberg）**：`buildDeleteFileContentMap` 加 visited 去重 + `@VisibleForTesting` 读计数；`buildDeleteFileContentMap` 降为包私有以便直测；`ActionTestTables` 加 position/equality delete 播种 helper；`IcebergExpireSnapshotsActionTest` 加去重 + 分类 parity 用例。**纯 iceberg，免 fe-core 段**。

---

## Risk Analysis

- **C19 顺序依赖**：STEP3 必须在 `runGroups` 之后（table + snapshot 已加载）、commit 之前。并集调用位置不变，只改循环体，风险低。
- **C19 未来非 iceberg 连接器**：SPI 契约明确「累积 + 收 Set」，一次传并集在契约内；且当前仅 iceberg override。低。
- **C19 单测覆盖有限**：driver 的分布式实路径（非空组 → 真 BE INSERT-SELECT）在类注释里就明确「留 flip rehearsal e2e」，STEP3 非集群不可达。故 C19 的单测只能守到「并集函数正确」这一层；「一次调用」的端到端由既有连接器侧 `registerRewriteSourceFilesThenCommit...` 测试（一次调用注册多 path）+ 后续 flip rehearsal e2e 兜底。诚实标注，不假装守全。
- **C21 destructive**：`execute()` 真过期快照；单测直调包私有 `buildDeleteFileContentMap`（非 destructive）验证 map + 读计数，避开提交副作用。
- **C21 delete 文件跨多个 distinct manifest**：两 manifest 都会被读（都是 distinct path），`putIfAbsent` first-win 因 content 恒定而顺序无关 → 一致。低。

## Test Plan

### Unit Tests
- **C19**（`ConnectorRewriteDriverTest`）：`unionSourceFilePaths` 对多组返回去重并集；跨组重复 path 只留一份；含空组时空组无贡献；空列表 → 空集。
- **C21**（`IcebergExpireSnapshotsActionTest` + `ActionTestTables`）：
  - 去重守门：构造 delete manifest 被 S 个 snapshot 前推共享的表，断言 `readDeleteManifest` 实际读次数 == distinct manifest 数（< S×M）。
  - 分类 parity：position + equality delete 文件分布在重叠 snapshot 上，断言结果 map 对每个 delete path 的 `FileContent` 分类正确（catch 去重误跳含唯一 delete 的 manifest 的回归）。

### E2E Tests
- 维护命令 e2e（独立 iceberg 目录 + HMS 网关委派跑 `rewrite_data_files` / `expire_snapshots` 断言计数）随 P6.6 切换阶段统一补（当前 iceberg 未进 `SPI_READY_TYPES`，provider 休眠；对齐 `hms-iceberg-delegation-needs-e2e`）。

---

## 设计红队（对抗 parity review，2 独立 agent）

两条修复均判 `parity_preserved = true`，无 breaks-parity 发现。采纳的残余提醒：

- **C19（medium，非破坏）**：union 的 List 去重正确性依赖「组之间 path 互斥」——iceberg 侧可证（`planFiles()` 每 data file 一个 task + bin-pack 分桶划分），但 SPI 未强制。**落地动作**：STEP3 注释显式记该不变量；补空组 no-op parity 单测。stale-plan 报错文案的计数从「首个缺失组」变为「并集」——同类型异常、同 rollback/不提交结局，非稳定契约，可接受。
- **C21（medium，非破坏但可能使优化空转）**：`visited` set **必须在 snapshot 外层循环之前（方法作用域）声明**——若误放进内层则每轮重置 → dedup 空转（仍 parity，但零收益）。**落地动作**：确保方法作用域声明；单测用「同一 delete manifest 被 S 个 snapshot 前推共享」断言读次数坍缩。transient IO 失败面变小（读得少 → 失败机会少）属预期收益，非破坏。

## 度量收益

- **C19**：整表 `planFiles()` 扫描 G+1 → **1**（G≈50~200 时分钟级 → 秒级）。
- **C21**：delete manifest 远程读 O(S×M) → **O(distinct M)**（相邻 snapshot 重叠越多，收益越大）。
