# FIX-PERF-08 — 维护路径逐单位重扫 / 无去重（C19 + C21）小结

> 设计 + 红队详见 [`FIX-PERF-08-maintenance-path-rescan-design.md`](./FIX-PERF-08-maintenance-path-rescan-design.md)。
> 一个任务、两处独立修复、两个独立 commit。硬约束达成：只减远程读，行为逐字节不变。

## Problem

两条 iceberg `ALTER TABLE ... EXECUTE` 维护命令把「读一次就够」的整表元数据扫描按单位数重复执行：
- `rewrite_data_files`：G 个 rewrite group **逐组**登记源文件 → 连接器每次一遍整表 `planFiles()` → **G+1 次整表扫描**。
- `expire_snapshots`：构建 delete 分类表时**逐 snapshot** 读全部 delete manifest，**无去重** → 相邻 snapshot 大量重叠 → **S×M 串行远程读**。

## Root Cause

- C19：`ConnectorRewriteDriver.run()` STEP3 逐组调 `registerRewriteSourceFiles`；iceberg 实现每次跑一遍 `useSnapshot(startingSnapshotId).planFiles()`。所有调用 pin 同一快照、组间互斥 → 可合成一次并集扫描。
- C21：`IcebergExpireSnapshotsAction.buildDeleteFileContentMap` 双层循环无 visited 判断；iceberg manifest 不可变、跨 snapshot 原样前推 → 同一 manifest 被重复远程读。

## Fix

- **C19（fe-core，commit `89cc39c8d88`）**：STEP3 逐组循环 → `unionSourceFilePaths(groups)` 并集**一次登记**。抽包私有静态纯函数便于单测。iceberg 连接器零改动。行为不变靠：同一固定快照 + 组互斥 + 连接器按 path 去重 → `filesToDelete` 逐元素一致；四列计数来自组元数据；缺失路径两种写法都 fail-loud + rollback。
- **C21（iceberg，commit `be0035eff62`）**：`buildDeleteFileContentMap` 加**方法作用域** visited set，`if (!visitedDeleteManifests.add(manifest.path())) continue;` 同一 manifest 只读一次。结果 map 逐字节不变（manifest 不可变 + `putIfAbsent` 与顺序无关）。新增 `@VisibleForTesting int lastDeleteManifestReadCount` 作度量守门。

## Tests

- **C19**（`ConnectorRewriteDriverTest`，+2）：`unionSourceFilePaths` 多组并集、跨组重复去重、含空组、空 plan → 5/5 绿。分布式实路径留 flip rehearsal e2e（既有约定）。
- **C21**（`IcebergExpireSnapshotsActionTest`，+1；`ActionTestTables` 加 position/equality delete 播种 helper）：构造「同一 delete manifest 被多 snapshot 前推共享」的表，断言实际读次数 == distinct manifest 数（< 逐 snapshot 总数），且 position/equality delete 分类正确。

## Result

- **C19**：整表 `planFiles()` 扫描 G+1 → 1。
- **C21**：delete manifest 远程读 O(S×M) → O(distinct M)。
- 两条经 2 独立对抗 agent 逐一证伪均判 `parity_preserved=true`，无破坏性发现。
- 验证：`ConnectorRewriteDriverTest` 5/5、fe-core BUILD SUCCESS、0 checkstyle；iceberg `IcebergExpireSnapshotsActionTest` 9/9 绿、0 checkstyle。
