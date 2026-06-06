# P3-T06 设计 — MVCC / snapshot SPI（保持 default opt-out，无代码）

> 批 B / metadata 补全 · behind 关闭的 gate · 零 live-path 风险
> 关联：[tasks/P3](../P3-hudi-migration.md) · [HANDOFF 未完成 #2](../../HANDOFF.md) · [DV-007](../../deviations-log.md)（批 B scope 校正）· [P3-T04 设计](./P3-T04-fail-loud-design.md)
> 状态：决策完成（code-grounded，用户签字 2026-06-05「Keep defaults + document」）

---

## Problem

`HudiConnectorMetadata` 未 override SPI 的三个 MVCC/snapshot 方法（`ConnectorMetadata.java:60-77`）：
`beginQuerySnapshot` / `getSnapshotAt(timestampMillis)` / `getSnapshotById(snapshotId)`，均默认返 `Optional.empty()`。

HANDOFF 原提示 T06「**大概率显式 unsupported（与 T04 fail-loud 一致）**」——暗示**新增抛异常的 override**。

## 决策：**不 override，保持 SPI default（`Optional.empty()` = opt-out）+ 文档化**（无代码改动）

code-grounded recon（5-reader workflow，mvcc-t06 reader）证明「新增抛异常 override」是**错的**：

1. **SPI 约定 = default opt-out**：三方法 default 即 `Optional.empty()`，语义是「连接器**不支持** MVCC」。`FakeConnectorPluginTest`（fe-core）有显式断言「all three mvcc defaults return Optional.empty() — connector opts out of MVCC」。
2. **无任何连接器 override**：`Iceberg` / `Paimon`（均 MVCC-capable 表格式）/ `Hive` / `Trino` **全部依赖 default**，无一 override。Hudi 若新增抛异常 override = **唯一打破 opt-out 约定**的连接器。
3. **无 production caller**：全仓 `beginQuerySnapshot`/`getSnapshotAt`/`getSnapshotById` 仅出现在 SPI 接口、`ConnectorMvccSnapshot` 类型、`ConnectorMvccSnapshotAdapter`（fe-core，仅测试用）——**fe-core 查询路径从不调用**。抛异常的 override = **不可达死代码**。
4. **T04 已在唯一可触发点 fail-loud**：Hudi 唯一可能请求 snapshot 的位置是 time-travel（`FOR TIME/VERSION AS OF`），`PhysicalPlanTranslator.visitPhysicalHudiScan` SPI 分支（T04，`feceabb`）**已抛 `AnalysisException`**。即便批 E 接通 MVCC 调用链，请求也在到达 metadata 前被 T04 拦截。重复在 metadata 层抛 = 冗余。

**结论**：正确的「unsupported」表达 = **保持 default `Optional.empty()`（与全体连接器一致）+ T04 的 translator 守卫**。T06 是**文档化决策**，非代码任务。完整 MVCC（`HudiMvccSnapshot` 语义、snapshot 透传、增量时序）入**批 E**（与 T03/T04 完整实现、T09–T11、hive/HMS migration 一并），见 [DV-007](../../deviations-log.md)。

## Why no code

- 新增 override 违反 SPI opt-out 约定（R11 conformance）、是不可达死代码（R2 nothing speculative）、与全体连接器分叉（R7 surface conflicts—此处选更一致的一方）。
- T04 已提供唯一可触发路径的 fail-loud；不重复（R3 surgical）。

## Risk Analysis

- **零改动 → 零回归 / 零 live 风险**。
- 行为正确性：gate 关时 MVCC 方法不可达；批 E 翻闸后，time-travel 被 T04 拦截（fail-loud），非 time-travel 查询不请求 snapshot → `Optional.empty()` 的 opt-out 语义正确（连接器不参与 MVCC，走默认 latest 快照语义由 provider 的 `timeline.lastInstant()` 承接，与当前一致）。

## Test Plan

**不适用（无代码）**。SPI default opt-out 行为已由 fe-core `FakeConnectorPluginTest`（T08 三方法返 empty）覆盖。批 E 接通 MVCC 调用链 + 实现 `HudiMvccSnapshot` 后，于 regression 验证 time-travel 语义（与 T04 的批 E regression 同套：`FOR TIME AS OF` 当前 fail-loud，批 E 后返正确快照）。**显式登记，不静默（R12）**。
