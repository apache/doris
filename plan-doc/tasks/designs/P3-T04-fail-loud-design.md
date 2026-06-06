# P3-T04 设计 — time-travel + 增量读 fail-loud 守卫

> 批 A / scan 正确性 · behind 关闭的 gate · 零 live-path 风险（SPI hudi 分支 dormant，gate 关时不可达）
> 关联：[tasks/P3](../P3-hudi-migration.md) · [HANDOFF 1b HIGH ③④](../../HANDOFF.md) · [DV-005](../../deviations-log.md)
> 状态：设计完成（code-grounded）

---

## Problem

SPI hudi 路径对两类查询**静默给错结果**：

- **time-travel**（`FOR TIME AS OF` / `FOR VERSION AS OF`）：`PhysicalPlanTranslator.visitPhysicalHudiScan` SPI 分支（`PhysicalPlanTranslator.java:835`）把 snapshot 经 `setQueryTableSnapshot` 设到 node，但 `HudiScanPlanProvider.planScan`（`HudiScanPlanProvider.java:103`）**永远用 `timeline.lastInstant()`**、忽略 snapshot → **静默返最新**。
- **增量读**（incremental relation）：SPI 分支（`:828-838`）**根本不传** `hudiScan.getIncrementalRelation()`（仅 legacy 分支 `:848` 传）；SPI 无任何增量表示 → **静默全量扫描**。

二者都是**正确性 bug（静默错结果）**，不是性能问题。

## Root Cause

- snapshot 透传链路未接：node 拿到 snapshot 但 provider 不消费。
- 增量读在 SPI 层无模型（`IncrementalRelation` 是 fe-core 概念，4 个 `*IncrementalRelation` 类仍在 fe-core；P1-T04 已知 gap）。
- 完整实现（snapshot 透传到 provider + 增量 SPI 表示 + MVCC）属**批 E**（与 hive/HMS migration、模型落地一并），见 T06/批 E。

## Design

**仅做 fail-loud（task 范围）**：在 `visitPhysicalHudiScan` 的 SPI 分支**顶部**显式报错，绝不静默。这是**唯一**同时可见 snapshot + incrementalRelation 的位置（SPI surface 拿不到 incrementalRelation），故守卫只能落在该 dormant 分支（gate 关时不可达，零 live 风险）。

```java
if (table instanceof PluginDrivenExternalTable) {
    // Fail loud: SPI hudi 路径尚不支持 time travel / 增量读（provider 永远读最新，
    // 增量 relation 无 SPI 表示）。静默返最新/全扫会给错结果。完整支持推迟批 E。
    if (hudiScan.getIncrementalRelation().isPresent()) {
        throw new AnalysisException("Hudi incremental read is not yet supported via the "
                + "catalog SPI; it is deferred to the Hudi connector migration.");
    }
    if (hudiScan.getTableSnapshot().isPresent()) {
        throw new AnalysisException("Hudi time travel (FOR TIME/VERSION AS OF) is not yet "
                + "supported via the catalog SPI; it is deferred to the Hudi connector migration.");
    }
    ... // 原 node 创建逻辑；删去已被守卫覆盖为 dead 的 setQueryTableSnapshot 行
}
```

- 守卫只在**真有** time-travel/增量时触发；普通快照查询 `Optional.empty()` → 正常通过，零影响。
- `AnalysisException`（`org.apache.doris.nereids.exceptions.AnalysisException`，unchecked，已 import `:76`）= nereids 用户向错误的惯用类型。
- 守卫后 `hudiScan.getTableSnapshot()` 必为空 → 原 `:835` 的 `ifPresent(setQueryTableSnapshot)` 成 dead，删除（surgical）。更新 `:825-827` 注释为新行为 + 批 E 指向。

## Implementation Plan

1. `PhysicalPlanTranslator.visitPhysicalHudiScan` SPI 分支加两守卫 + 删 dead `setQueryTableSnapshot` 行 + 更新注释。
2. 守门：`mvn -pl fe-core -am compile`（fe-core 大，rebase 后失败先 clean fe-core，关键认知 2）；checkstyle 0。

## Risk Analysis

- **零 live 风险**：SPI 分支仅当 `table instanceof PluginDrivenExternalTable`（即 hudi 走 SPI）才进；gate 关（`SPI_READY_TYPES` 不含 hms/hudi）→ hudi 永远是 `HMSExternalTable`，**该分支运行期不可达**。改动是硬化 dormant 代码。
- **不碰** SPI_READY_TYPES / legacy / 其他连接器 `instanceof` 分支（HANDOFF 关键认知 3）。
- **行为改进**：从「静默错结果」→「显式报错」。对 legacy 路径（line 844+）零影响。

## Test Plan

### Unit Tests

**不适用（显式登记，不静默——CLAUDE.md R12）**：守卫在 fe-core nereids translator 的 **dormant 分支**，gate 关时**运行期不可达**；单测需构造 `PhysicalHudiScan` + `PlanTranslatorContext` + `PluginDrivenExternalTable`（重 nereids 脚手架），且无法真正 exercise（分支不可达）。抽 boolean-helper 仅为测一个近乎恒真的 2-行守卫 = 为单用代码加抽象（违 R2）、测试近同义反复（违 R9）。

**验证推迟批 E**（与 T03/DV-006、T11 一致；precedent DV-003）：批 E 翻闸后在 regression-test 加 `FOR TIME AS OF <ts>` / 增量查询 → **断言报错**（而非静默最新/全扫）。本 task 的正确性由 code review + 编译保证；运行期断言入批 E。

### E2E Tests

同上：推迟批 E（gate 关，端到端不可触达）。
