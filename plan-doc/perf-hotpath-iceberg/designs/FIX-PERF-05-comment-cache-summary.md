# FIX-PERF-05 — Summary（凭证 gated 目录的表 comment 跨查询缓存）

> 权威设计见 [`FIX-PERF-05-comment-cache-design.md`](./FIX-PERF-05-comment-cache-design.md)。本文件只记落地结果。
> commit `aea3ebdd40e`（`[perf]`，iceberg 自包含）。基线 HEAD `25fc2bead7a`。用户 2026-07-18 拍板 = 补注释缓存（仅特殊目录）。

## Problem（含范围缩小）

fe-core `FrontendServiceImpl.listTableStatus` 每表**无条件** `getComment` → 连接器 `getTableComment` → **每表一次 loadTable** 只为读 `comment`。N 表 = N 次串行远端 load，BI 高频触发。
**复核（审计早于 PERF-01）**：`getTableComment` 现走 `loadTable → resolveTableForRead → IcebergTableCache`（PERF-01）→ **普通目录重复查询已命中缓存**。残余 = **凭证 gated 目录**（tableCache=null）次次重载。

## Fix（连接器侧，零 fe-core 改动）

- 新增**无 gate** `IcebergCommentCache`（键 `TableIdentifier`、值 comment `String`，镜像 `IcebergTableCache`）。
- **仅当 vended-credentials 且非 session=user 时构建**（`restVendedCredentialsEnabled && !isUserSessionEnabled()`）；普通目录（tableCache 已覆盖）与 session=user 均 null。`getTableComment` 有缓存则查、否则直穿 `loadTableComment`。
- 失败不缓存（view loadTable 抛 → 透传 caller catch → `""`，行为不变）。REFRESH TABLE/DB/CATALOG 失效。

## 安全范围（为何不用 `tableCache==null` 做 gate —— 红队 HIGH）

**session=user 故意排除**：其授权在 per-user loadTable 调用本身；共享 comment 缓存会把 A 加载的 comment 发给 token 从未被校验的 B = 元数据泄漏，且是相对现状（session=user 无缓存、次次 live per-user）的**回退**。vended 单一静态身份、所有用户 load 同一表 → 共享安全。红队原判 `tableCache==null` 过宽（含 session=user），已收窄。

## 红队（7 攻击）

- **HIGH**：`tableCache==null` gate 把 session=user 卷入 → 泄漏；改 vended-only（`&& !isUserSessionEnabled()`）——已修。
- **LOW**：镜像须保 `ttl≤0→disabled` 映射（vended 无缓存目录不缓 comment）——已保 + 单测。
- **LOW**：vended 新增 comment 陈旧窗口（≤TTL，REFRESH 失效）——记入 parity。
- 其余（范围缩小、异常/view 契约、失效完整、键用远程名、无冗余）均 SURVIVES。

## Tests

- **单测 `IcebergCommentCacheTest`**（8）：TTL 命中 loadCount=1、不同表不同键、`ttl<=0`/负值关、invalidate/DB/All、失败不缓存可重试。
- **度量守门（集成）** `IcebergConnectorMetadataTest`：带 commentCache 的 metadata 重复 `getTableComment` 同表 → `loadCountForTest()==1` + 远端 `loadTable` 恰 1（`ops.log` 计数）+ 与直读 parity。**MUTATION**：不接缓存 → 每次重载 → 红。
- **连接器 gate + 失效** `IcebergConnectorCacheTest`（+2）：**plain→null / vended非session→非null / session=user→null / vended+session=user→null**；vended REFRESH TABLE/DB/CATALOG 逐级清空。

## Result

- 全 iceberg 模块 **973 pass / 0 fail / 0 error / 1 skip**（`install -am`），checkstyle 0 违规，0 回归。
- 减负：**vended 目录**重复 `information_schema.tables` 从"次次 N load"→"命中缓存近 0 load"；普通目录不变（PERF-01 已覆盖）；session=user 不变（安全优先，正确付 N load）。
- 诚实局限：任何目录**首次** N load 硬下限（不改 fe-core 消不掉）；view comment 未缓存。
- 可复用：第 4 套 `TableIdentifier`-keyed MetaCacheEntry 缓存；**「gate 是授权决策非凭证泄漏决策」判据**（session=user 授权在 load 调用里，缓存会绕过 → 与「值含凭证才 gate」正交，二者叠加）。
