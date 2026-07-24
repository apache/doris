# FIX-PERF-05 — Design（凭证 gated 目录的表 comment 跨查询缓存）

> 覆盖审计发现 **C9**（information_schema.tables / SHOW TABLE STATUS 每表 loadTable 只为取 comment）。
> 用户 2026-07-18 拍板方向 = **补注释缓存（仅特殊目录）**。基线 HEAD `25fc2bead7a`。行号信 grep。

## Problem（含复核后的范围缩小）

fe-core `FrontendServiceImpl.listTableStatus` 的 `for (TableIf table : tables)` 循环**无条件**（不看请求是否要 comment 列、且在 `table.readLock()` 下**串行**）调 `table.getComment()` → `PluginDrivenExternalTable.getComment`（无缓存字段）→ `IcebergConnectorMetadata.getTableComment` → **每表一次 `loadTable`**（HMS getTable RPC + metadata.json GET / REST loadTable），只为读一句 `comment` 属性。N 表 = N 次串行远端 load。

**⚠ 复核（审计写于 PERF-01 之前，范围已缩小）**：`getTableComment` 现在走 `loadTable → resolveTableForRead → PERF-01 的 IcebergTableCache`（键 `TableIdentifier`，跨查询）。故：
- **普通目录**：首查 N load（顺带把 N 张 raw Table 缓进 tableCache），**重复查询命中 tableCache → 0 load**。BI 高频反复查这个最痛点，普通目录**已被 PERF-01 兜住**。
- **凭证 gated 目录**（`iceberg.rest.session=user` / REST vended-credentials）：tableCache **被 gate 掉为 null**（缓存的 raw Table 带 per-user/vended 凭证，跨用户共享会泄漏）→ `getTableComment` 每次裸 `catalogOps.loadTable` → **无跨查询复用 → 每查询 N load**。这是 PERF-01 未覆盖的残余。
- **任何目录首次查询**：仍 N load（消除需批量 SPI + 改 fe-core 循环 → 违反「fe-core 只出不进」→ 越界）。

## Root Cause & 修法方向

comment 是纯元数据字符串。但**能否跨用户共享取决于目录类型**（红队 HIGH 修正，见下）：
- **REST vended-credentials 目录**：单一静态目录身份，所有 Doris 用户 load 的是**同一张表**，comment 跨用户共享**安全** → 可缓存。
- **`iceberg.rest.session=user` 目录**：授权发生在 `loadTable` 调用本身（用带查询用户 delegated token 的 per-user 目录）。**缓存命中会绕过这次 per-user 授权**：用户 A（有权）load 出 comment 入缓存，用户 B 虽有合法 token（能建 metadata）且过 Doris SHOW 权限、但其 REST 侧对该表**无权**——今天 B 的 live loadTable 抛→`""`（不泄漏），**缓存后 B 命中 A 的条目、拿到 comment 而 B 的 token 从未被校验** = 元数据泄漏，且是**相对现状（session=user 无 tableCache、次次 live per-user）的回退**。故 **session=user 不可缓存**。

修法：**给 comment 单独做一个轻量、无凭证 gate 的缓存，仅在 vended-credentials 且非 session=user 时启用**。恰好补 PERF-01 对 vended 目录的空缺；普通目录零冗余（tableCache 已管重复）；session=user 保持次次 live（安全优先，正确付 N load）。

## Design（连接器侧，零 fe-core 改动）

### 1. 新建 `IcebergCommentCache`（挂 `IcebergConnector`）
- **镜像 `IcebergTableCache`**（键 `TableIdentifier` 直接、非复合——comment 是表级、与快照无关；`invalidateKey`/`invalidateDb`(namespace 相等)/`invalidateAll`）；**值 = comment `String`**；`isEnabled`/`getOrLoad`/`size`/`loadCountForTest`（度量守门）。
- **无凭证 gate**：值纯元数据字符串。
- TTL = `meta.cache.iceberg.table.ttl-second`（同其余缓存，`<=0` 关；comment 随 DDL 变靠 REFRESH 失效，无需 snapshot 键）。

### 2. `IcebergConnector`
- field `commentCache`；构造（**在 `this.tableCache = …` 之后**）
  **`this.commentCache = (IcebergScanPlanProvider.restVendedCredentialsEnabled(this.properties) && !isUserSessionEnabled()) ? new IcebergCommentCache(ttl, cap) : null;`**
  ——**仅 vended-credentials 且非 session=user 时建**（红队 HIGH：session=user 缓存会绕过 per-user 授权泄漏 comment；session=user 与 vended 同开时 `!isUserSessionEnabled()` 为 false → 不建，session=user 语义优先）。普通目录 / session=user → commentCache=null，`getTableComment` 直穿 loadTable（普通目录用 tableCache；session=user 次次 live per-user）。
- `getMetadata` 传 `commentCache`；`invalidateTable/Db/All` 各加 `if (commentCache != null) commentCache.invalidate*`；`commentCacheForTest()`。
- **ttl≤0 语义 load-bearing**（红队 LOW）：vended + ttl≤0 目录 commentCache 虽建但内部 disabled（镜像 `IcebergTableCache` 的 ttl≤0→disabled 映射）→ 不缓存，尊重「无 meta 缓存」意图。镜像必须保留该映射 + 保 ttl≤0 单测。

### 3. `IcebergConnectorMetadata`
- ctor 加 `commentCache` 参 + 字段（便利 ctor 传 null）。
- `getTableComment`：
  ```
  TableIdentifier id = TableIdentifier.of(dbName, tableName);
  return commentCache != null
          ? commentCache.getOrLoad(id, () -> loadTableComment(dbName, tableName))
          : loadTableComment(dbName, tableName);
  ```
  抽 `loadTableComment(db, tbl)` = 现逻辑 `loadTable(new IcebergTableHandle(db, tbl)).properties().getOrDefault(TABLE_COMMENT_PROP, "")`。

## Parity / 正确性

- **同表同 comment**：comment 是表级属性；缓存值 `String`，命中即返回，与裸 loadTable 读同一属性一致。
- **跨用户共享安全**（**仅 vended**）：vended 目录单一静态身份，所有用户 load 同一张表 → comment 跨用户共享安全。**session=user 已排除**（其授权在 per-user loadTable 里，缓存会绕过 → 泄漏，见 Root Cause）。
- **staleness 新窗口**（红队 LOW，诚实记账）：vended 目录本无 tableCache（次次 live），本缓存引入至多 TTL（默认 24h）的 comment 陈旧窗口（靠 REFRESH 失效），是标准缓存权衡，与普通目录对整表早已接受的一致。
- **失败不缓存**：loader（loadTable）抛异常（如 view handle → NoSuchTable / 无 delegated token 被拒）→ `MetaCacheEntry` 不 put → 透传给 `PluginDrivenExternalTable.getComment` 的 catch → `""`（现行为不变），下次重试。
  - **View 局限**（诚实记账）：view 的 `getTableComment` loadTable 抛 → 不缓存 → 每次仍抛一次 loadTable（与今天同）；view comment 本走 `getViewDefinition` 渲染，本任务不覆盖。
- **staleness**：ALTER 改 comment 靠 REFRESH TABLE/DB/CATALOG 失效（与 tableCache 同口径）；TTL 到期自然过期。

## Implementation Plan

1. 新建 `IcebergCommentCache.java`（≈110 行，镜像 `IcebergTableCache` + `loadCountForTest`）。
2. `IcebergConnector`：字段 + 构造（inverse gate：tableCache==null 才建）+ getMetadata 传参 + 三 invalidate（null-guard）+ `commentCacheForTest`。
3. `IcebergConnectorMetadata`：ctor 参 + 字段 + `getTableComment` 路由 + 抽 `loadTableComment`。

## Test Plan

- **单测 `IcebergCommentCacheTest`**（镜像 `IcebergTableCacheTest`/`IcebergFormatCacheTest`）：TTL 内命中同键 loadCount=1、`ttl<=0`/负值关、`invalidate`/`invalidateDb`/`invalidateAll`、loader 异常不入缓存。
- **度量守门（集成）** `IcebergConnectorMetadataTest`（真 InMemoryCatalog / fake ops 返回带 comment 属性的表 + 直接构造带 commentCache 的 metadata）：重复 `getTableComment` 同表 → 远端 loadTable 恰 1 次（`loadCountForTest()==1`），comment 值 == 原始属性。**MUTATION**：不接缓存 → 每次 loadTable → loadCount>1 → 红。
- **连接器 gate（vended-only）+ 失效** `IcebergConnectorCacheTest`：
  - **plain 目录 → `commentCacheForTest()==null`**（tableCache 开 → comment 缓存关，无冗余）；
  - **vended（非 session=user）目录 → `commentCacheForTest()!=null`**；
  - **session=user 目录 → `commentCacheForTest()==null`**（红队 HIGH：per-user 授权不可绕过，保持 live）；
  - **vended + session=user 同开 → `commentCacheForTest()==null`**（session=user 优先）；
  - vended 目录 REFRESH TABLE/DB/CATALOG 逐级清空。
- 全 iceberg 模块 UT 绿 + checkstyle 绿。

## Risk

- **低**：连接器自包含小改动（一新小缓存 + 3 处 wiring），镜像已落地 3 套同形缓存。
- inverse gate（tableCache==null 才建 comment 缓存）是非常规但正确的选择——普通目录已由 tableCache 覆盖，避免双缓存冗余内存；须测试锁死"plain=null / gated=非 null"。
- 首次 N load 硬下限、view 未缓存：诚实记为已知局限，不假装消除。
