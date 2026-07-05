# ENG-1 / F1 完成记录：CREATE 时 iceberg-v3 行级血缘保留列校验漏读 catalog 级 format-version

- 日期：2026-07-04
- 设计：`ENG1-F1-create-v3-rowlineage-catalog-formatversion-design.md`（同目录）
- Status：**DONE**（UT + mutation 全绿；e2e flip-gated 未跑，登记 ENG-3）

## Problem
翻闸后 iceberg catalog 是 `PluginDrivenExternalCatalog`。`CREATE TABLE` 分析期解析 iceberg format-version 时用
`instanceof IcebergExternalCatalog`（翻闸后死码）门控 catalog 级 `table-default/override.format-version` 读取→
退回 emptyMap→表级无 format-version 时恒解析为 2→v3 行级血缘保留列（`_row_id`/`_last_updated_sequence_number`）
校验被 no-op。而连接器建表侧尊重 catalog 级 format-version 真按 v3 建表→FE 按 v2 校验、表按 v3 建，可静默建成含
冲突保留列的 v3 表。

## Root Cause
`CreateTableInfo.getEffectiveIcebergFormatVersion()`（:1160）缺 `PluginDrivenExternalCatalog`（iceberg 类型）
平行臂；master 的 legacy 分析臂 + `IcebergMetadataOps:384-385` ops 时二次校验翻闸后皆死；连接器无补偿校验。

## Fix
`getEffectiveIcebergFormatVersion()` 门控扩为与紧邻 `paddingEngineName`（:918-924）逐点同型的 plugin-iceberg
平行臂：`catalog instanceof PluginDrivenExternalCatalog && ENGINE_ICEBERG.equals(pluginCatalogTypeToEngine(...))`
时同样读 `catalog.getProperties()`。非新 seam（复用文件内既有 `pluginCatalogTypeToEngine` 引擎名判别范式，无新
import）；保留 legacy `IcebergExternalCatalog` 臂（HMS-DLA/legacy）。前端复刻精确对齐 master 分析期报错时机与文案。

- `fe/fe-core/.../nereids/trees/plans/commands/info/CreateTableInfo.java`（getEffectiveIcebergFormatVersion，+6 行含注释）

## Tests
`CreateTableInfoEngineCatalogTest`（复用既有 Env/CatalogMgr/PluginDriven mock 脚手架）新增 5 UT：
1. catalog 级 `table-default.format-version=3` → 解析为 3；
2. `table-override.format-version=3` → 解析为 3；
3. catalog 级 v3 + `_row_id` 列 → 无参 `validateIcebergRowLineageColumns()` 抛 `AnalysisException`（端到端）；
4. 无 catalog 级 format-version → 解析为 2、`_row_id` 允许（不过度触发）；
5. max_compute plugin catalog 即便设 `table-default.format-version=3` → 仍解析为 2（引擎门控正确）。

Mutation（Rule 9/12）2/2 KILLED：M1 删 plugin-iceberg 臂→测试 1/2/3 红；M2 引擎门控改恒 true→测试 5 红。

## Result
- fe-core 目标测试 fresh recompile：`CreateTableInfoEngineCatalogTest` 10/10、`CreateTableInfoTest` 8/8，0 失败；BUILD SUCCESS；checkstyle 0。
- mutation 2/2 KILLED。
- **e2e flip-gated 未跑**（翻闸后才能真建 v3 表）→ 登记 ENG-3（DV/V3 项）。
