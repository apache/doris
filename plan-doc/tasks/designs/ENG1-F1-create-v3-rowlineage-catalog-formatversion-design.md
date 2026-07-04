# ENG-1 / F1 修复设计：CREATE 时 iceberg-v3 行级血缘保留列校验漏读 catalog 级 format-version

- 日期：2026-07-04
- 来源：ENG-1 能力孪生审计 F1（唯一有正确性后果的确认缺口）
- 证据源：`plan-doc/reviews/P6.6-ENG1-capability-twin-audit-2026-07-04.md` §三 F1
- 用户裁定（2026-07-04）：**前端活路径复刻校验**（非连接器侧拒绝）；本轮只修此一条正确性缺口。

---

## Problem

翻闸后 iceberg catalog 运行时类型是 `PluginDrivenExternalCatalog`。`CREATE TABLE` 分析期的 iceberg v3
行级血缘保留列校验（`_row_id` / `_last_updated_sequence_number` 在 format-version ≥ 3 下必须拒绝，因为
v3 表读取时会追加同名隐藏血缘列）依赖 catalog 级 `table-default.format-version` /
`table-override.format-version` 来解析真实 format-version。翻闸后该解析用 `instanceof IcebergExternalCatalog`
门控（对 plugin catalog 恒 false）→退回 `Collections.emptyMap()`→表级无 format-version 时解析为 **2**→v3 校验
被 no-op。而连接器建表侧（`IcebergSchemaBuilder.buildTableProperties`）**尊重** catalog 级 format-version、真按
v3 建表 → **FE 按 v2 校验、表按 v3 建**，可静默建成含冲突保留列的 v3 表，CREATE 时零报错。

### 触发场景
```sql
-- catalog 设 table-default.format-version = 3（或 table-override.format-version = 3）
CREATE TABLE ctl.db.t (_row_id BIGINT);   -- 无表级 format-version
```
- master（legacy）：分析期抛 `AnalysisException`「Cannot create Iceberg v3 table with reserved row lineage column: _row_id」。
- branch（翻闸后）：静默建成 v3 表且含用户列 `_row_id`；读路径又对 v3 表追加同名隐藏血缘列 → schema 冲突/歧义。

---

## Root Cause

`fe/fe-core/.../nereids/trees/plans/commands/info/CreateTableInfo.java:1160-1167`
`getEffectiveIcebergFormatVersion()` 只对 `catalog instanceof IcebergExternalCatalog`（翻闸后死码）读取
catalog 级 format-version，缺 `PluginDrivenExternalCatalog`（iceberg 类型）平行臂。master 另有 ops 时二次校验
（`IcebergMetadataOps:384-385` 带 catalogProperties），翻闸后亦死；连接器 `IcebergConnectorMetadata.createTable`
/ `IcebergSchemaBuilder` 无保留列名校验 → 活路径无任何补偿。

注意：v3 校验方法 `validateIcebergRowLineageColumns()` 本身**对 plugin iceberg 会跑**——引擎名经
`paddingEngineName`/`pluginCatalogTypeToEngine`（:918-947）padding 成 `ENGINE_ICEBERG`，:800-801 的
`engineName.equalsIgnoreCase(ENGINE_ICEBERG)` 门控成立。缺的**只是** format-version 解析里的 catalog 级读取臂。

---

## Design

在 `getEffectiveIcebergFormatVersion()` 补一条与紧邻 `paddingEngineName`（:918-924）**逐点同型**的
plugin-iceberg 平行臂：当 catalog 是 `PluginDrivenExternalCatalog` 且 `pluginCatalogTypeToEngine(...)` ==
`ENGINE_ICEBERG` 时，同样读取 `catalog.getProperties()`。

```java
private int getEffectiveIcebergFormatVersion() {
    CatalogIf catalog = Strings.isNullOrEmpty(ctlName) ? null
            : Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlName);
    if (catalog instanceof IcebergExternalCatalog
            || (catalog instanceof PluginDrivenExternalCatalog
                && ENGINE_ICEBERG.equals(pluginCatalogTypeToEngine((PluginDrivenExternalCatalog) catalog)))) {
        return IcebergUtils.getEffectiveIcebergFormatVersion(properties, catalog.getProperties());
    }
    return IcebergUtils.getEffectiveIcebergFormatVersion(properties, Collections.emptyMap());
}
```

### 为何不是新 seam（符合用户铁律）
- `PluginDrivenExternalCatalog` cast + `pluginCatalogTypeToEngine`（引擎名而非 `instanceof Iceberg*`）判别 plugin
  iceberg，是本文件翻闸已确立的孪生范式，**同一文件 :920-924 的 `paddingEngineName` 逐字如此**。非新增 `if(iceberg)`。
- `CreateTableInfo` 本就含 iceberg 感知代码（`validateIcebergRowLineageColumns`/`getEffectiveIcebergFormatVersion`/
  `pluginCatalogTypeToEngine`），本修复只补齐既有孪生臂的一处遗漏（与 H-10 同型），不引入新 import（`IcebergUtils`/
  `PluginDrivenExternalCatalog`/`ENGINE_ICEBERG` 均已在文件内）。
- 保留原 `IcebergExternalCatalog` legacy 臂（HMS-DLA/legacy 仍需，Rule 3 不动）。

### 为何不落连接器侧
用户裁定前端复刻。且前端修复精确对齐 master 的**分析期**报错时机与文案（`IcebergConnectorMetadata`/
`IcebergSchemaBuilder` 无既有测试断言此校验，连接器侧修改会改报错时机到执行期、并需另配 e2e）。

---

## Implementation Plan

1. 改 `getEffectiveIcebergFormatVersion()`（单方法，~3 行门控扩展），无新 import。

---

## Risk Analysis

- **过度触发风险**：新臂仅当 catalog 是 plugin iceberg 时读 catalog 属性；`IcebergUtils.getEffectiveIcebergFormatVersion`
  只读 `table-override./table-default.format-version` 两键，对非该键属性无副作用。且 `getEffectiveIcebergFormatVersion()`
  仅经 `validateIcebergRowLineageColumns()`（ENGINE_ICEBERG 门控）调用，reachable path 下 catalog 必为 iceberg。低风险。
- **回归风险**：原 emptyMap 分支保留给非 iceberg；legacy `IcebergExternalCatalog` 臂保留。仅新增 plugin iceberg 一臂，
  对既有非 iceberg / legacy 路径零影响。
- **表级 format-version 优先级不变**：`IcebergUtils.getEffectiveIcebergFormatVersion` 内 override>table>default 顺序
  由 IcebergUtils 决定，本修复不改该逻辑，只把 catalog 属性喂进去。

---

## Test Plan

### Unit Tests（加入既有 `CreateTableInfoEngineCatalogTest`，已有 Env/CatalogMgr/PluginDriven mock 脚手架）

1. **catalog 级 v3 → 解析为 3**：plugin iceberg catalog 设 `table-default.format-version=3`，
   `getEffectiveIcebergFormatVersion()` 反射调用返回 3（直接测修复点）。
2. **catalog 级 v3 + 保留列 → 抛异常**（端到端用户可见行为）：同上 catalog + `_row_id` 列，无参
   `validateIcebergRowLineageColumns()` 反射调用抛 `AnalysisException`。
3. **table-override 亦生效**：`table-override.format-version=3` 同样解析为 3。
4. **无 catalog 级 format-version → 解析为 2**（不过度触发）：plugin iceberg catalog 无该属性，
   `_row_id` 列不抛（v2 允许）。
5. **非 iceberg plugin catalog 不读 catalog 属性**（引擎门控正确）：max_compute plugin catalog 即便设了
   `table-default.format-version=3`，解析仍为 2（走 emptyMap 分支）——证明新臂被 `pluginCatalogTypeToEngine`
   正确限定在 iceberg。

### Mutation（Rule 9/12）
- 删除新 plugin-iceberg 臂 → 测试 1/2/3 转红（证明测试锚定修复）。
- 把新臂 `ENGINE_ICEBERG.equals(...)` 改成恒 true（去引擎门控）→ 测试 5 转红（证明门控被测）。

### E2E
- flip-gated（翻闸后才能真建 v3 表），本轮不跑；登记进 ENG-3。校验逻辑由上述 UT + master parity 保证。
