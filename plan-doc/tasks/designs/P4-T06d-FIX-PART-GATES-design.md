# P4-T06d · FIX-PART-GATES — 分区可见(SHOW PARTITIONS / partitions() TVF)+ 分区裁剪恢复

> issue 5 / 6,phase 3 分区,sev=major,layer=fe-core(+新 cache 类)。来源: `P4-cutover-fix-design.md` §FIX-PART-GATES(:300-389,verdict=needs-revision)+ review DDL-C1/CACHE-C1/CACHE-C2 + READ-P3/CACHE-C-SELECT/CACHE-P1。
> **用户决策(2026-06-07)**: OQ-6 = **(b) 新增 `PluginDrivenSchemaCacheValue` 缓存子类**(非每次重取);scope = **一并恢复分区裁剪**(`supportInternalPartitionPruned` + `getNameToPartitionItems`)。
> 已据 recon(workflow `wvccvhv38`,7 readers)+ 当前代码树重新推导;parent critic 5 项更正全折入(逐条标 ✅)。

## Problem(翻闸即坏,3 缺口)
翻闸后 MC catalog = `PluginDrivenExternalCatalog`,表 = `PLUGIN_EXTERNAL_TABLE`。对真实分区 MC 表:
1. **DDL-C1/CACHE-C1** `SELECT * FROM partitions(...)` → `PartitionsTableValuedFunction` analyze 双网关(catalog allow-list + table-type)不含 PluginDriven → 抛 `AnalysisException`/`MetaNotFound`;已接好的 BE handler(`MetadataGenerator.dealPluginDrivenCatalog`)成死代码。
2. **CACHE-C2** `SHOW PARTITIONS` → `ShowPartitionsCommand:263-266` 调 `table.isPartitionedTable()`,`PluginDrivenExternalTable` 无 override → `TableIf` default false → 抛 "is not a partitioned table"。(T06c 已接 allow-list/表类型/dispatch/handler,独缺此门。)
3. **READ-P3/CACHE-C-SELECT** 分区裁剪丢失 → `PluginDrivenExternalTable` 不暴露任何分区 API(`getPartitionColumns`/`getNameToPartitionItems`/`supportInternalPartitionPruned` 全默认)→ 大分区表退化整表扫。

## Root Cause
- `PluginDrivenExternalTable.initSchema()`(:78-109)取 `ConnectorTableSchema` 后**丢弃 `getProperties()`**(含 `partition_columns` prop,producer `MaxComputeConnectorMetadata.java:160`),只 `new SchemaCacheValue(columns)`(base 类,无 partition 字段)→ 无处可读分区列。
- 无分区 API override → `ExternalTable` 默认 `getPartitionColumns`=empty(:468)、`getNameToPartitionItems`=empty(:457)、`supportInternalPartitionPruned`=false(:478);`TableIf.isPartitionedTable`=false(:364)。
- `PartitionsTableValuedFunction.analyze()`(:172-176 catalog allow-list、:184-185 table-type、:200-204 非分区守卫)keyed on legacy `MaxComputeExternalCatalog`/`MAX_COMPUTE_EXTERNAL_TABLE`,无 PluginDriven 分支(eager analyze in ctor :149/:131)。

## Design

### A. 新增 `PluginDrivenSchemaCacheValue`(OQ-6=b)
`fe/fe-core/.../datasource/PluginDrivenSchemaCacheValue.java`,extends `SchemaCacheValue`,mirror `HMSSchemaCacheValue` 最小模式但多存 remote 名:
```java
public class PluginDrivenSchemaCacheValue extends SchemaCacheValue {
    private final List<Column> partitionColumns;          // Doris 列(mapped 名),供 getPartitionColumns + types
    private final List<String> partitionColumnRemoteNames; // raw 远端名,供索引 ConnectorPartitionInfo.values
    public PluginDrivenSchemaCacheValue(List<Column> schema, List<Column> partitionColumns,
            List<String> partitionColumnRemoteNames) { super(schema); ... }
    public List<Column> getPartitionColumns() { return partitionColumns; }
    public List<String> getPartitionColumnRemoteNames() { return partitionColumnRemoteNames; }
}
```
✅ **parent gap PROP-SOURCING**: 用缓存子类(非每次 `getTableSchema()` 重取),mirror legacy 缓存、避热路径远端往返。✅ **parent gap COLUMN-NAME-MAPPING**: 同时存 raw + mapped 名,解析时把 prop 的 raw 名经 `fromRemoteColumnName` 映射后匹配 mapped 列(MC 默认 identity,但通用对 remapping 连接器成立)。

### B. `PluginDrivenExternalTable.initSchema()` 扩展(填充分区列)
在现有 mapped columns 之后,读 `partition_columns` prop、解析 raw→mapped、过滤出分区 Doris 列,产 `PluginDrivenSchemaCacheValue`:
```java
List<Column> columns = ConnectorColumnConverter.convertColumns(mappedColumns);
// partition_columns prop = raw 远端列名 CSV(producer: MaxComputeConnectorMetadata:160)
List<Column> partitionColumns = new ArrayList<>();
List<String> partitionColumnRemoteNames = new ArrayList<>();
String partProp = tableSchema.getProperties().get("partition_columns");
if (partProp != null && !partProp.isEmpty()) {
    Map<String,Column> byName = columns.stream().collect(toMap(Column::getName, c->c, (a,b)->a));
    for (String rawName : partProp.split(",")) {
        rawName = rawName.trim();
        if (rawName.isEmpty()) continue;
        String mapped = metadata.fromRemoteColumnName(session, dbName, tableName, rawName);
        Column col = byName.get(mapped);
        if (col != null) { partitionColumns.add(col); partitionColumnRemoteNames.add(rawName); }
    }
}
return Optional.of(new PluginDrivenSchemaCacheValue(columns, partitionColumns, partitionColumnRemoteNames));
```
注:`columns` 已含分区列(连接器 `initSchema` append,mirror legacy);此处仅**标识**哪几列是分区列,不重复加列。

### C. `PluginDrivenExternalTable` 分区 API override(mirror legacy MaxComputeExternalTable:83-114)
```java
@Override public boolean isPartitionedTable() { makeSureInitialized(); return !getPartitionColumns().isEmpty(); }     // CACHE-C2 门
@Override public List<Column> getPartitionColumns(Optional<MvccSnapshot> s) { return getPartitionColumns(); }
public List<Column> getPartitionColumns() {                                                                          // 读缓存子类
    makeSureInitialized();
    return getSchemaCacheValue().map(v -> ((PluginDrivenSchemaCacheValue) v).getPartitionColumns()).orElse(emptyList());
}
@Override public boolean supportInternalPartitionPruned() { return !getPartitionColumns().isEmpty(); }   // ⚠见决策①
@Override public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> s) {           // READ-P3
    if (getPartitionColumns().isEmpty()) return emptyMap();
    List<String> remoteNames = getSchemaCacheValue()
            .map(v -> ((PluginDrivenSchemaCacheValue) v).getPartitionColumnRemoteNames()).orElse(emptyList());
    List<Type> types = getPartitionColumns().stream().map(Column::getType).collect(toList());
    // 单次远端 round-trip(CACHE-P1 已定:per-query 直连,无二级 cache),mirror MaxComputeExternalMetaCache.loadPartitionValues
    <build session/metadata/handle like fetchRowCount>;
    List<ConnectorPartitionInfo> parts = metadata.listPartitions(session, handle, Optional.empty());
    List<String> names=...; List<List<String>> values=...;   // names=p.getPartitionName(); values[i]=remoteNames.map(p.getPartitionValues()::get)
    TablePartitionValues tpv = new TablePartitionValues();
    tpv.addPartitions(names, values, types, Collections.nCopies(names.size(), 0L));   // 与 legacy 同一构造(ListPartitionItem, isHive=false)
    // invert idToPartitionItem via partitionIdToNameMap(mirror MaxComputeExternalTable:109-113)
    return nameToPartitionItem;
}
```

### D. `PartitionsTableValuedFunction.analyze()` 双网关 + 守卫(DDL-C1/CACHE-C1)
- SEAM1 catalog allow-list(:172-173):`|| catalog instanceof PluginDrivenExternalCatalog`(**ADD,不删 MaxCompute 分支** 🔴红线)。
- SEAM2 table-type(:184-185):`, TableType.PLUGIN_EXTERNAL_TABLE`。
- SEAM3 非分区守卫(:200-204 旁):`else if (table instanceof PluginDrivenExternalTable && !((PluginDrivenExternalTable) table).isPartitionedTable()) throw "Table X is not a partitioned table"`。
- imports:`PluginDrivenExternalCatalog`、`PluginDrivenExternalTable`。

### E. SHOW PARTITIONS — 零改 `ShowPartitionsCommand`(C 的 isPartitionedTable override 自然放行 :263-266;allow-list/dispatch/handler T06c 已接)。

## 决策与须显式登记的偏差(Rule 7/12)
- **决策① `supportInternalPartitionPruned()` 改为 keyed-on-partition-columns(非 legacy MC 的无条件 `true`)**: `MaxComputeExternalTable` 是 MC 专属故可 `return true`;`PluginDrivenExternalTable` 被 **jdbc/es/trino + max_compute 共享**,无条件 true 会令非分区连接器从 default false 翻 true(行为变更)。`!getPartitionColumns().isEmpty()` 对 MC 分区表 = true(裁剪恢复),对 MC 非分区表 = false(与 legacy true **可观察等价** —— 无分区列时 `initSelectedPartitions` 本就 NOT_PRUNED),对 jdbc/es/trino = false(零变更)。✅ parent 额外风险(状态不一致)由此规避。
- **决策② TVF SEAM3 守卫用 `!isPartitionedTable()`(分区列空)而非 legacy MC 的 `getPartitions().isEmpty()`(分区实例空)**: 二者对"有分区列但 0 实例"的空分区表有别 —— legacy 抛 "not a partitioned table",本设计放行返 0 行(与 SHOW PARTITIONS 一致、更正确)。登记为**有意 minor 偏差**(parent 设计 B 已选 isPartitionedTable)。
- ✅ **parent gap 列名映射**: prop 存 raw 名、schema 存 mapped 名;B 经 `fromRemoteColumnName` 桥接(MC identity 故今等价,通用对 remapping 连接器成立)。
- ✅ **parent gap NPE 不变式**: `supportInternalPartitionPruned==true` ⇒ `getPartitionColumns` 非空(决策①)⇒ 仅"分区列非空但 0 实例"时 `getNameToPartitionItems` 空。该结构与 legacy MC **完全相同**(MC 亦 supportInternalPartitionPruned=true + 可空 map),空 map ⇒ 空裁剪 ⇒ 无 name 错配 ⇒ 不 NPE。继承 MC 既有安全性,不额外加固(Rule 3)。
- ✅ **parent gap 性能偏差**: `getNameToPartitionItems` per-call 远端 `listPartitions`(无二级 cache)—— **CACHE-P1 已定的 cutover 方向**(二级 cache 成死代码、改 per-query 直连,一致性更安全),登记。
- ✅ **parent gap partition_values() TVF 出范围**: `PartitionValuesTableValuedFunction:132-134` 仅支持 HMS('Currently only support hive table'),MC legacy 即抛、非回归、无 Batch-D 红线 → **不动**(区分而非漏;recon 建议加 SEAM4/5 被本设计**否决**,遵 parent 设计 + critic)。
- ✅ **Batch-D 红线**: 本 fix **新增** `PartitionsTableValuedFunction:173` 旁的 PluginDriven 分支(首次令"T06c 已加"假设成真);Batch-D 删 :173 MaxCompute 分支须**排在本 fix 后**。需更新 Batch-D 设计 :70-77/:102 amendment 措辞("T06c adds"→"FIX-PART-GATES adds")+ decisions-log D-028。
- **cast 安全**: `getSchemaCacheValue()` 翻闸后恒产 `PluginDrivenSchemaCacheValue`(runtime cache,FE 重启重建,无跨重启旧值);无条件 cast,mirror `MaxComputeExternalTable` cast `MaxComputeSchemaCacheValue` 的既有模式。

## Implementation Plan(fe-core only,`-pl :fe-core -am`)
1. [fe-core][new] `PluginDrivenSchemaCacheValue.java`(extends SchemaCacheValue)。
2. [fe-core] `PluginDrivenExternalTable.java`: initSchema 填分区列(B)+ 4 override(C)+ imports(`Type`/`PartitionItem`/`MvccSnapshot`/`ConnectorPartitionInfo`/`Maps`/`Map`/`Map.Entry`/`Collections`/`stream`)。
3. [fe-core] `PartitionsTableValuedFunction.java`: SEAM1/2/3 + 2 imports(D)。
4. [docs] commit ②: Batch-D 设计 amendment 措辞 + decisions-log D-028 ordering(本 fix 先于 Batch-D 删 :173)。
5. **不涉及**: fe-connector(已 expose partition_columns/listPartition*)、fe-connector-api、be、thrift、`ShowPartitionsCommand`、`PartitionValuesTableValuedFunction`。

## Risk
- 回归面: C/D 仅新增 override/分支;非分区连接器经决策① 零变更。`PluginDrivenSchemaCacheValue` cast 仅对 PluginDriven 表(其 initSchema 恒产该子类)。
- 🔴 Batch-D 红线守住(只增不删 :173)。
- checkstyle: 新类 license 头 + 新 import 顺序(`Type`/`PartitionItem`/`MvccSnapshot` 等)须过 import-gate。

## Test Plan(UT,fe-core,`-pl :fe-core -am`;Rule 9 mutation 自证)
- **`PluginDrivenExternalTablePartitionTest`(新)**: Testable 子类 override `getSchemaCacheValue()` 返 `PluginDrivenSchemaCacheValue`,+ mock catalog→connector→metadata(`listPartitions` 返 2 个 `ConnectorPartitionInfo`)。断言:
  - `isPartitionedTable()` true(有分区列)/ false(空);mutation: 改 `!isEmpty`→`false` 令 true 用例红。
  - `getPartitionColumns()` 返正确 Doris 列(mapped 名、顺序)。
  - `getNameToPartitionItems()` key=分区名("p=v"),value=`ListPartitionItem` 含正确值;空分区列→emptyMap。mutation: 用本地名/错列序索引 values 令值断言红。
  - `supportInternalPartitionPruned()` = 有分区列时 true、无时 false(锁决策①;mutation: 无条件 true 令"无分区列"用例红)。
- **initSchema 分区填充**: 驱动 initSchema(stub connector 返带 `partition_columns` prop 的 `ConnectorTableSchema` + `fromRemoteColumnName`),断言产 `PluginDrivenSchemaCacheValue` 且 partitionColumns/remoteNames 正确(含 raw≠mapped 用例锁列名桥接)。
- **`PartitionsTableValuedFunctionPluginDrivenTest`(新/扩)**: PluginDriven catalog + PLUGIN_EXTERNAL_TABLE 过 analyze 双网关(不抛 not-allowed/MetaNotFound);非分区 PluginDriven 表抛 "not a partitioned table"。需 CatalogMgr/Env mock(参 DDL routing test 模式)。
- **扩 `ShowPartitionsCommandPluginDrivenTest`**: 新增驱动 `validate()`/analyze 网关用例(现有用例反射直调 handler 跳网关),分区表不抛、非分区表抛 —— 锁 isPartitionedTable 门(CACHE-C2)。
- E2E(p2 真实 ODPS,user-run):`test_external_catalog_maxcompute.groovy`/`test_max_compute_schema.groovy`/`test_max_compute_partition_prune.groovy` 的 `show partitions` + 新增 `partitions()` TVF 断言;翻闸态由 FAIL→PASS。CI 默认跳过,守门靠 UT。

## 成功标准
新类 + override + TVF 网关编译过;Checkstyle=0;新/改 UT 全绿且 mutation 自证;Batch-D 红线未破;对抗 review ≤5 轮收敛。

## Review 轮次(2 轮收敛)
详见 `plan-doc/reviews/P4-T06d-FIX-PART-GATES-review-rounds.md`。
- **Round 1** `needs-revision`: 4 findings 全 test-quality(F6/F13/F16 minor + F15 major),production code CLEAN —— TVF 测试 stub 了 `db.getTableOrMetaException(name, types...)` 绕过真实表类型 allow-list,SEAM-2 覆盖 vacuous;正向用例无断言、null 解析可 vacuous 通过。F9(per-call 远端往返)= already-registered-non-goal(本设计 CACHE-P1)。修法 test-only:`invokeAnalyze` 改 `Mockito.mock(DatabaseIf.class, CALLS_REAL_METHODS)` 仅 stub 单参 resolver + `table.getType()`,跑真实 allow-list;正向加 `verify(table).isPartitionedTable()`。
- **Round 2** `converged`: 3 lens 一致 resolved,无新缺陷。
- mutation 总账:round-1 4 红(initSchema raw→mapped / getNameToPartitionItems 远端名 / SEAM-3 守卫 / 决策① gating)+ round-2 双红×2(删 allow-list PLUGIN / 删 SEAM-3 块)。最终 UT 38/38、CS=0、BUILD SUCCESS。

> **测试实现要点(供防回退)**: TVF analyze 网关测试用 `Mockito.mock(PartitionsTableValuedFunction.class, CALLS_REAL_METHODS)` + 反射调私有 `analyze()`(无实例态);`DatabaseIf` 用 `CALLS_REAL_METHODS` 跑真实 `getTableOrMetaException` 成员检查(仅 stub 单参 resolver + `table.getType()`),使 SEAM-2 非 vacuous;`checkTblPriv` 用 `nullable(ConnectContext.class)` + `any(PrivPredicate.class)` 消两个 5 参重载歧义。
