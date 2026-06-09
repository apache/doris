# [P4-T06e] FIX-AGG-COLUMN-REJECT (GAP5) — design

> 来源：Batch-D 红线扩充对抗复审 workflow `wbw4xszrg`（GAP5，Tier 2，minor）。**证伪 P2-8「非-OLAP 路径已覆盖聚合列」**。
> 用户定夺（2026-06-08）：**Option B — 加 SPI 字段 `isAggregated`**（逐字镜像 P2-8 FIX-AUTOINC-REJECT 的 `isAutoInc`，见 [[catalog-spi-p2-ddl-decisions]]）。
> 关联：legacy 对照 `MaxComputeMetadataOps.validateColumns:426-429`（`if (col.isAggregated())` 抛，**紧邻**已镜像的 auto-inc 分支 :422-425）；`Column.isAggregated():553-555` = `aggregationType != null && != AggregateType.NONE`。

## Problem

翻闸后 `CREATE TABLE (c INT SUM) ENGINE=... ` 对 max_compute（external、非-OLAP）表**静默建普通列**——聚合列（SUM/REPLACE/MAX…）对非-OLAP 外表非法，legacy 显式拒绝，新路丢失该拒绝、悄悄把 `c` 建成无聚合的普通列（数据模型回归，用户意图无声蒸发）。

两使能条件（与 P2-8 auto-inc **同构**）：
1. **nereids 上游不拒非-OLAP 的 bare 聚合列**。唯一 nereids 闸 `ColumnDefinition.validate(isOlap,...)`（`:358-385`）只校验 key+aggType 冲突 / 类型兼容 / GENERIC 需 enable_agg_state；真正拒「非-key 列带 aggType」的 `validateKeyColumns()`（`:1068-1089`）**仅在 `CreateTableInfo.validate()` 的 `ENGINE_OLAP` 块内被调**（`:645`）→ 非-OLAP 外表不可达。`isOlap==false` 时 `validate` 的隐式 aggType 赋值块（`:374-385`）亦被 gate 跳过，故用户写的 aggType 原样留存、无人拒。
2. **SPI 载体无法表示聚合**。`ConnectorColumn` 无 aggType/isAggregated 字段（仅 P2-8 加的 isAutoInc）→ 即便连接器想拒也看不到。

## Root Cause（已核码确认，branch catalog-spi-05）

| 层 | 位置 | 现状 |
|---|---|---|
| SPI 载体丢标志 | `ConnectorColumn`（`fe-connector-api/.../ConnectorColumn.java:25-111`）| 7 字段 `name,type,comment,nullable,defaultValue,isKey,isAutoInc`（:27-33）。**无 isAggregated**。ctor 链 5→6→7-arg（:35-54）。 |
| 转换器丢标志 | `CreateTableInfoToConnectorRequestConverter.convertColumns:90-92` | 用 7-arg ctor 传 `name,type,comment,nullable,null,isKey, getAutoIncInitValue()!=-1`——**从不读 getAggType()**。 |
| 连接器看不到 | `MaxComputeConnectorMetadata.validateColumns:476-498`（`createTable` 内 tableExist 短路后调）| 仅查 empty/null（:477-480）、isAutoInc（:486-490，P2-8）、dup name（:491-494）、可表示类型（:496）。**无 aggregated 查**（标志从未被载）。 |

净：聚合列抵达连接器但不可见 → 静默丢弃。

## Parity Reference（被镜像的 legacy 代码）

legacy `MaxComputeMetadataOps.validateColumns`（`fe-core/.../maxcompute/MaxComputeMetadataOps.java:416-437`），聚合半在 **:426-429**（与 auto-inc :422-425 **相邻**）：

```java
for (Column col : columns) {
    if (col.isAutoInc()) { throw ...; }          // :422-425  ← P2-8 已镜像
    if (col.isAggregated()) {                     // :426       ← 本 fix 镜像
        throw new UserException(
                "Aggregation columns are not supported for MaxCompute tables: " + col.getName());  // :427-428
    }
    ...
}
```

`Column.isAggregated()`（`fe-catalog/.../Column.java:553-555`）= `aggregationType != null && aggregationType != AggregateType.NONE`——本 fix 在转换器侧用 `ColumnDefinition.getAggType()` 复现此布尔。

## Design（用户定 Option B：加 SPI 字段，逐字镜像 P2-8）

**WHY Option B over Option A（FE-core guard）**（用户定夺，2026-06-08）：
- **一致性 / 完整镜像**：聚合拒绝是 legacy `validateColumns` 中 auto-inc 拒绝的**下一行**；连接器 `validateColumns` 已含 `if (col.isAutoInc())`，本 fix 在其后加 `if (col.isAggregated())` = 完成同一方法的 legacy 镜像。P2-8 设计明文将聚合分支记为「out of scope... 仅做 auto-inc」，本 fix 即其遗留续作。
- **同层 parity**：在连接器 `validateColumns` 拒绝 = legacy 同层（非 nereids 早拒）；CTAS + 显式列路径统一覆盖（两路径都过 `createTable→validateColumns`）。
- **additive、零破坏**：8-arg ctor + default `isAggregated=false`，全部既有 call site（5/6/7-arg）原样编译、保持 false（P2-8 同款 pattern，已验 16 文件）。

### 1. SPI `ConnectorColumn`（additive 第 8 字段）

- 加字段 `private final boolean isAggregated;`（isAutoInc 后）。
- 现 7-arg ctor 改为**委托** 8-arg、`isAggregated=false`（保 7-arg 调用方=转换器旧行为，但转换器本 fix 即改 8-arg）。
- 加 8-arg ctor（唯一全赋值）。
- 加 getter `isAggregated()`。
- `equals` 加 `&& isAggregated == that.isAggregated`；`hashCode` 加 `isAggregated`。
- `toString` 不动（聚合非既有文本契约，Rule 3）。

### 2. 转换器 `CreateTableInfoToConnectorRequestConverter.convertColumns`

加 `import org.apache.doris.catalog.AggregateType;`；在循环内算布尔（镜像 `Column.isAggregated()`）并传第 8 arg：
```java
boolean isAggregated = d.getAggType() != null && d.getAggType() != AggregateType.NONE;
out.add(new ConnectorColumn(
        d.getName(), type, d.getComment(),
        d.isNullable(), null, d.isKey(), d.getAutoIncInitValue() != -1, isAggregated));
```

### 3. 连接器 `MaxComputeConnectorMetadata.validateColumns`

在 `if (col.isAutoInc())` 块**后**加（镜像 legacy 相邻分支）：
```java
// MaxCompute has no aggregate-key model; reject aggregate columns (SUM/REPLACE/...),
// mirroring legacy MaxComputeMetadataOps.validateColumns:426-429. The nereids non-OLAP path
// does not reject these (validateKeyColumns is ENGINE_OLAP-gated), so without this the user's
// aggregate intent is silently dropped to a plain column.
if (col.isAggregated()) {
    throw new DorisConnectorException(
            "Aggregation columns are not supported for MaxCompute tables: " + col.getName());
}
```

## Blast Radius

8-arg ctor additive（default `isAggregated=false`）→ 全 25 处 `new ConnectorColumn(` call site（16 文件）：唯一改动 = 转换器（7→8-arg）；其余 5/6-arg 经委托链保持 isAggregated=false（es/jdbc/hive/hudi/iceberg/paimon/trino/hms + MC 读路径 data/part 列 + fe-core PluginDrivenExternalTable/PhysicalPlanTranslator/ConnectorColumnConverter + 各 test）字节不变。无 SPI 方法签名变更（仅加 ctor 重载）。import-gate 净（isAggregated 在 fe-connector-api；getAggType()/AggregateType 在 fe-core 转换器，已可见）。equals/hashCode 加字段是正确不变式（两列仅 isAggregated 异即不同）。

**rebuild**：SPI 模块（fe-connector-api）变 → 须 rebuild api + maxcompute + fe-core。

## Risk Analysis

| Risk | Mitigation |
|---|---|
| 合法非聚合列被误拒 | 闸 = `getAggType() != null && != NONE`，逐字镜像 `Column.isAggregated()`；converter 测钉普通列 → isAggregated=false。 |
| 其余 6 连接器行为漂移 | additive default false（25 call site 全验）；其 producer 从不设聚合、validateColumns（若有）不读它。 |
| equals/hashCode 改动破坏 set/map | 加字段为正确不变式；无生产代码跨 isAggregated 边界 key 集合（全 producer default false）。 |
| 现有 converter 测因 mock 未 stub getAggType 而 NPE/变红 | Mockito mock 未 stub 的 getAggType() 返 null → isAggregated=false（不抛、不改既有断言）；real ColumnDefinition 测列 aggType=null/NONE → false。 |
| CTAS 路径 | 连接器 validateColumns 对 CTAS+显式列统一覆盖（两路径都过 createTable）。 |

## Test Plan

钉 **WHY**（Rule 9）：MaxCompute 无聚合-key 模型；legacy 显式拒（`:426-429`）。静默接受 = 用户聚合意图无声丢弃（数据模型回归）。

### A. SPI equals/hashCode — `fe-connector-api`（扩 `ConnectorColumnTest`）
- `equalsAndHashCodeDistinguishAggregated`：两列仅 isAggregated 异（8-arg `...false,false,false` vs `...false,false,true`）→ `assertNotEquals` + hashCode 异。MUTATION：删 `&& isAggregated == that.isAggregated` → 红。
- `defaultCtorsLeaveAggregatedFalse`：5/6/7-arg ctor → isAggregated=false（锁 additive-default 契约）。

### B. 转换器 passthrough — `fe-core`（扩 `CreateTableInfoToConnectorRequestConverterTest`）
- `aggTypePropagatedAsIsAggregated`：mock ColumnDefinition `getAggType()→AggregateType.SUM`、`getAutoIncInitValue()→-1L` → convert → `isAggregated()==true`。MUTATION：转换器丢第 8 arg / 布尔改常量 false → 红。
- `plainColumnIsNotAggregated`：`getAggType()→null`（或 NONE）→ `isAggregated()==false`（守 boundary）。

### C. 连接器拒绝 — `fe-connector-maxcompute`（扩 `MaxComputeValidateColumnsTest`）
- `aggregatedColumnIsRejected`：`new ConnectorColumn("c", INT, "", false, null, false, false, true)` → `validateColumns` 抛 `DorisConnectorException`，msg 含 `"Aggregation columns are not supported for MaxCompute tables: c"`。MUTATION：删 `if (col.isAggregated()) throw` → 红。
- `nonAggregatedColumnPasses`：isAggregated=false → 不抛（守 over-rejection）。

### E2E（CI 跳）
纯 FE 校验、抛在任何 ODPS RPC 前 → 无需 live ODPS（同 P2-8）。可选 regression 用例：`CREATE TABLE (c INT SUM)` 对 mc 表报含「Aggregation columns are not supported」。

## 决策类型

明确修复（用户定 Fix Option B，Tier 2 minor）。加 SPI 字段 `isAggregated`、逐字镜像 P2-8 isAutoInc + legacy `MaxComputeMetadataOps.validateColumns:426-429`。证伪 P2-8「非-OLAP 已覆盖聚合列」假设（doc-sync：更正 P2-8 design「out of scope，已覆盖」措辞）。
