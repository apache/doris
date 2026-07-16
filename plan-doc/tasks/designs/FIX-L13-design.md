# FIX-L13 — Doris→Paimon 建表丢嵌套 nullability

> reverify §1 L13 (P5-4)。严重度 🟡低。模块 = fe-connector-paimon（连接器局部）。

## Problem

`CREATE TABLE` / CTAS 建 paimon 表时，Doris 列类型经 `PaimonTypeMapping.toPaimonType` 反向映射为 paimon
`DataType`。对**嵌套子类型的 nullability**——ARRAY 元素、MAP value、STRUCT 字段——当前**一律丢弃**声明的
`NOT NULL`，落盘为可空。这让 `ARRAY<INT NOT NULL>` 建成 `ARRAY<INT>`（元素可空），物理 schema 与 DDL 不符。

## Scope 界定（重要）

reverify 标题写「发 4-arg `DataField(id,name,type.copy(isChildNullable),comment)`」，但**comment 半已登记**：
`deviations-log` **DV-035 §(c) M10.1**（用户 D-057 签字，2026-06-12）已把「CREATE 嵌套 struct comment 丢」
**接受为 display-only 偏差**。故本条 scope = **仅嵌套 nullability**，**不含 comment**（加 comment 会重开已接受偏差，
且非本 bug）。field-id 亦不改：legacy `toPaimonRowType` 用 `new AtomicInteger(-1).incrementAndGet()` 顺序 id，
现码同（`fieldId.incrementAndGet()`），保持 parity。

## HEAD recon（对 HEAD 复核）

`PaimonTypeMapping.java`：
- `:253-254` ARRAY — `new ArrayType(toPaimonType(children.get(0)))`：**元素 nullability 丢**。
- `:255-259` MAP — key 已 `.copy(false)`（legacy 强制非空，正确保留）；value
  `toPaimonType(children.get(1))`：**value nullability 丢**。
- `:269-281` `toPaimonRowType` — `new DataField(id, name, toPaimonType(children.get(i)))`：**字段 nullability 丢**。

`ConnectorType`（fe-connector-api）**已具备**所需输入（无需 SPI 加法）：
- `isChildNullable(int index)` — 未携带时默认 `true`（`:242-244`）。
- `getChildrenNullable()`、工厂 `arrayOf(elem, elementNullable)` / `mapOf(k,v,valueNullable)` /
  `structOf(names, types, fieldNullable, comments)` 均已按索引编码 nullability。
- paimon `DataType.copy(boolean isNullable)` 返回同类型改 nullability（现码 MAP key `.copy(false)` 已用）。

## Legacy parity target

legacy `DorisToPaimonTypeVisitor`/`PaimonUtil`（已删）：ARRAY/MAP/STRUCT 子类型经 `.copy(isNullable)` 保留声明
nullability，MAP key `.copy(false)`。iceberg 兄弟 `IcebergSchemaBuilder` 亦按 required/optional 保留嵌套
nullability（本条参照其模式）。

## Design（surgical，纯 `.copy(isChildNullable)`）

```java
case "ARRAY":
    return new ArrayType(
            toPaimonType(type.getChildren().get(0)).copy(type.isChildNullable(0)));
case "MAP":
    // key 强制非空（legacy .copy(false)）；value 保留声明 nullability。
    return new MapType(
            toPaimonType(type.getChildren().get(0)).copy(false),
            toPaimonType(type.getChildren().get(1)).copy(type.isChildNullable(1)));
```

`toPaimonRowType`：
```java
fields.add(new DataField(
        fieldId.incrementAndGet(), fieldName,
        toPaimonType(children.get(i)).copy(type.isChildNullable(i))));
```

- 递归语义正确：子的**深层**嵌套 nullability 由递归调用处理；子在**本层**的 nullability 由本层
  `.copy(isChildNullable(i))` 施加（nullability 是父对子的视图属性，存于父的 `childrenNullable`）。
- 顶层列 nullability（列 `NOT NULL`）由 `PaimonSchemaBuilder` 顶层 RowType 构造处理，**不在本条 scope**。
- 默认行为兼容：未携带 nullability 时 `isChildNullable` 默认 `true` → `.copy(true)`；paimon 类型缺省即可空，
  故对未声明 NOT NULL 的旧路径**逐字节不变**。

**不改**：comment（DV-035 M10.1 已接受）、field-id（顺序 id parity）、scalar 臂、char/decimal/timestamp 臂。

## Risk

- `.copy(true)` 是否与「原本就可空」逐字节等价？paimon `DataType` 默认 `isNullable=true`，`toPaimonType`
  返回的子类型默认可空 → `.copy(true)` 是恒等。红队须确认无 `equals`/序列化差异（用现有 parity 测兜底）。
- MAP key 保持 `.copy(false)`：不回退既有正确行为。

## Test Plan

### Unit（RED-able）— `PaimonTypeMappingToPaimonTest`

- **新** `nestedNullabilityPreservedForArrayElement`：`ConnectorType.arrayOf(ConnectorType.of("INT"), false)` →
  断言结果 `ArrayType` 元素类型 `isNullable()==false`。MUTATION：丢 `.copy` → 元素可空 → RED。
- **新** `nestedNullabilityPreservedForMapValue`：`ConnectorType.mapOf(key, value, /*valueNullable*/ false)` →
  value 非空 + key 恒非空。MUTATION：value 丢 `.copy` → RED。
- **新** `nestedNullabilityPreservedForStructField`：`ConnectorType.structOf(names, types, [false,true], comments)` →
  第 0 字段非空、第 1 可空。MUTATION：DataField 丢 `.copy` → RED。
- **回归守卫**：未声明 nullability 的既有嵌套 parity 测（若有）保持绿（默认 `.copy(true)` 恒等）。

### E2E（live-gated，登记）

`CREATE TABLE ... (a ARRAY<INT NOT NULL>, m MAP<STRING, BIGINT NOT NULL>, s STRUCT<x:INT NOT NULL>)` on paimon
catalog → `DESCRIBE`/paimon SDK 读回 schema，断言嵌套非空落盘。→ 真集群回归。

---

## 设计红队结论（`wf_05574ccb-bd2`，3 lens · 全 SOUND，无 changes-required）

- **legacy-parity SOUND**：逐一核对 legacy `DorisToPaimonTypeVisitor`:array=`elementResult.copy(array.getContainsNull())`、
  map=`key.copy(false)+value.copy(getIsValueContainsNull())`、struct=`fieldResults.get(i).copy(field.getContainsNull())`——
  本 fix 经 `type.isChildNullable(idx)` 逐一镜像。CREATE 桥 `ConnectorColumnConverter.toConnectorType` 从**同** Doris 源
  (`getContainsNull`/`getIsValueContainsNull`)填 `childrenNullable`→本 fix **有效非惰性**。comment 丢=DV-035 M10.1 已接受(确认 scope)。
- **correctness SOUND**（关键风险已证真）：paimon 1.3.1 字节码证 `DataType.copy(boolean)` 每子类型均有;`equals/hashCode/serializeJson`
  均折入 `isNullable`→`.copy(true)` 对默认可空子类型**逐字节等价**(恒等)。顶层列 `PaimonSchemaBuilder:127` `.copy(col.isNullable())`
  不 clobber 嵌套(ArrayType.copy 保留 elementType)。
- **MINOR（test）已折入**：新 struct 测须经 `field.type().isNullable()` 断言(**非** DataField equals/description——comment 丢致
  description=null,equals 会因无关原因假 RED)。已按 `.type().isNullable()` 写。
