# FIX-H4 — hudi 混大小写 Avro 列名 → JNI/MOR reader 崩

> 来源：`plan-doc/task-list-65185-reverify-fixes.md` §H4；证据 `plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §2 H4。
> 批次 0 第 1 条（最小、hudi-only）。

## Problem

翻闸后 hudi-on-HMS 表若列名混大小写（如 `Id/Name/Addr`）**且**走 MOR-带-log 或 `force_jni`，每个 JNI split 硬崩、整查询失败。

## Root Cause

`HudiScanPlanProvider.planScan:180-181` 用 `.map(Schema.Field::name)` 取 JNI reader 列名列表（**原始大小写**），经 `HudiScanRange` → `THudiFileDesc.column_names` 传给 BE。BE `HadoopHudiJniScanner.initRequiredColumnsAndTypes` 用**原始大小写** key 建 `hudiColNameToType`，再对每个 **lowercase** 的 `requiredField` 做精确 `containsKey`——`Id/Name/Addr` 表下 lowercase `id` 不在 `{Id,Name,Addr}` → `throw IllegalArgumentException`。

对比：同类的两条列名路径**已** lowercase——Doris 列 schema（`HudiConnectorMetadata.avroSchemaToColumns:905` `toLowerCase(ROOT)`）和 native `history_schema_info` 字典（`HudiSchemaUtils.buildField:137`）。唯 JNI 列名列表漏改。legacy `HudiScanNode:223` 发的是 lowercase（`map(Column::getName)`，Column 名已在 `HMSExternalTable:749` lowercase）。故这是**回归**。

## Design

`planScan` 的 JNI 列名 `.map(Schema.Field::name)` 改为 `.map(f -> f.name().toLowerCase(Locale.ROOT))`，与 `avroSchemaToColumns:905` / `HudiSchemaUtils:137` 一致。列**类型**顺序不变（仍按 `jniSchema.getFields()` 顺序），名↔类型位置对应不变；Hive ObjectInspector 大小写不敏感，文件读仍解析。

为可离线单测（`planScan` 需 live metaClient，无法离线跑），把该 inline transform 抽成包私有静态 helper `jniColumnNames(Schema)`，最小 seam（仿 `parsePartitionValues`/`chooseJniSchema` 已有范式）。

## Implementation Plan

`HudiScanPlanProvider.java`：
1. 新增 `import java.util.Locale;`（List 之后、Map 之前，checkstyle 顺序）。
2. 新增包私有静态 helper：
   ```java
   static List<String> jniColumnNames(Schema jniSchema) {
       return jniSchema.getFields().stream()
               .map(f -> f.name().toLowerCase(Locale.ROOT))
               .collect(Collectors.toList());
   }
   ```
3. `planScan:180-181` 的 `columnNames = jniSchema.getFields().stream().map(Schema.Field::name)...` 改为 `columnNames = jniColumnNames(jniSchema);`（`columnTypes` 那段不动）。

## Risk Analysis

- 仅改列名大小写、不改顺序/类型 → 名↔类型对应不变；Hive OI 大小写不敏感，文件读不受影响。
- 混大小写但**纯 COW/全小写**表不受此路径影响（`avroSchemaToColumns` 早已 lowercase Doris 列；仅 JNI column_names 这一路曾漏）。
- 无 fe-core 改动、connector-agnostic 铁律无关。

## Test Plan

### Unit Tests
`HudiSchemaParityTest`（复用其 `Id/Name/Addr` 混大小写 `SCHEMA_JSON` fixture）加一条：
```java
Assertions.assertEquals(
    Arrays.asList("id","name","price","event_date","created_at","tags","props","addr"),
    HudiScanPlanProvider.jniColumnNames(schema()));
```
RED（改前）：`jniColumnNames` 不存在 / 若直接断言旧 inline 会得 `Id/Name/...` → 失败。GREEN：lowercase 列表。

### E2E Tests
MOR-带-log / `force_jni` + 混大小写列的读回归 = **live-gated**（需真 hudi 集群），显式登记 gated，不静默略过（Rule 12）。

## 守门结果（DONE，commit `03f4c12dffa`）

`mvn -o -pl :fe-connector-hudi -am test -Dtest=HudiSchemaParityTest` → **BUILD SUCCESS**；`HudiSchemaParityTest` 5 run / 0 fail / 0 err / 0 skip（新 `jniColumnNamesAreLowerCased` + 4 既有）；`You have 0 Checkstyle violations`（全模块）。测试可 RED：fixture 混大小写 `Id/Name/Addr` vs 期望 lowercase，去掉 `.toLowerCase(Locale.ROOT)` 即变红。MOR/JNI 混大小写读端到端 = live-gated（真 hudi 集群）。
