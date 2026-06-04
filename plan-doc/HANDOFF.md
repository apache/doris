# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-05
- **本 session 主题**：**P3 批 B 编码完成**——T05（applyFilter 真实 EQ/IN 分区裁剪，镜像 Hive）✅ 落地；T06（MVCC/snapshot SPI）经 code-grounded recon + 用户签字定为 **keep default opt-out（零代码）**；两处 scope 校正记 [DV-007]（T05 `listPartitions*` override 推迟批 E、T06 不抛异常 override）。
- **分支**：`catalog-spi-04`（P3 工作分支，基于 `branch-catalog-spi`；P0→P1→P2→recon(D-019) 之上）。工作树 clean（仅本地未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.bak`）。

---

## ✅ 本 session 完成项

| Task | 结果 | commits |
|---|---|---|
| **P3-T05** applyFilter EQ/IN 分区裁剪 | ✅ 镜像 Hive 7 步 + 7 helper；`HudiPartitionPruningTest` 8 测全绿（模块 19）；checkstyle 0 + import-gate 通过 | `10b72d4`(code) + 本 doc commit |
| **P3-T06** MVCC/snapshot SPI | ✅ **keep default opt-out 决策**（[DV-007]，零代码）——recon 证抛异常 override 错（破 opt-out 约定/死代码/T04 已 fail-loud）；完整 MVCC→批 E | 本 doc commit（含 `designs/P3-T06-*` + DV-007）|

**commit stack**（新→旧）：本 doc commit→`10b72d4`(T05)→`301fe38`(批 A handoff)→`2758cf9`(T04 doc)→`feceabb`(T04)→`517c9cf`(T03 defer)→`ac0dc7c`(T02 doc)→`95f23e9`(T02)→`9fcf21a`(recon/D-019)→`0793f03`(P2 #64096)→`2b1a3bb`(P1 #63641)→`72d6d01`(P0 #63582)。

**批 B 净产出** = 1 个正确性/性能修复（分区裁剪 + 修复"分区来源静默切换"，gate 后硬化，零 live 风险，零回归）+ 1 个 code-grounded 决策（MVCC opt-out）。**批 A+B 编码完成**。每 task 走 design 备忘（`tasks/designs/P3-T0X-*.md`）→ impl → build → commit → 5 步文档同步。

---

## 🚧 未完成 / 待办（下一 session = 批 C）

1. **P3-T07（批 C，下一 session 第一件事）**：三模块（hms/hive/hudi）测试基线 + **COW/MOR parity 测试**。当前 fe-connector-hms/hive/hudi **零测试**（hudi 已有 `HudiTypeMappingTest`/`HudiScanRangeTest`/`HudiPartitionPruningTest` 3 个；hms/hive 仍零）。parity = SPI `HudiConnectorMetadata` schema/partition 输出 vs legacy `HiveMetaStoreClientHelper.getHudiTableSchema`（fe-core），COW & MOR 各一。注：parity 也该覆盖 T02 的 column_names/types 列集合+顺序 vs legacy。**Rule 9：测意图**（type-string 编码、裁剪正确、列集合一致）。
2. **批 D（T08）**：`tableFormatType` 分流消费设计备忘（design-only，**不动 fe-core**）。
3. **批 E（deferred，不在 P3 hybrid 编码）**：T03（schema_id/history 完整 field-id evolution，DV-006）、T05 的 `listPartitions*` override（DV-007）、T06 的完整 MVCC（`HudiMvccSnapshot`+snapshot 透传+增量时序，DV-007）、T04 的完整 snapshot 透传+增量 SPI、T09–T11（模型落地/gate flip/删 legacy/集群验证）。并入 hive/HMS migration。
4. **T05 regression 推迟批 E**（gate 关，端到端不可触达；批 E 翻闸后断言带分区谓词查询仅扫匹配分区，explain/profile 校 partition 数；precedent DV-003）。

---

## ⚠️ 关键认知 / 临时发现

### 1.【批 B 已用】SPI 分区裁剪链路 + Hive 是 parity 基准（T05 已修）
- **链路**（end-to-end，spi-invoke recon 确认正确）：`PluginDrivenScanNode.convertPredicate` 调 `metadata.applyFilter` → `currentHandle = result.getHandle()`（:293）→ `getSplits` 把 `currentHandle` 传 `scanProvider.planScan`（:371）→ `HudiScanPlanProvider.planScan` 调 `resolvePartitions`（:137）→ 读 `handle.getPrunedPartitionPaths()`（:287-289）。**plumbing 一直正确，缺的是 applyFilter 里的裁剪逻辑**。
- **T05 修法**：`HudiConnectorMetadata.applyFilter` 忠实镜像 `HiveConnectorMetadata.applyFilter`（:193-234 + 7 helper：extractPartitionPredicates/extractPredicatesRecursive/extractColumnName/extractLiteralValue/prunePartitionNames/parsePartitionName/matchesPredicates）。**关键差异**：Hudi 保留 `List<String> prunedPartitionPaths`（resolvePartitions 喂路径给 FileSystemView，非 HmsPartitionInfo）；上限保留 `-1`（不静默截断，安全于 Hive 100000）。**只裁 EQ/IN over partition cols**，范围/非分区谓词不裁、`remaining=全表达式`交 BE 复评（不漏行）。
- **`ConnectorFilterConstraint.getColumnDomains()` 在 fe-core 侧为空**（`PluginDrivenScanNode.buildFilterConstraint` 传 `Collections.emptyMap()`）→ 唯一可用谓词表示是 `getExpression()`（与 Hive 同）。
- **helper duplicate 不共享**：fe-connector-hudi 仅依赖 fe-connector-hms（**不依赖 fe-connector-hive**，pom 确认）；连接器互 import metadata 类错误分层；import-gate 禁连接器 import fe-core。故复刻，待 P7 hive migration consolidate（同 T02 `toHiveTypeString` 先例）。

### 2.【批 C 必读】SPI partition/MVCC 方法零 live caller（决定批 C 测什么、批 E 做什么）
- **`listPartitions/listPartitionNames/listPartitionValues`（SPI）在 fe-core 零 live caller**：`PluginDrivenScanNode` 不调用（分区走 applyFilter 链路）；`ShowPartitionsCommand`/`HudiExternalMetaCache`/`MetadataGenerator` 调的是 **legacy** metastore 路径（`dorisTable.getRemoteName()`）非 SPI。Hive 基准**也不 override** 这三方法。→ T05 只做 applyFilter 裁剪，`listPartitions*` override 推迟批 E（fe-core 长出 SPI 消费 + `SHOW PARTITIONS` 改走 SPI 时）。
- **MVCC 三方法（`beginQuerySnapshot/getSnapshotAt/getSnapshotById`）无 production caller**（仅 `ConnectorMvccSnapshotAdapter` 测试用）；全体连接器（Iceberg/Paimon/Hive/Trino）**无一 override**，default `Optional.empty()`=opt-out（`FakeConnectorPluginTest` 断言）。→ T06 保持 default + 文档化，**不新增抛异常 override**（会破约定 + 死代码；T04 已在 time-travel fail-loud）。

### 3.【沿用】BE Hudi JNI column_types/names/delta 契约（T02）
- `THudiFileDesc.{delta_logs,column_names,column_types}` thrift `list<string>`；**BE 自做 join**：names `,` / types **`#`** / delta `,`（`hudi_jni_reader.cpp:52-54`），Java `HadoopHudiJniScanner` split 同。**FE 永远传 typed list、不 join/split**；类型串用 Hive 串（`HudiTypeMapping.toHiveTypeString`），**不是** `getTypeName()`。

### 4.【沿用】T03 schema_id/history 为何批 E（DV-006）
- BE `table_schema_change_helper.h:219-267`：`history_schema_info` unset → `by_parquet_name`（鲁棒名匹配）= 现状；裸 `current==file==-1`→`ConstNode`(大小写敏感) **弱于**现状 = 净回归。faithful 需连接器 field-id + Hudi `InternalSchema` + type→thrift（现无），批 E 与 hive/HMS 一并建。

### 5.【沿用】T04 fail-loud + 完整实现去向
- 守卫在 `PhysicalPlanTranslator.visitPhysicalHudiScan` SPI 分支（唯一同时可见 snapshot+incremental 处）。完整 snapshot 透传 + 增量 SPI + MVCC 入批 E。

### 6.（沿用）rebase 后 fe-core 编译坑 + import gate + docs-next
- rebase 后 `fe-core/target/generated-sources/.../DorisParser.java` 残留 → cannot find symbol。**修法：clean fe-core**（非 fe-sql-parser），别当代码 bug 查。
- `tools/check-connector-imports.sh`：双向 import 守门（fe-core ↔ connector）。
- `PhysicalPlanTranslator` 里 hudi **之外**的连接器 `instanceof` 分支待各自 P 阶段迁完再删，**本场只动 hudi 的 SPI 路径**，别乱碰其他。
- 用户向文档在 doris-website 仓（DV-004），本仓只有 `docs/`。

---

## 🎯 下一个 session 第一件事（P3 批 C / T07）

```
1. 自检：
   git branch --show-current → catalog-spi-04
   git log --oneline -6 → <doc commit>(批 B handoff) 10b72d4(T05) 301fe38(批 A handoff) 2758cf9(T04 doc) feceabb(T04) 517c9cf(T03 defer)
   git status → clean（除 .audit-scratch/ conf.cmy/ regression-conf.bak）
   Read plan-doc/tasks/P3-hudi-migration.md（批 C = T07）+ 本文件关键认知 2/3

2. 启动【批 C / T07】（仍 behind 关闭的 gate，零 live 风险）：三模块测试基线 + COW/MOR parity。
   recon 锚点：
     legacy parity 源: fe-core HiveMetaStoreClientHelper.getHudiTableSchema / HMSExternalTable.initHudiSchema(~722-759)
     SPI 被测: HudiConnectorMetadata.getTableSchema / getColumnHandles / applyFilter（已有 3 测试类可扩展）
     hms/hive 模块: 当前零测试——补 metadata/schema 单测基线
   先 recon（可用 workflow，参 .audit-scratch/p3-t0X-recon.workflow.js 模板）→ 设计备忘 → 实现 → 单测 → 守门。
   注意：parity 测试可能需 mock/fake HMS（HmsClient 是 8 方法 interface，可手写替身——见 HudiPartitionPruningTest.FakeHmsClient 先例）；
        legacy 侧在 fe-core，import-gate 禁连接器直接调——parity 比对可能需在 fe-core test 或用固定 golden 值。

3. 守门 / 构建（每 task）：
   mvn -pl fe-connector/fe-connector-hudi -am test -Dmaven.build.cache.enabled=false -DfailIfNoTests=false（cwd=fe/）
   checkstyle: mvn -pl <module> checkstyle:check（test 阶段不跑 checkstyle！需单独跑）+ import-gate（bash tools/check-connector-imports.sh）。
   **Doris checkstyle 禁 static import**（用 Assertions.assertX）。HmsClient 有 9 方法（含 getDatabase + Closeable.close）。

4. 文档同步（每 task，playbook §5.1 五步）：tasks/P3 状态+phase log + PROGRESS §一/三/四/六 + connectors/hudi（+ 新 DV/D 如有）。

5. commit：`[feat|doc](connector) [P3-Tnn] ...`；PR base = `apache/doris:branch-catalog-spi`（base 已对齐）。
```

---

## 📂 P3 关键文件锚点

```
T02（已修）:  fe-connector-hudi/HudiTypeMapping.toHiveTypeString / HudiScanPlanProvider:118-120 / HudiScanRange（typed list）
              BE: hudi_jni_reader.cpp:52-54 / HadoopHudiJniScanner.java:106/113/212
T03（批 E）:  ExternalUtil.initSchemaInfo / HudiScanNode:240,288-324 / BE table_schema_change_helper.h:219-267 / HudiColumnHandle（无 field id）
T04（已修）:  PhysicalPlanTranslator.visitPhysicalHudiScan SPI 分支（两守卫）
T05（已修）:  HudiConnectorMetadata.applyFilter（:144-209，7 步 + 7 helper）/ HudiTableHandle.prunedPartitionPaths
              对标 HiveConnectorMetadata.applyFilter:193-234 + helpers / 链路 PluginDrivenScanNode:289-293,371 → HudiScanPlanProvider.resolvePartitions:287-289
              测试: HudiPartitionPruningTest（FakeHmsClient 替身先例）
T06（决策）:  ConnectorMetadata.java:60-77（MVCC 三 default）/ designs/P3-T06-mvcc-design.md / 无 override（opt-out）
T07（批 C 下一步）: fe-core HiveMetaStoreClientHelper.getHudiTableSchema / HMSExternalTable.initHudiSchema
              SPI HudiConnectorMetadata.getTableSchema/getColumnHandles；hms/hive 模块零测试待补
gate:         CatalogFactory.java:52（SPI_READY_TYPES，不含 hms/hudi——别动）
设计备忘:     plan-doc/tasks/designs/P3-T02-*.md / T04 / T05 / T06
scratch:      .audit-scratch/p3-t0X-*.workflow.js（本地 workflow 脚本，未跟踪）
```

---

## 🧠 给下一个 agent 的 meta 建议

- **批 C 仍是 model-agnostic SPI 硬化/测试网**（gate 关，零 live 风险）；不要碰 `SPI_READY_TYPES`、不删 legacy、不动 hudi 之外的连接器 `instanceof` 分支。
- **批 C 的难点是 parity 比对的可行性**：legacy 在 fe-core、SPI 在连接器模块、import-gate 隔离。先 recon 确认怎么拿 legacy 输出（fe-core test？golden 值？）再设计，别假设能直接跨模块调。
- **测试基准**：hudi 模块已有 3 测试类 + FakeHmsClient 先例（`HmsClient` 8 方法 interface + close 易 fake）；hms/hive 模块零测试，先补 metadata 单测基线。
- 偏差先记 `deviations-log.md`（本场记 DV-007）再改文档；架构/可行性 fork 先问用户（本场 T05 list*/T06 MVCC 两 fork 已问并签字）。
- Maven：cwd=`fe/`；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`；**checkstyle 单独跑**（test 阶段不触发）；**禁 static import**。
```
