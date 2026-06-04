# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-04
- **本 session 主题**：**P2 已合入** `branch-catalog-spi`（#64096）；**P3 Hudi 两轮 recon + 定策略**——code-grounded 确认 HMS-over-SPI 现状（记 DV-005），用户定 **hybrid**（记 D-019），已建 [`tasks/P3-hudi-migration.md`](./tasks/P3-hudi-migration.md)
- **分支**：`branch-catalog-spi`（P0 → P1 → P2 已干净 stack，工作树 clean）

---

## ✅ 本 session 完成项

### 1. P2 trino-connector 已合入主集成分支

`catalog-spi-03` 已 squash-merge 为单条 commit `0793f032662 [feat](connector) P2 migrate trino-connector to catalog SPI (T01-T13) (#64096)`，叠在 `2b1a3bb2197 [P1 #63641]` → `72d6d0109b9 [P0 #63582]` 之上。**旧 HANDOFF 的头号阻塞「PR base 错位（191-commit）」已消失**——`branch-catalog-spi` 已重建到新 master（P0/P1 hash 随之更新）。P2 阶段除 T12（回归测试，DV-003 推迟）外全部完成。

### 2. P3 Hudi 启动 recon（8-agent workflow，code-grounded + 对抗验证）

用户准备启动 P3，要求**根据代码**确认 `fe-connector-hms` 的 SPI 元数据路径是否就绪。结论见下「关键认知 1」——简言之：**原计划「P3 需等 P5/P7 交付 HMS-over-SPI」的依赖假设与代码不符**（读码早已存在但 dormant），**真正阻塞是 catalog 模型错配 + gate 关闭**。verdict `hmsMetadataOverSpiReady=false`（high confidence，2 路对抗验证一致）。已记 **DV-005**，并同步 PROGRESS / connectors/hudi。

### 3. 第二轮 scan/split recon + 定 hybrid 策略 + 建 tasks/P3

第二轮 recon（scan/split 路径，verified）确认**混合 COW-native/MOR-JNI 不是问题**、plumbing 正确（详见关键认知 1b）。基于两轮 recon，用户拍板 **hybrid 策略**（记 **D-019**）：现做 (b) 连接器硬化+测试（behind 关闭的 gate，零 live-path 风险），推迟 (a) 模型落地+cutover 到 hive/HMS migration。已建 [`tasks/P3-hudi-migration.md`](./tasks/P3-hudi-migration.md)（hybrid 范围 + 批 A–E + file:line 锚点），批 A 待启动。

---

## 🚧 未完成 / 待办

1. **P3 Hudi 批 A 待启动**——策略已定 **hybrid**（[D-019](./decisions-log.md)），已建 [`tasks/P3`](./tasks/P3-hudi-migration.md)。下一 session 第一件事见下（批 A T02 起）。批 E（live cutover/模型落地）deferred 到 hive/HMS migration。
2. **T12 回归测试推迟**（DV-003）——`trino_connector_migration_compat`（CREATE CATALOG→image→重启读回 + 旧 image 含 `TRINO_CONNECTOR` 枚举反序列化），需有 Trino plugin + docker/集群环境。
3. **DV-001 后续**：评估 `MetastoreProperties.Type.TRINO_CONNECTOR` + `TrinoConnectorPropertiesFactory` 是否真被 SPI 路径使用（纯死代码可清）。
4. **DV-004 后续**：trino-connector 用户向安装文档在 doris-website 仓补（不在本仓）。

---

## ⚠️ 关键认知 / 临时发现

### 1. 【P3 核心】HMS-over-SPI「读码已存在但 dormant」；真正阻塞是 catalog 模型错配（DV-005，verified high-confidence）

recon（8 agent，读真实方法体 + 2 路对抗验证）的关键事实：

- **读码早已存在且非 stub**（在 `branch-catalog-spi` HEAD，源自更早的 #62183/#62821，一直 dormant 在 gate 后）：
  - `fe-connector-hms` = 共享 **HMS Thrift 客户端库**（`HmsClient`/`ThriftHmsClient`，**不是** ConnectorMetadata）。
  - `fe-connector-hive` = `HiveConnectorMetadata`(type `"hms"`)，真实读路径 + applyFilter 真分区裁剪。
  - `fe-connector-hudi` = `HudiConnectorMetadata`(type `"hudi"`)，从 Hudi Avro MetaClient 读 schema（HMS fallback），COW/MOR 探测，`HudiScanPlanProvider` 快照扫描规划。
  - D-005 区分符 `ConnectorTableSchema.tableFormatType` 已存在并被各连接器写入。
- **但全部未接线（zero live caller）**：`CatalogFactory.SPI_READY_TYPES = {jdbc, es, trino-connector}`（`CatalogFactory.java:52`）不含 hms/hudi → 任何 HMS 系 catalog 永远走 legacy `HMSExternalCatalog`，不进 SPI。
- **真正阻塞 = catalog 模型错配（架构级，用户/设计决策）**：现存连接器注册的是**独立 `"hudi"` catalog type**（`HudiConnectorProvider.getType()=="hudi"`），而 Doris 真实模型是 hudi **寄生**在 `"hms"` catalog 内、以 `HMSExternalTable.DLAType.HUDI` 暴露；fe-core **没有 `"hudi"` catalog type**，且 `PluginDrivenExternalTable` **从不消费** `tableFormatType`（只读 `getColumns()`，按 catalog TYPE 字串路由）。即单个 `"hms"` 连接器**没有 per-table HUDI/HIVE/ICEBERG 分流的 SPI 机制**。
- **其他确认缺口**：增量读无 SPI 表示（P1-T04 的 `visitPhysicalHudiScan` SPI 分支丢弃 `getIncrementalRelation()`；MVCC trio 未实现；4 个 `*IncrementalRelation` 仍在 fe-core）；hive/hudi 未 override `listPartitions*`（Hudi applyFilter 列全部分区**不做**约束裁剪，Hive applyFilter **做** EQ/IN 裁剪）；三个连接器模块**零测试**。
- **已验证非阻塞**：SPI scan/split **通用链路**（`PluginDrivenScanNode.planScan` → BE）已被合入的 trino-connector 走通；**hudi-specific** 的「单 ScanNode 混合 COW-native + MOR-JNI 每-split 格式」正确性才是待验证项。

**→ catalog 模型决策【已定 2026-06-04 = hybrid，[D-019](./decisions-log.md)】**：现做 (b)（批 A–D，behind gate，零 live 风险），推迟 (a)（批 E，并入 hive/HMS migration）。原选项供回溯：

- **(a) hms-first**：把 `HiveConnectorProvider(type="hms")` 接入 `PluginDrivenExternalCatalog` + fe-core 消费 `tableFormatType` 分流，hudi 作薄增量。一次命中真正架构阻塞、契合现存 `type="hms"` 设计；但把 P7(hive/HMS) 范围拉进 P3、触碰 **live 重度使用的 HMS 路径**、零测试网，回归风险大。
- **(b) gate 后建脚手架**：先做 format-dispatch / 增量 SPI hook / MVCC + 补测试（design+stub，**不动 live 路径，零回归**）；但 hudi 不会单独端到端可用，推迟模型决策。
- **(c) 直接 flip gate** —— **否决**（模型错配下 `"hudi"` provider 不可达；把 live hms catalog 推到未测 SPI；增量丢失；高回归）。
- 对抗验证的 framing nit：**不要**在备忘里预设「推荐 (a)」——(a) 让 P3 依赖完成 P7 模型迁移、且在零测试的 live 路径上，是大而易回归的首交付；把 (a)/(b) 留给用户真实选择。

### 1b. 【P3 scan 侧】scan/split 路径 recon 完成（verified；mixed-format 是非问题，parity gap 多数可在 SPI 层修）

第二个 recon（4 reader + synthesis + 2 对抗验证，verdict `spiHudiCanReachBeModuloGate=false`——false **只因 gate/模型未解，不是 plumbing 问题**）：

- **✅ 混合 COW-native + MOR-JNI 不是问题（之前最担心的点，已排除）**：单 `PluginDrivenScanNode` 能正确服务混合格式。node 级 `getFileFormatType()` 只是**默认种子**；真正生效的是 **per-range** `TFileRangeDesc.format_type`（`PlanNodes.thrift:566`），node 级 `TFileScanRangeParams.format_type`（:469）已标 **deprecated**。BE `file_scanner.h:291-295` per-range 优先于 node 级，且**每 range 新建 reader**（`_get_next_reader`），FORMAT_JNI+hudi→HudiJniReader / PARQUET/ORC→native。`HudiScanRange.populateRangeParams`(160-176) 自做 JNI→native 降级，与 legacy `HudiScanNode.setScanParams`(263-276) **逐行等价**。两路对抗验证确认，其中一路尝试 refute 失败。
- **parity gap（FE 侧数据/特性缺口，多数可在 SPI surface 内修，不动 fe-core）**：
  - **HIGH ① schema_id / history_schema_info 全缺**：legacy 每个 native split 设 `THudiFileDesc.schema_id` + `params.history_schema_info`；SPI 都没设 → BE 退化为**列名匹配**（`table_schema_change_helper.h:219-236`），重命名/schema-evolution/大小写不一致的列返 null/错列（**退化非崩溃**）。可经现有 SPI hook `ConnectorScanPlanProvider.populateScanLevelParams`（Paimon/ES 已 override，hudi 没）+ 在 `HudiScanRange` 设 schema_id 修复，**无需 fe-core 改动**。
  - **HIGH ② column_types 双 bug**（对抗验证从 MEDIUM 升级）：(a) 用 `ConnectorType.getTypeName()` 返裸 `"DECIMAL"`/`"STRUCT"`，**丢精度/scale/子类型**（legacy 发 `decimal(10,2)`/`struct<...>`）；(b) `HudiScanRange` 把 column_names/types/delta_logs 用**逗号 join 再 split**，而 Hive 类型串本身含逗号 → decimal/复杂类型元素被**打碎**、names 与 types 长度错位。命中**任何含 decimal/复杂列的 MOR-with-logs JNI split**。修：发完整 Hive 类型串 + 改 typed list 端到端（停止逗号 join/split）。
  - **HIGH ③ time-travel 静默返最新**：snapshot 传到 node（`PhysicalPlanTranslator:835 setQueryTableSnapshot`）但 `HudiTableHandle` 无 snapshot 字段、`HudiScanPlanProvider` 永远用 `timeline.lastInstant` → `FOR TIME AS OF` 静默返最新。修：透传 snapshot，否则 fail-loud。
  - **HIGH ④ 增量读无 SPI 表示**（代码已 defer）：失败模式未验证，**必须 fail-loud 而非静默全扫**。
  - **MEDIUM**：MOR node（node=JNI）下 per-range 降级的 native split 继承 node 级 JNI location-props——很可能 parity（native reader 按 resolved format 派发、直接读 schema_id），但是唯一需 runtime 验证项。
  - **LOW**：`forceJniScanner` 被忽略；runtime-filter 分区裁剪值缺；limit pushdown 被丢（4-arg vs 5-arg planScan，perf 非正确性）；`nested_fields` 两路都没设（at parity）。
- **仍 false 的原因**：gate（`SPI_READY_TYPES` 不含 hms/hudi）+ 关键认知 1 的 catalog 模型错配。**scan plumbing 正确是必要非充分。**
- **对 a/b 的影响**：scan 侧 HIGH 修复项（①schema_id/history、②column_types、④增量 fail-loud）**与模型选择无关、多数在 SPI surface 内可修**——正是选项 (b) 的高价值内容；无论 a/b 都要做。

### 2.（沿用）rebase 后 fe-core 编译坑：stale `DorisParser`

rebase 拉入 #63823（nereids 语法拆到 `fe-sql-parser`）后，`fe-core/target/generated-sources/.../DorisParser.java` 旧生成物残留导致 `LogicalPlanBuilder` 报 cannot find symbol。**修法：clean fe-core**（不是 fe-sql-parser）。任何 rebase 后遇此先 clean，别当代码 bug 查。

### 3.（沿用）P1 fallback `instanceof` 分支

`PhysicalPlanTranslator` 里其余连接器的 `instanceof` 分支待各自 P 阶段迁完再删；本场未动。hudi 的 `visitPhysicalHudiScan` 已有 SPI 分支（P1-T04）但 incrementalRelation 缺口见关键认知 1。**不要乱碰 hudi 之外的连接器分支。**

### 4.（沿用）import gate

`tools/check-connector-imports.sh`：fe-core 不能 import `org.apache.doris.connector.*`。

### 5.（沿用）docs-next 不在本仓（DV-004）

用户向文档在 doris-website 仓；本仓只有 `docs/`。

---

## 🎯 下一个 session 第一件事（P3 Hudi）

```
1. 自检：
   git branch --show-current → branch-catalog-spi
   git log --oneline -3 → 0793f032662 (P2 #64096) → 2b1a3bb2197 (P1 #63641) → 72d6d0109b9 (P0 #63582)
   git status → clean（除 .audit-scratch/ conf.cmy/ 等本地未跟踪物）
   Read plan-doc/tasks/P3-hudi-migration.md（hybrid 范围 + 批 A–E + file:line 锚点）

2. 从 branch-catalog-spi 切 P3 工作分支（catalog-spi-04 / catalog-spi-p3-hudi）。

3. 启动【批 A】（全部 behind 关闭的 gate，零 live-path 风险；不要碰 SPI_READY_TYPES、不删 legacy）。
   第一个 task = P3-T02（column_types 双 bug）：
   - 读 fe-connector-hudi: HudiScanPlanProvider.java（getScanNodeProperties / collectMorSplits 塞 column_types 处）
            HudiScanRange.java（~89/92/95 逗号 join、~194/199/202 split）
            HudiTypeMapping.java（fromAvroSchema().getTypeName() —— 丢精度/子类型）
     对照 fe-core legacy HudiUtils.convertAvroToHiveType（发完整 Hive 类型串）
   - 关键：先读 BE hudi_jni_reader.cpp 确认 JNI scanner 期望的精确串格式（names ',' / types '#'），再改
   - 改：发完整 Hive 类型串（decimal(10,2)/struct<...>）+ 停止逗号 join/split（typed list 端到端）+ 单测

4. 批 A 续：P3-T03（schema_id/history —— override populateScanLevelParams，无需动 fe-core）
            → P3-T04（time-travel/增量 fail-loud）。
   再 批 B（T05 partition 裁剪 / T06 MVCC）→ 批 C（T07 测试基线 + parity）→ 批 D（T08 dispatch 设计 design-only）。

5. 守门 / 构建（每 task）：
   mvn -pl fe-connector/fe-connector-hudi -am test -Dmaven.build.cache.enabled=false -DfailIfNoTests=false
   （cwd=fe/ 或 -f fe/pom.xml）；fe-connector 编译 + checkstyle 0 + import-gate 通过；
   rebase 后 fe-core 编译失败先 clean fe-core（关键认知 2）。

6. 文档同步（每 task）：tasks/P3 状态 + PROGRESS §三 + connectors/hudi（playbook §5.1）。

7. 批 E（T09–T11，deferred）不在本阶段编码——并入 hive/HMS migration（D-019）。
   T12（trino 回归，DV-003）在有集群/plugin 环境补。
```

---

## 📂 P3 关键文件锚点（recon 已定位，附 file:line）

```
gate:          fe-core/.../datasource/CatalogFactory.java:52  (SPI_READY_TYPES，不含 hms/hudi)
legacy 模型:   fe-core/.../datasource/hive/HMSExternalTable.java:208-210 (DLAType enum),
                 250-281 / 297-306 (HUDI 探测), 722-759 (initHudiSchema 走 legacy)
               fe-core/.../datasource/hive/HMSExternalCatalog.java:52 (extends ExternalCatalog，非 PluginDriven)
               fe-core/.../datasource/hive/HudiDlaTable.java (dlaType=HUDI 表对象，在 hive/ 不在 hudi/)
relation 分流: fe-core/.../nereids/rules/analysis/BindRelation.java:483-548
                 (HMS_EXTERNAL_TABLE + dlaType==HUDI → LogicalHudiScan)
scan 桥:       fe-core/.../nereids/glue/translator/PhysicalPlanTranslator.java:828-854
                 (SPI 分支 828-838 丢 incrementalRelation；legacy 分支 840-854 传)
legacy hudi:   fe-core/.../datasource/hudi/  15 文件 ~2403 LOC（top 9 + source/ 6），
                 含 4 个 *IncrementalRelation + HudiScanNode(614 LOC)；live caller 仅 7 个 fe-core 文件，零测试
SPI 已存在:    fe-connector/fe-connector-hms/  (HmsClient/ThriftHmsClient 客户端库，非 ConnectorMetadata)
               fe-connector/fe-connector-hive/HiveConnectorMetadata.java  (type "hms")
               fe-connector/fe-connector-hudi/HudiConnectorMetadata.java  (type "hudi")
                 + HudiConnector(自建 ThriftHmsClient) + HudiScanPlanProvider
               fe-connector/fe-connector-api/ConnectorTableSchema.java:33/58
                 (tableFormatType；fe-core 从不消费 schema 级区分符)
```

---

## 🧠 给下一个 agent 的 meta 建议

- **P3 是架构决策先行**：先 recon scan 路径 + 写模型决策备忘 + 用户签字，**再**编码。别被「读码已存在」误导成「flip gate 就行」（DV-005 选项 c 已否决）。
- 偏差先记 `deviations-log.md` 再改文档（本场记了 DV-005）。
- commit message 沿用 `[feat|refactor|test|doc](connector) [P3-Tnn] ...`；PR base = `apache/doris:branch-catalog-spi`（base 已对齐，不再有 P2 那种错位）。
- Maven：cwd=`fe/` 或 `-f fe/pom.xml`；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`。rebase 后编译失败先 clean fe-core。
- 不要乱碰 P1 fallback 中 hudi 之外的连接器 `instanceof` 分支。
