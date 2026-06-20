# Connector: `paimon`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `paimon` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-paimon/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/` |
| **共享依赖** | `fe-connector-hms`（paimon-HMS-flavor 用） |
| **计划迁移阶段** | **P5**（B0–B7 迁移+翻闸已合入 `branch-catalog-spi` #64446 `38e7140ce56`；下一 = **P5-T29 删 legacy**）|
| **当前状态** | ✅ 迁移 + 翻闸已合入（paimon 入 `SPI_READY_TYPES`，FE 走 SPI 路径）；仅剩 **B8 = P5-T29 删 fe-core legacy + maven 依赖** + B9 回归 |
| **完成度** | 95%（B0–B7 全实现并合入：read/DDL/sys-tables(E7)/MVCC(E5)/MTMV桥(E10)/时间旅行/翻闸 + P6 review deviation fix；剩删 legacy(B8/P5-T29)+回归(B9)）|
| **主 owner** | @morningman / TBD |

---

## 迁移 Playbook 进度

> 全部已合入 #64446，除步骤 13（= P5-T29）。
| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | ✅ | fe-core legacy 已盘点（`datasource/paimon/` 30 + `metacache/paimon/` 3 + `systable/PaimonSysTable`）|
| 2 | ✅ | fe-connector 全功能完整（scan/predicate/handle/DDL/sys-table/MVCC/MTMV/时间旅行）|
| 3 | ✅ | 反向 instanceof 已盘点（热区 + infra 死引用）|
| 4 | ✅ | ConnectorMetadata 全实现；flavor 装配=单 Catalog + `createCatalog` flavor switch（D-037，**非** backend 模块——5 个 `fe-connector-paimon-backend-*` 是空壳）|
| 5 | ✅ | validateProperties + preCreateValidation 全 flavor |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ✅ | `SPI_READY_TYPES += "paimon"`（翻闸已合入 #64446）|
| 8-9 | ✅ | GSON 原子转 `registerCompatibleSubtype` + db/table compat |
| 10 | 🟡 | 热区 instanceof 已清（翻闸）；**infra 死引用 8 处待 P5-T29** |
| 11 | ✅ | PhysicalPlanTranslator 删 `PAIMON` 分支（翻闸已合入）|
| 12 | ✅ | 连接器 UT ~300+ 绿 + fe-core PluginDriven* 测 |
| 13 | ⏳ | **删 `datasource/paimon/` = P5-T29（下一 session）** |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ✅ 需要 | 含 bucket spec | |
| E2 Procedures | ❌ 不需要 | **零可迁**：fe-core 无 paimon procedure（expire_snapshots=iceberg、CALL migrate_table=Spark，皆非 paimon）| doc-only no-op |
| E3 MetaInvalidator | 🟡 | paimon-HMS-flavor 需要 | 复用 `fe-connector-hms` |
| E4 Transactions | ✅ 需要 | |
| E5 MvccSnapshot | ✅ 需要 | ✅ **已合入 #64446**（B5 wire 通用 `PluginDrivenMvccExternalTable`→MvccTable 消费 `beginQuerySnapshot`）| 首个 E5 消费者 |
| E6 VendedCredentials | ✅ 需要 | ✅ 已迁（REST flavor）| |
| E7 SysTables | ✅ 需要 | ✅ **已合入**（D-039：复用 live `SysTableResolver`，非 RFC §10 [DV-023]）：连接器 `listSupportedSysTables`+`getSysTableHandle`；fe-core 通用 `PluginDrivenSysExternalTable`+`PluginDrivenSysTable`（报 PLUGIN_EXTERNAL_TABLE）；forceJni binlog/audit_log；`buildTableDescriptor`→HIVE_TABLE | greenfield SPI，未来 iceberg/hudi 复用 |
| E8 ColumnStatistics | 🟡 | snapshot summary 已含部分 | 可选 |
| E9 Delete/Merge sink | 🟡 | merge-on-read 路径 | |
| E10 listPartitions | ✅ 需要 | ✅ **已合入**（连接器 `listPartitionNames/listPartitions/listPartitionValues` + FE 消费 + `partition_columns` key 翻，B5）| |
| **MTMV（无 E 号）** | ✅ 需要 | ✅ **已合入 #64446**：通用 **`PluginDrivenMvccExternalTable`**（capability-selected，源无关，D-042）+ 时间旅行全 parity（AS-OF/tag/branch/@incr，D-043/044）| D-038（P5 内实现）|

---

## 已知特殊性

- **flavor 装配（D-037=单 Catalog）**：6 flavor（hms/filesystem/dlf/rest/jdbc + base）经 `PaimonConnector.createCatalog` 内 flavor switch on `paimon.catalog.type`（MC 一致，拷常量/conf/**每-flavor authenticator** 入模块）。⚠️ 5 个 `fe-connector-paimon-backend-*` 模块只是**空壳**（gitignore `.flattened-pom.xml`，零 src），**不采用**其 backend-SPI 设计。
- **MTMV（D-038）**：✅ 已合入 #64446——翻闸落通用 **`PluginDrivenMvccExternalTable`**（capability-selected，**源无关**，D-042，非 paimon 专类；可复用 iceberg/hudi）implements MTMVRelatedTableIf+MTMVBaseTableIf+MvccTable；paimon 是**首个真消费 E5(MVCC)/E6(vended)/E7(sys-table)** 的 adopter，MC 无先例。
- **重复类 `PaimonPredicateConverter`**（fe-core `source/PaimonPredicateConverter` vs 连接器版）：连接器版 TZ 已 parity-correct（NTZ 保 UTC、LTZ 不下推，D4）；**fe-core 重复版 = P5-T29 删除目标**（P1-T02 推迟项）。
- BE 经 JNI（**及 C++ native** `paimon_cpp_reader`）调 paimon-reader；连接器经 `ConnectorScanPlanProvider.getSerializedTable` 序列化 `Table`。BE 冻结不动；序列化身份是契约（Base64 非 blocker，BE 有 STD fallback；须 pin paimon-core 版本三方对齐）。
- **测试**：连接器测试模块已建（no-mockito recording seam，~300+ 测）+ FE→BE serde round-trip smoke + parity baseline（live-e2e CI-gated `enablePaimonTest`）。
- 详尽 code-grounded 分析见 [recon](../research/p5-paimon-migration-recon.md) + [P5 设计 doc](../tasks/P5-paimon-migration.md)。

---

## 关联

- 阶段 task：[tasks/P5-paimon-migration.md](../tasks/P5-paimon-migration.md)（30 TODO / B0–B9 批）
- recon：[research/p5-paimon-migration-recon.md](../research/p5-paimon-migration-recon.md)
- 决策：D-037（flavor=单 Catalog + switch）、D-038（MTMV/MVCC P5 内实现，翻闸 gated）、**D-039**（B4 E7=复用 live SysTable 机制非 RFC §10）、D7（B3 DDL authenticator=legacy parity）、D-006（cache 放连接器内）、D-005（HMS flavor 走 tableFormatType）
- 偏差：**DV-023**（RFC §10 E7 设计被 B4 取代）、**DV-024**（B4 修 B2 遗留 BE 描述符 SCHEMA_TABLE→HIVE_TABLE）
- 风险：R-004（classloader）、R-007（FE/BE 共享 jar）、R-012（snapshotId 类型）

---

## 进度日志

### 2026-06-20（阶段里程碑 · 迁移+翻闸合入 #64446）
- **B0–B7 全完成并 squash-合入 `branch-catalog-spi`**（PR **#64446 / `38e7140ce56`** + `e9c5b3e70ce` 修编译）：B5 MTMV 桥（通用 `PluginDrivenMvccExternalTable`，D-040/041/042）+ B5b 时间旅行全 parity（AS-OF/tag/branch/@incr，D-043/044）+ B6 procedure no-op + **B7 翻闸**（入 `SPI_READY_TYPES` + GSON 原子 compat + D-045/046/047 restore SHOW PARTITIONS/SHOW CREATE）+ P6 全路径 clean-room review 全部 deviation fix。
- **下一 = P5-T29（B8 删 fe-core legacy + maven 依赖）**：见 [tasks/P5 §P5-T29 执行计划](../tasks/P5-paimon-migration.md)（DEAD `datasource/paimon/`(30)+`metacache/paimon/`(3)+`systable/PaimonSysTable`；硬前置=迁出 `PaimonExternalCatalog` 常量；STILL-CONSUMED `property/metastore/Paimon*`(7) 保留；maven 方案 A/B）。

### 2026-06-10（B0–B4 实现里程碑，未提交）
- **B4（本 session，T16-T20）= sys-tables E7 + MVCC E5**：连接器 SPI `listSupportedSysTables`/`getSysTableHandle`（D-039 复用 live `SysTableResolver` 机制）；fe-core 通用 `PluginDrivenSysExternalTable`/`PluginDrivenSysTable`；forceJni(binlog/audit_log)；`buildTableDescriptor`→HIVE_TABLE（同修 B2 遗留 [DV-024]）；sys 表 fail-loud 拒 time-travel/scan-params；E5 三方法（inert until B5）+ caps。3-lens 复审 1 BLOCKER（scan-path 丢 forceJni）已修。连接器 124 绿 + fe-core 100 绿。
- B0–B3 此前已落（测基建 / flavor 装配 / normal-read / DDL metadata；见 tasks/P5 阶段日志）。
- 下一 = B5 MTMV 桥（接活 E5 + GAP-LISTPART-AT-SNAPSHOT + `partition_columns` key 翻 + FE 消费 listPartitions）。

### 2026-06-09
- P5 kickoff：14-agent code-grounded recon + cross-cut 对抗复审；产 recon + 设计 doc（30 TODO/B0–B9）。
- 用户签字 D-037（flavor=单 Catalog + switch）、D-038（MTMV/MVCC P5 内实现，翻闸 gated on 它）。
- 证伪 3 先验：backend 模块空壳（非已建工厂）、FE 分发部分已预接（残留=连接器 listPartitions）、Base64 非 blocker（BE 有 STD fallback）。

### 2026-05-24
- 跟踪文件建立。scan 路径已就绪，但 6 个 catalog flavor + MVCC + sys-tables + vended creds 都还在 fe-core。
