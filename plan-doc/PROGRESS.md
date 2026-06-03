# 📊 项目进度仪表盘

> 最后更新：**2026-06-04** | 当前阶段：**P2 trino-connector 代码完成**（T07–T11,T13 ✅；T12 推迟）；PR 待开（分支基线对齐中） | 项目总进度：**30%**
> [README](./README.md) · [Master Plan](./00-connector-migration-master-plan.md) · [SPI RFC](./01-spi-extensions-rfc.md) · [Decisions](./decisions-log.md) · [Deviations](./deviations-log.md) · [Risks](./risks.md) · [Agent Playbook](./AGENT-PLAYBOOK.md) · [Handoff](./HANDOFF.md)

---

## 一、阶段进度（P0–P8）

| 阶段 | 范围 | 估时 | 进度 | 状态 | 任务文档 |
|---|---|---|---|---|---|
| **P0** | SPI 缺口补齐 | 2 周 | ▰▰▰▰▰▰▰▰▰▰ 100% | ✅ 完成（PR #63582 squash-merge `c6f056fa5bd`，T24-T25 流水线全绿）| [tasks/P0](./tasks/P0-spi-foundation.md) |
| **P1** | scan-node 收口 + 重复清理 | 1 周 | ▰▰▰▰▰▰▰▰▰▰ 100% | ✅ 完成（PR [#63641](https://github.com/apache/doris/pull/63641) squash-merged `778c5dd610f`；T1 推迟 P8；T2 推迟 P4/P5）| [tasks/P1](./tasks/P1-scan-node-cleanup.md) |
| **P2** | trino-connector 迁移 | 2 周 | ▰▰▰▰▰▰▰▰▰▰ 100% | ✅ 代码完成（T01-T11,T13；T12 推迟；PR 待开） | [tasks/P2](./tasks/P2-trino-connector-migration.md) |
| P3 | hudi 迁移 | 2 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P4 | maxcompute 迁移 | 2 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P5 | paimon 迁移 | 3 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P6 | iceberg 迁移 | 5 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P7 | hive (+HMS) 迁移 | 6 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P8 | 收尾清理 | 2 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |

**全局进度：12%**（25 周计划中 P0+P1 共 3 周完成）

---

## 二、连接器迁移看板

> 维度："SPI 设计" = RFC 中该连接器涉及的 SPI 是否定稿；"实现" = fe-connector 模块中代码完成度；"SPI_READY" = 是否已加入 `CatalogFactory.SPI_READY_TYPES`；"删除旧代码" = fe-core/datasource/<name>/ 是否清空；"反向 instanceof" = nereids/planner 等热区中 `instanceof XExternal*` 是否清理。

| 连接器 | SPI 设计 | 实现完成度 | SPI_READY | 删除旧代码 | 反向 instanceof | 状态 | 详细 |
|---|---|---|---|---|---|---|---|
| **jdbc** | ✅ | ✅ 100% | ✅ | 🟡 (13 个旧 client，P1 删) | n/a | **95%** | [详情](./connectors/jdbc.md) |
| **es** | ✅ | ✅ 100% | ✅ | ✅ | ✅ | **100%** | [详情](./connectors/es.md) |
| trino-connector | ✅ | ✅ 100% | ✅ | ✅ | ✅ | **100%** | [详情](./connectors/trino-connector.md) |
| hudi | 🟡 | 🟨 50% | ❌ | ❌ | 0/0（寄生 hive） | **20%** | [详情](./connectors/hudi.md) |
| maxcompute | 🟡 | 🟨 60% | ❌ | ❌ | 0/12 | **25%** | [详情](./connectors/maxcompute.md) |
| paimon | 🟡 | 🟨 50% | ❌ | ❌ | 0/10 | **20%** | [详情](./connectors/paimon.md) |
| iceberg | 🟡 | 🟥 10% | ❌ | ❌ | 0/19 | **5%** | [详情](./connectors/iceberg.md) |
| hive (+hms) | 🟡 | 🟥 20% | ❌ | ❌ | 0/31 | **10%** | [详情](./connectors/hive.md) |

---

## 三、当前活跃 task

> 状态非 ✅ 的项，按阶段聚合。详细见各阶段 task 文件。

### P2 — trino-connector 迁移（🚧 进行中）
| ID | Task | 批次 | Owner | 状态 | 启动 | 备注 |
|---|---|---|---|---|---|---|
| P2-T01 | `TrinoConnectorProvider.validateProperties` + `TrinoDorisConnector.preCreateValidation` | 批 A | @me | ✅ | 2026-05-25 | required-property check + preCreateValidation 触发 plugin loading；+20 LOC |
| P2-T02 | `ConnectorPushdownOps.applyFilter` + `applyProjection`（桥接 Trino 原生下推） | 批 A | @me | ✅ | 2026-05-25 | `TrinoConnectorDorisMetadata` 复用 `TrinoPredicateConverter`；+125 LOC；单测推 P2-T11 |
| P2-T03 | `GsonUtils` Trino 三处 `registerSubtype` 替换为 `registerCompatibleSubtype` | 批 B | @me | ✅ | 2026-05-25 | **scope 校正**：必须 atomic replace（避免 RuntimeTypeAdapterFactory 撞名 IAE） |
| P2-T04 | `PluginDrivenExternalCatalog.gsonPostProcess` 加 trinoconnector logType migration | 批 B | @me | ✅ | 2026-05-25 | 新 helper `legacyLogTypeToCatalogType`；`name().toLowerCase()` 不通用 |
| P2-T05 | ~~`ExternalCatalog.registerCompatibleSubtype` 注册~~ | 批 B | @me | ✅ | 2026-05-25 | duplicate of T03，自动满足 |
| P2-T06 | `PluginDrivenExternalTable.getEngine() / getEngineTableTypeName()` 加 trino-connector 分支 | 批 B | @me | ✅ | 2026-05-25 | toEngineName 返 null（保留 legacy 行为） |
| P2-T07 | `CatalogFactory.SPI_READY_TYPES` 加 `"trino-connector"` | 批 C | @me | ✅ | 2026-06-04 | commit `0fe4b8a93d6`；翻闸 |
| P2-T08 | `PhysicalPlanTranslator` 删 `instanceof TrinoConnectorExternalTable` 分支 | 批 D | @me | ✅ | 2026-06-04 | commit `ed81a063fe8`；SPI 分支接管 |
| P2-T09 | `CatalogFactory` 删 `case "trino-connector"` + import | 批 D | @me | ✅ | 2026-06-04 | commit `ed81a063fe8` |
| P2-T10 | 删 `datasource/trinoconnector/` 整目录 + legacy test | 批 D | @me | ✅ | 2026-06-04 | commit `ed81a063fe8`；GsonUtils 不碰（批 B 已处理）；+ExternalCatalog db case（DV-001）|
| P2-T11 | fe-connector-trino 单元测试 | 批 E | @me | ✅ | 2026-06-04 | commit `9bba12a44b2`；3 类/29 测试；无 mock，json/schema 砍（DV-002）|
| P2-T12 | regression-test `trino_connector_migration_compat`（image 兼容） | 批 E | @me | 🟡 | — | **推迟**（无集群/plugin；DV-003）|
| P2-T13 | 同步跟踪文档 + 开 PR | 批 E | @me | ✅ | 2026-06-04 | 文档已同步；docs-next 不在本仓（DV-004）；**PR 待开**（分支对齐）|

详细任务说明、阶段日志见 [tasks/P2-trino-connector-migration.md](./tasks/P2-trino-connector-migration.md)

### P1 — scan-node 收口 + 重复清理（✅ 已完成）
| ID | Task | 批次 | Owner | 状态 | 启动 | 备注 |
|---|---|---|---|---|---|---|
| P1-T03 | `PhysicalPlanTranslator.visitPhysicalFileScan` 收口（保留 fallback） | 批 A | @me | ✅ | 2026-05-25 | `PluginDrivenExternalTable` 分支已前置；7 个老分支保留 |
| P1-T04 | `visitPhysicalHudiScan` 委托给 `PluginDrivenScanNode` | 批 A | @me | ✅ | 2026-05-25 | SPI 分支已加；`incrementalRelation` 待 P3 SPI 扩展 |
| P1-T05 | `LogicalFileScan.computeOutput` 改走 SPI | 批 A | @me | ✅ | 2026-05-25 | `computePluginDrivenOutput` + `supportPruneNestedColumn` 显式分支 |
| P1-T01 | 删除 13 个 `Jdbc*Client.java` + `JdbcFieldSchema.java` | 🚫 推迟 P8 | — | 🚫 | — | 2026-05-25 决议（Q4）：3 个 fe-core caller 是活的 CDC streaming 代码，删除需 SPI 扩展，P8 收尾时一并做 |
| P1-T02 | 重复 PaimonPredicateConverter + McStructureHelper 处理 | 🚫 推迟 P4/P5 | — | 🚫 | — | 用户决议 Q2（2026-05-25） |

### P0 — SPI 缺口补齐（✅ 已完成）
| ID | Task | Owner | 状态 | 启动 | 备注 |
|---|---|---|---|---|---|
| P0-T01 | RFC §16.2 决策点闭环 | @me | ✅ | 2026-05-24 | 全部 18 条决策已敲定 |
| P0-T02 | 项目跟踪机制建立 | @me | ✅ | 2026-05-24 | commit 63159837043 |
| P0-T03 | E3：`ConnectorMetaInvalidator` 接口 | @me | ✅ | 2026-05-24 | spi 包 / 5 invalidate 方法 |
| P0-T04 | E3：`ConnectorContext.getMetaInvalidator()` default | @me | ✅ | 2026-05-24 | 返回 NOOP |
| P0-T05 | E4：`ConnectorTransaction` 继承 `ConnectorTransactionHandle` | @me | ✅ | 2026-05-24 | 新增不替换 |
| P0-T06 | E4：`ConnectorWriteOps.beginTransaction` default | @me | ✅ | 2026-05-24 | throws unsupported |
| P0-T07 | E4：`ConnectorSession.getCurrentTransaction` default | @me | ✅ | 2026-05-24 | Optional.empty() |
| P0-T08 | E5：`ConnectorMvccSnapshot` 类型 + 3 default 方法 | @me | ✅ | 2026-05-24 | mvcc 包 + ConnectorMetadata 3 default |
| P0-T09 | `DefaultConnectorContext.getMetaInvalidator()` impl | @me | ✅ | 2026-05-24 | 返回新建 invalidator |
| P0-T10 | `ExternalMetaCacheInvalidator`（fe-core 新类） | @me | ✅ | 2026-05-24 | 包装 `ExternalMetaCacheMgr`；2 个 no-op 限制留 TODO |
| P0-T11 | `PluginDrivenTransactionManager` 通用化 | @me | ✅ | 2026-05-24 | 新增 `begin(ConnectorTransaction)` 重载；legacy 不变 |
| P0-T12 | `ConnectorMvccSnapshotAdapter`（fe-core 新类） | @me | ✅ | 2026-05-24 | impl `MvccSnapshot` |
| **批 1 DDL + Partition SPI** | | | | | |
| P0-T13 | `ConnectorCreateTableRequest` + 4 spec POJO（ddl 包） | @me | ✅ | 2026-05-24 | 5 个新 final 类 |
| P0-T14 | `ConnectorTableOps.createTable(request)` default | @me | ✅ | 2026-05-24 | 退化到 legacy createTable |
| P0-T15 | `CreateTableInfoToConnectorRequestConverter`（fe-core） | @me | ✅ | 2026-05-24 | 覆盖 4 种 partition + hash/random bucket |
| P0-T16 | `PluginDrivenExternalCatalog.createTable(stmt)` 接通 SPI | @me | ✅ | 2026-05-24 | override + edit log |
| P0-T17 | `listPartitionNames` default | @me | ✅ | 2026-05-24 | emptyList |
| P0-T18 | `listPartitions(handle, filter)` default | @me | ✅ | 2026-05-24 | filter 用 Optional&lt;ConnectorExpression&gt; |
| P0-T19 | `listPartitionValues` default | @me | ✅ | 2026-05-24 | emptyList |
| P0-T20 | `ConnectorPartitionInfo` 追加 rowCount/sizeBytes/lastModifiedMillis | @me | ✅ | 2026-05-24 | UNKNOWN=-1L；3-arg 委托到 6-arg |
| **批 2 守门 + 测试** | | | | | |
| P0-T21 | `tools/check-connector-imports.sh` 实现 | @me | ✅ | 2026-05-24 | grep 守门；正/负冒烟均通过 |
| P0-T22 | exec-maven-plugin 接入脚本（fe-connector aggregator validate） | @me | ✅ | 2026-05-24 | `inherited=false`；RFC §15.4 等价实现 |
| P0-T23 | `FakeConnectorPlugin` + 11 个 default 行为测试 | @me | ✅ | 2026-05-24 | 覆盖 Connector/Metadata/TableOps/WriteOps/Session/Context 全 default |
| P0-T24 | JDBC regression-test 全套跑通 | @用户 | ✅ | 2026-05-25 | PR #63582 流水线绿 |
| P0-T25 | ES regression-test 全套跑通 | @用户 | ✅ | 2026-05-25 | PR #63582 流水线绿 |
| P0-T26 | `ConnectorMetaInvalidator` 路由测试 | @me | ✅ | 2026-05-24 | 5 个 @Test；MockedStatic&lt;Env&gt; |
| P0-T27 | `CreateTableInfoToConnectorRequestConverter` 单元测试 | @me | ✅ | 2026-05-24 | 7 个 @Test；4 partition style + 2 bucket |

完整 P0 任务清单：[tasks/P0-spi-foundation.md](./tasks/P0-spi-foundation.md)

---

## 四、最近 14 天动态

> 倒序，新内容置顶；超过 14 天的条目移除（git log 保留历史）。

- **2026-06-04** ✅ **P2 批 C+D+E 完成**（T07–T11,T13；T12 推迟；PR 待开）：批 C T07 翻闸（`0fe4b8a93d6`）；批 D 删 fe-core legacy trino 代码 14 文件 / −2508（`ed81a063fe8`，含 recon 补回的 `ExternalCatalog` db-case DV-001，保留 MetastoreProperties / 两个 image-compat 枚举 / GsonUtils redirect）；批 E T11 加 3 个纯转换器 JUnit5 测试 29 个全绿（`9bba12a44b2`，无 mock，DV-002）。T12 推迟（无集群/plugin，DV-003）；T13 文档同步本条。**rebase 构建坑**：fe-core 因 stale 生成的 `DorisParser`（grammar 随 #63823 拆到 `fe-sql-parser`）编译失败，clean fe-core 即解。**PR 待开**——`catalog-spi-03` 现基于 master、与 `branch-catalog-spi`（仍 P1，分叉于 #63552）错位（191-commit），分支对齐由用户处理
- **2026-05-25（晚 ④）** ✅ **P2 批 B 完成**（T03+T04+T05+T06 fe-core 桥接）：recon 揭示 HANDOFF 三处描述误差并校正——(1) T03 不能"只加 redirect 不删旧"，必须 atomic replace 否则 `RuntimeTypeAdapterFactory.labelToSubtype` 撞名抛 IAE → FE 起不来；(2) T05 是 duplicate of T03，没有独立的 `ExternalCatalog.registerCompatibleSubtype` API；(3) T04 `name().toLowerCase()` 不通用——`Type.TRINO_CONNECTOR.name().toLowerCase()` 出 "trino_connector" 但 CatalogFactory 期望 "trino-connector"，新增 `legacyLogTypeToCatalogType` helper 做显式 case 映射；(4) T06 `TRINO_CONNECTOR_EXTERNAL_TABLE.toEngineName()` 返 null（switch 没 case，legacy 也是 null），保留此行为不修。3 files / +29 LOC 全在 fe-core。守门：fe-core compile + checkstyle + import gate 全绿。**重要**：批 B 后到批 C T07 翻闸前，新建 trino 目录无法序列化（registerSubtype 已删但 CatalogFactory 仍走 legacy）；不要在中间状态部署
- **2026-05-25（晚 ③）** ✅ **P2 批 A 完成**（T01+T02 fe-connector-trino SPI 补齐）：`TrinoConnectorProvider.validateProperties` 校验 `trino.connector.name` 必填；`TrinoDorisConnector.preCreateValidation` 在 CREATE CATALOG 时触发 `ensureInitialized()` 完成 plugin 加载 + connector factory 解析，把延迟到首次查询的失败前移到 catalog 创建期。`TrinoConnectorDorisMetadata.applyFilter / applyProjection` 桥接 Trino 原生 push-down：复用现有 `TrinoPredicateConverter` 把 `ConnectorExpression` 转 `TupleDomain<ColumnHandle>`，调 Trino `metadata.applyFilter / applyProjection`，把回来的 trino-side `ConnectorTableHandle` 包成新的 `TrinoTableHandle`（保留 column maps）；`remainingFilter` 保守返回原表达式，匹配 legacy fe-core 行为（BE 端继续 re-evaluate）。+143 LOC 跨 3 文件，全部 `fe-connector-trino` 侧（**未触碰 fe-core**，严格守批 A 边界）；import gate + compile + checkstyle 全绿。单元测试推迟到 P2-T11 批 E 一起做
- **2026-05-25（晚 ②）** 🚧 **P2 (trino-connector) 启动 + recon 完成**：用 3 路 Explore subagent 并行调研，输出代码侧 facts —— fe-core 旧目录 10 个 .java / ~1760 LOC、5 个 live external caller（全部机械路由，无 P1-T01 那种"活业务逻辑"问题）；fe-connector-trino 13 类 / 2162 LOC / 0 测试，SPI 表面 ~95% 已覆盖（真缺 validateProperties / preCreateValidation / pushdown ops）；反向 instanceof 实测 1 处（PhysicalPlanTranslator:779）；SPI_READY 翻闸点定位 `CatalogFactory.java:53`；Gson 兼容路径与 ES/JDBC 同 pattern 可复用。**用户决议**：Q1 pushdown ops 纳入 P2 批 A；Q2 fe-core 目录删除时 GsonUtils 三个 class-token 注册同步清。**task 划分定**：13 tasks / 5 批次（A SPI 补齐 / B fe-core 桥接 / C 翻闸 / D 清旧 / E 测试+文档）。P2 task 文件 [tasks/P2-trino-connector-migration.md](./tasks/P2-trino-connector-migration.md) 已建
- **2026-05-25（晚）** ✅ **P1 PR 合入**：PR [#63641](https://github.com/apache/doris/pull/63641) `[P1-T03-T05] route plugin-driven scans first in nereids translator` 流水线全绿，squash-merged 到 `apache/doris:branch-catalog-spi`，hash `778c5dd610f`。本地新分支 `catalog-spi-03` 已建立，承载 P2 工作
- **2026-05-25（白天 ④）** ✅ **P1 阶段关闭**：批 B (T1) recon 揭示 3 个 fe-core JDBC client caller（PostgresResourceValidator / StreamingJobUtils / CdcStreamTableValuedFunction）均为活的 CDC streaming 代码（非 dead code），删除需要在 ConnectorPlugin/ConnectorMetadata 上为 CDC 暴露新 capability（getPrimaryKeys / getColumnsFromJdbc / listTables）。用户决议（Q4）：**推迟 T1 到 P8 收尾**（与 streaming CDC 重构一起做）。P1 in-scope（T3+T4+T5）100% 完成；剩余动作：batch A push + PR
- **2026-05-25（白天 ③）** ✅ **P1 批 A 完成**（T03+T04+T05 scan-node SPI 收口）：`PhysicalPlanTranslator.visitPhysicalFileScan` `PluginDrivenExternalTable` 分支前置（T3）；`visitPhysicalHudiScan` 加 SPI 分支并通过 `FileQueryScanNode` setters 透传 `scanParams`/`tableSnapshot`，`incrementalRelation` 记 P3 TODO（T4）；`LogicalFileScan.computeOutput` 新增 `computePluginDrivenOutput()` helper + 显式 `supportPruneNestedColumn → false` 分支（T5）。fe-core BUILD SUCCESS + checkstyle 0；对当前 SPI 表（JDBC/ES）行为等价；7 个连接器特定分支原地保留作 P3-P7 fallback
- **2026-05-25** ✅ **P0 全阶段完成**：PR [#63582](https://github.com/apache/doris/pull/63582) squash-merge 到 `apache/doris:branch-catalog-spi`（hash `c6f056fa5bd`）；T24/T25 流水线全绿；P0 阶段进度 100%。新本地分支 `catalog-spi-02` 基于最新 base 创建，**P1 启动**（scan-node 收口 + 重复清理，1 周）
- **2026-05-24（夜 ③）** ✅ **P0 批 2 守门 + 单测完成**（T21-T23, T26-T27；T24-T25 用户跑）：新增 `tools/check-connector-imports.sh` grep 守门 + 通过 exec-maven-plugin 在 `fe-connector` aggregator validate 阶段调起（`inherited=false`）；新增 `FakeConnectorPlugin`（fe-core test）+ 23 个新 @Test 覆盖 11 个 default 路径 + ConnectorMetaInvalidator 5 个 routing + Converter 7 个（4 partition style × IDENTITY/TRANSFORM/LIST/RANGE + hash/random bucket + 列穿透）；39/39 tests green；checkstyle 0；JDBC/ES regression-test 转交用户在本地执行
- **2026-05-24（夜 ②）** ✅ **P0 批 1 DDL + Partition SPI 完成**（T13-T20）：新增 `connector.api.ddl` 包 5 个 POJO（CreateTableRequest + 4 spec）；`ConnectorTableOps` 加 4 个 default（createTable(request) + listPartitionNames/listPartitions/listPartitionValues）；`ConnectorPartitionInfo` 追加 rowCount/sizeBytes/lastModifiedMillis；fe-core 新 `CreateTableInfoToConnectorRequestConverter` 覆盖 IDENTITY/TRANSFORM/LIST/RANGE 四种 partition + hash/random bucket；`PluginDrivenExternalCatalog.createTable` 路由到 SPI；fe-core BUILD SUCCESS + checkstyle 0；JDBC/ES 下游 zero-impact
- **2026-05-24（深夜）** ✅ **P0 批 0 fe-core 桥接完成**（T09-T12）：`ExternalMetaCacheInvalidator` + `ConnectorMvccSnapshotAdapter` 新类、`DefaultConnectorContext.getMetaInvalidator()` override、`PluginDrivenTransactionManager` 加 SPI `ConnectorTransaction` 重载（legacy auto-commit 不变）；fe-core 全编译通过 + checkstyle 0 violations；JDBC/ES 下游 zero-impact
- **2026-05-24（晚）** ✅ **P0 批 0 SPI 接口三件套完成**（T03-T08）：`ConnectorMetaInvalidator`、`ConnectorTransaction`、`ConnectorMvccSnapshot` 共 3 个新类型 + 4 个 default 方法；JDBC/ES clean compile 通过，零下游修改
- **2026-05-24** ✅ 项目跟踪机制建立（README、PROGRESS、decisions-log、deviations-log、risks、tasks/、connectors/、AGENT-PLAYBOOK、HANDOFF）
- **2026-05-24** ✅ SPI RFC §16.2 6 个未决问题（U1-U6）全部决议（D-013..D-018）
- **2026-05-24** ✅ SPI RFC v1 落地（[01-spi-extensions-rfc.md](./01-spi-extensions-rfc.md)）
- **2026-05-24** ✅ Master Plan §5 12 个项目决策点（D1-D12）全部确认（D-001..D-012）
- **2026-05-24** ✅ Master Plan v1 落地（[00-connector-migration-master-plan.md](./00-connector-migration-master-plan.md)）
- **2026-05-24** ✅ 初步代码侦察（177 个 fe-connector 文件、408 个 fe-core/datasource 文件、96 处反向 instanceof）

---

## 五、风险监控（active risks）

| ID | 风险 | 影响 | 当前状态 | 触发阶段 | Owner |
|---|---|---|---|---|---|
| R-001 | Image 反序列化兼容回归 | High | 🟢 监控中 | P2-P7 每个迁移 | @me |
| R-002 | Hive ACID 写路径数据不一致 | High | 🟡 待启动 | P7.3 | TBD |
| R-003 | Iceberg Procedure SPI 抽象失败 | Med | 🟢 监控中 | P6.4 | @me |
| R-004 | classloader 隔离打破 SDK 单例 | Med | 🟢 监控中 | P5/P6 | @me |
| R-005 | nereids 写命令深度耦合 | Med | 🟡 待 P6.3 评估 | P6.3 | TBD |
| R-006 | 通过 SPI 性能回归 | Low | ⏸ 未启动 | P0 末加 benchmark | TBD |
| R-007 | FE/BE 共享 jar 冲突 | Low | ⏸ 未启动 | P5/P6 | TBD |
| R-008 | 文档与流程脱节 | Low | 🟢 缓解中 | 全周期 | @me |

完整列表见 [risks.md](./risks.md)（含 R-009..R-014 从 RFC §16.1 迁入的 Q1-Q6 类技术风险）

---

## 六、决策与偏差快速跳转

| 类型 | 总数 | 最新条目 | 文档 |
|---|---|---|---|
| **决策**（D-NNN） | 18 | D-018（U6: ConnectorColumnStatistics 类型契约） | [decisions-log.md](./decisions-log.md) |
| **偏差**（DV-NNN） | 0 | — | [deviations-log.md](./deviations-log.md) |
| **风险**（R-NNN） | 14 | R-014（thrift sink 选择灵活性） | [risks.md](./risks.md) |

---

## 七、Session 协作状态（Agent / Human）

> 当本项目通过 Claude Code 这类 LLM agent 推进时，跟踪当前 session 状态、handoff 状况和 context 健康度。

- **本 session 已完成**：P2 批 C（T07 翻闸 `0fe4b8a93d6`）+ 批 D（T08-T10 删 legacy `ed81a063fe8`）+ 批 E（T11 单测 `9bba12a44b2`）+ T13 文档同步。T12 推迟。本地 fe-core + fe-connector-trino 全绿（compile / test-compile / checkstyle / import-gate）。DV-001..004 已记
- **下一个 session 应做**：(1) 解决 PR base 错位——`catalog-spi-03` 现基于 master，需从远端 `branch-catalog-spi` 拉新分支 cherry-pick 7 个 P2 commit 后开 PR；(2) T12 回归测试在有集群/plugin 的环境补；(3) 之后启动 P3 Hudi 迁移
- **是否需要 handoff**：**是**——用户准备开新 session 跑批 C；本场已 rewrite [HANDOFF.md](./HANDOFF.md)（含 batch B→C regression window 警告 + T07/T08/T09/T10 详细 step-by-step）
- **协作规范**：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)（context 预算、subagent 使用、handoff 触发条件）

---

## 八、维护规则速记

| 何时更新本文件 | 改什么 |
|---|---|
| 完成一个 task | §三表中删除 / 标 ✅；§四加一行 |
| 完成一个阶段 | §一进度条 + §三整体清理 + §四加里程碑 |
| 新增决策 | §四加一行 + §六计数 +1 |
| 发现偏差 | §四加一行 + §六计数 +1 |
| 每周一例行 | §四清过期、§五状态滚动、§七 session 状态 review |

📖 详细规则见 [README.md §4 维护规则](./README.md)
