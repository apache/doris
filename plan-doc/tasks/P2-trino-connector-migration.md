# P2 — trino-connector 迁移

> 阶段总览见 [00-master-plan §3.3](../00-connector-migration-master-plan.md)。
> 协作规范见 [AGENT-PLAYBOOK.md](../AGENT-PLAYBOOK.md)。
> 连接器看板：[connectors/trino-connector.md](../connectors/trino-connector.md)。

---

## 元信息

- **状态**：🚧 进行中（recon 完成，task 划分已确认；待启动批 A）
- **启动日期**：2026-05-25
- **目标完成**：2026-06-08（2 周，master plan §3.3 估算）
- **实际完成**：—
- **阻塞**：无（P0 ✅，P1 ✅）
- **阻塞下游**：本阶段是"首个完整 playbook 实施样板"，P3-P7 复用本阶段的流程模板
- **主 owner**：@me
- **分支**：`catalog-spi-03`（基于 `upstream-apache/branch-catalog-spi`，含 P1 merge `778c5dd610f`）

---

## 阶段目标

把 `trino-connector` 完整迁移到 SPI 模式，作为后续 P3-P7 连接器迁移的样板：

1. **补齐 SPI 实现侧缺口**：在 `fe-connector-trino` 内补 `validateProperties` / `preCreateValidation` / pushdown ops 三处缺失（recon 揭示）。
2. **接通 fe-core 桥接**：`GsonUtils` 加 string-name redirect；`PluginDrivenExternalCatalog.gsonPostProcess` 加 logType 迁移；`ExternalCatalog.registerCompatibleSubtype`；`PluginDrivenExternalTable.getEngine() / getEngineTableTypeName()` 加 trino 分支。
3. **翻闸 SPI_READY**：`CatalogFactory.SPI_READY_TYPES` 加 `"trino-connector"`，老 factory 分支只在 fallback 走。
4. **清旧代码**：删 `PhysicalPlanTranslator` 的 trino-connector instanceof 分支（P1 批 A 已加 SPI fallback 在它之上）；删 `CatalogFactory` 中 `case "trino-connector"` + `TrinoConnectorExternalCatalogFactory`；删 `datasource/trinoconnector/` 整目录 + `GsonUtils` 中对应 3 个 class-token subtype 注册（用户决议 Q2，2026-05-25）。
5. **测试 + 文档**：补 fe-connector-trino 单元测试（0 → ≥ 主路径覆盖）；regression-test 加 image 兼容场景；docs-next 加插件安装文档；同步看板 + PROGRESS。

完成后：

- `datasource/trinoconnector/` 不再存在
- `PhysicalPlanTranslator` 无 `TrinoConnector*` import
- `CatalogFactory` 无 `case "trino-connector"`
- 老 FE image 反序列化通过 GsonUtils string-name redirect 落到 `PluginDrivenExternalCatalog`
- fe-connector-trino 模块完成度看板从 70% 翻到 100%

---

## 验收标准

从 master plan §3.3 同步（含 recon 揭示的额外项）：

- [ ] `TrinoConnectorProvider.validateProperties` 实现，CREATE CATALOG 阶段即校验 `trino.connector.name` 等必填属性
- [ ] `TrinoDorisConnector.preCreateValidation` 实现，CREATE CATALOG 时验证 Trino plugin 可加载
- [ ] `ConnectorPushdownOps.applyFilter` + `applyProjection` 桥接 Trino 原生下推（用户决议 Q1，2026-05-25：纳入 P2 批 A）
- [ ] `GsonUtils.java` 加 3 行 string-name redirect（`TrinoConnectorExternalCatalog` / `Database` / `Table` → 对应 `PluginDriven*`）
- [ ] `PluginDrivenExternalCatalog.gsonPostProcess` 加 `trinoconnector → plugin` logType 迁移分支
- [ ] `ExternalCatalog.registerCompatibleSubtype` 注册 trino 子类型
- [ ] `PluginDrivenExternalTable.getEngine() / getEngineTableTypeName()` 加 `case "trino-connector":` 返回 `TRINO_CONNECTOR_EXTERNAL_TABLE` 对应字符串
- [ ] `CatalogFactory.SPI_READY_TYPES` 加 `"trino-connector"`
- [ ] `PhysicalPlanTranslator.visitPhysicalFileScan` 删 `TrinoConnectorExternalTable` instanceof 分支（P1 批 A 加的 fallback 让位）
- [ ] `CatalogFactory.java` 删 `case "trino-connector":` 分支；删 `TrinoConnectorExternalCatalogFactory.java` 整文件
- [ ] `fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/` 整目录删除
- [ ] `GsonUtils.java:402 / 457 / 476` 三个 class-token subtype 注册同步删除（与目录一起清，用户决议 Q2）
- [ ] `TableIf.TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` **保留**（image compat，master plan §3.3 task 2.4 明示）
- [ ] fe-connector-trino 单元测试：schema 解析 / predicate 转换 / type mapping / json ser-deser（最少 4 个 test class）
- [ ] regression-test `trino_connector_migration_compat`：模拟旧 FE image 反序列化通过
- [ ] 现有 trino-connector regression-test 全套通过
- [ ] `docs-next/` 加 trino-connector 插件安装步骤
- [ ] 看板 + PROGRESS 同步：trino-connector 进度 30% → 100%
- [ ] fe-core 全编译 + checkstyle 0；`mvn -pl fe-connector validate` 通过 import-gate
- [ ] PR CI 全绿

---

## 任务清单

> ID 永不复用。批次方案 2026-05-25 用户已确认：批 A=T01+T02（含 pushdown）；批 B=T03..T06；批 C=T07；批 D=T08..T10；批 E=T11..T13。

| ID | 任务 | 批次 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P2-T01 | `TrinoConnectorProvider.validateProperties` + `TrinoDorisConnector.preCreateValidation` | **批 A** | @me | ⏳ | — | — | — | Port 自 fe-core `TrinoConnectorExternalCatalog`；把 `trino.connector.name` 等运行时校验 lift 到 CREATE CATALOG 阶段。预估 ~30 LOC |
| P2-T02 | `ConnectorPushdownOps.applyFilter` + `applyProjection`（桥接 Trino 原生下推） | **批 A** | @me | ⏳ | — | — | — | 用户决议 Q1（2026-05-25）：纳入 P2。Trino 自身有 push-down，需要把 Doris `ConnectorExpression` → Trino `TupleDomain` 转换、把 SPI projection list → Trino `ConnectorTableHandle` 投影。复用现有 `TrinoPredicateConverter`。预估 ~150-200 LOC + 单测 |
| P2-T03 | `GsonUtils` 加 3 行 string-name redirect（Trino*ExternalCatalog/Database/Table → PluginDriven*） | **批 B** | @me | ⏳ | — | — | — | 在 `GsonUtils.java:414` 后插入（ES/JDBC redirect 之后），用 `.registerSubtype(PluginDrivenExternalCatalog.class, "TrinoConnectorExternalCatalog")` 同 pattern。**注意**：本 task 只加 redirect；删旧 class-token 注册在 T10 |
| P2-T04 | `PluginDrivenExternalCatalog.gsonPostProcess` 加 `trinoconnector → plugin` logType 迁移 | **批 B** | @me | ⏳ | — | — | — | 参照 ES/JDBC 已有分支（lines 318-341）；backfill `"type"` 属性。预估 ~5-10 LOC |
| P2-T05 | `ExternalCatalog.registerCompatibleSubtype` 注册 `TrinoConnectorExternalCatalog → PluginDrivenExternalCatalog` | **批 B** | @me | ⏳ | — | — | — | 找 fe-core 中 ES/JDBC 已有注册位置 mirror 一下 |
| P2-T06 | `PluginDrivenExternalTable.getEngine()` + `getEngineTableTypeName()` 加 `case "trino-connector":` 分支 | **批 B** | @me | ⏳ | — | — | — | 当前覆盖 `"jdbc"` / `"es"`（lines 196-225）；返回 `TableType.TRINO_CONNECTOR_EXTERNAL_TABLE.toEngineName()` 与 `.name()` |
| P2-T07 | `CatalogFactory.SPI_READY_TYPES` 加 `"trino-connector"` | **批 C** | @me | ⏳ | — | — | — | `CatalogFactory.java:53`，把 `ImmutableSet.of("jdbc", "es")` 改 `("jdbc", "es", "trino-connector")`。**翻闸点**——必须在批 A/B 全部完成且本地 smoke pass 之后操作 |
| P2-T08 | `PhysicalPlanTranslator.visitPhysicalFileScan` 删 `instanceof TrinoConnectorExternalTable` 分支 | **批 D** | @me | ⏳ | — | — | — | 文件 `nereids/glue/translator/PhysicalPlanTranslator.java:779`；P1 批 A 已加 `PluginDrivenExternalTable` 前置分支，trino 翻闸后这里成死代码 |
| P2-T09 | `CatalogFactory` 删 `case "trino-connector":` + 删 `TrinoConnectorExternalCatalogFactory.java` 整文件 | **批 D** | @me | ⏳ | — | — | — | `CatalogFactory.java:147-150`；factory 文件 30 LOC，整删 |
| P2-T10 | 删 `datasource/trinoconnector/` 全目录（10 文件）+ 删 `GsonUtils:402/457/476` 三个 class-token 注册 | **批 D** | @me | ⏳ | — | — | — | 用户决议 Q2（2026-05-25）：class-token 注册随类删除一起清；image compat 全靠 T03 的 string-name redirect 承接。**保留** `TableIf.TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` 枚举值 |
| P2-T11 | `fe-connector-trino/src/test/` 单元测试（schema / predicate / type-map / json） | **批 E** | @me | ⏳ | — | — | — | 0 → 最少 4 个 test class；mock Trino plugin。**这一项与批 A 可并行起步**（批 A 完成 ≥ T01-T02 后可开测试） |
| P2-T12 | regression-test `trino_connector_migration_compat`（旧 FE image 反序列化） | **批 E** | @me | ⏳ | — | — | — | 类似 P0 的 ES/JDBC migration compat；放入 `regression-test/suites/external_catalog/` |
| P2-T13 | `docs-next/` 加 trino-connector 插件安装步骤 + 同步 `connectors/trino-connector.md` + `PROGRESS.md` | **批 E** | @me | ⏳ | — | — | — | 文档放 docs-next 对应 connector 章节；看板把进度从 30% 翻 100%，SPI_READY ✅，删旧代码 ✅，反向 instanceof ✅ |

**状态图例**：⏳ pending / 🚧 in_progress / ✅ done / ❌ blocked / 🚫 deleted

---

## 阶段日志（倒序）

### 2026-05-25（晚 ②）— P2 启动 + recon 完成

新 session 启动 P2，在 `catalog-spi-03` 上工作。Recon 5 个子任务（用 Explore subagent 并行）输出代码侧 facts：

- **fe-core 旧代码**：`datasource/trinoconnector/` 共 10 个 .java，~1760 LOC（最大头：`TrinoConnectorExternalCatalog` 329 / `TrinoConnectorScanNode` 342 / `TrinoConnectorPredicateConverter` 334）；3 个 source 子文件（`TrinoConnectorSource` / `TrinoConnectorSplit` / `TrinoConnectorPredicateConverter`）只被内部引用，无外部 caller。
- **外部 caller**：5 个 live 引用点，全部是机械路由（无 P1-T01 那种藏起来的活业务逻辑）：
  - `CatalogFactory.java:148`：`TrinoConnectorExternalCatalogFactory.createCatalog(...)`（T09 删）
  - `ExternalCatalog.java:948`：enum switch 实例化 `TrinoConnectorExternalDatabase`（随 T10 目录删除一起清）
  - `PhysicalPlanTranslator.java:779`：`instanceof TrinoConnectorExternalTable` → `new TrinoConnectorScanNode(...)`（T08 删）
  - `GsonUtils.java:402 / 457 / 476`：3 个 class-token subtype 注册（T10 删，T03 用 string-name redirect 替代承接 image compat）
- **反向 instanceof**：实际只 1 处（PhysicalPlanTranslator:779），dashboard "0/2" 为过时数字。`TrinoConnectorScanNode.java:232` 内部对 split 类型的 instanceof **不算**（连接器内部自洽）。
- **fe-connector-trino 完成度**：13 个 class / 2162 LOC / **0 测试**。SPI 表面 ~95% IMPL/DEFAULT；真缺：`validateProperties`、`preCreateValidation`、pushdown ops 三处。pom.xml 干净（无 `fe-core` 依赖泄漏）；`plugin-zip.xml` assembly 已就位。
- **SPI_READY 翻闸点**：`CatalogFactory.java:53` `SPI_READY_TYPES = ImmutableSet.of("jdbc", "es")`，consume 模式 line 106 → SPI；fallback switch line 135 处理非 SPI。
- **Gson 兼容**：`GsonUtils.java:411,414` 已有 ES/JDBC 的 string-name redirect 范式，trino 复用即可；`PluginDrivenExternalCatalog.gsonPostProcess` lines 318-341 已有 ES/JDBC 的 logType 迁移分支。
- **import gate**：`fe-connector-trino` 反向 import `fe-core` **0 次**，干净。

**用户决议**（2026-05-25 晚 session）：
- **Q1**：pushdown ops 纳入 P2 批 A（不推迟）。理由：避免 trino 走 SPI 后查询性能暂时退步
- **Q2**：fe-core 旧目录删除时，`GsonUtils:402/457/476` 三个 class-token 注册同步删除（不留 stub 类）；image compat 全部由 T03 的 string-name redirect 承接。和 ES/JDBC 一致

task 划分敲定为 13 tasks / 5 批次（A=SPI 补齐 / B=fe-core 桥接 / C=翻闸 / D=清旧 / E=测试+文档）。

下一步：启动批 A T01-T02 编码。

---

## 关联

- Master plan 章节：[§3.3 P2 阶段](../00-connector-migration-master-plan.md)
- RFC 章节：n/a（P2 是 P0 SPI baseline 的首次完整消费方实施；不修改 SPI 设计）
- 决策：D-002（scan-node 复用 FileQueryScanNode）
- 偏差：—
- 风险：R-001（image 反序列化兼容回归——T03/T10 是直接相关 surface）、R-004（classloader 隔离——Trino plugin loader 在 fe-connector-trino 内部，需要单测验证）
- 连接器：[trino-connector](../connectors/trino-connector.md)

---

## 当前阻塞项

无。recon 完成 + task 划分敲定，可立即启动批 A。
