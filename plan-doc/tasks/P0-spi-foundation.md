# P0 — SPI 缺口补齐

> 阶段总览见 [00-master-plan §3.1](../00-connector-migration-master-plan.md)。
> SPI 详细设计见 [01-spi-extensions-rfc.md](../01-spi-extensions-rfc.md)。
> 协作规范见 [AGENT-PLAYBOOK.md](../AGENT-PLAYBOOK.md)。

---

## 元信息

- **状态**：🚧 进行中
- **启动日期**：2026-05-24
- **目标完成**：2026-06-07（2 周）
- **实际完成**：—
- **阻塞**：无（项目第一个阶段）
- **阻塞下游**：P1 (scan-node 收口)、P3–P7（所有连接器迁移依赖本阶段 SPI baseline）
- **主 owner**：@me

---

## 阶段目标

完成 [RFC §2.1 表](../01-spi-extensions-rfc.md) 中全部 10 项 SPI 缺口的接口 / 类型定义 + 默认行为 + fe-core 侧 converter，保证 JDBC 和 ES 现有实现零修改通过。

具体分两批：
- **批 0**（W0 D3-5）：E3 MetaInvalidator、E4 Transaction、E5 MvccSnapshot —— 后续所有连接器实现 ConnectorMetadata 时的 baseline。
- **批 1**（W1）：E1 CreateTableRequest、E10 listPartitions —— 阻塞 P3 hudi、P5 paimon。
- **批 2-4** 在对应 P 阶段开始时随主任务做（不在 P0 范围内）。

---

## 验收标准

从 [RFC §17 验收清单](../01-spi-extensions-rfc.md) 同步：

- [x] `mvn -pl fe-connector validate` 全绿，新增类型 / 方法全部就位（含 import 守门）
- [x] `fe-connector-spi` 仅新增 `ConnectorMetaInvalidator` 接口与 `ConnectorContext.getMetaInvalidator()` 默认方法
- [x] fe-core 侧 converter 就位：`CreateTableInfoToConnectorRequestConverter`、`ExternalMetaCacheInvalidator`、`ConnectorMvccSnapshotAdapter`
- [x] `PluginDrivenTransactionManager` 通用化（不再依赖任何具体连接器）
- [ ] JDBC、ES 现有 regression-test 全绿（T24-T25 用户在本地跑）
- [x] `FakeConnectorPlugin` 覆盖所有新增 default 行为路径（11 个 @Test）
- [x] `tools/check-connector-imports.sh` 接入 exec-maven-plugin（RFC §15.4 等价实现：enforcer 无原生 shell-exec rule，见日志 trade-off #1）
- [x] 本阶段关闭未决问题 U1-U6（2026-05-24 完成，决策 D-013..D-018）
- [ ] master plan §3.1 全部任务勾选（T24-T25 用户跑完后由用户勾选）

---

## 任务清单

### 批 0：基础三件套（W0 D3-5，2026-05-27 → 2026-05-29）

| ID | 任务 | 设计参考 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P0-T01 | RFC §16.2 决策点闭环（U1-U6） | RFC §16 | @me | ✅ | n/a | 2026-05-24 | 2026-05-24 | D-013..D-018 |
| P0-T02 | 项目跟踪机制建立 | README/PROGRESS/...| @me | ✅ | 63159837043 | 2026-05-24 | 2026-05-24 | 本文件等 |
| P0-T03 | E3：`ConnectorMetaInvalidator` 接口（fe-connector-spi）| RFC §6.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 5 个 invalidate 方法 |
| P0-T04 | E3：`ConnectorContext.getMetaInvalidator()` default | RFC §6.3 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | spi 包 |
| P0-T05 | E4：`ConnectorTransaction` 继承 `ConnectorTransactionHandle` | RFC §7.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 新增不替换 handle |
| P0-T06 | E4：`ConnectorWriteOps.beginTransaction` default | RFC §7.3 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | throws unsupported |
| P0-T07 | E4：`ConnectorSession.getCurrentTransaction` default | RFC §7.6 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | Optional.empty() |
| P0-T08 | E5：`ConnectorMvccSnapshot` 类型 + 3 个 default 方法 | RFC §8.2-8.3 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | mvcc 包 + 3 默认在 ConnectorMetadata |

### 批 0：fe-core 桥接（W0 D5 - W1 D1）

| ID | 任务 | 设计参考 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P0-T09 | `DefaultConnectorContext.getMetaInvalidator()` impl | RFC §6.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 返回新建 invalidator |
| P0-T10 | `ExternalMetaCacheInvalidator`（fe-core 新类） | RFC §6.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 包装 `ExternalMetaCacheMgr`；2 个 no-op 限制留 TODO |
| P0-T11 | `PluginDrivenTransactionManager` 通用化 | RFC §7.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 新增 `begin(ConnectorTransaction)` 重载；legacy `begin()` 不变 |
| P0-T12 | `ConnectorMvccSnapshotAdapter`（fe-core 新类） | RFC §8.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | impl `MvccSnapshot` 标记接口 |

### 批 1：DDL + Partition SPI（W1 D1-3）

| ID | 任务 | 设计参考 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P0-T13 | E1：`ConnectorCreateTableRequest` + `Partition/Bucket Spec` POJO（ddl 包） | RFC §4.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 5 个类（Request + PartitionSpec/Field/ValueDef + BucketSpec） |
| P0-T14 | E1：`ConnectorTableOps.createTable(request)` default | RFC §4.3 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 退化到旧 `createTable(schema, props)` |
| P0-T15 | E1：`CreateTableInfoToConnectorRequestConverter`（fe-core） | RFC §4.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 覆盖 IDENTITY / TRANSFORM / LIST / RANGE 四种 partition + hash/random bucket |
| P0-T16 | E1：`PluginDrivenExternalCatalog.createTable(stmt)` 接通 SPI | RFC §4.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | override `createTable(CreateTableInfo)`；包 DorisConnectorException → DdlException |
| P0-T17 | E10：`ConnectorTableOps.listPartitionNames` default | RFC §13.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 返回 `Collections.emptyList()` |
| P0-T18 | E10：`ConnectorTableOps.listPartitions(handle, filter)` default | RFC §13.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | filter 用 `Optional<ConnectorExpression>` |
| P0-T19 | E10：`ConnectorTableOps.listPartitionValues` default | RFC §13.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 返回 `Collections.emptyList()` |
| P0-T20 | E10：`ConnectorPartitionInfo` 追加字段（rowCount/sizeBytes/lastModifiedMillis） | RFC §13.3 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 3 个 long 字段（UNKNOWN=-1）；3-arg 构造器委托到 6-arg；equals/hashCode 更新 |

### 批 2：守门 + 测试（W1 D4-5）

| ID | 任务 | 设计参考 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P0-T21 | `tools/check-connector-imports.sh` 实现 | RFC §15.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | grep 守门；script 自含正/负冒烟测试 |
| P0-T22 | exec-maven-plugin 接入脚本（aggregator pom validate 阶段） | RFC §15.4 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | `inherited=false` 避免 11 个子模块重复扫描 |
| P0-T23 | `FakeConnectorPlugin`（fe-core test）覆盖所有 default 行为 | RFC §15.1 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 11 个测试覆盖 Connector/Metadata/TableOps/WriteOps/Session/Context 全 default |
| P0-T24 | JDBC regression-test 全套跑通 | RFC §17 | @用户 | ⏳ | — | — | — | 用户在本地跑（needs docker） |
| P0-T25 | ES regression-test 全套跑通 | RFC §17 | @用户 | ⏳ | — | — | — | 用户在本地跑（needs docker） |
| P0-T26 | `ConnectorMetaInvalidator` 路由测试 | RFC §15.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 5 个测试 mockStatic(Env)；pin 当前 partition fallback & stats no-op 行为 |
| P0-T27 | `CreateTableInfoToConnectorRequestConverter` 单元测试 | RFC §15.2 | @me | ✅ | — | 2026-05-24 | 2026-05-24 | 7 个测试覆盖 IDENTITY/TRANSFORM/LIST/RANGE + hash/random bucket + 列穿透 |

---

## 阶段日志（倒序）

### 2026-05-24（夜 ③）— 批 2 守门 + 单测完成（T21-T23, T26-T27；T24-T25 用户跑）

- P0-T21 ✅：新增 `tools/check-connector-imports.sh`。在 `fe-connector/*/src/main/java` 下 grep 禁词 `org.apache.doris.(catalog|common|datasource|qe|analysis|nereids|planner)`，allowlist `thrift / connector / extension / filesystem`。脚本接受可选 ROOT 参数（默认 `$(dirname $0)/../fe/fe-connector`），自动适配 cwd。当前 baseline 全绿（fe-connector 模块仅引用 `connector / extension / thrift / trinoconnector`）；自构造的负样本（注入 `import org.apache.doris.catalog.Column`）正确报错退出
- P0-T22 ✅：fe-connector 聚合 pom 加 `exec-maven-plugin` 调用脚本，绑 `validate` 阶段，`inherited=false`（避免 11 个子模块每次都跑同一份扫描）。`executable` 使用 `${project.basedir}/../../tools/check-connector-imports.sh`——不依赖 `directory-maven-plugin` 的 `fe.dir` 属性（后者在 `initialize` 阶段才设值，早于 `validate`）。`mvn -pl fe-connector validate` BUILD SUCCESS
- P0-T23 ✅：fe-core test 包新增 `org.apache.doris.connector.fake.FakeConnectorPlugin`（4 个静态嵌套：`FakeConnector` / `FakeMetadata`（**零** override）/ `FakeSession` / `FakeContext`）。同包测试类 `FakeConnectorPluginTest` 11 个 `@Test` 覆盖：Context.getMetaInvalidator()=NOOP（且 5 个 invalidate 方法 callable）；Session.getCurrentTransaction()=Optional.empty()；Metadata MVCC 3 方法=Optional.empty()；TableOps listTableNames / getTableHandle / listPartitionNames / listPartitions / listPartitionValues / getPrimaryKeys / getTableComment defaults；createTable(request) 退化到 legacy createTable(schema, props) 并抛 "CREATE TABLE not supported"；WriteOps supports*=false + beginTransaction throws；Connector top-level defaults。Tests run: **11/11 green**
- P0-T26 ✅：新增 `org.apache.doris.connector.ExternalMetaCacheInvalidatorTest`。5 个测试：invalidateAll→invalidateCatalog(id)、invalidateDatabase→invalidateDb(id, db)、invalidateTable→invalidateTable(id, db, t)、invalidatePartition→**fallback** 到 invalidateTable（pin 当前 SPI 不携 column 名的行为）、invalidateStatistics→**no-op**（pin 当前缺 stats-only entry point 的行为）。用 `MockedStatic<Env>` + `Mockito.mock(ExternalMetaCacheMgr)` 完全隔离 FE bootstrap。Tests run: **5/5 green**
- P0-T27 ✅：新增 `org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverterTest`。7 个测试覆盖：列穿透（name/type/nullable/comment）+ scalar 字段穿透（dbName/tableName/comment/properties/ifNotExists/isExternal）+ IDENTITY partition（UnboundSlot）+ TRANSFORM partition（UnboundFunction `bucket(16, id)` + `YEAR(d)` 验证 lowercase normalization + IntegerLiteral 提取）+ LIST partition（PartitionType.LIST）+ RANGE partition（PartitionType.RANGE）+ hash bucket 算法 `doris_default` + random bucket 算法 `doris_random`。用 `Mockito.mock(CreateTableInfo)` 绕开 18-arg 构造器与 `PropertyAnalyzer.getInstance()` 调用；PartitionTableInfo/DistributionDescriptor/ColumnDefinition/UnboundFunction 等都用真实构造器。Tests run: **7/7 green**
- 验证：
  - `tools/check-connector-imports.sh` 正/负冒烟测试通过
  - `mvn -pl fe-connector validate -Dmaven.build.cache.enabled=false` → BUILD SUCCESS（脚本被 maven 调起）
  - `mvn -pl fe-core -am test -Dtest='FakeConnectorPluginTest,ExternalMetaCacheInvalidatorTest,CreateTableInfoToConnectorRequestConverterTest,ConnectorPluginManagerTest,ConnectorSessionImplTest' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false` → **39/39 tests green**（含 batch 2 新增 23 个 + 既有 16 个相邻 connector 测试）
  - `mvn -pl fe-core checkstyle:check` → **0 violations**
- 已知 trade-off（**未升 DV**，是 RFC §15 / §17 范围内的实现取舍）：
  1. 守门脚本挂到 `exec-maven-plugin` 而非 `maven-enforcer-plugin`——RFC §15.4 原文写"挂到 maven enforcer plugin"，但 enforcer 没有原生 shell-exec rule（要么写自定义 Java Rule 类，要么用 `EvaluateBeanshell`）。`exec-maven-plugin` 在 fe-common 已是既有 dep（make + protoc 都用它），引入零新依赖。效果等价：脚本 non-zero exit → maven `BUILD FAILURE`
  2. 守门绑 `validate` 阶段且 `inherited=false`——只在 fe-connector aggregator 一次运行；devs 跑 `mvn -pl fe-connector/fe-connector-iceberg compile` 时不会自动触发，但 CI 跑顶层 `mvn install` 必扫。Trade-off：少 11 次重复扫，换"单模块增量构建本地无守门"
  3. ConnectorMetaInvalidator 的 partition fallback 测试明确 pin 当前"回退到 invalidateTable"的行为——一旦未来 SPI 在 invalidatePartition 中加 column 名携带能力可以做精确失效，bridge 和这个测试必须同步更新；测试已留 inline comment 描述意图
  4. CreateTableInfo 用 Mockito.mock 而非真实构造器——RFC §15.2 没规定单测必须用真实输入对象。Trade-off：测试更聚焦于 converter 自身逻辑（不必维护 18-arg 输入构造），但代价是如果 CreateTableInfo 加新 getter 且 converter 改用之，需要在 stubInfo helper 加新 stub
- T24/T25 转交用户：用户在本地跑 JDBC + ES regression-test（containers / docker 在本地环境下更稳）。任务状态保持 ⏳，owner @用户

### 2026-05-24（夜 ②）— 批 1 DDL + Partition SPI 完成（T13-T20）

- P0-T13 ✅：新增 `connector.api.ddl` 包 5 个 POJO：`ConnectorCreateTableRequest`（带 Builder）、`ConnectorPartitionSpec`（Style enum：IDENTITY/TRANSFORM/LIST/RANGE）、`ConnectorPartitionField`、`ConnectorPartitionValueDef`、`ConnectorBucketSpec`
- P0-T14 ✅：`ConnectorTableOps.createTable(session, request)` default 退化到 legacy `createTable(session, schema, props)`（丢弃 partition / bucket / external / ifNotExists）
- P0-T15 ✅：新增 `fe-core/.../connector/ddl/CreateTableInfoToConnectorRequestConverter`。覆盖：（1）columns 经 `ConnectorColumnConverter.toConnectorType()`；（2）partition 通过 `PartitionTableInfo.getPartitionType()` + `getPartitionList()` 判别四种 style；（3）TRANSFORM 解析 `UnboundFunction.getName()` + children 提取 `IntegerLikeLiteral` 参数；（4）bucket 通过 `DistributionDescriptor.translateToCatalogStyle().getBuckets()` 读取桶数
- P0-T16 ✅：`PluginDrivenExternalCatalog` 新加 `createTable(CreateTableInfo)` override：build session → converter → `connector.getMetadata(s).createTable(s, req)` → wrap `DorisConnectorException` 为 `DdlException` → 写 edit log
- P0-T17 ✅：`listPartitionNames(session, handle)` default 返回 `Collections.emptyList()`
- P0-T18 ✅：`listPartitions(session, handle, Optional<ConnectorExpression> filter)` default 返回 `Collections.emptyList()`
- P0-T19 ✅：`listPartitionValues(session, handle, List<String> partitionColumns)` default 返回 `Collections.emptyList()`
- P0-T20 ✅：`ConnectorPartitionInfo` 新增 3 个 long 字段（rowCount / sizeBytes / lastModifiedMillis），`UNKNOWN = -1L` 常量；3-arg 旧构造器委托到 6-arg 新构造器；equals/hashCode/toString 同步更新
- 验证：
  - `mvn -pl fe-connector/fe-connector-api -am compile` → BUILD SUCCESS
  - `mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false` → BUILD SUCCESS
  - `mvn -pl fe-core checkstyle:check` → **0 violations**
  - `mvn -pl fe-connector/fe-connector-jdbc,fe-connector/fe-connector-es -am compile` → BUILD SUCCESS（下游连接器零修改）
- 已知 trade-off（**未升 DV**，是 RFC 范围内的实现取舍）：
  1. `ColumnDefinition.defaultValue` 是 private `Optional<DefaultValue>` 且无 public getter——converter 暂传 `null`。等 SPI 在 ConnectorColumn 上增加 typed default-value carrier 时再补
  2. LIST/RANGE 的 `initialValues` 暂不下沉到 `List<List<String>>`——`PartitionDefinition` 子类（InPartition/LessThanPartition/FixedRangePartition/StepPartition）含 nereids `Expression`，需要完整分析才能 flatten；先返回空列表，未来 Iceberg/Hive 走 TRANSFORM/IDENTITY 路径不依赖此
  3. `PluginDrivenExternalCatalog.createTable` 总返回 `false`（=新建并写 edit log）——SPI 的 `createTable(session, request)` 是 void，不区分"已存在 + IF NOT EXISTS"与"新建"。留待 P5/P6/P7 真正实现连接器 createTable 时细化
  4. bucket 算法名硬编码为 `"doris_default"` / `"doris_random"`——RFC §4.2 列了 `hive_hash` / `iceberg_bucket`，但 Doris 内部 `DistributionDescriptor` 只携带 isHash 布尔。由 Hive/Iceberg 连接器实现时根据 properties 推导真实算法

### 2026-05-24（深夜）— 批 0 fe-core 桥接完成（T09-T12）

- P0-T09 ✅：`DefaultConnectorContext.getMetaInvalidator()` override → `new ExternalMetaCacheInvalidator(catalogId)`
- P0-T10 ✅：新增 `fe-core/.../connector/ExternalMetaCacheInvalidator`（5 个方法：3 个直接代理 `ExternalMetaCacheMgr` 的 invalidateCatalog/Db/Table；`invalidatePartition` 暂回退到 `invalidateTable`（SPI 未携带 partition column 名）；`invalidateStatistics` 暂 no-op（fe-core 暂无 stats-only invalidation 入口））
- P0-T11 ✅：`PluginDrivenTransactionManager` 加 `begin(ConnectorTransaction)` 重载，inner `PluginDrivenTransaction` 加 nullable `connectorTx` 字段；legacy `long begin()` 路径完全不变 → JDBC/ES auto-commit 零回归
- P0-T12 ✅：新增 `fe-core/.../connector/ConnectorMvccSnapshotAdapter`，包装 `ConnectorMvccSnapshot` 并 implements 标记接口 `MvccSnapshot`
- 验证：`mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false` → BUILD SUCCESS；checkstyle 0 violations；JDBC + ES 下游 connector clean compile 通过

### 2026-05-24（晚）— 批 0 基础三件套完成
- P0-T02 ✅ 闭环：跟踪机制 17 个文件已落 commit 63159837043（早场 session 完成正文，本场 session 翻状态）
- P0-T03 ✅：新增 `connector.spi.ConnectorMetaInvalidator`（5 个 invalidate 方法 + `NOOP` 常量）
- P0-T04 ✅：`ConnectorContext.getMetaInvalidator()` default → `NOOP`
- P0-T05 ✅：新增 `connector.api.handle.ConnectorTransaction extends ConnectorTransactionHandle, Closeable`（保留旧 24 行 marker 不破坏现有引用）
- P0-T06 ✅：`ConnectorWriteOps.beginTransaction(session)` default 抛 `DorisConnectorException("Transactions not supported")`
- P0-T07 ✅：`ConnectorSession.getCurrentTransaction()` default 返回 `Optional.empty()`
- P0-T08 ✅：新增 `connector.api.mvcc.ConnectorMvccSnapshot`（final value class + Builder），`ConnectorMetadata` 上 3 个 default：`beginQuerySnapshot` / `getSnapshotAt` / `getSnapshotById`
- 验证：`mvn -pl fe-connector/fe-connector-api,spi -am clean compile` 全绿；JDBC + ES 下游 connector clean compile 通过（无修改）；checkstyle 0 violations

### 2026-05-24（早）
- 创建本文件（跟踪机制建立的一部分）
- P0-T01 ✅ 完成：master plan §5（D1-D12）+ RFC §16.2（U1-U6）全部决策闭环 → decisions-log D-001..D-018
- P0-T02 🚧 进行中：跟踪机制文件建立（README/PROGRESS/decisions-log/deviations-log/risks/tasks/_template/本文件 已成；待完成 connectors/× 8 + 00-master-plan cross-link）

---

## 关联

- Master plan 章节：[§3.1 P0 阶段](../00-connector-migration-master-plan.md)
- RFC 详细设计：[01-spi-extensions-rfc.md](../01-spi-extensions-rfc.md)
- 决策：D-013, D-014, D-015, D-016, D-017, D-018
- 偏差：（暂无）
- 风险：R-008（文档脱节）— 通过本跟踪机制缓解中

---

## 当前阻塞项

无。

---

## 注意事项

1. **批 0 三个 SPI 是后续所有连接器迁移的 baseline**。一旦合入主线，每个连接器都开始用，调整成本急剧上升。**先在批 0 完成后让用户 review**，再开始批 1。
2. **P0-T11（`PluginDrivenTransactionManager` 通用化）需要小心**：它是 fe-core 内类，可能影响现有 ES/JDBC 路径。需要回归测试保证 JDBC auto-commit 不退化。
3. **P0-T21（grep 守门）必须在 P0 结束前合入**。一旦后续连接器迁移开 PR，没有守门就可能引入禁用 import，难追溯。
4. **P0 末加 benchmark**（R-006 缓解措施）：1k catalog × `listTableNames` 性能基线。不在当前任务清单——是否要加 P0-T28？**决定**：暂不加为 P0 范围，列入 P1 task。
