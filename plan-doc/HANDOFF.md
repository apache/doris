# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-24（夜 ②，含 commit）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：P0 批 1 DDL + Partition SPI（T13-T20）—— `ConnectorCreateTableRequest` + 4 spec POJO + 4 个 default 方法 + 1 fe-core converter + PluginDrivenExternalCatalog 路由；**已 commit**（用户人工 review 通过；hash 见 `git log --oneline -3`，subject `[feat](connector) add P0 batch 1 SPI: CreateTableRequest + listPartitions (T13-T20)`）
- **预估 context 使用**：~55%（健康）

---

## ✅ 本 session 完成项

### 1. P0 批 1：DDL + Partition SPI（T13-T20）

| ID | 任务 | 文件 | 备注 |
|---|---|---|---|
| T13 ✅ | `ConnectorCreateTableRequest` + 4 spec POJO | **新** 5 个文件 在 `fe-connector-api/.../connector/api/ddl/` | Request 带 Builder；PartitionSpec 含 4 个 Style（IDENTITY/TRANSFORM/LIST/RANGE） |
| T14 ✅ | `ConnectorTableOps.createTable(session, request)` default | edit `fe-connector-api/.../ConnectorTableOps.java` | 退化到 legacy `createTable(session, schema, props)` |
| T15 ✅ | `CreateTableInfoToConnectorRequestConverter`（fe-core） | **新** `fe-core/.../connector/ddl/CreateTableInfoToConnectorRequestConverter.java` | 4 种 partition style + hash/random bucket；DataType→ConnectorType 复用 `ConnectorColumnConverter.toConnectorType()` |
| T16 ✅ | `PluginDrivenExternalCatalog.createTable` 路由 SPI | edit `fe-core/.../datasource/PluginDrivenExternalCatalog.java` | override `createTable(CreateTableInfo)`；wrap `DorisConnectorException` → `DdlException`；写 edit log；总返回 false（=新建） |
| T17 ✅ | `ConnectorTableOps.listPartitionNames` default | edit `ConnectorTableOps.java` | `Collections.emptyList()` |
| T18 ✅ | `ConnectorTableOps.listPartitions(handle, filter)` default | edit `ConnectorTableOps.java` | filter 用 `Optional<ConnectorExpression>` |
| T19 ✅ | `ConnectorTableOps.listPartitionValues` default | edit `ConnectorTableOps.java` | `Collections.emptyList()` |
| T20 ✅ | `ConnectorPartitionInfo` 追加 3 字段 | edit `fe-connector-api/.../ConnectorPartitionInfo.java` | rowCount/sizeBytes/lastModifiedMillis（`UNKNOWN = -1L`）；3-arg 旧构造器委托到 6-arg；equals/hashCode/toString 同步更新 |

### 2. 验证

- `mvn -pl fe-connector/fe-connector-api -am compile` → **BUILD SUCCESS**
- `mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false` → **BUILD SUCCESS**（1 次因 unused import checkstyle 失败 → fix → green）
- `mvn -pl fe-core checkstyle:check` → **0 violations**
- `mvn -pl fe-connector/fe-connector-jdbc,fe-connector/fe-connector-es -am compile` → **BUILD SUCCESS**（下游连接器零修改）

### 3. 文档同步（§5.1 五步纪律）

- ✅ `tasks/P0-spi-foundation.md`：T13-T20 状态翻 ✅，新增 2026-05-24（夜 ②）日志条目（含 4 项 trade-off 说明）
- ✅ `PROGRESS.md`：§一 P0 进度条 44% → 74%（20/27 任务）；§三 P0 表追加 T13-T20 行；§四加 2026-05-24（夜 ②）条目；§七 session 状态滚动
- ✅ 本 HANDOFF.md 覆写
- N/A `connectors/<name>.md`（本次工作不属任何具体连接器）
- N/A `decisions-log.md` / `deviations-log.md`（trade-off 在 RFC 范围内，未升 DV——见下"开放问题"）

### 4. Commit（用户人工 review 通过后）

- ✅ `[feat](connector) add P0 batch 1 SPI: CreateTableRequest + listPartitions (T13-T20)`（hash 见 `git log --oneline -3`）
- 12 files changed：6 新文件（5 ddl POJO + 1 fe-core converter）+ 3 surgical edits（ConnectorTableOps / ConnectorPartitionInfo / PluginDrivenExternalCatalog）+ 3 plan-doc 更新
- 工作树 clean

---

## 🚧 本 session 进行中 / 未完成

**无**。批 1 全部 8 项任务（T13-T20）+ 编译验证 + 文档同步 + commit 全部收尾。

---

## 📝 关键认知 / 临时发现

继承上版认知不变。**本场新增**：

1. **`ColumnDefinition.defaultValue` 没有 public getter**——字段是 private `Optional<DefaultValue>`，只有 `hasDefaultValue()`。converter 当前传 `null`，等 SPI 在 `ConnectorColumn` 上增加 typed default-value carrier 时再补。这是 SPI 设计层面的 follow-up，不是 fe-core 侧补丁。
2. **`PartitionTableInfo` 的 partition style 判别**：discriminator 不是单字段，需要两步——
   - `getPartitionType()` 等于 `LIST` / `RANGE` → Doris 自有风格
   - `getPartitionType() == UNPARTITIONED` 但 `getPartitionList()` 非空 → 进一步看 expressions：全 `UnboundSlot` → IDENTITY（Hive 风格）；含 `UnboundFunction` → TRANSFORM（Iceberg 风格）
3. **LIST/RANGE 的 `initialValues` 暂未下沉**——`PartitionDefinition` 子类（InPartition/LessThanPartition/FixedRangePartition/StepPartition）携带 nereids `Expression`，需要完整 analyzer 才能 flatten 到 `List<List<String>>`。目前 Iceberg/Hive 走 TRANSFORM/IDENTITY 路径不依赖此，先返回空 list 延迟到具体连接器需要时再补。
4. **`DistributionDescriptor` 的 `bucketNum` 字段是 private 且无 public getter**——只能通过 `translateToCatalogStyle().getBuckets()` 间接读取。converter 已封装为 `readBucketNum()` 私有 helper，未来 DistributionDescriptor 加 getter 可一行替换。
5. **`PluginDrivenExternalCatalog.createTable` 的返回值语义**：SPI 的 `createTable(session, request)` 是 void，不区分"已存在 + IF NOT EXISTS"与"新建"。当前 override 总返回 false（=新建）并写 edit log。这是保守选择——对 P0 forward-compat plumbing 够用；真正需要"存在判定"的连接器（P5/P6/P7）可扩展 SPI 返回 boolean。
6. **bucket 算法名硬编码为 `"doris_default"` / `"doris_random"`**——RFC §4.2 列了 `hive_hash` / `iceberg_bucket` 等真实算法，但 Doris 内部 `DistributionDescriptor` 只携带 `isHash` 布尔。真实算法名由 Hive/Iceberg 连接器在自己的 metadata.createTable 里根据 properties 推导（P7/P6）。
7. **fe-core 编译验证的 cwd 是 `fe/`**，不是 workspace 根目录——`mvn -pl fe-core` 要从 `fe/` 跑。沿用上版认知。

---

## 🎯 下一个 session 第一件事

### Track A 已收尾（batch 1 commit 已合入 catalog-spi-00）

无需再开 Track A。新 session 直接进 Track B（批 2）。

### Track B：P0 批 2（守门 + 测试，T21-T27）

```
1. git branch --show-current  → 确认仍在 catalog-spi-00
2. Read plan-doc/PROGRESS.md + plan-doc/HANDOFF.md（本文件）
3. Read plan-doc/01-spi-extensions-rfc.md §15 (Testing) + §17 (Acceptance)
4. T21: tools/check-connector-imports.sh —— 禁用 import 守门脚本（grep 禁词如 fe-core 包名出现在 fe-connector 模块内）
5. T22: 在 fe-connector pom 接 maven-enforcer-plugin 调用 T21 脚本
6. T23: FakeConnectorPlugin（fe-core test 包）—— 覆盖所有 default 行为路径（不实现任何 SPI 方法也应跑通基本 SHOW DATABASES / CREATE TABLE / listPartitions 调用，验证 fallback）
7. T24-T25: JDBC + ES 全套 regression-test
8. T26: ConnectorMetaInvalidator 路由测试（mock ExternalMetaCacheMgr）
9. T27: CreateTableInfoToConnectorRequestConverter 单元测试（覆盖 IDENTITY / TRANSFORM / LIST / RANGE 四种 partition + hash/random bucket）
10. 更新 tasks / PROGRESS / HANDOFF + commit
```

预计 context 用量 ~55%（T23 FakeConnectorPlugin 与 T27 converter 单测较重；其余偏脚本/regression-driver）。

### Track C（可选，仅在 P0 全部收尾后）：批 2 同步评估开放问题 #1-#3

如果在写 FakeConnectorPlugin / converter 单测过程中触发了"开放问题 #1 default 值 / #2 LIST initialValues / #3 createTable 返回值"中的实际需求，**走 DV 流程登记** 再调整 SPI，不要 silent edit。

---

## ⚠️ 开放问题 / 风险提示

继承上版 6 项不变。**本场新增 / 更新**（删除了"未 commit"项；batch 1 commit 已收尾）：

1. **`ColumnDefinition.defaultValue` 在 SPI 层缺位**：converter 当前传 null。是否在 `ConnectorColumn` 上加 typed default-value carrier？建议 P5/P6 Hive/Iceberg 真正用到 CREATE TABLE 时再评估——如必要可走 DV 流程。
2. **LIST/RANGE `initialValues` flatten 缺位**：当前 converter 返回空。是否在 `PartitionDefinition` 上加 `toFlatValues(): List<List<String>>` helper？建议同上：P5 Paimon / P7 Hive 真正用到时再评估。
3. **`PluginDrivenExternalCatalog.createTable` 返回值丢失"已存在"信息**：是否扩展 SPI `createTable(session, request)` 返回 boolean 或 `CreateTableResult`？建议留到 P5/P6/P7 连接器迁移时再决定——目前 forward-compat plumbing 不影响。
4. **bucket 算法名占位**：`"doris_default"` / `"doris_random"`。Hive/Iceberg 连接器侧需在自己的 metadata.createTable 里根据 properties 推导真实算法。
5. （沿用）Maven build cache 误导问题：所有 SPI 改动验证步骤强制加 `-Dmaven.build.cache.enabled=false` 或 `clean`；并且 `mvn -pl fe-core` 的 cwd 必须是 `fe/`，不是 workspace 根。
6. （沿用）`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 暂无 caller（P5/P6/P7 接通）。
7. （沿用）`invalidatePartition` fallback 到 `invalidateTable`；`invalidateStatistics` no-op——P7 hive ACID 场景再评估。

---

## 📂 当前关键文件清单

### 本场新增 / 修改（已 commit）

```
NEW  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ddl/ConnectorCreateTableRequest.java
NEW  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ddl/ConnectorPartitionSpec.java
NEW  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ddl/ConnectorPartitionField.java
NEW  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ddl/ConnectorPartitionValueDef.java
NEW  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ddl/ConnectorBucketSpec.java
NEW  fe/fe-core/src/main/java/org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverter.java
MOD  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorTableOps.java
MOD  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorPartitionInfo.java
MOD  fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java
MOD  plan-doc/PROGRESS.md
MOD  plan-doc/tasks/P0-spi-foundation.md
MOD  plan-doc/HANDOFF.md（本文件——post-commit roll 通过 git amend 与 batch 1 同 commit 落盘）
```

### 跟踪体系（沿用不变）

```
plan-doc/  (~225K, 17 文件)
├── 00-connector-migration-master-plan.md / 01-spi-extensions-rfc.md
├── README.md / PROGRESS.md / AGENT-PLAYBOOK.md / HANDOFF.md
├── decisions-log.md (18) / deviations-log.md (0) / risks.md (14)
├── tasks/{_template.md, P0-spi-foundation.md}
└── connectors/{_template.md, jdbc, es, trino-connector, hudi, maxcompute, paimon, iceberg, hive}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **当前分支是 `catalog-spi-00`**。新 session 开场 `git branch --show-current` 确认。
- **批 1（T13-T20）已合入 `catalog-spi-00`**（subject `[feat](connector) add P0 batch 1 SPI: CreateTableRequest + listPartitions (T13-T20)`），无需 review 老代码；直接读最新源即可。如果对 6 个新/改文件有调整建议，走 DV 流程登记后再改，不要 silent edit。
- **Maven build 的 cwd 必须是 `fe/`**，不是 workspace 根。`mvn -pl fe-core -am compile` 从根目录跑会报"Could not find the selected project in the reactor"。
- 本场无 RFC 修改、无新 decision / deviation——所有 trade-off 都在 RFC §4 / §13 设计范围内，由代码注释 + 本 HANDOFF "开放问题" 列出。沿用 meta 建议 "不要重新打开 D-001..D-018"。
- **本场对 `CreateTableInfoToConnectorRequestConverter` 的写法值得参考**：先 `convert(info, dbName)` 顶层 → 4 个 private helper（convertColumns/convertPartition/convertBucket/convertField + convertTransformField）。Helper 之间互相不调用、各自处理一种维度，方便后续 T27 单测分别覆盖。
- **必读 AGENT-PLAYBOOK §六 anti-patterns** 再开始动手。
- 本场用 Explore subagent 调研了 nereids `CreateTableInfo` / `ColumnDefinition` / `PartitionTableInfo` / `DistributionDescriptor` 4 个文件，把整源代码读取摊到 subagent，节省主 context（CreateTableInfo 单文件 1714 行）——批 2 如果要写 FakeConnectorPlugin 也建议同样套路。
- **本 HANDOFF roll 已通过 git amend 与 batch 1 代码同 commit 落盘**（HANDOFF 不内嵌 commit hash——hash 在 amend 后会变；用 `git log --oneline -3` 或 `git log --grep="P0 batch 1"` 即可定位）。下一个 session 不需要再处理 HANDOFF 落盘。
