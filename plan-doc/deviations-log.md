# 设计偏差日志

> **Append-only**：实施中发现原计划/RFC 设计**不可行 / 不必要 / 需要重新设计**时记入本文件。
> 与"决策"的区别见 [README §3.1](./README.md)：
> - 决策（D-NNN）= **事前**确定的选择
> - 偏差（DV-NNN）= **事后**对原计划的修正
>
> 编号规则：`DV-NNN` 三位数字，从 001 起单调递增，永不复用。
>
> 维护规则见 [README §4.3](./README.md)：**先记偏差再改文档**，不要 silent edit。

---

## 📋 索引

> 时间倒序；当前共 **5** 项。

| 编号 | 偏差主题 | 原计划位置 | 日期 | 当前状态 |
|---|---|---|---|---|
| DV-005 | P3 hudi「HMS-over-SPI 前置依赖」与代码不符；真阻塞=catalog 模型错配 | [connectors/hudi.md](./connectors/hudi.md) / [master plan §3.4](./00-connector-migration-master-plan.md) / D-005 | 2026-06-04 | 🟡 待修正（P3 模型决策）|
| DV-004 | T13 用户向安装文档不在本代码仓（在 doris-website 仓） | [tasks/P2 T13](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |
| DV-003 | T12 回归测试引用不存在的先例/目录且本地不可运行 | [tasks/P2 T12](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟡 推迟 |
| DV-002 | T11 无法 mock Trino plugin；JsonSerializer 非纯单元 | [tasks/P2 T11](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |
| DV-001 | 批 D 范围遗漏 ExternalCatalog db 路由 + legacy test | [tasks/P2 T08-T10](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |

---

## 详细记录（时间倒序）

### DV-005 — P3 hudi 的「HMS-over-SPI 前置依赖」与代码实际状态不符；真正阻塞是 catalog 模型错配

- **发现日期**：2026-06-04
- **发现 session / agent**：P3 启动 recon session（8-agent code-grounded workflow + 2 路对抗验证；verdict `hmsMetadataOverSpiReady=false`, high confidence）
- **当前状态**：🟡 待修正（P3 catalog 模型决策，待用户签字）
- **原计划位置**：[connectors/hudi.md](./connectors/hudi.md)（「P3 启动前必须 P5 paimon 或 P7 hive 进入到至少完成 hms metadata 路径」）、[master plan §3.4/§3.8](./00-connector-migration-master-plan.md)、决策 D-005（用 `tableFormatType` 区分 DLA）
- **偏差描述**：原计划假设 HMS-over-SPI 元数据读路径要等 P5/P7 才落地、是 P3 的前置硬依赖。recon 实测（`branch-catalog-spi` HEAD `0793f032662`）发现该读路径**代码早已存在且非 stub**（源自更早的 #62183/#62821，一直 dormant 在 gate 后）：
  - `fe-connector-hms` = 共享 **HMS Thrift 客户端库**（`HmsClient`/`ThriftHmsClient`，**不是** ConnectorMetadata）；
  - `fe-connector-hive` `HiveConnectorMetadata`(type `"hms"`) 真实读路径 + applyFilter 真分区裁剪；
  - `fe-connector-hudi` `HudiConnectorMetadata`(type `"hudi"`) 从 Hudi Avro MetaClient 读 schema（HMS fallback）+ COW/MOR 探测 + `HudiScanPlanProvider` 快照扫描；
  - D-005 区分符 `ConnectorTableSchema.tableFormatType`(`:33/:58`) 已存在并被各连接器写入。

  但全部 **dormant**：`CatalogFactory.SPI_READY_TYPES = {jdbc, es, trino-connector}`(`CatalogFactory.java:52`) 不含 hms/hudi → HMS 系 catalog 永远走 legacy `HMSExternalCatalog`（零 live caller）。**真正阻塞不是缺 HMS 读码，而是 catalog 模型错配**：现存连接器注册独立 `"hudi"` catalog type（`HudiConnectorProvider.getType()=="hudi"`），而 Doris 真实模型是 hudi 寄生在 `"hms"` catalog 内、以 `HMSExternalTable.DLAType.HUDI` 暴露；fe-core 无 `"hudi"` catalog type，且 `PluginDrivenExternalTable` 从不消费 `tableFormatType`（只读 `getColumns()`，按 catalog TYPE 字串路由）→ 单个 `"hms"` 连接器没有 per-table HUDI/HIVE/ICEBERG 分流的 SPI 机制。附带确认缺口：增量读无 SPI 表示（P1-T04 `visitPhysicalHudiScan` SPI 分支丢弃 `getIncrementalRelation()`；MVCC trio 未实现；4 个 `*IncrementalRelation` 仍在 fe-core）；hive/hudi 未 override `listPartitions*`（Hudi applyFilter 列全部分区不裁剪，Hive applyFilter 做 EQ/IN 裁剪）；三模块零测试。**已验证非阻塞**：SPI scan/split 通用链路（`PluginDrivenScanNode.planScan`→BE）已被合入的 trino-connector 走通；hudi-specific 的「单 ScanNode 混合 COW-native + MOR-JNI 每-split 格式」正确性才是待验证项。
- **触发场景**：用户准备启动 P3，要求 code-grounded 确认 HMS 就绪情况。
- **新方案**：P3 不再以「等 P5/P7 交付 HMS-over-SPI」为前提；改为 (1) recon SPI scan/split 路径（hudi-specific 正确性），(2) 写 catalog 模型决策备忘（见下），用户签字后再编码。**不要直接 flip `SPI_READY_TYPES`**。
- **替代方案（catalog 模型，待用户决策）**：
  - **(a) hms-first**：`HiveConnectorProvider(type="hms")` 接入 `PluginDrivenExternalCatalog` + fe-core 消费 `tableFormatType` 分流，hudi 作薄增量。一次命中真正架构阻塞、契合现存 `type="hms"` 设计；但把 P7(hive/HMS) 范围拉进 P3、触碰 live 重度使用的 HMS 路径、零测试网，回归风险大。
  - **(b) gate 后建脚手架**：先做 format-dispatch / 增量 SPI hook / MVCC + 补测试（design+stub，不动 live 路径、零回归）；但 hudi 不单独端到端可用，推迟模型决策。
  - **(c) 直接 flip gate** —— **否决**（模型错配下 `"hudi"` provider 不可达；live hms catalog 推到未测 SPI；增量丢失；高回归）。
- **影响范围**：
  - 文档：本条 + [connectors/hudi.md](./connectors/hudi.md)（已加更正注）+ [PROGRESS.md](./PROGRESS.md)（§一 P3 / §二看板 / §四 / §六 / §七 已同步）+ [HANDOFF.md](./HANDOFF.md)（P3 起点）✅；master plan / hudi.md 章节正文待 P3 按选定模型重写。
  - 代码：无（recon only）。
  - 计划：P3 性质从「等依赖」变为「先定模型 + 补 SPI 分流/增量/测试」；可能与 P7(hive/HMS) 部分合并或重排序——待模型决策。
- **关联**：D-005、P1-T04（incrementalRelation gap）、R-001（image 兼容）、P3、master plan §3.4/§3.8
- **后续动作**：
  - [x] P3 session：recon SPI scan/split —— **完成**（verdict：混合 COW-native/MOR-JNI 非问题、与 legacy 结构等价；plumbing 正确；parity gap 见下，详见 HANDOFF 1b）
  - [ ] scan 侧 HIGH 修复（与模型无关、多在 SPI surface 内）：①`HudiScanPlanProvider` override `populateScanLevelParams` 设 current_schema_id+history_schema_info + `HudiScanRange` 设 `THudiFileDesc.schema_id`；②column_types 改发完整 Hive 类型串（弃 `getTypeName()`）+ 停止逗号 join/split（typed list 端到端）；③time-travel 透传 snapshot 否则 fail-loud；④增量读 fail-loud
  - [x] 写 catalog 模型决策备忘（a/b），用户签字 —— **完成**：定 **hybrid**（[D-019](./decisions-log.md)），建 [tasks/P3](./tasks/P3-hudi-migration.md)（批 A 现做 b、批 E 推迟 a）
  - [ ] 选定后：补 `tableFormatType` 分流消费、增量 SPI hook、`listPartitions` override + 真实 applyFilter 裁剪、三模块测试

### DV-004 — T13 用户向安装文档不在本代码仓（在 doris-website 仓）

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正
- **原计划位置**：[tasks/P2 §P2-T13](./tasks/P2-trino-connector-migration.md)：「`docs-next/` 加 trino-connector 插件安装步骤」
- **偏差描述**：原计划假设本代码仓有 `docs-next/`；实际本仓只有 `docs/`，用户向文档（docs-next / i18n）在独立的 doris-website 仓。
- **新方案**：T13 在本 PR 内只同步 plan-doc 跟踪文档；用户向安装文档另在 doris-website 仓提交。
- **影响范围**：文档 — 本仓只更新 plan-doc；website 仓待办。代码/计划 — 无。
- **关联**：P2-T13
- **后续动作**：[ ] 在 doris-website 仓补 trino-connector 插件安装文档

### DV-003 — T12 迁移兼容回归测试：先例与目标目录均不存在，且本地不可运行

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟡 推迟
- **原计划位置**：[tasks/P2 §P2-T12](./tasks/P2-trino-connector-migration.md)：「类似 P0 的 ES/JDBC migration compat；放入 `regression-test/suites/external_catalog/`」
- **偏差描述**：(1) 不存在「P0 ES/JDBC migration_compat」先例套件；(2) 不存在 `external_catalog/` 目录（实际为 `external_table_p0/` 与 `external_table_p2/`）；(3) 该测试需真实 Trino plugin + 外部数据源 + 运行集群，本开发环境无 docker/集群，无法编写后验证。
- **触发场景**：批 E 启动 T12 时 recon 发现。
- **新方案**：推迟到有 Trino plugin + docker/集群的环境再编写并验证；不往本 PR 加无法验证的套件。
- **替代方案**：盲写 groovy 放 `external_table_p0/trino_connector/` 但本地不可验证——否决（违反"测试要可验证"）。
- **影响范围**：测试 — 迁移 image 兼容回归缺位（现有 trino_connector 功能套件仍在）。代码/计划 — 无。
- **关联**：P2-T12、R-001（image 兼容回归风险）
- **后续动作**：[ ] 集群/CI 环境补 `trino_connector_migration_compat`（CREATE CATALOG→image→重启读回 + 旧 image 含 `TRINO_CONNECTOR` 枚举反序列化）

### DV-002 — T11 单测无法 mock Trino plugin；`TrinoJsonSerializer` 非纯单元

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正（commit `9bba12a44b2`）
- **原计划位置**：[tasks/P2 §P2-T11](./tasks/P2-trino-connector-migration.md)：「最少 4 个 test class（schema / predicate / type-map / json）；mock Trino plugin」
- **偏差描述**：(1) fe-connector-trino 仅依赖 junit-jupiter，无 Mockito；(2) `TrinoJsonSerializer` 构造需 `HandleResolver` + Trino `TypeRegistry`（来自已加载 plugin 的 `TrinoBootstrap`），非纯单元；(3) schema / applyFilter / preCreateValidation 需活的 connector。无 plugin 无法在单测覆盖。
- **触发场景**：T11 启动、读 3 个 SUT 源码时发现。
- **新方案**：写 3 个纯转换器 JUnit5 测试（`TrinoPredicateConverterTest` 14 / `TrinoTypeMappingTest` 11 / `TrinoConnectorProviderTest`=validateProperties 4 = 29 测试），本地 `mvn test` 全绿、不需 plugin；砍掉 json/schema，用 `validateProperties`（批 A T01）替补第 3 类。plugin 依赖路径由现有 `external_table_p0/p2` trino_connector regression 套件覆盖。
- **替代方案**：引 Mockito mock Trino connector 测 pushdown/metadata——否决（偏离 module 现有约定、脆弱、费时）。
- **影响范围**：测试 — 单测覆盖纯转换逻辑；集成路径靠 regression。代码/计划 — 无。
- **关联**：P2-T11、P2-T02
- **后续动作**：（无；plugin 路径覆盖见 T12 follow-up）

### DV-001 — 批 D（删 legacy）范围遗漏 `ExternalCatalog` db 路由与 legacy 测试

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正（commit `ed81a063fe8`）
- **原计划位置**：[tasks/P2 §P2-T08..T10](./tasks/P2-trino-connector-migration.md) / HANDOFF：批 D 只列 T08（translator 分支）+ T09（CatalogFactory case）+ T10（删目录）
- **偏差描述**：recon 发现还有两处引用 legacy 目录、计划未列：(1) `ExternalCatalog.java:948` enum switch `case TRINO_CONNECTOR` 实例化 `TrinoConnectorExternalDatabase`；(2) 测试 `fe-core/.../trinoconnector/TrinoConnectorPredicateTest.java` 测被删的 `TrinoConnectorPredicateConverter`。删目录后两者编译失败。另：原 T10 描述「删 GsonUtils 3 个 class-token 注册」已过时（批 B/T03 已 atomic-replace，T10 不碰 GsonUtils）。
- **触发场景**：批 D 删目录前 `grep datasource.trinoconnector` 全仓 recon。
- **新方案**：(1) `case TRINO_CONNECTOR` 改返 `PluginDrivenExternalDatabase`（照搬已迁移的 JDBC case line 936）+ 删 import；(2) 删该 legacy 测试（新测试见 T11）。**有意保留** `MetastoreProperties.Type.TRINO_CONNECTOR` + `TrinoConnectorPropertiesFactory`（在 `property/metastore/` 子系统，不引用被删目录，SPI 路径可能仍需）。
- **替代方案**：`case TRINO_CONNECTOR` 整删落 default 返 null——否决（JDBC 先例显式返 PluginDrivenExternalDatabase，SPI 需要）。
- **影响范围**：代码 — 已合入批 D commit `ed81a063fe8`。文档 — 本条 + tasks/P2 T10 备注已更正。计划 — 无。
- **关联**：P2-T08、P2-T09、P2-T10
- **后续动作**：[ ] 评估 `MetastoreProperties` trino 条目是否真被 SPI 路径使用（若纯死代码可后续清）

---

## 附录：偏差模板

发现偏差时复制以下模板到 §详细记录 顶部，并更新 §📋 索引表。

```markdown
### DV-NNN — <一句话主题>

- **发现日期**：YYYY-MM-DD
- **发现 session / agent**：（哪次 session 发现的）
- **当前状态**：🟢 已修正 / 🟡 待修正 / 🔴 阻塞中
- **原计划位置**：[文档名 §章节](./xxx.md)，引用原句或代码片段
- **偏差描述**：原计划说 X，实施中发现 Y
- **触发场景**：什么操作 / 什么连接器 / 什么 corner case 引发的
- **新方案**：现在的处理方式
- **替代方案**：考虑过的其他修正
- **影响范围**：
  - 文档：哪些文件需要同步修改（已修改的标 ✅）
  - 代码：哪些已合 PR / 待提 PR
  - 计划：是否影响阶段时长 / 顺序
- **关联**：[task ID]、[PR #]、[decision D-NNN（如果偏差催生了新决策）]
- **后续动作**：
  - [ ] 同步修改文档 X
  - [ ] 提 PR 调整代码 Y
  - [ ] 通知相关 task owner
```

---

## 何时应该写偏差日志（典型场景）

1. RFC 中某 SPI 方法签名在实际实现时发现参数不够 / 太多
2. 原计划某阶段时长估算严重偏差（如 2 周变 4 周）
3. 实施中发现某连接器有未预料的特殊性（如 Iceberg 某 catalog flavor 不支持某操作）
4. 原计划的某 task 拆分粒度太粗 / 太细，重新拆分
5. 原计划假设某个三方库行为 X，实际是 Y
6. 决策（D-NNN）在落地时发现执行不了，需要重新评估
7. 跨连接器假设的一致性被打破（如某 SPI 默认行为对 connector A 合理但对 B 不合理）

## 何时**不**应该写偏差日志

- 普通 bug 修复（写 commit message）
- task 的子步骤微调（在 task 文件里加备注）
- 文档错别字 / 链接错误（直接改）
- 命名重构 / 重命名（直接改）
- 已知的实施细节决策（如选用 `HashMap` vs `LinkedHashMap`）
