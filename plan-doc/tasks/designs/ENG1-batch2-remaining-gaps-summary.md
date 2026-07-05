# ENG-1 批量修复（第二批）= 除连通性外的 6 条剩余缺口

> 信源 = `plan-doc/reviews/P6.6-ENG1-capability-twin-audit-2026-07-04.md` §三 + 任务清单 §5b。
> 用户裁定（2026-07-04）：直接照审计结论动码、不逐条 recon/写单独 design、末尾统一 review。
> **本批 = F4/F13、F9/F10/F12、F11、F6/F7、F14**（排除连通性 F2/F3/F15/F16；F1 已修 `6e14fecc21b`；F8 接受偏差）。

## 逐条落地

### F4/F13（low）— SHOW CREATE `tbl$snapshots` 渲染 sys 壳非 base DDL
- **改**：`ShowCreateTableCommand.doRun` 抽出包级静态 helper `redirectSysTableToSource(TableIf)`，补 `PluginDrivenSysExternalTable → getSourceTable()` 臂（与 legacy `IcebergSysExternalTable` 臂对称，与 `validate()` 已有的解包对称）。中立 sys 类型，非 `Iceberg*` → 铁律干净。抽 helper 仅为可单测（驱动整个 doRun 需全套 ConnectContext/Env/access-manager，过重且脆），未动 `validate()`。
- **测**：新 `ShowCreateTableCommandTest`（2 case：unwrap sys→source / 普通表透传）。

### F9/F10/F12（low）— iceberg getComment 恒空
- **F9/F12 改（承载性）**：`IcebergConnectorMetadata.getTableComment` override，从 `table.properties().getOrDefault("comment","")` 取值（本地常量 `TABLE_COMMENT_PROP="comment"` 复刻 fe-core），auth 包装同其余元数据读。纯连接器代码。view handle 命中时 loadTable 抛→调用方 twin catch→""（view 的 comment 走 getViewDefinition/view SHOW CREATE 臂，出 F9 范围）。
- **F10 决定 = 不改共享转义（保留 twin 单引号转义）**：消费者 `Env.getDdlStmt:7520` 用单引号包裹 `COMMENT '...'`，故转义单引号（twin 现状）产出**合法可重解析** SQL；而 legacy `SqlUtils.escapeQuota` 只转双引号，一旦 comment 含 `'` 会产出**坏 SQL**。二者对无引号 comment（唯一被测/常见场景）字节相同。选正确性（Rule 1）+ 外科（Rule 3 不动共享 twin）。**Rule 7 记录**：这是有意偏离 legacy 字节（仅当 comment 含 `"` 时 legacy 多一个冗余 `\"`，双侧 round-trip 语义相同）。
- **测**：`IcebergConnectorMetadataTest.getTableCommentReadsCommentProperty`。

### F11（low）— iceberg 丢异步元数据预热
- **改**：新中立能力位 `ConnectorCapability.SUPPORTS_METADATA_PRELOAD`。`PluginDrivenExternalTable.supportsExternalMetadataPreload` 由 **capability 门控**（替换 legacy 引擎名 `"jdbc"` 字符串，铁律）。iceberg + jdbc 连接器均声明（jdbc 声明以保原行为）。参照 H-10/partition_operations 能力位范式。
- **测**：`PluginDrivenExternalTableTest`（capability 门控 + 无连接器降级）、`IcebergConnectorTest.declaresMetadataPreloadCapability`、`JdbcDorisConnectorTest.testDeclaresMetadataPreloadCapability`。

### F6/F7（low）— EXPLAIN VERBOSE nested columns 块消失
- **改**：`PluginDrivenScanNode.getNodeExplainString` 调用**继承的** `printNestedColumns(output, prefix, getTupleDesc())`（在 backend-detail 之后、连接器 appendExplainInfo 委派之前，匹配 legacy「FileScanNode body 后接连接器行」）。该节点是 `PluginDrivenScanNode`（永非 `IcebergScanNode`）→ 走通用 name-join 臂（PlanNode:954/970），legacy iceberg field-id 合并死臂（949/965）保持不触发（守 memory）。**恢复面 = 所有 plugin FileScan 连接器**（比 iceberg 更广）。
- **残留（记录）**：iceberg field-id 编号注解 `col(3).sub(5)` **不复刻**（cosmetic，FU-h10-deadcode 已跟踪；连接器 appendExplainInfo 无法内联注解访问路径——SlotDescriptor 是 fe-core 类不能越界；BE 仍收编号形路径，查询无影响）。恢复的块访问路径显示为 `col.sub`（名字形）。
- **测**：`PluginDrivenScanNodeVerboseExplainTest.emitsNestedColumnsBlockForPluginConnector`。

### F14（low，最难）— AWS 非 DEFAULT PROVIDER_CHAIN 凭证静默丢
- **可行性**：FEASIBLE-CHEAP（子 agent 审）。mode 字符串在连接器侧存活（catalog `props` = 原始配置，非 `rawProperties()` 那张「diagnostics」图）；FQCN 可在连接器复刻（AWS SDK v2 provider 类在 classpath）。
- **改**：新 `AwsCredentialsProviderModes`（连接器自包含 twin，复刻 legacy `getV2ClassName`/`createV2` + `AwsCredentialsProviderMode.fromString` 归一化 trim/upper/`-`→`_`；`.class.getName()` 取 FQCN，字节同 legacy）。三 loci 补非 DEFAULT provider 发射：`IcebergCatalogFactory.appendRestSigningProperties`（glue/s3tables + other signing-name 两分支）、`appendS3TablesFileIOProperties`（补 `props` 参）、`IcebergConnector.buildAwsCredentialsProvider`（final fallback）。DEFAULT/空/未知→不发射（SDK 默认链，常见场景）。无 fe-core import、无引擎名 seam。
- **残留（记录）**：ASSUME_ROLE 的 STS **base** 凭证仍走默认链（assume-role 本已「孪生」，非 F14 gap 焦点）。
- **附带更正**：rest other-name 分支重构为「AK+SK 齐→显式；否则 provider chain」，比原「无条件调 putRestExplicitCredentials（内部空守）」更贴 legacy `getCredentialType`（单 AK 亦落 PROVIDER_CHAIN）。
- **测**：`AwsCredentialsProviderModesTest`（6 mode→FQCN、DEFAULT/空/未知→null、归一化、provider 实例）、`IcebergCatalogFactoryTest`（3 wiring case + DEFAULT 不发射）。

## 验收（Rule 12 口径，已实测）
- **UT 全绿**：8 个测试类（ShowCreateTableCommandTest 2 / IcebergConnectorMetadataTest 48 / PluginDrivenExternalTableTest 24 / IcebergConnectorTest 14 / JdbcDorisConnectorTest 11 / PluginDrivenScanNodeVerboseExplainTest 4 / AwsCredentialsProviderModesTest / IcebergCatalogFactoryTest 61）+ 广义 fe-core 回归 19 类 0 fail（printNestedColumns 未破其余 explain 测）。
- **mutation KILLED = 7/7**：M1(F4 redirectSysTableToSource)/M2(F9 getTableComment)/M3(F11 supportsExternalMetadataPreload)/M4(F11 iceberg 声明)/M5(F11 jdbc 声明)/M6(F6/F7 printNestedColumns)/M7(F14 resolveMode) 各对应测试均转红（`--fail-never -Dcheckstyle.skip` 一轮全测；checkstyle 在 validate 相位，mutation 引入 unused import 需跳）。
- **checkstyle 0**（api/fe-core/iceberg/jdbc 四模块）、**import-gate 净**（新 `AwsCredentialsProviderModes` 仅 import AWS SDK + java）。
- **e2e flip-gated 未跑**（无集群；F14 credential e2e、F6/F7 EXPLAIN、F9 SHOW CREATE/information_schema、F4 SHOW CREATE 均翻闸后 e2e）→ 登记 ENG-3。**Rule 12：勿谎称已验。**

## 统一 review（末尾，多 agent 对抗，`.claude/wf-eng1-batch2-review.js`）
5 维度（correctness / iron-rule / parity-deviations / test-quality / regression-risk）× 对抗驳斥。结论：
- **1 medium 确认并已修**：F14 `S3_MODE_KEYS` 原漏 `iceberg.rest.credentials_provider_type` 别名（且误含 master 无的 `AWS_CREDENTIALS_PROVIDER_TYPE`）——glue/s3tables catalog 若把 mode 写在 rest 别名上会静默落回默认链。已对齐 master `S3Properties` @ConnectorProperty 三别名 + 加 pin 测试。**（review 抓到真 bug，是本轮价值所在）**
- **1 low 确认并已修**：F14 `providerFor` 6 模式仅测 2 → 补齐全 6 模式断言。
- **1 nit 驳回**：F4 `redirectSysTableToSource` 的 `IcebergSysExternalTable` 臂无测——翻闸后死臂（运行时恒 PluginDrivenSysExternalTable），驳回不补。
- correctness/iron-rule/regression-risk 三维度 0 finding。
