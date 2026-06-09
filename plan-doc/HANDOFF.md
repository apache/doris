# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🔥 2026-06-10 — P5 paimon B3 完成（DDL metadata，T11-T15）；下一步 = B4（sys-tables E7 + MVCC E5）

> **本 session**：按 [tasks/P5-paimon-migration.md](./tasks/P5-paimon-migration.md) 落地 **B3**（DDL metadata）。subagent-driven（understand workflow → 主线 firsthand 核读 → 用户签 D7 → 3 dispatch 各 implement→spec-review→quality-review，全 mutation-verified → 3-lens final holistic review + 主线 firsthand 复跑）。**B2+B3 改动均未提交**（用户决定何时 commit；同处 dirty tree）。

## ✅ 本 session 已完成（B3 = T11-T15，纯连接器侧，0 fe-core/SPI/api 改动）

- **T11**：`PaimonTypeMapping.toPaimonType(ConnectorType)` 反向（switch on `getTypeName()`，byte-parity `DorisToPaimonTypeVisitor.atomic:82-108`：char-family→`VarCharType(MAX)`、DATETIME→no-arg `TimestampType`(scale 丢)、map-key `.copy(false)`、struct id `AtomicInteger(-1)`、gap→`DorisConnectorException`保留 legacy 窄集）。
- **T12**：新 `PaimonSchemaBuilder.build(request)`（port `PaimonMetadataOps.toPaimonSchema:231-256`：PK from `properties["primary-key"]`、identity partitionKeys、location→`CoreOptions.PATH`、strip primary-key/comment、per-column `.copy(nullable)`）。**2 故意 safer 偏差**（已 doc + 测）：comment `properties["comment"]`优先否则 fallback `request.getComment()`（legacy 只读 prop，丢 COMMENT 子句）；PK drop-blank。非-identity transform→throw。
- **T13**：`createTable`（override request-overload，`PaimonSchemaBuilder` build 在 wrap **外** → schema-fail raw 不双包）+ `dropTable`（handle-based idempotent ignoreIfNotExists=true）。remote-vs-local 名 bug 在 SPI 层 **moot**（请求单名 from `db.getRemoteName()`）。
- **T14**：`supportsCreateDatabase=true` + `createDatabase`（HMS-only-props gate，flavor 读注入 `catalogProperties` via `resolveFlavor`，gate 在 auth **前**；ignoreIfExists=false 因 FE 已 short-circuit）+ 4-arg `dropDatabase(force)`（enumerate-loop **AND** native cascade，legacy `performDropDb:147-163` belt-and-suspenders，非 MC enumerate-only；整体一个 auth scope）。
- **T13/T14 D7=B**：seam `PaimonCatalogOps` 加 4 DDL 方法（+ `CatalogBackedPaimonCatalogOps` delegations + `RecordingPaimonCatalogOps` fake，paimon `Catalog` 签名 javap 核）；**thread `ConnectorContext` 入 `PaimonConnectorMetadata`（3-arg ctor，无 2-arg；`PaimonConnector.getMetadata` 传 context）**；4 个 DDL op 各包 `context.executeAuthenticated`，**read 路径不包**（B2 现状未改）。
- **T15**：4 新测类（`PaimonTypeMappingToPaimonTest`10 / `PaimonSchemaBuilderTest`10 / `PaimonConnectorMetadataDdlTest`9 / `PaimonConnectorMetadataDbDdlTest`11）+ `RecordingConnectorContext`（failAuth 钉 auth-wrap：seam 在 wrap 内）+ `RecordingPaimonCatalogOps` DDL 扩。no-mockito，WHY+MUTATION。
- **验证（主线 firsthand 复跑）**：`Tests run: 96, Failures: 0, Errors: 0, Skipped: 1`（1=live）+ BUILD SUCCESS + checkstyle 0 + import-gate 0 + **无 fe-core/fe-connector-api/fe-connector-spi 改动** + 无 B7 cutover 泄漏。每 dispatch 双审 mutation-verified；3-lens final holistic（parity/adversarial/scope-build）= 全 READY。

## 🧠 核心发现 / 纠偏（B3 understand 纠偏 1 处 plan 前提 → D7；另证实 1 处 plan 前提为真）

1. **T13 authenticator → D7=B（签字）**：plan「per-flavor authenticator」与 code 冲突——MC DDL **不**用 authenticator；legacy `PaimonMetadataOps` **每** call 包 `executionAuthenticator.execute`；**B2 read 路径不 re-wrap**（靠构建时一次 wrap）；metadata 当前不收 `ConnectorContext`。用户签 **B=legacy parity**（thread context + 每 DDL op 包 wrap，read 不动）。**遗留不一致**：read 未 wrap、DDL 已 wrap——若 live-e2e 证 Kerberized **读**也需 call-time doAs，则 read 须补 wrap（B2 回改，归翻闸前 live-e2e authenticator 门）。
2. **「PluginDrivenExternalCatalog 已 override FE 侧」证实为真**（非纠偏）：FE 4 个 DDL 分发（createTable:300 / createDb:355 / dropDb:387 / dropTable:439）已通用接 SPI（`connector.getMetadata`），MC 已证端到端通。memory [[catalog-spi-cutover-fe-dispatch-gap]] 警告的 FE 分发缺口**对 paimon DDL 不适用**——真闸是 `CatalogFactory.SPI_READY_TYPES` 成员（paimon 未入 → 现走 built-in `PaimonExternalCatalog`），属 **B7/T27**，非 B3 缺口。B3 纯连接器侧。
3. **understand workflow 韧性**：6 agent 中 2 个返回退化 stub（占位「test」值），其 scope 由其余 4 agent 全覆盖并 cross-verified——结论无损。下轮可在 prompt 里加「拒绝占位输出」。

## 🎯 下一 session = B4（sys-tables E7 + MVCC E5；gated on B2+B3 全完，现满足）

- **T16（greenfield SPI，签名须慎）**：`ConnectorMetadata.listSupportedSysTables`(default emptySet) + `getSysTableHandle`(default empty)；保 MC/jdbc/es/trino 不受影响。**被未来 iceberg/hudi 复用，设计错须二次迁移**。
- **T17**：paimon 实现 E7（名取 `SystemTableLoader.SYSTEM_TABLES`；`getSysTableHandle` 走 4-arg `Identifier(db,tbl,"main",sysName)`；handle 带 sysName+forceJni；reload fallback；branch="main" 限制保留+doc）。
- **T18（greenfield fe-core）**：通用 `PluginDrivenSysExternalTable extends PluginDrivenExternalTable`(报 PLUGIN_EXTERNAL_TABLE，**勿报 PAIMON_EXTERNAL_TABLE**) + `NativeSysTable` factory；override `getSupportedSysTables/findSysTable` 委托连接器。
- **T19**：`PaimonScanPlanProvider` 加 forceJni 分支（binlog/audit_log + 非 DataTable sys 全走 JNI——native = 行错静默）+ 通用节点 fail-loud 拒 sys 表 scan-params/time-travel；核 BE sys-table `TTableDescriptor`(HIVE_TABLE?)。
- **T20（首个 E5 消费者）**：`beginQuerySnapshot/getSnapshotAt/getSnapshotById`(返 `ConnectorMvccSnapshot(snapshotId)`，空表 -1)+声明 `SUPPORTS_MVCC_SNAPSHOT/TIME_TRAVEL`；sys 表不得透出 time-travel。
- 批次依赖见 [tasks/P5-paimon-migration.md](./tasks/P5-paimon-migration.md) §批次依赖。**B5**（MTMV 桥）gated on B4 全完。**B6**（procedure doc no-op，独立）可随时穿插。

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false`（**须 -am**，裸 -pl 会因 `${revision}` 兄弟解析虚假失败）；改连接器 `:fe-connector-paimon`、改 SPI `:fe-connector-api`、改 fe-core `:fe-core`。读真实 `Tests run:`/`BUILD`，勿信后台 echo exit（[[doris-build-verify-gotchas]]）。
- 连接器禁 import fe-core/fe-common（`org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`；import-gate `bash tools/check-connector-imports.sh`）。`org.apache.paimon.*` 全可（含 `catalog.Catalog` DDL、`schema.Schema`、`CoreOptions`、`types.*`、`utils.DateTimeUtils`）。`org.apache.doris.connector.{api,spi}.*` 可（`ConnectorContext.executeAuthenticated(Callable<T>) throws Exception` 默认 no-op）。连接器测试无 mockito（`RecordingPaimonCatalogOps`/`RecordingConnectorContext`/`FakePaimonTable` 手写 fake；测须带 WHY+MUTATION）；checkstyle 含 test 源、绑 validate。
- **subagent-driven 节奏（B3 用）**：understand workflow（read-only fan-out 验 plan 前提）→ 主线 firsthand 核读 + 用户签决策 → 每 dispatch 用 Agent 工具（非 worktree 隔离，共享 dirty tree，顺序 build-on-previous）implement→spec-review→quality-review→fix-loop → final holistic Workflow（多 lens 并行）+ 主线 firsthand 复跑。**所有 subagent prompt 里禁 `git checkout/restore/stash/reset`**（会抹未提交工作）+ 嘱「不要 commit」（用户控）。
- 翻闸（B7）GSON **7 注册原子齐迁**（5 catalog + db + table，[[catalog-spi-gson-migrate-all-three]] / [[catalog-spi-cutover-fe-dispatch-gap]]）；删 legacy（B8）后验 paimon-core FE classpath 恰一份（[[catalog-spi-be-java-ext-shared-classpath]]）。
- **未跟踪/本地 scratch 勿提交**：`regression-test/conf/regression-conf.groovy`(+`.bak`，**含明文 ak/sk/Kerberos 凭据**)、`.audit-scratch/`、`conf.cmy/`、`.claude/scheduled_tasks.lock`（用户本地集群配置）。B3 未碰它们。

## 🧠 给下一个 agent 的 meta

- **D1/D2/D4/D5/D6/D7 已签字**，B0+B1+B2+B3 已落 —— 按设计 doc B4→B9 续。
- **live e2e（真实 paimon 各 flavor 环境）= 翻闸真正完成门**（CI 跳），翻闸前用户验；现累计 live-e2e 硬门：hms/dlf metastore client 跨 loader、jdbc driver allow-list、hive-site.xml、live createCatalog（B1）；DDL `executeAuthenticated`(D7=B) 在 Kerberized HMS/HDFS 正确性（B3）；`lastFileCreationTime()` 跨 flavor + DATE 渲染 raw-vs-rendered（B2）。
- **B5 reconcile 项（仍 dormant）**：`partition_columns` schema key 未翻（FE 现把 paimon 当非分区）；`listPartitionValues` 返 RAW spec、legacy TVF 返 RENDERED；MTMV 单-pin 不变式（最高 correctness 风险）。
- auto-memory：[[catalog-spi-p5-paimon-design]]（设计决策）、[[catalog-spi-p5-b1-design]]（B1 flavor 装配）、[[catalog-spi-p5-b2-design]]（B2 3 处 plan 纠偏）、[[catalog-spi-p5-b3-design]]（B3 DDL：D7=B authenticator + FE-dispatch 缺口证伪 + 2 safer 偏差）、[[catalog-spi-connector-session-tz-gotcha]]（含 paimon 例外）。
