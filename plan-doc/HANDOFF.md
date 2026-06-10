# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🔥 2026-06-10 — P5 paimon B4 完成（sys-tables E7 + MVCC E5，T16-T20）；下一步 = B5（MTMV 桥）

> **本 session**：按 [tasks/P5-paimon-migration.md](./tasks/P5-paimon-migration.md) 落地 **B4**。subagent-driven（understand workflow 6-agent → 主线 firsthand 核读 → 用户签 D-039 + T20 留 B4 → 5 dispatch 各 implement→双审/fix-loop → 3-lens final holistic review + 主线 firsthand 复跑）。**B0–B3 已 commit（`a2b765677d1`）；B4 改动未提交**（用户控时机，工作树仅含 B4）。

## ✅ 本 session 已完成（B4 = T16-T20）

- **T16**（greenfield E7 SPI）：`ConnectorTableOps` 加 `listSupportedSysTables(session, baseHandle)`→`List<String>` + `getSysTableHandle(session, baseHandle, sysName)`→`Optional<ConnectorTableHandle>`，**default no-op**（MC/jdbc/es/trino 不受影响）。唯一 `fe-connector-api` 改动。
- **T17**（paimon 实现 E7）：`PaimonConnectorMetadata.listSupportedSysTables`=`SystemTableLoader.SYSTEM_TABLES`；`getSysTableHandle` 经**现有** `getTable(Identifier)` seam 喂 4-arg `new Identifier(db,table,"main",sysName)`（branch 硬编 "main"）。`PaimonTableHandle` 加 serializable `sysTableName`+`forceJni`(="binlog"||"audit_log")，`forSystemTable` factory，lowercase 规范化，equals/hashCode 含 sysTableName。**fix-loop**：抽共享 `PaimonTableResolver.resolve(catalogOps, handle)`（metadata+scan **一处** sys-aware reload；修 scan-twin 丢 sys-Table）+ Java 序列化 round-trip 测 + null-guard。
- **T18**（fe-core 通用）：`PluginDrivenExternalTable` 把 4 处 handle 获取集中入 `protected resolveConnectorTableHandle(session, metadata)` seam + `getSupportedSysTables()` override 委托连接器 `listSupportedSysTables`；新 `PluginDrivenSysExternalTable extends PluginDrivenExternalTable`（**报 PLUGIN_EXTERNAL_TABLE**，override `resolveConnectorTableHandle` 喂 sys handle，transient 不持久化/**不 GSON 注册**）+ 新 `PluginDrivenSysTable extends NativeSysTable`（`createSysExternalTable`）。复用 live `SysTableResolver`/`TableIf.getSupportedSysTables/findSysTable` 机制（D-039）。
- **T19**（forceJni + 描述符 + fail-loud）：`PaimonScanPlanProvider` DataSplit 分支 gate `shouldUseNativeReader(forceJni,…)`=`!forceJni && supportNativeReader`（ro 仍 native、metadata 表经 non-DataSplit JNI）；`PaimonConnectorMetadata.buildTableDescriptor`→`HIVE_TABLE`+`THiveTable`（**同修 B2 遗留** SCHEMA_TABLE fallback [DV-024]，普通+sys 表共修）；`PluginDrivenScanNode` 加 `checkSysTableScanConstraints()`（sys 表 + scan-params/snapshot → fail-loud，跑于 getSplits+startSplit 两入口）。
- **T20**（首个 E5 消费者，**inert until B5**）：`beginQuerySnapshot/getSnapshotAt/getSnapshotById`（snapshot seam `latestSnapshotId`/`snapshotIdAtOrBefore`/`snapshotExists`：SDK 实现在 `CatalogBackedPaimonCatalogOps`、fake 在 `RecordingPaimonCatalogOps`；sys handle→`Optional.empty`；空表→-1；SPI **empty-if-none** vs legacy throw 已 doc——B5 消费方 surface 用户错误）+ `PaimonConnector.getCapabilities`=`SUPPORTS_MVCC_SNAPSHOT/TIME_TRAVEL`。
- **验证（主线 firsthand 复跑）**：import-gate 0；连接器 `Tests run: 124, Failures: 0, Errors: 0, Skipped: 1`(live)；fe-core `PluginDriven*Test` `Tests run: 100, Failures: 0, Errors: 0`；checkstyle 0；**无 cutover 泄漏**（paimon 未入 `SPI_READY_TYPES`、GsonUtils/CatalogFactory/PhysicalPlanTranslator 零改）+ **无 B5 泄漏**（`PluginDrivenExternalTable` 仍非 MvccTable）。每 dispatch 双审/mutation-verified；3-lens final holistic = PARITY/SCOPE READY + 1 ADVERSARIAL BLOCKER 已修。

## 🧠 核心发现 / 纠偏（understand 纠偏 2 处 + final review 1 BLOCKER）

1. **D-039（签字）— E7 形状 = 复用 live SysTable 机制（非 RFC §10）**：RFC §10 的「`$`-后缀普通表 + 连接器 `getTableHandle` 内解析 + `listSysTableSuffixes`」**从未落地**；live fe-core 用 `SysTableResolver`+`NativeSysTable`+`TableIf.getSupportedSysTables/findSysTable`（iceberg+legacy-paimon 共用）。用户签复用该机制。RFC §10 已加 superseded 脚注（[DV-023]）。
2. **T20 MVCC inert until B5（签字留 B4）**：E5 SPI 方法早存在 default-no-op，但 `PluginDrivenExternalTable` 非 `MvccTable`、`ConnectorMvccSnapshotAdapter` 零构造方、capability 零 reader → T20 实现纯连接器侧 groundwork，无可观察行为，须 B5 接活。翻闸(B7) gated on B5，故 inert capability 不达用户（安全）。
3. **final review BLOCKER（已修）**：`PluginDrivenScanNode.create` 原直调 `metadata.getTableHandle(remoteName)` **绕过** T18 的 `resolveConnectorTableHandle` seam → sys 表得普通 handle（forceJni=false）→ binlog/audit_log DataSplit 走 native **静默错行**（inert today，翻闸后 live）。修 = `create` 改走 `table.resolveConnectorTableHandle(session, metadata)`（普通表字节等价、sys 表得 sys handle），TDD red→green。**教训**：scan node 有独立 handle 获取面，T18 集中 seam 时漏了它——下轮改 fe-core handle 流时须 grep 全 `metadata.getTableHandle(` 调用方。
4. **DV-024（B2 遗留缺陷，B4 顺修）**：连接器无 `buildTableDescriptor` override → 普通 paimon plugin 表 toThrift 走 SCHEMA_TABLE fallback（BE `descriptors.cpp:635` SchemaTableDescriptor），legacy+sys 须 HIVE_TABLE（`:644`）。T19 一处 override 同修普通+sys。

## 🎯 下一 session = B5（MTMV 桥；gated on B4 全完，现满足）

- **核心任务 = 把 B4 inert 的 E5 接活 + 落 MTMV 桥**。批次依赖见 [tasks/P5](./tasks/P5-paimon-migration.md) §批次依赖。**gated on D2=A（已签）**。
- **T21（GAP-LISTPART-AT-SNAPSHOT）**：`listPartitions` 加 at-snapshot 重载（按 pin 的 snapshotId 列分区）；连接器实现；默认保 latest 向后兼容。单-pin 不变式前提。
- **T22（fe-core）**：`PaimonPluginDrivenExternalTable extends PluginDrivenExternalTable` implements `MTMVRelatedTableIf`+`MTMVBaseTableIf`+`MvccTable`；`loadSnapshot`（`beginQuerySnapshot` 定 snapshotId + at-snapshot 物化分区集**一次**）。**这是 E5 接活点**：须调 `connector.getMetadata().beginQuerySnapshot` + 构造 `ConnectorMvccSnapshotAdapter`（现零构造方）；并把 scan-params/time-travel 接到 `PluginDrivenScanNode`（接活后 T19 sys-table fail-loud guard 才生效，否则现 dormant）。
- **T23**：子类 MTMV 方法（getTableSnapshot→`MTMVSnapshotIdSnapshot`(-1)/getPartitionSnapshot→`MTMVTimestampSnapshot`(缺抛 AnalysisException)/getAndCopyPartitionItems(读 pin 非重列)/getPartitionType/getPartitionColumnNames/isPartitionColumnAllowNull(true)/beforeMTMVRefresh(no-op)/getNewestUpdateVersionOrTime(**绕 pin**)）。
- **T24**：rehome fe-core `PaimonMvccSnapshot`（包 `ConnectorMvccSnapshot` + fe-core 物化 name→PartitionItem/lastModifiedMillis/listed-count）；downcast 留 fe-core 内。
- **T25**：isPartitionInvalid parity（捕 listPartitions count vs 成功构建 PartitionItem count，size 不匹配→UNPARTITIONED 全表刷）；MTMV 单-pin 不变式测 + UT。
- **B5 还须翻 `partition_columns` schema key + 6-arg planScan/requiredPartitions + FE 消费 `listPartitions*`**（B2 遗留前置硬门——`getTableSchema` 现发 `partition_keys`，fe-core `PluginDrivenExternalTable:181` 读 `partition_columns`，FE 现把 paimon 当非分区）。raw-vs-rendered（`listPartitionValues` 返 RAW epoch-day vs legacy TVF RENDERED）须核。
- **B6**（procedure doc no-op，独立）可随时穿插。

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false`（**须 -am**）；改连接器 `:fe-connector-paimon`、改 SPI `:fe-connector-api`、改 fe-core `:fe-core`（**fe-core 大，测试用 `-Dtest='PluginDriven*Test' -DfailIfNoTests=false` SCOPE**）。读真实 `Tests run:`/`BUILD`，勿信后台 echo exit（[[doris-build-verify-gotchas]]）。
- 连接器禁 import fe-core/fe-common（`org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`；import-gate `bash tools/check-connector-imports.sh`）。**允许** `org.apache.paimon.*`、`org.apache.doris.connector.{api,spi}.*`、**`org.apache.doris.thrift.*`**（thrift provided，MC/paimon 均用）。连接器测试无 mockito（手写 `RecordingPaimonCatalogOps`/`RecordingConnectorContext`/`FakePaimonTable`，测带 WHY+MUTATION）；**fe-core 测可用 mockito**；checkstyle 含 test 源、绑 validate。
- **subagent-driven 节奏（B4 用，B5 复用）**：understand workflow（read-only fan-out 验 plan 前提，**警惕退化 stub**）→ 主线 firsthand 核读 + 用户签决策 → 每 dispatch 用 Agent 工具（非 worktree、共享 dirty tree、顺序 build-on-previous）implement→spec/quality 双审→fix-loop → final holistic Workflow（3 lens 并行：parity/adversarial/scope）+ 主线 firsthand 复跑。**所有 subagent prompt 禁 `git checkout/restore/stash/reset`** + 嘱「不要 commit」。**无 SendMessage 工具**——fix-loop 用新 Agent dispatch（带全上下文）。
- 翻闸（B7）GSON **7 注册原子齐迁**（5 catalog + db + table→`PaimonPluginDrivenExternalTable` 非裸 base，[[catalog-spi-gson-migrate-all-three]]）；删 legacy（B8）后验 paimon-core FE classpath 恰一份（[[catalog-spi-be-java-ext-shared-classpath]]）。
- **未跟踪/本地 scratch 勿提交**：`regression-test/conf/regression-conf.groovy`(+`.bak`，含明文凭据)、`.audit-scratch/`、`conf.cmy/`、`.claude/scheduled_tasks.lock`、**`META-INF/`**（本 session 出现的 maven 构建产物，勿 `git add`）。B4 未碰它们。

## 🧠 给下一个 agent 的 meta

- **D1/D2/D4/D5/D6/D7/D-039 已签字**，B0+B1+B2+B3+B4 已落 —— 按设计 doc B5→B9 续。**B4 未提交**（工作树 = 仅 B4 改动）。
- **live e2e（真实 paimon 各 flavor 环境）= 翻闸真正完成门**（CI 跳）。**B4 新增 live-e2e 硬门**：① `buildTableDescriptor`→HIVE_TABLE 在 BE 真 paimon 普通表+sys 表（离线只到连接器边界，[DV-024]）；② MVCC SDK-delegation（`CatalogBackedPaimonCatalogOps` DataTable cast / `earlierOrEqualTimeMills` / `tryGetSnapshot`，离线仅 fake）；③ binlog/audit_log 真走 JNI（forceJni 端到端）+ snapshots/schemas sys 表查询；④ sys 表 time-travel fail-loud（须 B5 接活 scan-params/snapshot 后）。累计前批 live 门见 tasks/P5 §当前阻塞项。
- **B5 最高 correctness 风险**：MTMV 单-pin 不变式（snapshotId 与分区集同源）；`lastFileCreationTime()` 跨 flavor 可靠性（SDK 行为，源码不可验，须 live）。
- auto-memory：[[catalog-spi-p5-paimon-design]]（设计）、[[catalog-spi-p5-b1-design]]（B1）、[[catalog-spi-p5-b2-design]]（B2）、[[catalog-spi-p5-b3-design]]（B3）、[[catalog-spi-p5-b4-design]]（B4：D-039 E7 机制 + T20 inert + BLOCKER + DV-024）、[[catalog-spi-connector-session-tz-gotcha]]（含 paimon 例外）。
