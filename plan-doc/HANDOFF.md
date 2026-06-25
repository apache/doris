# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 C3b-core 继续：③-infra part2（连接器 lineage emit）+ ctx 中立重命名 → 耦合核心 → commit-bridge**

**impl-recon ✅ 本 session（[D-073]，O1-O4 全解、O1/O2 verdict upheld、用户裁 ③ carrier=A）+ ③-infra part1（`ConnectorColumn.uniqueId` carrier）✅**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core 设计 ✅ + step1+2 ✅ + impl-recon ✅ + ③-infra part1 ✅ → 余下待办**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（2 commit）
1. **impl-recon（[D-073]）**：1 对抗 wf `wf_fa7057d5-39b`（6-slice O1-O4+锚点+bridge + 2 verify，**O1/O2 upheld**）+ 主 session 亲核 → **O1-O4 全解**（详 design §11.1）。**用户 AskUserQuestion 裁 ③ v3-lineage uniqueId carrier = Option A**（`ConnectorColumn` 加中立 `uniqueId`，连接器声明）。
2. **③-infra part1（additive/dormant，TDD+mutation）**：`ConnectorColumn` 加中立 `uniqueId` 字段（`UNSET_UNIQUE_ID=-1` 默认 + `withUniqueId(int)` 不可变 copy + `getUniqueId()`，仿 `invisible()`/`withTimeZone()`；`withTimeZone`/`invisible` copy 链透传 uniqueId；equals/hashCode 含之）+ `ConnectorColumnConverter.convertColumn` 加 `if (cc.getUniqueId()>=0) column.setUniqueId(...)`。`ConnectorColumnConverterTest` **15/0/0**（+2：`convertColumnDefaultsToUnsetUniqueId` / `convertColumnPropagatesUniqueId` 用 `.withUniqueId(2147483540).invisible()` 证 copy 保真+converter 重应用）+ **mutation 红证**（去 setUniqueId 重应用 → propagate test FAIL，default test 仍绿）。import-gate PASS；SPI 无 `org.apache.iceberg`；`SPI_READY_TYPES` 未改。

## 🔑 impl-recon 决定性结论（动码前必读，详 design §11）
- **O1 解：合成列无 carrier 需求**。`$operation/$row_id` 不进 thrift sink，按名 `setMaterializedColumnName:615`→`colName` BE 按名匹配；连接器 `planWrite` 不读 `handle.getColumns()`。bridge 只需 (1) `WriteOperation=MERGE/DELETE` 透到写 handle（`PluginDrivenWriteHandle.getWriteOperation` 现默认 INSERT）+ (2) post-flip 走 `PluginDrivenTableSink` 时把 slot-name loop 复制到 `visitPhysicalConnectorTableSink:629-681`。⚠️**子项**：`visitPhysicalIcebergDeleteSink:588-598` 今天不跑 slot-name loop，impl 期须先证 DELETE 合成 slot colName 怎么到 BE。
- **O2 解（最深/危险）**：post-flip `IcebergRewritableDeletePlanner.collect():64` 按 `instanceof IcebergScanNode` 过滤，scan 变 `PluginDrivenScanNode`（translator `:750`先于`:790`）→ **静默 empty()** → v3 DV delete files 不 `removeDeletes`=**正确性回归（silent）**。native `DeleteFile` 过不了 classloader。**修=收集迁连接器**（`IcebergScanPlanProvider.buildDeleteFiles:515` 现转 Serializable carrier 丢弃 native → 须**新增保留** `Map<originalPath,List<native DeleteFile>>` 喂 `IcebergConnectorTransaction.setRewrittenDeleteFilesByReferencedDataFile:271`，**iceberg-only seam**）。**仅阻塞 step-4（commit-bridge）；做到时专门 recon**（per-request 连接器 ctx vs `beginWrite` 内按 pinned snapshot 重扫）。
- **O3 解：plan-time 可同步取**（今 legacy 已在 `getRequirePhysicalProperties` live-load）；`getWritePartitioning` 只需 `ConnectorSession+ConnectorTableHandle`。`DistributionSpecMerge.IcebergPartitionField(String transform,ExprId,Integer param,String name,Integer sourceId):45`。**3 parity 必 UT**：① null sourceColumnName/exprId → legacy **硬失败 clear**（非 skip）；② `hasNonIdentity` 从 transform 字符串 `'identity'` 重算；③ **新闸门 `enableStrictConsistencyDml`**（`RequestPropertyDeriver:192-195`）关时整段不调。
- **O4 解 + carrier=A**：format-version 信号已发（`buildTableSchema:232 "iceberg.format-version"`）。**Option A 关键简化**：③-lineage **全连接器侧**（连接器 `buildTableSchema` 按 format≥3 emit 两列 `invisible().withUniqueId`，走 schema-cache 自动注入）→ **fe-core 无需读 format-version 做 ③，无需重命名 key**；fe-core 仅处理**请求级 STRUCT row-id 列**（`getFullSchema` override + ctx 信号）+ `IcebergRowIdInjector:159` guard。
- **锚点几乎零漂移**（§9.5/§10.3 全对）。修正：(a) per-op 命令 `run/getExplainPlan` cast（Delete `:116/288` 等）**死码**，仅 3 reachable DB cast `:219/240/464`；(b) executor cast 仅**放宽 ctor 参数**（tx 层已 ExternalTable、字段 `table` 已 TableIf）；(c) `ExecuteActionFactory:61/87`+`InsertIntoTableCommand` branch-guard **已 dual-mode** 勿重做；(d) **ctx 中立重命名 ~28 调用行/6 生产文件+6 测试行**（比设计「~8」大 3 倍，含死命令方法须一并改）。
- **bridge**：事务管理器**按 catalog**（`BaseExternalTableInsertExecutor:69`），post-flip→`PluginDrivenTransactionManager`，legacy executor `(IcebergTransaction)` cast post-flip **必 CCE(loud)**→legacy 仅 pre-flip。dormant SPI commit 链（`RowLevelDmlCommand.run:98-100`→`connectorTx.commit`）就绪、**无需新 commit 管线**。`handles:69` 须与 `newExecutor:110`/`setupConflictDetection:166`/`finalizeSink:178` 的 legacy-executor downcast **lockstep** dual-mode。**conflict-detection**（native `Expression` 存 legacy executor）post-flip 须迁连接器经 `ConnectorTransaction.applyWriteConstraint`（中立 `ConnectorPredicate`，dormant 路）。

## C3b-core 剩余实现顺序（建议；coupled 核心仍是 1 个 coherent commit，但 additive 增量可单独提交、squash 合并）
1. **③-infra part2（additive/dormant，下个增量）**：连接器 `IcebergConnectorMetadata.buildTableSchema:227` 按 format≥3 append v3 lineage 两列（`_row_id` uid 2147483540 / `_last_updated_sequence_number` uid 2147483539，BIGINT，`new ConnectorColumn(...).invisible().withUniqueId(id)`；列名常量见 `Column.ICEBERG_ROW_ID_COL`/`ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL`，连接器须**本地字面量复制** + fe-core contract UT pin 防漂移）+ 连接器 UT + mutation。**⚠️ 先验 schema-cache round-trips invisible+uniqueId**（`PluginDrivenExternalTable.toSchemaCacheValue`→`ConnectorColumnConverter.convertColumns`→cached Column 保 invisible/uniqueId）。
2. **ctx 中立重命名（机械，additive）**：`ConnectContext.icebergRowIdTargetTableId:290`/`needIcebergRowIdForTable:1128`/`needIcebergRowId:1124`/get/set → 中立名（如 `syntheticWriteColTargetTableId`/`needsSyntheticWriteColForTable`）；~28 调用行 across `ExplainCommand`/`RowLevelDmlCommand`/`Iceberg{Delete,Update,Merge}Command`/`IcebergExternalTable:289` + `IcebergDDLAndDMLPlanTest`（含死命令方法须一并改）。**0 行为**。
3. **耦合核心（1 个 coherent commit）**：① `handles:69` 泛化（`instanceof IcebergExternalTable || (PluginDrivenExternalTable && pluginConnectorSupportsRowLevelDml)`，模板 `allowInsertOverwrite:320-329`+helper `:338-342`，能力 `supportsDelete:469`/`supportsMerge:474`）+ 全 cast 放宽（transform 4 + 3 sink ctor + 3 命令 DB cast `:219/240/464` + executor ctor + `InsertIntoTableCommand:568`/`BindSink:1058`，per §11.2 修正）+ `getIcebergPartitioning` dual-mode helper（替 `buildInsertPartitionFields`，pre-flip native walk / post-flip `getWritePartitioning` carrier→`IcebergPartitionField`，**含 §11.1-O3 三 parity**）+ ③ `PluginDrivenExternalTable.getFullSchema` override 注入请求级 STRUCT row-id 列 + `IcebergRowIdInjector:159` guard 放宽 + **pre-flip parity byte-identical 回归**。
4. **commit-bridge（同上 coherent commit 或紧随）**：translator `visitPhysicalIcebergMergeSink:601-627`/`DeleteSink:588-598` dual-mode（post-flip→`PluginDrivenTableSink`+connector `planWrite`，slot-name loop 透传，WriteOperation 透传）+ DML executor 改道 plugin-driven（开 connector `ConnectorTransaction`，`RowLevelDmlCommand` SPI commit）+ **O2 V3 DeleteFile 收集迁连接器**（impl 期专门 recon）+ conflict-detection 迁连接器。
5. **每 sub-step green+mutation**（Rule 9/12，dormant 易 trivially-pass 必变异）；import-gate PASS；新 SPI grep 无 `org.apache.iceberg`；**`SPI_READY_TYPES` 切忌动（C5 才动）**。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。**C3b-core 新代码全用中立能力/描述符/carrier**；保留的 `PhysicalIcebergMergeSink`/`DeleteSink`/`IcebergRowLevelDmlTransform`/`Iceberg*Command`/`IcebergNereidsUtils`/`IcebergTransaction`/`IcebergRewritableDeletePlanner` = **legacy 豁免**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支落这些豁免类合法）；V3 DeleteFile seam 的 `instanceof IcebergConnectorTransaction` downcast 在 finalize 钩子（iceberg-only seam，不入中立 SPI）。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre 已闭，C3b-core 设计+recon 闭、实现进行中，C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。
- **[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。
- **[C3a ✅]** INSERT OVERWRITE 能力 + 完整接通 @branch（[D-070]，闭 DV-T06-branch）。
- **[C3b-pre ✅]** partition_columns emit + ④b parallel-write（[D-071]，Option A 随机真 parity）。
- **[C3b-core 设计 ✅ / impl-recon ✅ / 实现进行中]**（[D-072]+[D-073]）：① handles + ②[Option A getWritePartitioning] + ③ row-id 双注入 + commit-bridge[Option(a) 路由连接器]，coupled。step1+2 ✅ + ③-infra part1 ✅。详 §10+§11。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 `IcebergExecuteActionFactory:89` throw）；唯一深水 = per-group 读端子设计。`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / `pluginCatalogTypeToEngine` iceberg / **GSON compat** / mvcc+partition-stats capability 核 / Show* parity。**C5 是唯一不可逆点**；C5 前须 C1–C4 全绿 + 用户二签。

## 🆕 翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（精确类白名单）→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③]** 隐藏列注入：翻闸后 `PluginDrivenExternalTable` 不注入 row-id/v3-lineage。C3b-core ③ 处理（v3-lineage 经 carrier=A 连接器 emit；STRUCT row-id 经 fe-core getFullSchema override）。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core(step1+2+③-infra) 无新 DV。**C3b-core 预计 0 新 DV**（dormant + pre-flip byte-identical）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant + C3b-core 实现锚点）

- **C3b-core 实现锚点（行号 §11.2 已核当前 HEAD；impl 期仍 re-grep 防漂移）**：
  - ① `IcebergRowLevelDmlTransform.handles:69`（+transform cast `:74/90/110/166`）；模板 `InsertOverwriteTableCommand.allowInsertOverwrite:320-329`+helper `:338-342`。
  - ② sink cast：`PhysicalIcebergMergeSink`（ctor `:63-64/78-79` + with* `:101/114/122/129` + **`:188` 冗余删** + **`:271/273` 唯一真 native = `getIcebergTable()` → dual-mode helper**）/`DeleteSink`(`:92/105/113/120`，零 native)/`TableSink`(`:78/91/99/106`，零 native)；命令 3 reachable DB cast（Delete`:219`/Update`:240`/Merge`:464`，其余 run/getExplainPlan cast 死）；executor ctor+cast（Delete `57/74/88/100`、Merge `47/62/76/88`、Insert `42/52/57`，tx 层已 ExternalTable）；新 carrier `ConnectorWritePartitionField`+`getWritePartitioning`（✅ step1+2）。`DistributionSpecMerge.IcebergPartitionField:45`。
  - ③ `ConnectorColumn`（✅ invisible step1+2 + uniqueId 本 session）+`ConnectorColumnConverter`（✅）；连接器 `buildTableSchema:227`（待 emit lineage 两列）；`ConnectContext.icebergRowIdTargetTableId:290`/`needIcebergRowIdForTable:1128`/`needIcebergRowId:1124`（中立重命名 ~28 站点+`IcebergDDLAndDMLPlanTest`）；`PluginDrivenExternalTable.getFullSchema`(base `ExternalTable:176`)override（注请求级 STRUCT row-id）；`IcebergRowId.createHiddenColumn`（STRUCT 定义）/`IcebergUtils.appendRowLineageColumnsForV3`（legacy 参考）；`IcebergNereidsUtils.IcebergRowIdInjector:159` guard。
  - bridge：translator `visitPhysicalIcebergMergeSink:601-627`/`DeleteSink:588-598` dual-mode（post-flip→`PluginDrivenTableSink`，仿 `visitPhysicalConnectorTableSink:629-681`）；`PluginDrivenInsertExecutor.beginTransaction:81`/`finalizeSink:91-103`/`getConnectorTransactionOrNull:86`；`RowLevelDmlCommand.run:98-100` SPI commit；连接器 `IcebergConnectorTransaction`（已全建）；`IcebergRewritableDeletePlanner.collect:52-86`（V3 DeleteFile，O2 迁连接器）；`PluginDrivenWriteHandle.getWriteOperation`（默认 INSERT，须透传 MERGE/DELETE）。
- **partition_columns + parallel-write（C3b-pre）/ 写@branch+OVERWRITE（C3a）/ classifyColumn（C2）/ sys pin-feed（C1）**：dormant，详 git log。
- **连接器**：`IcebergConnector`/`IcebergConnectorMetadata`〔`supportsDelete:469/supportsMerge:474/supportsInsertOverwrite/supportsWriteBranch` / `buildTableSchema:227` / `getFormatVersion:709`〕/`IcebergConnectorTransaction`〔INSERT/OVERWRITE/DELETE/UPDATE/MERGE commit 全建 + `setRewrittenDeleteFilesByReferencedDataFile:271`/`shouldRewritePreviousDeleteFiles:840`〕/`IcebergWritePlanProvider`〔`planWrite`/`beginWrite`/`getWritePartitioning:195`/`buildMergeSortFields`〕/`IcebergScanPlanProvider`〔`buildDeleteFiles:515`〕。
- **fe-core legacy 豁免（pre-flip 路 + P6.7 删）**：`IcebergTransaction`〔beginDelete/Merge/Insert 已 ExternalTable 签名〕 + `IcebergDeleteExecutor`/`IcebergMergeExecutor`/`IcebergInsertExecutor` + `IcebergNereidsUtils` + `IcebergRewritableDeletePlanner` + `IcebergExternalTable.getFullSchema:293-303`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan/sink/command/executor/transaction=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B`。**fe-core -am 单类 ~2.5min（首次 -am 全链 ~5-7min）**。后台 task exit code 是 echo 的非 maven 的，读 surefire XML。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（exit 0=PASS）。**连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`、v3 lineage 列名/uid）+ fe-core contract UT pin 值防漂移（C2 范式）。**classloader 隔离**：native `org.apache.iceberg.Table` 跨连接器↔fe-core 必 CCE（边界只走中立 SPI 类型）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS`+stub 包私 seam〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 易 trivially-pass → 必变异验真（每条逐做）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **本 session commit（③-infra part1 + recon doc）= 2 改 + 1 测 + 3 doc**：connector-api `connector/api/ConnectorColumn.java`〔+uniqueId carrier〕；fe-core `datasource/ConnectorColumnConverter.java`〔+setUniqueId 重应用〕·`src/test/.../ConnectorColumnConverterTest.java`〔+2 UT〕；`plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔+§11〕·`plan-doc/decisions-log.md`〔+D-073〕·`plan-doc/HANDOFF.md`。
- commit message：`[refactor](catalog) P6.6 iceberg: C3b-core impl-recon（O1-O4 全解/O1·O2 upheld）+ ③-infra part1 ConnectorColumn.uniqueId carrier（用户裁 ③ carrier=A；additive dormant；15/0/0+mutation）` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session）未 push**（确切数 `git rev-list --count origin/catalog-spi-10-iceberg..HEAD`）。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 = ✅ DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core 设计 ✅ + step1+2 ✅ + impl-recon ✅ + ③-infra part1 ✅ → 余下（③-infra part2 / ctx 重命名 / 耦合核心 / bridge）待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。本 session recon 已核 §11.2 锚点（几乎零漂移，但仍 re-grep）。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)` / `instanceof Iceberg*`（新 seam）** —— dual-mode helper 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗 + 先 code 独立判断、后交叉核历史结论。
- **C3b-core 耦合核心+bridge 是大活、跨 session**：fresh session 全 budget 做；逐 sub-step green+mutation；**O2（V3 DeleteFile 迁连接器）做到 step-4 时专门 recon**；**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**（本 session recon 较重，故在 ③-infra part1 干净节点交接）。
