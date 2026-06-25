# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 C3b-core 耦合核心 step 3：③ row-id 注入（新 tiny default-empty SPI accessor + `PluginDrivenExternalTable.getFullSchema` override + `IcebergRowIdInjector` guard 放宽 + pre-flip parity）** —— step 2（② cast 放宽 + `getIcebergPartitioning` dual-mode helper）✅ 本 session

**step 2 ✅ 本 session（1 commit，additive/dormant；2a dual-mode helper + 2b 3 sink ctor 放宽；9/0/0 + 7-mutation 全杀 + 对抗 review GO）**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core 设计 ✅ + impl-recon ✅ + ③-infra part1+part2 ✅ + ctx 重命名 ✅ + 耦合核心 step1（① handles）✅ + step2（② cast+partitioning）✅ → 余下（③ getFullSchema+新SPI / bridge）待办**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（step 2，1 commit + 对抗 review GO；详设计 §11.5）
**用户裁 cast 放宽范围 = 最小安全集**（recon 实证 doc cast 清单 over-inclusive：transform/executor/命令-DB/InsertInto/BindSink cast 多为 legacy 豁免 pre-flip、或级联 Logical sink ctor、或下游真需子类，dormant 现改属投机；真正 post-flip 路由留 step-6 bridge）。**transform cast 行号修正**：4 个 `(IcebergExternalTable)` cast 现 **:99/115/135/191**（doc 旧 :74/90/110/166 是 ① handles 前）。

### 2a — `getIcebergPartitioning` dual-mode helper（落 legacy 豁免 `PhysicalIcebergMergeSink`）
- `getIcebergPartitioning(insertPartitionFields, ExternalTable, columnExprIdMap)` 替 `:194-195` 调用 + 消 `:188` getPartitionColumns cast。pre-flip `instanceof IcebergExternalTable`→`buildInsertPartitionFields`（**原生 walk byte-identical 未改**）；post-flip→`buildInsertPartitionFieldsFromConnector`（仿 canonical `PhysicalPlanTranslator.visitPhysicalConnectorTableSink:636-667`；**null provider / absent handle → 降级 unpartitioned 绝不抛**）→`reconstructPartitionFields`（**pure static 包私供测**，连接器 carrier→legacy `InsertPartitionFieldResult` byte-for-byte，3 parity：P1 null/exprId-miss→clear+success=false 构造前短路；P2 hasNonIdentity `!"identity".equals` over 全 fields 独立 pre-pass；spec-id carry）。`InsertPartitionFieldResult` 类+字段 private→包私。

### 2b — 3 physical sink ctor 放宽（消 12 withX 向下转型）
`PhysicalIcebergMergeSink`/`DeleteSink`/`TableSink` ctor 参数 `IcebergExternalDatabase/Table`→`ExternalDatabase/Table`（base 字段早 ExternalDatabase/ExternalTable）；唯一 ctor 调用方 = `LogicalIceberg{Merge,Delete,Table}SinkToPhysical*` 三规则（喂子类仍 assignable，0 行为）；imports 换基类型（Merge 留 IcebergExternalTable 供 helper、Delete 留 IcebergMergeOperation）。

- **验证**：`PhysicalIcebergMergeSinkTest` **9/0/0**（6 pure reconstruct 0-mock + 3 mocked-chain：dispatch wiring / `enableIcebergMergePartitioning` gate / absent-handle 降级）；**7-mutation 全杀**〔M1 P1a clear / M2 P1b clear / M3 P2 invert non-identity / M4 dispatch→always-native(CCE) / M5 spec-id drop / MA absent-handle guard / MB `insertPartitionExprIds.add`〕；`IcebergDDLAndDMLPlanTest` **14/0/0**（pre-flip parity）；checkstyle 4 文件 PASS；import-gate PASS；`SPI_READY_TYPES` 未改；**0 新 DV**。
- **对抗 review GO**（`wf_5d322c9b-ae2`，3 reviewer + synthesis：pre-flip byte-identity + 3 parity + glue + iron-law 全确认；empty-spec finding refute〔连接器 unpartitioned 返 null〕；synthesis 2 minor 已补 = insertPartitionExprIds 断言 + absent-handle 降级测试）。
- **iron-law 合规**：唯一 `instanceof IcebergExternalTable`（:296）= legacy-exempt pre-flip dispatch；post-flip 全 neutral SPI；native `org.apache.iceberg.*` import 仅在未改的原生 walk。

## 🟡 已登记 follow-up（对抗 review 余项，非阻塞，**勿在本增量做**）
- **[FU-remap → cutover/coupled-core]** `PluginDrivenExternalTable.toSchemaCacheValue:188-199` 列名 remap 分支只重应用 `.withTimeZone()`，**丢弃 `.invisible()`/`.withUniqueId()`**（潜在 generic SPI 缺口）。**当前对 iceberg 安全**（lineage 列 identity-lowercase 名 → 走 line-198 直通，不重建）+ **全 SPI_READY 连接器均无 invisible/uniqueId 列 → 不可达**。修法（cutover 或防御性 follow-up）：line 194 后补 `if(!col.isVisible()) remapped=remapped.invisible();` + `if(col.getUniqueId()>=0) remapped=remapped.withUniqueId(col.getUniqueId());` 仿 `withTimeZone()`。**Rule 2/3（不投机/外科）：本增量不做**。

## 🔑 impl-recon 决定性结论（动码前必读，详 design §11）
- **O1 解：合成列无 carrier 需求**。`$operation/$row_id` 不进 thrift sink，按名 `setMaterializedColumnName:615`→`colName` BE 按名匹配；连接器 `planWrite` 不读 `handle.getColumns()`。bridge 只需 (1) `WriteOperation=MERGE/DELETE` 透到写 handle（`PluginDrivenWriteHandle.getWriteOperation` 现默认 INSERT）+ (2) post-flip 走 `PluginDrivenTableSink` 时把 slot-name loop 复制到 `visitPhysicalConnectorTableSink:629-681`。⚠️**子项**：`visitPhysicalIcebergDeleteSink:588-598` 今天不跑 slot-name loop，impl 期须先证 DELETE 合成 slot colName 怎么到 BE。
- **O2 解（最深/危险）**：post-flip `IcebergRewritableDeletePlanner.collect():64` 按 `instanceof IcebergScanNode` 过滤，scan 变 `PluginDrivenScanNode`（translator `:750`先于`:790`）→ **静默 empty()** → v3 DV delete files 不 `removeDeletes`=**正确性回归（silent）**。native `DeleteFile` 过不了 classloader。**修=收集迁连接器**（`IcebergScanPlanProvider.buildDeleteFiles:515` 现转 Serializable carrier 丢弃 native → 须**新增保留** `Map<originalPath,List<native DeleteFile>>` 喂 `IcebergConnectorTransaction.setRewrittenDeleteFilesByReferencedDataFile:271`，**iceberg-only seam**）。**仅阻塞 step-4（commit-bridge）；做到时专门 recon**（per-request 连接器 ctx vs `beginWrite` 内按 pinned snapshot 重扫）。
- **O3 解：plan-time 可同步取**（今 legacy 已在 `getRequirePhysicalProperties` live-load）；`getWritePartitioning` 只需 `ConnectorSession+ConnectorTableHandle`。`DistributionSpecMerge.IcebergPartitionField(String transform,ExprId,Integer param,String name,Integer sourceId):45`。**3 parity 必 UT**：① null sourceColumnName/exprId → legacy **硬失败 clear**（非 skip）；② `hasNonIdentity` 从 transform 字符串 `'identity'` 重算；③ **新闸门 `enableStrictConsistencyDml`**（`RequestPropertyDeriver:192-195`）关时整段不调。
- **O4 解 + carrier=A**：format-version 信号已发（`buildTableSchema:232 "iceberg.format-version"`）。**Option A 关键简化**：③-lineage **全连接器侧**（连接器 `buildTableSchema` 按 format≥3 emit 两列 `invisible().withUniqueId`，走 schema-cache 自动注入）→ **fe-core 无需读 format-version 做 ③，无需重命名 key**；fe-core 仅处理**请求级 STRUCT row-id 列**（`getFullSchema` override + ctx 信号）+ `IcebergRowIdInjector:159` guard。
- **锚点几乎零漂移**（§9.5/§10.3 全对）。修正：(a) per-op 命令 `run/getExplainPlan` cast（Delete `:116/288` 等）**死码**，仅 3 reachable DB cast `:219/240/464`；(b) executor cast 仅**放宽 ctor 参数**（tx 层已 ExternalTable、字段 `table` 已 TableIf）；(c) `ExecuteActionFactory:61/87`+`InsertIntoTableCommand` branch-guard **已 dual-mode** 勿重做；(d) **ctx 中立重命名 ~28 调用行/6 生产文件+6 测试行**（比设计「~8」大 3 倍，含死命令方法须一并改）。
- **bridge**：事务管理器**按 catalog**（`BaseExternalTableInsertExecutor:69`），post-flip→`PluginDrivenTransactionManager`，legacy executor `(IcebergTransaction)` cast post-flip **必 CCE(loud)**→legacy 仅 pre-flip。dormant SPI commit 链（`RowLevelDmlCommand.run:98-100`→`connectorTx.commit`）就绪、**无需新 commit 管线**。`handles:69` 须与 `newExecutor:110`/`setupConflictDetection:166`/`finalizeSink:178` 的 legacy-executor downcast **lockstep** dual-mode。**conflict-detection**（native `Expression` 存 legacy executor）post-flip 须迁连接器经 `ConnectorTransaction.applyWriteConstraint`（中立 `ConnectorPredicate`，dormant 路）。

## C3b-core 剩余实现顺序（coupled 核心 = 1 个 coherent commit，但 additive 增量单独提交、PR 时 squash 合并；每完一 sub-step 即 green+mutation+HANDOFF+commit）
1. **③-infra part1+part2 ✅ DONE**（`ConnectorColumn.uniqueId` carrier + 连接器 `buildTableSchema:240` format≥3 append v3 lineage 两列）。
2. **ctx 中立重命名 ✅ DONE**（`ConnectContext` → `syntheticWriteColTargetTableId`/`needsSyntheticWriteCol[ForTable]`；8 文件；0 行为）。
3. **耦合核心 step 1 = ① handles 泛化 ✅ DONE 本 session**（`handles:69` + 中立 capability helper；TDD+4-mutation；详上「✅ 本 session 完成」+ 设计 §11.4.2）。
4. **耦合核心 step 2 = ② cast 放宽（最小安全集）+ `getIcebergPartitioning` dual-mode helper ✅ DONE 本 session**（2a helper 落 `PhysicalIcebergMergeSink`〔3 parity，pure `reconstructPartitionFields`〕+ 2b 3 sink ctor 放宽消 12 withX；9/0/0+7-mutation+review GO；详上「✅ 本 session 完成」+ 设计 §11.5）。
5. **耦合核心 step 3 = ③ row-id 注入 ⟵ 下个 session 起点**：**新增 tiny additive default-empty SPI accessor**（候选 `ConnectorWritePlanProvider`/`ConnectorWriteOps` 返 `List<ConnectorColumn> getSyntheticWriteColumns(session,handle)`，iceberg override 声明 STRUCT row-id；连接器禁 import fe-core）+ fe-core `PluginDrivenExternalTable.getFullSchema` override（base `ExternalTable:176` schema-cache only→PluginDriven override；`ctx.needsSyntheticWriteColForTable(id)||Util.showHiddenColumns()` 门控、经 `ConnectorColumnConverter` 注入）+ `IcebergNereidsUtils.IcebergRowIdInjector:159` guard 放宽（中立能力，非 `instanceof Iceberg*`）+ **pre-flip parity 回归**。**两处 IcebergExternalTable 耦合 post-flip 破、③ 须双治**（getFullSchema + injector）。详设计 §11.4.1「③ KEY 缺口」+ O4 carrier=A（v3-lineage 已连接器侧 emit，③ fe-core 仅处理请求级 STRUCT row-id）。helper/连接器 SPI 范式可仿 step-2 的 `getWritePartitioning`/`ConnectorWritePartitionField`（连接器字面量复制 Doris 常量 + fe-core 契约钉）。
6. **commit-bridge**：translator `visitPhysicalIcebergMergeSink:601-627`/`DeleteSink:588-598` dual-mode（post-flip→`PluginDrivenTableSink`+connector `planWrite`，slot-name loop 透传，`WriteOperation` 经 `ConnectorWriteHandle.getWriteOperation` 透传）+ DML executor 改道 plugin-driven（connector `ConnectorTransaction` + `RowLevelDmlCommand` SPI commit）+ **O2 V3 DeleteFile 收集迁连接器**（`collectFor*` + `buildDeleteFiles:514`→`setRewrittenDeleteFilesByReferencedDataFile:271`，**专门 recon**）+ conflict-detection 迁连接器。
7. **每 sub-step green+mutation**（Rule 9/12，dormant 易 trivially-pass 必变异）；import-gate PASS；新 SPI grep 无 `org.apache.iceberg`；**`SPI_READY_TYPES` 切忌动（C5 才动）**。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。**C3b-core 新代码全用中立能力/描述符/carrier**；保留的 `PhysicalIcebergMergeSink`/`DeleteSink`/`IcebergRowLevelDmlTransform`/`Iceberg*Command`/`IcebergNereidsUtils`/`IcebergTransaction`/`IcebergRewritableDeletePlanner` = **legacy 豁免**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支落这些豁免类合法）；V3 DeleteFile seam 的 `instanceof IcebergConnectorTransaction` downcast 在 finalize 钩子（iceberg-only seam，不入中立 SPI）。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre 已闭，C3b-core 设计+recon 闭、实现进行中，C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。
- **[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。
- **[C3a ✅]** INSERT OVERWRITE 能力 + 完整接通 @branch（[D-070]，闭 DV-T06-branch）。
- **[C3b-pre ✅]** partition_columns emit + ④b parallel-write（[D-071]，Option A 随机真 parity）。
- **[C3b-core 设计 ✅ / impl-recon ✅ / 实现进行中]**（[D-072]+[D-073]）：① handles ✅ + ②[Option A getWritePartitioning dual-mode helper] ✅ + ③ row-id 双注入[新SPI accessor] 待 + commit-bridge[Option(a) 路由连接器] 待，coupled。③-infra part1+part2 ✅ + 耦合核心 step1（① handles）✅ + step2（② cast+partitioning）✅。详 §10+§11（§11.5 = 本 session step2）。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 `IcebergExecuteActionFactory:89` throw）；唯一深水 = per-group 读端子设计。`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / `pluginCatalogTypeToEngine` iceberg / **GSON compat** / mvcc+partition-stats capability 核 / Show* parity。**C5 是唯一不可逆点**；C5 前须 C1–C4 全绿 + 用户二签。

## 🆕 翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（精确类白名单）→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③]** 隐藏列注入：翻闸后 `PluginDrivenExternalTable` 不注入 row-id/v3-lineage。C3b-core ③ 处理（v3-lineage 经 carrier=A 连接器 emit；STRUCT row-id 经 fe-core getFullSchema override）。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core(③-infra part1+part2 + 耦合核心 step1 ① handles + step2 ② cast+partitioning) 无新 DV。**C3b-core 余下预计 0 新 DV**（dormant + pre-flip byte-identical）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant + C3b-core 实现锚点）

- **C3b-core 实现锚点（行号 §11.2 已核当前 HEAD；impl 期仍 re-grep 防漂移）**：
  - ① `IcebergRowLevelDmlTransform.handles:69` ✅（transform 4 cast 现 `:99/115/135/191` = legacy 豁免、本线不动）；模板 `InsertOverwriteTableCommand.allowInsertOverwrite:320-329`+helper `:338-342`。
  - ② ✅ DONE（step2）：3 sink ctor 放宽消 12 withX；`PhysicalIcebergMergeSink.getIcebergPartitioning`（dual-mode）+`buildInsertPartitionFieldsFromConnector`+`reconstructPartitionFields`（pure，3 parity）；carrier `ConnectorWritePartitionField`/`ConnectorWritePartitionSpec`+`getWritePartitioning`；`DistributionSpecMerge.IcebergPartitionField:45`。**未动（留 step-6 bridge / legacy 豁免）**：命令 reachable DB cast（feed Logical sink ctor，级联）、executor cast、`collectFor*`(O2)、InsertInto:568/BindSink:1058（leaf-cast-after-instanceof）。
  - ③ `ConnectorColumn`（✅ invisible step1+2 + uniqueId part1）+`ConnectorColumnConverter`（✅）；连接器 `buildTableSchema:240`（✅ part2：format≥3 emit lineage 两列）；`ConnectContext.syntheticWriteColTargetTableId:290`/`needsSyntheticWriteColForTable:1128`/`needsSyntheticWriteCol:1123`（✅ 中立重命名 DONE；消费方 `IcebergExternalTable.needInternalHiddenColumns:289`）；`PluginDrivenExternalTable.getFullSchema`(base `ExternalTable:176`)override（注请求级 STRUCT row-id）；`IcebergRowId.createHiddenColumn`（STRUCT 定义）/`IcebergUtils.appendRowLineageColumnsForV3`（legacy 参考）；`IcebergNereidsUtils.IcebergRowIdInjector:159` guard。
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
- **本 session commit（step2）= 3 改 + 1 新测 + 2 doc**：`fe/fe-core/.../physical/PhysicalIcebergMergeSink.java`〔2a helper +2b ctor 放宽〕、`.../physical/PhysicalIcebergDeleteSink.java`、`.../physical/PhysicalIcebergTableSink.java`〔2b ctor 放宽〕；新测 `fe/fe-core/src/test/.../physical/PhysicalIcebergMergeSinkTest.java`〔9 UT〕；`plan-doc/HANDOFF.md` + `plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔§11.5〕。**勿** `git add -A`（path-whitelist；scratch/游离文件如仓根 `fe/IcebergScanPlanProvider.java`、`plan-doc/reviews/P5-paimon-rereview3-*` 勿混入）。
- commit message：`[refactor](catalog) P6.6 iceberg: C3b-core 耦合核心 step2 — ② cast 放宽(最小安全集)+getIcebergPartitioning dual-mode helper（additive dormant；3 parity pure reconstruct + 3 sink ctor 放宽消 12 withX；9/0/0+7-mutation 全杀+14/0/0；对抗 review GO）` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session）未 push**（确切数 `git rev-list --count origin/catalog-spi-10-iceberg..HEAD`）。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 = ✅ DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core 设计 ✅ + impl-recon ✅ + ③-infra part1+part2 ✅ + ctx 重命名 ✅ + 耦合核心 step1（① handles）✅ + step2（② cast+partitioning）✅ → 余下（③ getFullSchema+新SPI / bridge）待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。本 session recon 已核 §11.2 锚点（几乎零漂移，但仍 re-grep）。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)` / `instanceof Iceberg*`（新 seam）** —— dual-mode helper 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗 + 先 code 独立判断、后交叉核历史结论。
- **C3b-core 耦合核心+bridge 是大活、跨 session**：fresh session 全 budget 做；逐 sub-step green+mutation；**O2（V3 DeleteFile 迁连接器）做到 step-4 时专门 recon**；**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**。本 session = 耦合核心 step 2（② cast 放宽最小安全集 + `getIcebergPartitioning` dual-mode helper，9/0/0+7-mutation+对抗 review GO），在干净节点交接。**下个 session 起点 = step 3（③ row-id 注入）**：先读设计 §11.4.1「③ KEY 缺口」+ §11.5 + O4 carrier=A（动码前 re-grep 锚点防漂移，尤其 `IcebergRowIdInjector:159`/`getFullSchema`/`ConnectContext.needsSyntheticWriteColForTable` 行号）；新 tiny SPI accessor 范式仿 step-2 `getWritePartitioning`/`ConnectorWritePartitionField`（连接器字面量复制 Doris 常量 `__DORIS_ICEBERG_ROWID_COL__` + fe-core 契约钉）；两处耦合双治（getFullSchema override + injector guard）；逐 sub-step green+mutation（dormant 必变异，注意 mutation 锚点别误命中同名 legacy 方法——本 session M1/M2 初版踩过）；O2（V3 DeleteFile 迁连接器）做到 step-6 bridge 时专门 recon；**C5 前切忌动 `SPI_READY_TYPES`**。
