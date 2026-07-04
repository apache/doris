# 行级 DML 去 SDK 化设计（实证重裁定：死车道删除，非新建 SPI）

> Status: **APPROVED（用户 2026-07-04 已裁定 §7 全部四项）— 按 §8 TODO 动码**
> 裁定结果：Q1=接受改判为删除；Q2=INSERT 死车道并入本刀；Q3=四个 SDK-free 小类**现在搬**中立包；Q4=两个顺带活问题随本轮各自独立 commit 修。
> 生成：2026-07-04。方法：11-agent 研究工作流（4 路并行清点：fe-core DML 车道 / 连接器 SPI 面 / Trino merge 模型 / BE 契约 → 完备性批评家 → 6 路补盲），全部结论带 file:line 实证。
> 上游：`plan-doc/fe-core-iceberg-removal-plan.md` §6b（行级 DML 计划合成）+ §8 Q4=A（去 SDK 化现在做，设计先行）、Q5（设计签字后动码）。
> ⚠️ **本文对上游计划 §6b 的工作定义做出实证更正**（见 §2），冲突点已逐条列明（Rule 7）。

---

## 1. 目标与非目标

**目标**：fe-core 的行级 DML 车道（iceberg 表的 DELETE / UPDATE / MERGE INTO 计划合成，含相邻的 rewrite_data_files 旧车道）**不再含任何 `org.apache.iceberg.*` import**。签字的临时架构不变：merge 计划合成留在引擎侧（fe-core），直至出现第二个行级 DML 消费者。

**非目标**：
- 不改变任何用户可见行为（错误消息、结果形状、隐藏列名、EXPLAIN 输出、thrift 线协议——红线清单见 §6.3）。
- 不做 HMS-DLA iceberg（hive 目录下的 iceberg 表）的任何迁移（随 hive 整体迁移，已签字）。HMS-DLA iceberg 表的 DELETE/UPDATE/MERGE/EXECUTE 今天就报"仅支持 olap 表"类错误（DeleteFromCommand.java:497-501 等），**master 基线同为 broken，维持现状**。
- 不重命名 `__DORIS_ICEBERG_ROWID_COL__` / `"operation"` 列名、不改操作码数值（FE↔BE 名字/数值契约，§5.2）。
- HMS-DLA `$sys` 表在 bind 期抛 IllegalArgumentException（IcebergSysTable.java:76-78）为 master 基线既有行为，非本刀范围。

---

## 2. 研究结论：原计划四项工作的实证重裁定

上游计划 §6b 原定四项：① 中立 merge 操作码枚举；② 行标识列描述 SPI；③ IcebergNereidsUtils SDK 表达式转换下沉连接器；④ StatementContext `List<FileScanTask>` 暂存句柄化。**逐项重裁定如下**：

| 原定工作 | 实证裁定 | 证据 |
|---|---|---|
| ① 中立操作码枚举 | **已存在且已中立**。`IcebergMergeOperation` 是纯常量类（零 SDK import），操作码 1/2/3/4/5 与 Trino `ConnectorMergeSink` 常量逐值同源；FE 只发 1/2/3，经 "operation" TinyInt 数据列到 BE，非 thrift 枚举 | IcebergMergeOperation.java:24,32-36；BE viceberg_merge_sink 按列值分发 |
| ② 行标识列描述 SPI | **已存在且已中立**。连接器经 `ConnectorWritePlanProvider.getSyntheticWriteColumns` 声明 rowid 结构列（file_path/row_position/partition_spec_id/partition_data，ConnectorColumn+ConnectorType，invisible）；引擎在 `PluginDrivenExternalTable.getFullSchema:444-457` 注入。整个行标识模型**零 SDK 类型** | fe-connector-iceberg IcebergWritePlanProvider.java:116-126,286-294；fe-core IcebergRowId.java:60-67（SDK-free 兜底工厂） |
| ③ 表达式转换下沉 | **目标是死码，改判删除**。`convertNereidsToIcebergExpression` 仅两个调用方且全死（旧冲突过滤器 IcebergConflictDetectionFilterUtils.java:257 + 旧 rewrite RewriteDataFilePlanner.java:97）；连接器**早已自带**转换器 `IcebergPredicateConverter`（Mode.SCAN/CONFLICT/REWRITE 三模式，IcebergPredicateConverter.java:105），活路径冲突约束走中立 `WriteConstraintExtractor → ConnectorTransaction.applyWriteConstraint(ConnectorPredicate)` | 下沉 = 迁移零活调用方的代码 |
| ④ 暂存句柄化 | **目标是死码，改判删除**。SDK 暂存唯一生产者 RewriteGroupTask.java:117、唯一消费者 IcebergScanNode.java:492-505/929-935，两端皆旧车道死码；中立替身**已端到端上线**：`StatementContext.rewriteSourceFilePaths`(:331) → ConnectorRewriteGroupTask:121 → PluginDrivenScanNode:758 → connector `applyRewriteFileScope` | 句柄化 = 给零活调用方的 API 造 SPI（投机性 API，违反 Rule 2） |

**死码判定的双重证明**（对抗核验通过）：
1. **实例源证明**：fe-core src/main 仅 3 处构造旧 iceberg 身份对象（ExternalCatalog.buildDbForInit:973 / IcebergExternalDatabase:39 / IcebergSysTable:80），三处全不可达——SPI_READY_TYPES 含 "iceberg"（CatalogFactory.java:49-50）、内建 fallback switch 无 iceberg 臂（:130-140）、PluginDrivenExternalCatalog 强制 logType=PLUGIN 并在 gsonPostProcess 迁移旧值（:960-963,:984-989）。
2. **镜像回放证明**：GsonUtils 把全部 8 个旧 catalog 变体 + db + table 读侧重映射到 PluginDriven*（GsonUtils.java:395-411,464-466,491-494），且检查点**写侧**只写 PluginDriven 类名 → 翻闸后二进制内旧车道无任何配置可复活（复活 = git revert，删了的文件也会回来）。**保留死码买不到任何运维回滚能力**。

**结论**：本设计从"迁移/包装"重裁定为——**删除死车道 + 保留面清点 + 防回归门禁**。删完后行级 DML 车道（含 rewrite 旧道）fe-core 零 SDK import，`iceberg-core` 依赖摘除不再被本车道阻塞（只剩 HMS-iceberg 一根钉）。

---

## 3. Trino 对照（master @ 280b81bbc4e，2026-07-04 读取）

结论：**Doris 现行中立面与 Trino 模型同构，本刀无需新增任何 SPI**。映射表（供后续"第二个行级 DML 消费者"出现时参考）：

| Trino | Doris 现状 | 同构度 |
|---|---|---|
| `ConnectorMergeSink` 操作码 INSERT=1/DELETE=2/UPDATE=3/UPDATE_INSERT=4/UPDATE_DELETE=5 | `IcebergMergeOperation` 同值常量（FE 只发 1/2/3，BE merge sink 内部拆 delete+insert） | 逐值同源 |
| `getMergeRowIdColumnHandle` 返回不透明 struct 列（iceberg: file_path/pos/spec_id/partition_data/[source_row_id]），引擎注入隐藏扫描列、原样回传 sink | `getSyntheticWriteColumns` 返回 rowid ConnectorColumn struct（同四字段），引擎 getFullSchema 注入、按名回传 BE sink | 同构；Trino 把 spec_id+partition_data 放**行标识内部**从而引擎无需理解分区——Doris 同样如此 |
| 引擎合成 merge 计划（JOIN + CASE 操作码投影），连接器不可见 SQL 形状；UPDATE/DELETE 编译成退化 merge | IcebergRowLevelDmlTransform.synthesize 委派 Iceberg*Command 合成器（JOIN + If 链 + 操作码投影），UPDATE=单扫 merge | 同构（Doris 合成器暂 iceberg 命名，见 §7-Q3） |
| `beginMerge → fragments → finishMerge`：sink 回传连接器自定义序列化提交载荷 | BE 回传 thrift TIcebergCommitData → LoadProcessor:230-236 → TBinaryProtocol byte[] → `ConnectorTransaction.addCommitData`（SDK DataFile/DeleteFile 仅在连接器内重建） | 同构 |
| 表达式转换只在连接器 commit/冲突层与 scan 下推层出现，merge 行管道纯位置/不透明 | 冲突约束 = 中立 ConnectorPredicate 计划期暂存、commit 时连接器 lazy 转 SDK Expression（IcebergConnectorTransaction:822-840） | 同构——**佐证"表达式转换属于连接器"的裁定** |
| `RowChangeParadigm`（iceberg=DELETE_ROW_AND_INSERT_ROW） | 无对应物；Doris 现有三个湖格式全为 MoR delete-and-insert，暂不需要 | 缺位但当前不需要；未来第二消费者需要时再加 2 值枚举 |

---

## 4. 删除闭包（全清单，动码时逐条对照）

### 4.1 整文件删除 — src/main（27 文件 ≈ 3951 LOC + INSERT 车道可选项）

**rewrite/action 旧车道（17 文件）**：`datasource/iceberg/action/` 全部 11 文件（BaseIcebergAction、IcebergExecuteActionFactory、9 个 Iceberg*Action）+ `datasource/iceberg/rewrite/` 全部 6 文件（RewriteDataFileExecutor、RewriteDataFilePlanner、RewriteDataGroup、RewriteGroupTask、RewriteManifestExecutor、RewriteResult）。唯一外部进入点 = ExecuteActionFactory.java:66-68/:98-99 两个 `instanceof IcebergExternalTable` 死分支（活路径 :61-64/:87-96 走 ConnectorExecuteAction/ConnectorProcedureOps）。

**DML 死类（7 文件）**：`commands/IcebergDmlCommandUtils`、`datasource/iceberg/IcebergConflictDetectionFilterUtils`、`commands/insert/IcebergDeleteExecutor`、`commands/insert/IcebergMergeExecutor`、`commands/insert/IcebergRewriteExecutor`、`planner/IcebergDeleteSink`、`planner/IcebergMergeSink`。

**helper（2 文件）**：`datasource/iceberg/helper/IcebergRewritableDeletePlanner` + `IcebergRewritableDeletePlan`（消费者仅两个死 executor；活替身 = 连接器 IcebergRewritableDeleteStash 在 planWrite 内自填）。

**scan 死源（1 文件）**：`datasource/iceberg/source/IcebergApiSource`（唯一消费者 = IcebergScanNode 死构造臂 :189-209 与死 sys-table 流）。

**INSERT 死车道（§7-Q2 裁定后并入）**：`planner/IcebergTableSink`、`commands/insert/IcebergInsertExecutor`、`plans/logical/LogicalIcebergTableSink`（目标硬类型 IcebergExternalTable）、`plans/physical/PhysicalIcebergTableSink`、`datasource/iceberg/IcebergTransaction`、`transaction/IcebergTransactionManager` + `TransactionManagerFactory.createIcebergTransactionManager`(:33-34)。唯一创建根 = 死的 IcebergExternalCatalog.java:128-129。

### 4.2 成员级手术 — 存活共享文件（10+ 文件）

| 文件 | 删除内容 | 保留（勿动） |
|---|---|---|
| `StatementContext` | :323 SDK 暂存字段 + :1351-1353 + :1359-1363；伴生 :326 useGatherForIcebergRewrite + :1368-1377（生产者 RewriteGroupTask:263 死、消费者 PhysicalIcebergTableSink:120 死；中立 rewrite 把该旗载在 PhysicalConnectorTableSink:165） | **:331 rewriteSourceFilePaths / :336 rewriteSharedTransaction（活替身！）** |
| `IcebergScanNode`（**HMS-DLA 活类，禁整删**——PhysicalPlanTranslator:862 为 hive 目录 iceberg 表构造） | getFileScanTasksFromContext :492-505；doGetSplits 暂存分支 :929-935；死构造臂 :189-209；deleteFilesByReferencedDataFile 双字段 :167-168 + 双 put :360/:367（仅死 executor 经 helper 消费）；sys-table 成员 :164,:190-191,:922-923,:974+（IcebergSysExternalTable 不可构造）；classifyColumn 的 ICEBERG_ROWID_COL 分支 :909-911（可选修剪） | 整条 HMS-DLA 读管道：ctor HMS 臂 :186-188、doInitialize、createTableScan、manifest cache、vended 凭据、GLOBAL_ROWID/row-lineage 分支 :912-916、TIcebergDeleteFileDesc 构造 :350 |
| `IcebergNereidsUtils` | :228-639 整个 SDK 转换半（convertNereidsToIcebergExpression/convertNereidsBinaryPredicate/convertNereidsInPredicate/convertNereidsBetween/extractColumnName/extractNereidsLiteralValue）+ 5 个 SDK import :59-63 | **:87-226 行标识注入半（活，SDK-free）**：injectRowIdColumn/hasRowIdSlot/findRowIdSlot/hasRowIdProject/getRowIdColumn/isRowIdInjectionTarget/pluginConnectorSupportsRowLevelDml/hasUnboundPlan/IcebergRowIdInjector |
| `PhysicalIcebergMergeSink` | 旧臂 buildInsertPartitionFields :303-346 + 分派守卫 :296-297 + SDK imports :50-54 | 活类本体 + 活臂 buildInsertPartitionFieldsFromConnector（中立 ConnectorWritePartitionSpec） |
| `IcebergDeleteCommand` | run :107-180、executeWithExternalTableBatchModeDisabled :182-195、getPhysicalSink :264、childIsEmptyRelation :277、getExplainPlan :283-299（内含 FQN SDK 引用） | **合成半（活）**：completeQueryPlan :202-231、buildPositionDeletePlan :243-262 及 row-id helper |
| `IcebergUpdateCommand` | run :107-138、executeMergePlan :140-176、:178+ 死半 | **buildMergePlan :221-249、buildMergeProjectPlan :194-218、getRowIdColumnExpr :251、buildUpdateSelectItems :262-284（活）** |
| `IcebergMergeCommand` | run :136-153、getExplainPlan :155-171、executeMergePlan :475-511、:513+ 死半、getRowIdColumn(IcebergExternalTable) :563-565 | **合成半（活）**：generateBasePlan/generateBranchLabel/三个投影 builder/generateFinalProjections/buildMergeProjectPlan/buildMergePlan :187-473 |
| `IcebergRowLevelDmlTransform` | handles() 第一析取 :79、checkMode 旧臂 :110-121、newExecutor 旧臂 :192-196、setupConflictDetection 旧臂 :255-263、finalizeSink 旧臂 :278-281 | 插件臂全部（checkPluginMode/synthesize/PluginDrivenInsertExecutor/extractWriteConstraint）+ ICEBERG_EXCLUSION :73-75 |
| `PhysicalPlanTranslator` | visitPhysicalIcebergDeleteSink 死 else 臂 :609-613、visitPhysicalIcebergMergeSink 死臂 :652-656、（Q2 并入时）visitPhysicalIcebergTableSink :581-592、死 IcebergScanNode 构造臂 :885-887、imports :204-206 | buildPluginRowLevelDmlSink :673-709、HMS-DLA IcebergScanNode 臂 :855-864 |
| `ExecuteActionFactory` | :66-68 与 :98-99 两个 instanceof IcebergExternalTable 分支 + import :28 | PluginDriven 分支 :61-64/:87-96 |
| `RewriteTableCommand` | PhysicalIcebergTableSink 臂 :190-202 | PhysicalConnectorTableSink 活臂 :203-213（ConnectorRewriteGroupTask:205 构造） |
| （Q2 并入时）`InsertIntoTableCommand`:568-583、`BindSink`:729-731/:829/:1170-1177、`UnboundTableSinkCreator`:63,:99,:137 | 各自 iceberg 死臂 | 通用臂 |

### 4.3 明确保留（KEEP 集，删了就破坏活路径或兼容）

- **活合成车道**（引擎侧留驻，签字架构）：RowLevelDmlRegistry/Transform/Args/Op/Command 外壳、IcebergRowLevelDmlTransform 插件臂、三个 Iceberg*Command 合成半、Logical/PhysicalIcebergDeleteSink、Logical/PhysicalIcebergMergeSink（目标已是泛型 ExternalTable，翻闸后装载 PluginDrivenExternalTable）。
- **SDK-free 小类**：IcebergMergeOperation（两个保留 UT 依赖其编译：PhysicalPlanTranslatorIcebergRowLevelDmlTest:37、PhysicalIcebergMergeSinkTest:32）、IcebergRowId、IcebergMetadataColumn、IcebergNereidsUtils :87-226。**是否搬出 `datasource.iceberg` 包 = §7-Q3**。
- **HMS-DLA 共享面**（另一根钉，本刀禁触）：IcebergScanNode（除 §4.2 死成员）、IcebergHMSSource、IcebergSource、IcebergSplit、IcebergDeleteFileFilter、IcebergMetricsReporter、IcebergUtils 读侧、IcebergExternalMetaCache/IcebergManifestCacheLoader/ManifestCacheValue、IcebergMetadataOps（loadTable 臂，HMSExternalCatalog:242-249 构造）、IcebergDlaTable、IcebergSnapshotCacheValue/IcebergSchemaCacheValue、IcebergExternalCatalog 的常量面（IcebergUtils.isManifestCacheEnabled:1874-1884 在 HMS-DLA 扫描活读）。
- **thrift/线协议**：TIcebergDeleteSink/TIcebergMergeSink/TIcebergRewritableDeleteFileSet/TIcebergCommitData/TIcebergDeleteFileDesc/TIcebergFileDesc——插件路径同样使用，FE-BE 契约。
- **名字/数值契约**：`__DORIS_ICEBERG_ROWID_COL__`（Column.java:63 == BE consts.h:33）、`"operation"` 列名、操作码 1-5 数值、rowid struct 四字段名与类型（BE viceberg_delete_sink.cpp:306-374 逐字校验）。

### 4.4 测试面处置（12 直接 + 3 混合 + 2 需搬迁）

**随类整删（9 文件）**：IcebergDmlCommandUtilsTest、IcebergConflictDetectionFilterUtilsTest、IcebergDeleteExecutorTest、IcebergMergeExecutorTest、IcebergDeleteSinkTest、IcebergMergeSinkTest、IcebergRewritableDeletePlannerTest、RewriteDataFilePlannerTest（1165 行）、RewriteGroupTaskTest（524 行）。（Q2 并入时 + IcebergTransactionTest 614 行整删，CommitDataSerializerTest 的 icebergFeedEqualsLegacyUpdate :123-136 改写为直接断言 feed 累积。）

**case 级手术（3 文件）**：
- IcebergNereidsUtilsTest：~57 个 convert* case（:110-1035）随转换半删；**:1039-1091 的 6 个 isRowIdInjectionTarget case 中 5 个中立 KEEP** → 抽出到新测试文件（1 个 legacy case 随臂删）。
- IcebergRowLevelDmlTransformTest：删 handlesLegacyIcebergExternalTable :115-120；3 个 extractWriteConstraint case（:270-295）把 IcebergExternalTable mock 换成 PluginDrivenExternalTable（断言原样保留——它们守护冲突约束的 rowid/元数据列排除）；其余 11 个插件臂 case = 必须保绿 gate。
- ExplainIcebergDeleteCommandTest：仅 fixture 手术（删 imports :22-24 + 死 mock :52-55,:60-63,:71-74），11 个 case 零删改、全保绿。

**目录删除陷阱**：`fe-core/src/test/.../datasource/iceberg/` 树内有两个 KEEP 内容必须先搬出——IcebergDeletePlanTest（539 行，零死类依赖，纯 parser/plan-shape）+ 上述 isRowIdInjectionTarget 抽出半。**严禁对该测试目录 rm -rf。**

**必须保绿 gate（不完全清单）**：PhysicalPlanTranslatorIcebergRowLevelDmlTest（4 case，全插件臂）、PhysicalIcebergMergeSinkTest（9 case，SDK-free）、PhysicalPlanTranslatorAdmissionGateTest、PluginDrivenTableSinkTest、PluginDrivenExternalTableTest、WriteConstraintExtractorTest、NereidsToConnectorExpressionConverterTest、UnboundExpressionToConnectorPredicateConverterTest、IcebergDeletePlanTest（搬迁后）、IcebergGsonCompatReplayTest（升级兼容守卫）。

### 4.5 防回归门禁（新增）

`fe/check/checkstyle/import-control.xml:36` 现仅禁 `org.apache.iceberg.relocated`——**没有任何门禁守护本刀成果**。新增：对 `org.apache.doris.nereids.**`（含 StatementContext、commands、glue/translator）与 `org.apache.doris.planner.**` 禁 `org.apache.iceberg` import（datasource/iceberg、hive、statistics、property 等 HMS-DLA 残留面暂不设禁，随后续阶段收紧）。具体 subpackage 语法实现时定。

---

## 5. 行为不变性红线（e2e 已 pin 的字符串/形状，本刀一律不许动）

1. 错误消息 pin：`must have format version 2 or higher for position deletes`（连接器 IcebergConnectorTransaction.java:288 发出——**删除 fe-core 死重复副本 IcebergTransaction.java:293,320 不影响**）；`Doris does not support DELETE/UPDATE/MERGE INTO on Iceberg copy-on-write tables`（连接器 IcebergConnectorMetadata.java:1341 发出——删死副本 IcebergDmlCommandUtils.java:56 不影响）；EXECUTE 车道 ~30 条参数校验消息（发射器全在连接器/通用层，fe-core 死 action 类的副本删除不影响）。
2. 结果形状 pin：dml/*.out 的受影响行数与表内容；rewrite 结果列位置（[0]=rewritten/[1]=added/[2]=bytes/[3]=deleted-bytes）；expire_snapshots 结果行 `0 0 0 0 2 0`。
3. 文件名约定：`$delete_files` 的 `/data/delete_pos_` 前缀（BE 发出）与 .puffin 后缀（v3）。
4. 隐藏列 pin：desc 暴露 `doris_iceberg_rowid`；v3 `_row_id`/`_last_updated_sequence_number` 默认隐藏、show_hidden_columns 暴露；SELECT * 宽度不变。
5. 唯一残留 SDK 序列化面（**保留**）：sys-table FileScanTask base64 经 TIcebergFileDesc.serialized_split 给 BE JNI（IcebergScanPlanProvider.java:653，连接器侧）——与本刀无关，勿动。

---

## 6. 顺带发现的活问题（不属于本刀，登记 follow-up；处置 = §7-Q4）

- **[FU-rewrite-kerberos-bare]（high 候选，活 bug）**：`ConnectorTransaction.registerRewriteSourceFiles`（EXECUTE rewrite_data_files 提交前 re-derive）在 IcebergConnectorTransaction.java:361-397 用**裸 table 字段**跑 `planFiles()`（读 manifest-list 远程 IO），链上无任何 doAs/TCCL 包装——kerberized HDFS 上会复现与扫描规划期修复（d5541bbb384）完全同型的 SASL 拒；S3 上首触 lazy client 有 TCCL ClassCast 风险。修形 = 镜像 commit():438 包 `context.executeAuthenticated`。仅 rewrite 车道（WriteOperation.REWRITE），DELETE/UPDATE/MERGE 不经过。
- **[FU-dml-pretx-rollback-gap]（medium，预存在 legacy gap 忠实移植）**：RowLevelDmlCommand.run 在 beginTransaction(:98) 之后、executeSingleInsert(:103) 之前抛错（applyWriteConstraint/finalizeSink/planWrite beginWrite 失败）**无人回滚**——PluginDrivenTransactionManager 表项 + GlobalExternalTransactionInfoMgr 表项泄漏至 FE 重启（iceberg rollback 本身是 no-op 故只漏注册表内存，但 SPI 契约上其它连接器可能持真实资源）。修形 = 镜像 InsertIntoTableCommand.java:372-388 的 catch(Throwable)→onFail。
- **[FU-dml-kill-window]（low，与 INSERT 同型预存在）**：KILL 只 cancel coordinator，而 coord 在 finalizeSink 之后才 set（:102）——规划/beginWrite 期间 KILL 静默无效，语句照常提交。
- **[FU-synthcol-never-throws]（info）**：fetchSyntheticWriteColumns javadoc 称 never-throws（PluginDrivenExternalTable.java:458-462）但无 try/catch，远端/鉴权失败会从 getFullSchema 抛出。
- **[FU-stash-orphan-ttl]（info，设计如此）**：连接器 rewritable-delete 暂存孤儿仅靠下次 accumulate 触发的 300s TTL 扫（无 query-end 钩子）；键唯一故有界内存、永不脏读。留档不修。
- **鉴权姿势总结（后续动码通则，来自逐 crossing 清点）**：本车道引擎侧对连接器的每个调用都是**裸调**，覆盖全靠连接器在自身远程 IO 段自包 `TcclPinningConnectorContext.executeAuthenticated`（getTableHandle:304 / loadTable:512 / beginWrite:207 / commit:438 / resolveTable:665）；纯内存方法（applySnapshot/getSyntheticWriteColumns/applyWriteConstraint/addCommitData/beginTransaction）不包。**任何新增/移动 crossing 必须按"纯内存可裸调 / 碰 catalogOps 或 table.io() 必须连接器内自包"分类**；线程级包装覆盖不了 iceberg worker 池扇出（那需要对象级 IcebergAuthenticatedFileIO + 每 JVM 池 TCCL pin）。

---

## 7. 用户裁定（2026-07-04 已全部裁定，原问题原文保留备查）

> **Q1 = 接受，改判为删除；Q2 = 并入一起删；Q3 = 现在搬；Q4 = 随本轮独立 commit 修（两个都修）。**
>
> Q3 落地形状（搬家目标，实施时可微调包名但方向已定）：
> - `IcebergMergeOperation` → `org.apache.doris.nereids.trees.plans.commands.merge.MergeOperation`（常量语义与 Trino 同源、纯中立，**改中立名**；`OPERATION_COLUMN="operation"` 与 1-5 数值原样保留）；
> - `IcebergNereidsUtils` 存活半（:87-226）→ `org.apache.doris.nereids.trees.plans.commands.RowLevelDmlRowIdUtils`（行标识注入工具，中立命名；SDK 转换半随死码删除，不搬）；
> - `IcebergRowId`、`IcebergMetadataColumn` → 同包搬迁（`nereids.trees.plans.commands`），**保留类名**——其载荷（`__DORIS_ICEBERG_ROWID_COL__` 四字段 struct、`$file_path` 等虚拟列词表）是 iceberg 契约语义，属 legacy 豁免命名；改名反而失真。
> - 涟漪面：~20 处 import（三个 Iceberg*Command、IcebergRowLevelDmlTransform、BindExpression、PhysicalIcebergDeleteSink/MergeSink、PhysicalPlanTranslator）+ 2 个保留 UT（PhysicalPlanTranslatorIcebergRowLevelDmlTest:37、PhysicalIcebergMergeSinkTest:32）。搬完后 nereids/planner 对 `datasource.iceberg` 的活 import 归零（残余仅为实体类死臂，属后续死臂清剿阶段）。

### 原裁定问题（存档）

- **Q1（方向重裁定）**：接受 §2 的实证重裁定——表达式下沉与暂存句柄化改判为**删除死码**，不新建 SPI/不迁移？（推荐 = 是；备选 = 隔离不删：砍断入口分支但留文件——买不到回滚能力还留下 SDK import，与目标直接矛盾，不推荐）
- **Q2（闭包范围）**：是否把**原生 INSERT 死车道**（LogicalIcebergTableSink/PhysicalIcebergTableSink/IcebergTableSink/IcebergInsertExecutor/IcebergTransaction/IcebergTransactionManager + 各 bind/translator 死臂）并入本刀一起删？（推荐 = 并入：IcebergTransaction 被 DML 死 executor 与 INSERT 死 executor 共同钉住，一起删闭包干净、测试处置一次到位；不并入则 IcebergTransaction 及其 614 行测试暂留，本刀后 fe-core 该车道仍有 SDK import 残留）
- **Q3（SDK-free 小类搬家）**：IcebergMergeOperation/IcebergRowId/IcebergMetadataColumn/IcebergNereidsUtils(行标识半) 四个 SDK-free 类是否现在搬出 `datasource.iceberg` 包改中立命名？（推荐 = 暂不搬：本刀零改名零 churn，语义不变；留到死臂清剿阶段清空 `datasource/iceberg/` 时一并搬，避免两次 import 涟漪）
- **Q4（顺带活问题处置）**：§6 前两条（rewrite kerberos 裸奔 / DML 预执行回滚缺口）= 随本轮各自**独立小 commit** 修，还是仅登记 follow-up 待排期？（两者修形都已明确、改动面小；kerberos 一条有 CI e2e 可实证）

---

## 8. TODO（已裁定，按序执行，每步独立 commit + 全量验证）

1. **删 rewrite/action 死车道**：§4.1 的 17 文件 + ExecuteActionFactory 两分支 + RewriteTableCommand 死臂 + StatementContext 暂存对（SDK 对 + gather 旗）+ IcebergScanNode 暂存消费分支；随删 RewriteDataFilePlannerTest/RewriteGroupTaskTest。验证：fe-core test-compile + nereids/datasource 相关套件 + checkstyle。
2. **删 DML 死臂闭包**：§4.1 DML 死类 7 文件 + helper 2 文件 + §4.2 各成员级手术（三 Command 死半、Transform 旧臂、PhysicalIcebergMergeSink 旧臂、Translator 死臂）+ §4.4 测试手术（含两个 KEEP 搬迁）。验证：同上 + §4.4 gate 清单全绿。
3. **删 INSERT 死车道（Q2=并入）**：§4.1 INSERT 项 + InsertIntoTableCommand/BindSink/UnboundTableSinkCreator 死臂 + IcebergScanNode 死构造臂/sys 成员/双 map + IcebergApiSource + IcebergTransactionTest 整删 + CommitDataSerializerTest 改写。
4. **SDK-free 小类搬中立包（Q3=现在搬）**：按 §7 落地形状搬 4 个类 + ~20 处 import + 2 个 UT import；纯移动/改名 commit，不夹带语义改动。验证：test-compile + 两 UT 绿 + checkstyle。
5. **门禁**：import-control 增 nereids/planner 禁 org.apache.iceberg；`grep -rn 'org.apache.iceberg' fe-core/src/main/java/org/apache/doris/nereids fe-core/src/main/java/org/apache/doris/planner` must be empty 作为验收；另验 nereids/planner 零 `datasource.iceberg` 活 import（死臂残余登记豁免清单）。
6. **两个独立 fix commit（Q4=修）**：① rewrite kerberos 裸奔——`IcebergConnectorTransaction.registerRewriteSourceFiles` 的 planFiles 段包 `context.executeAuthenticated`（镜像 commit():438），UT 走 RecordingConnectorContext 接线断言 + mutation 击杀；② DML 预执行回滚缺口——RowLevelDmlCommand.run 的 :98-102 窗口加 catch(Throwable)→executor.onFail（镜像 InsertIntoTableCommand.java:372-388），UT 断言 begin 后 finalize 抛错时 rollback 被调、注册表被清。顺序在删除/搬家之后，避免同文件 staging 纠缠。
7. **文档收尾**：更新 `fe-core-iceberg-removal-plan.md`（§6b 改判记录 + 阶段清单同步）、HANDOFF、本设计 Status→DONE。

**验收（整刀）**：fe-core `test-compile` 过；§4.4 gate 全绿；checkstyle 净（不带 -am）；上游计划的 IcebergGsonCompatReplayTest 保绿；grep 验收（TODO-4）；docker e2e（4 个 dml 套件 + v3 row-lineage 两套 + v2→v3 对比 + action/ 8 套件 + 位置/等值删除读套件）——死码删除理论上零行为差，套件如未跑须显式标注 flip-gated/未跑（Rule 12）。
