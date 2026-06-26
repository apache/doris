# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 commit-bridge S5d（translator dual-mode，末步、最深、BE-facing、C5 阻塞）**

**本 session（session 12）= S5 起步对抗 recon + S5a/S5b/S5b2/S5c 实现**：dispatch dual-mode 拆 4 子步全绿提交，只剩 **S5d（translator dual-mode）**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ → commit-bridge：recon ✅ + S1 ✅ + S2/S3 ✅（option D）+ S4 part1=Fix B ✅ + S4 part2=supply ✅ + S5a ✅ + S5b ✅ + S5b2 ✅ + S5c ✅ → S5d 待办（末步）**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（dispatch dual-mode S5a–S5c，详 design §11.7.8；4 commit）
> 起步并行对抗 recon（wf `wf_9d649ce2-b2d`，6-slice〔synthesize/checkMode/executor-finalize/translator/BE-dialect/conflict-parity〕 + 2 对抗 verify〔synthesize 类参数化 CONFIRMED + BE-dialect CONFIRMED〕 + synthesis）→ 结论 S5 ≈「dual-mode 接线 + 1 小中立 SPI(checkMode) + logical-sink 类参数化放宽」，冲突对等机器已建好。**2 用户裁决**：① **冲突过滤 = accept-widen**（SPI-only 冲突路；转换器已存在且逐项等价，残留差异仅 OCC 安全向变宽=偶发假冲突重试，纯 perf）② **DELETE 下发 = 走 SPI 统一通道**（与 MERGE 一致、对齐 Trino、旧专用 sink 后续可删）。

- **S5a（`21b806c7b40`）= checkMode 中立 SPI**：新 `ConnectorWriteOps.validateRowLevelDmlMode(session, handle, WriteOperation)`（default no-op）+ `IcebergConnectorMetadata` override（按 op 读 `write.{delete,update,merge}.mode`，COPY_ON_WRITE→throw `DorisConnectorException`，镜像 legacy `IcebergDmlCommandUtils.checkNotCopyOnWrite`）+ transform `checkMode` plugin arm（DorisConnectorException→AnalysisException 保 parity）。**注意 iceberg 默认 mode=copy-on-write**（空 props 即拒）。34/0/0/0+11/0/0/0+5-mutation 全杀。
- **S5b（`602167d5d9f`）= logical-sink 类参数化放宽**：`LogicalIcebergDeleteSink`/`MergeSink` 的 `targetTable`/`database` 字段 `IcebergExternalTable`/`IcebergExternalDatabase`→`ExternalTable`/`ExternalDatabase`（physical sink 早泛型；rules/ExplainCommand/BindExpression 全 pass-through 不动）。纯类型放宽、byte-identical；iron-law 严格改善。`IcebergDDLAndDMLPlanTest` 14/0 + `ExplainIcebergDeleteCommandTest` 11/0。
- **S5b2（`edc291d4b13`）= synthesize plugin arm**：`synthesize` cast `(IcebergExternalTable)`→`(ExternalTable)` + 3 command 合成入口/helper 链放宽（`IcebergDeleteCommand`/`IcebergUpdateCommand`/`IcebergMergeCommand`）+ 3 处 `(IcebergExternalDatabase)`→`(ExternalDatabase)`。链底 `IcebergNereidsUtils` 早泛型。新 `synthesizeDeleteOnPluginTableBuildsSinkTargetingIt` 证 synthesize 产 sink 锚定 plugin 表。
- **S5c（`fe999d4f2f6`）= dispatch 三 plugin arm**：`newExecutor`→`PluginDrivenInsertExecutor(Optional.empty(),-1L)`；`setupConflictDetection`→early-return（SPI-only）；`finalizeSink`→按 executor instanceof 经新 public `PluginDrivenInsertExecutor.finalizeRowLevelDmlSink`（无 rewritable-delete overlay，单次 finalize）。14/0/0/0+8/0/0/0+2-mutation 全杀。`newExecutor` plugin arm 不单测（executor ctor 重）。

## 🔵🔵 S5d 详设计（下个 session 起点；动码前 re-grep 防漂移；锚点 = 本 session HEAD `fe999d4f2f6`）

**目标**：`PhysicalPlanTranslator` 两 visitor dual-mode，让 post-flip（targetTable=`PluginDrivenExternalTable`）的 DELETE/MERGE 经 `PluginDrivenTableSink`+`WriteOperation` 下发，pre-flip（`IcebergExternalTable`）走原 native `IcebergDeleteSink`/`IcebergMergeSink` 不变。**模板 = `visitPhysicalConnectorTableSink:631`**（INSERT 路，已建 PluginDrivenTableSink 全套：catalog/connector/metadata 解析 → `getTableHandle` → `applyMvccSnapshotPin`〔Fix B〕 → `connectorColumns` from `getCols()` → `writeSortInfo` → `PluginDrivenTableSink` ctor）。

- **`visitPhysicalIcebergDeleteSink:590`**（现无 slot-name loop）：加 plugin arm `if (getTargetTable() instanceof PluginDrivenExternalTable)` → 仿模板建 `PluginDrivenTableSink(..., WriteOperation.DELETE)`：`connectorColumns` from `icebergDeleteSink.getCols()`〔`getCols()` 继承自 `PhysicalBaseExternalTableSink:74`〕、`writeSortInfo=null`〔DELETE 无 write sort〕、`applyMvccSnapshotPin`〔Fix B，钉读快照〕。else→原 native `IcebergDeleteSink`。**DELETE 不需** slot-name loop（BE `viceberg_delete_sink.cpp` 按 **block-name** 解 row_id，recon SLICE E + verify-E CONFIRMED）。
- **`visitPhysicalIcebergMergeSink:602`**（slot-name loop `setMaterializedColumnName:616`，gate `OPERATION_COLUMN||ICEBERG_ROWID_COL`）：**slot-name loop 上提至 instanceof 分叉之上**（native+plugin 都跑）——BE `viceberg_merge_sink.cpp:319-326` 按 **expr_name** 解 operation/row_id，故 FE 须对两 arm 都 `setMaterializedColumnName`。然后 plugin arm → `PluginDrivenTableSink(..., WriteOperation.MERGE)`；else→原 native `IcebergMergeSink`。
- **BE 契约（recon SLICE E + verify-E CONFIRMED）**：连接器 `IcebergWritePlanProvider.buildMergeSink`/`buildDeleteSink` 须填全 `TIcebergMergeSink`/`TIcebergDeleteSink` 字段（含 **`format_version`**〔漏=BE 默认 0 强走 position-delete 损坏 v3〕 + **`rewritable_delete_file_sets`**〔漏/空 v3=旧删静默复活，最高危；由 S4 part2 β-stash 供〕）。**须核**：materialized-column-name 经 `PluginDrivenTableSink`→`planWrite`→`buildMergeSink` 到 BE（连接器只收 `List<ConnectorColumn>`+表元数据，**不能** 复制 SlotDescriptor hint→故 FE translator 必须在建 sink 前 setMaterializedColumnName，这是 loop 上提的根因）。
- **⚠️ S5d 动码前 re-grep + 实证**：① `visitPhysicalConnectorTableSink` 全 body（连 `buildConnectorWriteSortInfo`/`applyMvccSnapshotPin`/ctor 参）作模板；② `PhysicalIcebergDeleteSink`/`MergeSink.getCols()`+output（合成的 operation+rowid 列）；③ MERGE 路 materialized-name 经 `PluginDrivenTableSink.bindDataSink`→连接器 `buildMergeSink` 是否保留到 `TIcebergMergeSink`（亲核 `viceberg_merge_sink.cpp:309-333` expr_name 解 + 连接器 buildMergeSink 取 output tuple/slot names 的路）；④ `viceberg_delete_sink.cpp`/`viceberg_merge_sink.cpp` 实证 DELETE block-name / MERGE expr_name。
- **验证**：green + mutation（BE-facing dormant）。重点 mutation = **slot-name loop 上提**（须对两 arm 生效，移除上提→plugin MERGE 漏 materialized-name→BE 解不出 operation/row_id）。`visitPhysicalIcebergMergeSink` 路可单测（不需建真 executor，translator 层）。**[FU-flip-e2e]** 真翻闸 DELETE/MERGE 端到端未跑（CI-gated，勿谎称）。

## 🟡 待裁决策（动到时请用户裁；中文先讲清）
- **[DEC-S2] ✅ = option D**（commit-time re-derive，design §11.7.4–5）。
- **[SHOULD-2] ✅ = 方案 B（写入遵循读快照）+ Fix B + supply 接线**（design §11.7.6–7）。
- **[DEC-S5] ✅ 本 session 裁**：① 冲突过滤 = **accept-widen**（S5c 已实现）② DELETE 下发 = **走 SPI 统一通道**（S5d 实现）。

## 🟡 已登记 follow-up（非阻塞，勿在本增量做）
- **[FU-flip-e2e]** S5 全程 pre-flip byte-identical + UT 锁，但真翻闸 DELETE/MERGE/UPDATE 端到端（旧删不复活、operation/row_id BE 解析、冲突 OCC）**未跑**（CI-gated）。
- **[FU-step1-nullconn → cutover]** `pluginConnectorSupportsRowLevelDml`/`checkPluginMode` 的 `getConnector()` unguarded（与 step-3 同名 helper 对齐）。
- **[FU-getRowIdColumn]** `IcebergMergeCommand.getRowIdColumn(562)` 仍 `IcebergExternalTable`（不在合成链，S5b2 未触）；翻闸/P6.7 时核是否 dead。
- **[FU-order/remap/dualdelete/supply-leak/Bnit]** 见 git log 历史 HANDOFF（design §11.7.5–7）。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。**legacy 豁免类**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支合法）：`IcebergRowLevelDmlTransform`/`Iceberg{Delete,Update,Merge}Command`/`Logical/PhysicalIceberg{Delete,Merge}Sink`/`IcebergNereidsUtils`/`Iceberg*Executor`/translator `visitPhysicalIceberg*` visitor/`IcebergTransaction`/`IcebergRewritableDeletePlanner`。**通用 fe-core 类**（`PluginDrivenTableSink`/`PluginDrivenInsertExecutor`/`PluginDrivenExternalTable`）须全经中立 SPI——S1/S5a/S5c 注入点只引中立 `WriteOperation`/`ConnectorMetadata`/`DorisConnectorException` 等，已合规。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre+C3b-core+commit-bridge S1–S5c 已闭，S5d+C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。**[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。**[C3a ✅]** INSERT OVERWRITE @branch（[D-070]）。**[C3b-pre ✅]** partition_columns+parallel-write（[D-071]）。
- **[C3b-core ✅ step1-3 / commit-bridge: recon+S1+S2/S3+S4+S5a/b/b2/c ✅ → S5d 待]**（[D-072]+[D-073]）。详 §10+§11（§11.7 = commit-bridge 全程）。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 throw）；`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / mvcc+partition-stats capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core+commit-bridge S1/S4/S5a/S5b/S5b2/S5c 无新 DV。**S2/S3=[DV-S2-rederive]**（post-flip 旧删源 commit-time manifest 读 + 升级表 legacy+DV 双删）。S5d 预计可能引入 DV（translator→PluginDrivenTableSink 改道）——做到时评估。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🗺️ 代码脚手架（S5d 实现锚点；impl 期 re-grep 防漂移；HEAD=`fe999d4f2f6`）

- **S5d translator（下个 session 起点）**：
  - `PhysicalPlanTranslator.visitPhysicalIcebergDeleteSink:590`（无 slot-loop）/`visitPhysicalIcebergMergeSink:602`（slot-loop `setMaterializedColumnName:616` gate `IcebergMergeOperation.OPERATION_COLUMN||Column.ICEBERG_ROWID_COL`）/**模板 `visitPhysicalConnectorTableSink:631`**。
  - `PhysicalIcebergDeleteSink`/`MergeSink`：`getTargetTable()` 返 `ExternalTable`（已泛型）；`getCols()` 继承 `PhysicalBaseExternalTableSink:74`。
  - `PluginDrivenTableSink` 7-arg ctor（含 `WriteOperation`）`:103`（S1 建）。`PluginDrivenScanNode.applyMvccSnapshotPin`（public，Fix B）。
  - 连接器 `IcebergWritePlanProvider.planWrite`〔DELETE→`buildDeleteSink:157`/MERGE→`buildMergeSink:162`〕（消费侧已建+休眠）。BE `be/src/vec/sink/writer/viceberg_merge_sink.cpp`〔expr_name 解 `:309-333`〕/`viceberg_delete_sink.cpp`〔block-name 解 row_id〕。
- **commit-bridge 已建（休眠）**：S1 `PluginDrivenTableSink`(WriteOperation 透传)/S2-S3 `IcebergConnectorTransaction.collectRewrittenDeleteFiles`→`readExistingFileScopedDeletes`(option D)/S4 part1 Fix B(`applyMvccSnapshotPin`+`IcebergWriteContext.readSnapshotId`)/S4 part2 `IcebergRewritableDeleteStash`(scan stash→write 盖 rewritable_delete_file_sets)/S5a `validateRowLevelDmlMode`/S5c `PluginDrivenInsertExecutor.finalizeRowLevelDmlSink`。
- **fe-core legacy 豁免**：见上「铁律」清单。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；S5d 全在 `fe-core`（translator）。验证读 surefire **XML**（python ET）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **fe-core -am 单类首次 ~5-7min、热 cache ~2.5min**；**Bash 工具前台 120s 超时会杀构建** → 长构建用 `run_in_background:true` 再读 surefire XML（别用前台等）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仓根 `tools/`）。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`/`RecordingIcebergCatalogOps`+`FakeIcebergTable`）；fe-core **Mockito**（`mockito-inline` 可 mock final；`Mockito.mock(...)` + `when`/`verify`/`verifyNoInteractions`）。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 易 trivially-pass → 必变异验真。脚本范式 `scratchpad/mutate_s5c.py`：cp 备份→exact-string mutate（**「行为禁用形」`&& false` 而非删引用形**，避 UnusedImport 假阴）→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire failures+errors>0（KILLED）→restore。**⚠️python3.6 无 `capture_output`/`text=`** → 用 `stdout=subprocess.DEVNULL`。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔真文件在 `fe/fe-connector/...`，勿提交〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **S5d commit 预计 = 1 改 main（`PhysicalPlanTranslator.java`）+ 1 改 test（translator/merge-sink 测）+ 可能连接器 `IcebergWritePlanProvider`〔若 BE 契约核出缺字段〕 + design `§11.7.8`**。逐子步 green+mutation+design+commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session S5a–S5c 4 commit）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core + commit-bridge recon+S1+S2/S3+S4part1+S4part2+S5a+S5b+S5b2+S5c ✅ → S5d（末步）待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 起 86G free，全程 OK 未掉）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。**多次增量 build 后 .class 可能 stale→误判**；全量验证用 clean `package`。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **🆕 用户铁律：fe-core 不得新 `instanceof Iceberg*` seam** —— dual-mode 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法；通用类须全经中立 SPI。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（本 session recon `wf_9d649ce2-b2d` 6-slice+2-verify+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **commit-bridge 是大活、最深、跨 session**：S1–S5c ✅，**S5d（translator dual-mode）= 末步、最深、BE-facing、C5 阻塞**；fresh session 全 budget 做；逐子步 green+mutation；**C5 前切忌动 `SPI_READY_TYPES`**。
- **下个 session 起步 = S5d**：先读 design §11.7.8（S5d 详设计）+ 本 HANDOFF「S5d 详设计」+「[DEC-S5] ✅」。动码前 re-grep translator 锚点（行号会漂）+ 亲核 `visitPhysicalConnectorTableSink` 模板 + BE `viceberg_{merge,delete}_sink.cpp`。S5d 涉 materialized-column-name 经 PluginDrivenTableSink→planWrite→BE 的保真，须实证。
- **上下文超 30% 即交接**。本 session = S5a/b/b2/c（dispatch dual-mode 4 子步全绿+mutation+对抗 recon），在干净节点交接 S5d。
