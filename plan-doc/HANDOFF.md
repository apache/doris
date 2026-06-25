# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 C2（WS-SYNTH-READ）**（C1 已 DONE）

**P6.1–P6.5 = ✅ 全 DONE**。**P6.6 翻闸 = 进行中：C1 WS-PIN ✅ DONE（本 session）→ 下一步 C2 WS-SYNTH-READ**。翻闸 = 5 commit-stream（C1 WS-PIN / C2 WS-SYNTH-READ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP），每 C dormant-for-iceberg / live-connector parity / 自带 UT+mutation，**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。

## ⚠️ C1 起步 recon 推翻了 RFC 签字 D4/D5（[D-068]，已落地）——C2 起步同样先 recon 再信文档
RFC `P6.6-flip-rfc.md` §6 各 WS 的细节**可能同样过时**（D4/D5 已证伪）。**进 C2 前先用对抗 recon 核 RFC §6.WS-SYNTH-READ + D7**（仿 C1 做法），勿直接照 RFC TDD。

## C2 起步指引（先做这个）
1. **读 `tasks/designs/P6.6-C1-ws-pin-design.md`**（C1 实证范式 + D4/D5 推翻的完整理据）+ RFC §6.WS-SYNTH-READ（**待 recon 复核**）。
2. **C2 = 合成列读路径**（通用 FE + iceberg BE，dormant）。RFC 初判：
   - **FE**：`PluginDrivenScanNode` 当前**无** `classifyColumn` override → 加之，对 `GLOBAL_ROWID_COL`/`ICEBERG_ROWID_COL` 返 `SYNTHESIZED`、row-lineage 返 `GENERATED`（移植 legacy `IcebergScanNode:907-919`）。
   - **BE**：`iceberg_reader.cpp` 合成列入 `_id_to_block_column_name`+field_id → `table_schema_change_helper.h` `get_children_node` DCHECK 崩。修：合成列走 `add_not_exist_children`（非 `add_children`），**不碰 `paimon_reader.cpp`**。
3. **D7 必须 C2 设计期查证**（用户已签 D7=「C2 查证」）：**paimon 是否经 `LazyMaterializeTopN` 触达 `GLOBAL_ROWID`**？决定 `classifyColumn` 泛化是 connector-guard（仅 iceberg 发 SYNTHESIZED）还是通用（若 paimon 触达则 BE `paimon_reader.cpp` 须配套，否则静默丢列）。这是 C2 的核心 cross-connector 风险——**先查证再定 guard 形态**。
4. **C2 完**：更 HANDOFF + commit（每 C 完即更）。**C5 前切忌动 `SPI_READY_TYPES`**。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1 已闭，C2–C5 待办）

> 5 commit-stream（C1 WS-PIN ✅ / C2 WS-SYNTH-READ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。**C1 已 DONE**（详 `tasks/designs/P6.6-C1-ws-pin-design.md` + [D-068]）。

- **[C1 ✅ 完成纪要]** sys 表时间旅行 pin-feed：fe-core 唯一改点 `PluginDrivenScanNode.pinMvccSnapshot` 加 `resolveSysTableSnapshotPin()` fallback（context 空时委派源表 `MvccTable.loadSnapshot`→`applyMvccSnapshotPin` 落 sys handle）。**recon 推翻 D4/D5**：① 普通表 pin reorder = 非阻塞（连接器 workaround `IcebergScanPlanProvider:705-714` 已正确）+ 全局改打破 paimon `@branch` → **移出 C1 留 P6.7**；② sys `implements MvccTable`（D5）因 `MvccTableInfo` key 不匹配 + `loadSnapshots(sysTable)` 从不被调用而行不通 → 改 `getQueryTableSnapshot()` 线程。零 SPI/零连接器/零 StatementContext 改；连接器侧（`getSysTableHandle`+`buildScan` useRef/useSnapshot）已 ready。5 UT + mutation 绿。e2e 留 P6.8。
- **[C2 待 recon]** D7 = paimon lazy top-N 触达 GLOBAL_ROWID 查证（定 classifyColumn guard 形态）。RFC「DV-038 纠正」：legacy `IcebergScanNode:907-919` 分类**正确**，真问题 = 通用 `PluginDrivenScanNode` **无** override，翻闸后 iceberg 走通用路丢 SYNTHESIZED + BE field-id DCHECK。
- **[C3 待办]** `visitPhysicalConnectorTableSink` 合成列物化 + 分布（connector-guard，移植 `visitPhysicalIcebergMergeSink:613-615`）。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 `IcebergExecuteActionFactory:89` throw）；**唯一深水 = per-group 读端子设计**。`IcebergRewriteDataFilesAction:173,196` `(IcebergExternalTable)` 翻闸即 ClassCastException（**不能纯 defer**）。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory:137 case` / `pluginCatalogTypeToEngine` iceberg / **GSON compat 8 catalog+db+table**（`registerSubtype`→`registerCompatibleSubtype`，镜像 paimon）/ mvcc+partition-stats capability 核 / Show* parity。**C5 是唯一不可逆点**（GSON 身份变→不可降级回读 image）；C5 前须 C1–C4 全绿 + 用户二签。

**[pre-flip 行为偏差中央登记]**：P6.4 = DV-045/046/047；P6.5 = DV-048/049。**C1 无新 DV**（[D-067] 所记 sys-pin「休眠翻闸接线」由 C1 [D-068] 落地消解）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**P6.4 T07/T08/T09 + P6.5 T01–T08 + P6.6-RFC + 本 session C1 已 commit 未 push（14 commit）**。**用户未要求 push**——留用户裁量。工作树 tracked 干净（除既有 untracked scratch）。
- **P6.1–P6.5 = ✅ DONE**。**P6.6 翻闸进行中：C1 WS-PIN ✅ DONE → C2 WS-SYNTH-READ 待办**。
- iceberg **不在** `SPI_READY_TYPES`（仍走 switch-case `:137 case "iceberg"`，pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 = P6.6 C1 WS-PIN（rescoped）：sys 表时间旅行 pin-feed + 推翻 D4/D5

- **起步 recon**（独立读码 clean-room + 6-agent 对抗 wf `wf_b1bd42e4-675`/526k token：4 area reader + 2 cross-connector 安全论断 adversarial verify）→ 证伪 RFC §6.WS-PIN 的 D4（普通表 reorder）/D5（sys MvccTable）→ 用户 AskUserQuestion 双裁（[D-068]）。
- **设计** `tasks/designs/P6.6-C1-ws-pin-design.md`（rescoped C1 = 仅 sys 表 pin-feed）。
- **实现**（fe-core 唯一改点）：`PluginDrivenScanNode.pinMvccSnapshot` 加 `resolveSysTableSnapshotPin()` fallback + `MvccTable` import。**零 SPI / 零 MvccTable-on-sys / 零 StatementContext / 零连接器 / 零 BE 改**。
- **验证**：新 `PluginDrivenScanNodeSysTablePinTest` 5 UT；全 `PluginDrivenScanNode*Test` 家族 **57/0/0**；mutation 实证（fallback→empty→T1/T2 红；去 both-null 短路→T3 红）；生产码 clean。**链路全 code-grounded 亲验**（`BindRelation:467-474`→`PhysicalPlanTranslator:802-805`→guard→pin→`buildScan:338-342`）。e2e（真 `t$snapshots FOR TIME AS OF`）留 P6.8。
- **文档同步**：design（新）/ decisions-log [D-068] / connectors/iceberg.md 进度日志 / RFC §6.WS-PIN 标 superseded / HANDOFF 覆盖。**0 新 DV**。

---

# 🗺️ 代码脚手架（iceberg sys-table，全 DONE/dormant）

- **sys 时间旅行 pin-feed（C1 新增）**：`PluginDrivenScanNode.pinMvccSnapshot`〔context 空→`resolveSysTableSnapshotPin()`〕+ `resolveSysTableSnapshotPin()`〔package-private，sys-gated，委派 `getSourceTable().loadSnapshot(getQueryTableSnapshot, getScanParams)`〕。链路：`BindRelation.handleMetaTable:467-474`〔sys LogicalFileScan 携 snapshot/scanParams〕→ `PhysicalPlanTranslator:802-805`〔FileQueryScanNode 公共尾 setQueryTableSnapshot〕→ guard `checkSysTableScanConstraints`〔capability=true 放行，[D-067]〕→ pin → 连接器 `IcebergScanPlanProvider.planSystemTableScan→buildScan:338-342`〔`useRef/useSnapshot` when `hasSnapshotPin()`〕。连接器 `getSysTableHandle:398-400`〔从 base handle 继承 pin〕。
- **连接器**：`IcebergTableHandle`〔sys 变体 + 保留 pin〕/`IcebergConnectorMetadata`〔`listSupportedSysTables`/`getSysTableHandle`〔LAZY〕/`getTableSchema`·`getColumnHandles` sys 分支/`loadSysTable`/`buildTableDescriptor` hms↔iceberg fork〕/`IcebergScanPlanProvider`〔`planSystemTableScan`/`supportsSystemTableTimeTravel()`=true:196/`SYS_TABLE_DUMMY_PATH`〕/`IcebergScanRange`〔`serializedSplit` 早返〕。
- **fe-core**：`PluginDrivenScanNode.checkSysTableScanConstraints`〔capability-aware〕 + `PluginDrivenExternalTable.getEngine/getEngineTableTypeName` iceberg case + `getSupportedSysTables` + `TableIf.TableType.toMysqlType` PLUGIN→"BASE TABLE" + `findSysTable`〔case-SENSITIVE〕 + `ShowCreateTableCommand.validate` 解包。零改动复用：`PluginDrivenSysExternalTable`〔`getSourceTable:129-131`；C1 委派目标〕/`PluginDrivenSysTable`/`SysTableResolver`。
- **新 SPI**：`ConnectorScanPlanProvider.supportsSystemTableTimeTravel()` default false——仅 iceberg override。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：`datasource/iceberg/IcebergSysExternalTable` + `datasource/systable/IcebergSysTable` + `IcebergScanNode`〔`createTableScan:569` sys time-travel honor〕。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B` 即可。**fe-core -am 单类/多类 ~2.5min**。后台 task 通知的 exit code 是 echo 的非 maven 的，读 surefire XML。**注**：fe-core 构建尾常有 antlr `->`/`super::` "mismatched input" 行 = parser-gen 噪音（每次都有、exit 0、测全绿），非本改动。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔guard/pin 用 `CALLS_REAL_METHODS` + 仅 stub 访问器〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow/verifyNoInteractions 测易 trivially-pass → 必变异验真（C1 已逐条做：fallback→empty 验正路、去短路验 verifyNoInteractions）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。本 session = C1 改：
  - 产品：`fe/fe-core/.../datasource/PluginDrivenScanNode.java`
  - 测试：`fe/fe-core/.../datasource/PluginDrivenScanNodeSysTablePinTest.java`（新）
  - 文档：`plan-doc/tasks/designs/P6.6-C1-ws-pin-design.md`（新）· `plan-doc/tasks/designs/P6.6-flip-rfc.md`〔§6.WS-PIN superseded marker〕· `plan-doc/decisions-log.md`〔D-068〕· `plan-doc/connectors/iceberg.md`〔进度日志〕· `plan-doc/HANDOFF.md`
- message `[refactor](catalog) P6.6 iceberg: C1 — <subj>` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。**注意 P6.4-T07..T09 + P6.5-T01..T08 + RFC + 本 C1 已 commit 未 push（14 commit）**（rebase/force 须谨慎）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。P6.7 删 legacy 时尤其。
- **HANDOFF/设计/RFC/audit-spec 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep + 实证）再信文档。**C1 实证推翻 RFC 签字 D4/D5**（普通表 reorder 非阻塞 + 打破 paimon @branch；sys MvccTable 因 key 不匹配行不通）——C2 起步**同样先对抗 recon 核 RFC §6.WS-SYNTH-READ + D7** 再 TDD。
- **大文件用 subagent/workflow 总结**；`decisions-log.md`（>30k token）/PROGRESS.md 巨行编辑前先 grep 定位精确子串再 surgical Edit（Edit 前必先 Read 一次该文件，哪怕只读锚点几行）。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗 + 先 code 独立判断、后交叉核历史结论（C1 即此范式：主 session 亲读 getSplits/getColumnHandles/handle 链路，再用 6-agent wf 交叉核 + 2 adversarial verify paimon-parity/sys-guard）。
- **P6.6 进行中**：C1 ✅ DONE。下个 session 进 **C2 WS-SYNTH-READ**（先 recon + 查证 D7 paimon top-N）。**大阶段分多 session**，每 C 完即更 HANDOFF + commit。翻闸（C5）是全有或全无不可逆点，C5 前须 C1–C4 全绿 + 用户二签。
