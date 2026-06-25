# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 C3（WS-WRITE）**（C1+C2 已 DONE）

**P6.1–P6.5 = ✅ 全 DONE**。**P6.6 翻闸进行中：C1 WS-PIN ✅ / C2 WS-SYNTH-READ ✅ DONE（本 session）→ 下一步 C3 WS-WRITE**。翻闸 = 5 commit-stream（C1 WS-PIN / C2 WS-SYNTH-READ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP），每 C dormant-for-iceberg / live-connector parity / 自带 UT+mutation，**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。

## ⚠️ C1/C2 起步 recon 均推翻了 RFC 签字（D4/D5 / BE+D7）——C3 起步同样先 recon 再信文档
RFC `P6.6-flip-rfc.md` §6 各 WS 细节**反复被证伪**（C1 推 D4/D5；C2 推 BE「DCHECK 崩」+ D7 问错方向 + 挖出 GAP-A/GAP-B）。**进 C3 前先用对抗 recon 核 RFC §6.WS-WRITE**（仿 C1/C2），勿直接照 RFC TDD。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得出现 `if (iceberg)` / `import IcebergUtils` 等连接器判断**
iceberg-specific 逻辑必须落 `fe-connector`，经 SPI（capability / 中立 default 方法）表达；fe-core 只认通用概念（如 `GLOBAL_ROWID_COL` 是 Doris 全局机制可留 fe-core）。**C3 的 `visitPhysicalConnectorTableSink`（fe-core/nereids）合成列物化 + 分布同理**——须 connector-capability 驱动，不能 `instanceof Iceberg*`。

## C3 起步指引（先做这个）
1. **读 `tasks/designs/P6.6-C2-ws-synth-read-design.md`**（C2 SPI 化范式 + GAP-A/GAP-B）+ `P6.6-C1-ws-pin-design.md` + RFC §6.WS-WRITE（**待 recon 复核**）。
2. **C3 = sink 合成列物化 + 分布**（通用 sink，connector-guard，dormant）。RFC 初判：
   - `visitPhysicalConnectorTableSink:630-681`（通用）补：合成列 `setMaterializedColumnName`（`$operation`/`ICEBERG_ROWID_COL`，移植 `visitPhysicalIcebergMergeSink:613-615`）+ 分布（`getRequirePhysicalProperties`/`toDataPartition` 含 `DistributionSpecMerge:3224-3247`，替 :634 硬编码 UNPARTITIONED）。
   - **待 recon 查证**：分布 enforcement 是否 planner 层已 enforce；`MERGE_PARTITIONED` BE 认否；`$operation`/`$row_id` 是否纯 iceberg；**如何 connector-guard 而不 `instanceof Iceberg*`**（用户铁律——大概率须新 SPI capability/钩子，仿 C2 `classifyColumn` / C1 `supportsSystemTableTimeTravel`）。
3. **⚠️ C3↔C2-GAP-B 交叉依赖**：C3 写路径要物化的 `ICEBERG_ROWID_COL` 须先**存在于 schema/plan**（= C2 挖出的 [GAP-B] 隐藏列注入，翻闸后缺失）。C3 设计期须确认 rowid 来源（DML 命令直接加 `UnboundSlot` vs schema 注入），定 GAP-B 是否 C3 的前置阻塞。
4. **C3 完**：更 HANDOFF + commit（每 C 完即更）。**C5 前切忌动 `SPI_READY_TYPES`**。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2 已闭，C3–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。**C1/C2 已 DONE**（详 `tasks/designs/P6.6-C1-ws-pin-design.md` + [D-068]、`P6.6-C2-ws-synth-read-design.md` + [D-069]）。

- **[C1 ✅]** sys 表时间旅行 pin-feed：`PluginDrivenScanNode.pinMvccSnapshot` 加 `resolveSysTableSnapshotPin()` fallback。recon 推翻 D4/D5。零 SPI/零连接器改。5 UT + mutation 绿。
- **[C2 ✅]** 合成列读路径 classifyColumn **SPI 化**：新 `ConnectorColumnCategory` + `ConnectorScanPlanProvider.classifyColumn(name)` default DEFAULT；fe-core `PluginDrivenScanNode.classifyColumn`（GLOBAL_ROWID 留 fe-core + 委派 `classifyColumnByConnector` seam）；iceberg `IcebergScanPlanProvider.classifyColumn` override。**recon 推翻 RFC BE「DCHECK 崩」（已完整处理→零 BE 改）+ D7（paimon 被 `MaterializeProbeVisitor` 精确类白名单挡、不触达 GLOBAL_ROWID→对 paimon 怎样都安全）**。对 live 连接器零行为变更。连接器 4 UT + fe-core 6 UT + mutation 绿，回归 63/0/0 + 67/0/0。
- **[C3 待办]** `visitPhysicalConnectorTableSink` 合成列物化 + 分布（connector-guard，移植 `visitPhysicalIcebergMergeSink:613-615`）。**须遵 fe-core-no-if(iceberg) 铁律**；查 C3↔GAP-B 依赖。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 `IcebergExecuteActionFactory:89` throw）；**唯一深水 = per-group 读端子设计**。`IcebergRewriteDataFilesAction:173,196` `(IcebergExternalTable)` 翻闸即 ClassCastException（**不能纯 defer**）。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory:137 case` / `pluginCatalogTypeToEngine` iceberg / **GSON compat 8 catalog+db+table** / mvcc+partition-stats capability 核 / Show* parity。**C5 是唯一不可逆点**（GSON 身份变→不可降级回读 image）；C5 前须 C1–C4 全绿 + 用户二签。

## 🆕 C2 recon 挖出的两处翻闸前置项（RFC 完全漏，已登记防失）
- **[GAP-A → C5]** 翻闸后 iceberg 表类 `IcebergExternalTable`→`PluginDrivenMvccExternalTable`（**与 paimon 同类**），掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（`:58-63` **精确类**白名单）→ iceberg lazy-top-N（GLOBAL_ROWID）静默失效 + C2 的 GLOBAL_ROWID 分类对 iceberg 死码。修须 **capability/engine 判别（非 class，因与 paimon 同类）**，仿 `:129-133` HMS `getDlaType()`；宜一并去该文件今天的 `import IcebergExternalTable` fe-core iceberg 泄漏。**用户裁登记入 C5**（RFC §6.WS-1 已记）。
- **[GAP-B → 随翻闸追踪]** 隐藏列**注入**：`ICEBERG_ROWID_COL`（show_hidden/DML）+ v3 row-lineage 现由 legacy `IcebergExternalTable.initSchema:297-301` 注入；翻闸后 `PluginDrivenExternalTable.initSchema:172` 仅从连接器 native `getTableSchema`→`parseSchema` 建、**不注入** → 翻闸后这些列不存在（DML 绑定失败 / show_hidden 不暴露 / v3 row-lineage 空），且使 C2 的 ICEBERG_ROWID/row-lineage 分类**无列可分**。须迁注入到连接器 `getTableSchema`（**rowid 条件注入**依赖查询上下文 show_hidden/DML，与连接器无状态 getTableSchema 有张力，需设计）。**用户裁随翻闸追踪**；**C3 设计期须先核 C3↔GAP-B 依赖**。

**[pre-flip 行为偏差中央登记]**：P6.4 = DV-045/046/047；P6.5 = DV-048/049。**C1/C2 无新 DV**（C2 = dormant SPI 化，对 live 连接器零行为变更）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`（=`bdc38b14810` 后续，详 [D-068/T09 修正]）；**P6.4 T07/T08/T09 + P6.5 T01–T08 + P6.6-RFC + C1 + 本 session C2 已 commit 未 push**（确切数用 `git rev-list --count origin/catalog-spi-10-iceberg..HEAD`）。**用户未要求 push**——留用户裁量。工作树 tracked 干净（除既有 untracked scratch，见下 commit 须知）。
- **P6.1–P6.5 = ✅ DONE**。**P6.6 翻闸进行中：C1 ✅ / C2 ✅ DONE → C3 WS-WRITE 待办**。
- iceberg **不在** `SPI_READY_TYPES`（仍走 switch-case `:137 case "iceberg"`，pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 = P6.6 C2 WS-SYNTH-READ：classifyColumn SPI 化（dormant）

- **起步对抗 recon**（独立读码 + 8-agent 对抗 wf `wf_9bf8730b-05b`/655k token：4 area reader + 3 adversarial verify + synthesis，3 verdict 全 confirmed）→ 证伪 RFC §6.WS-SYNTH-READ 的 BE「DCHECK 崩」（iceberg reader 已完整处理 SYNTHESIZED/GENERATED）+ D7（paimon 被优化器层 `MaterializeProbeVisitor` 精确类白名单挡、不触达 GLOBAL_ROWID）→ 挖出 GAP-A/GAP-B → 用户 AskUserQuestion 双裁（[D-069]：GLOBAL_ROWID 留 fe-core / GAP-B 随翻闸追踪）。
- **设计** `tasks/designs/P6.6-C2-ws-synth-read-design.md`。
- **实现（classifyColumn SPI 化，遵「fe-core 不得 if(iceberg)」铁律）**：
  - SPI（`fe-connector-api`）：新 `ConnectorColumnCategory{DEFAULT,SYNTHESIZED,GENERATED}` + `ConnectorScanPlanProvider.classifyColumn(name)` default DEFAULT（仿 `supportsSystemTableTimeTravel`）。
  - fe-core：`PluginDrivenScanNode.classifyColumn` override（`startsWith(GLOBAL_ROWID_COL)`→SYNTHESIZED + 委派 `classifyColumnByConnector` seam）+ 4 import。
  - 连接器：`IcebergScanPlanProvider.classifyColumn` override（ICEBERG_ROWID→SYNTHESIZED / row-lineage→GENERATED / else DEFAULT，本地字面量）。
  - **零 BE 改 / 零 paimon 改 / 对 live 连接器零行为变更**。
- **验证**：连接器 `IcebergScanPlanProviderClassifyColumnTest` 4/0/0、fe-core `PluginDrivenScanNodeClassifyColumnTest` 6/0/0；mutation 逐条实证（连接器 M4/M5 双红；fe-core M1+M2+M3 三红）；回归 `PluginDrivenScanNode*Test` 63/0/0 + `IcebergScanPlanProviderTest` 67/0/0 + import-gate PASS。e2e 留 P6.8（且依赖 GAP-A/B）。
- **文档同步**：design（新）/ decisions-log [D-069] / RFC §6.WS-SYNTH-READ superseded + WS-1 加 GAP-A/B / connectors/iceberg.md 进度日志 / HANDOFF 覆盖。**0 新 DV**。

---

# 🗺️ 代码脚手架（iceberg read/sys，全 DONE/dormant）

- **合成列分类（C2 新增，dormant）**：`ConnectorColumnCategory`〔`fe-connector-api/.../scan/`，中立枚举〕+ `ConnectorScanPlanProvider.classifyColumn(name)`〔default DEFAULT〕+ `PluginDrivenScanNode.classifyColumn`〔fe-core override：GLOBAL_ROWID 前缀→SYNTHESIZED + `classifyColumnByConnector` seam〕+ `IcebergScanPlanProvider.classifyColumn`〔连接器 override：`__DORIS_ICEBERG_ROWID_COL__`/`_row_id`/`_last_updated_sequence_number`〕。BE iceberg reader 已处理（`iceberg_reader.cpp:162-208/444-489`，**勿改**）。
- **sys 时间旅行 pin-feed（C1，dormant）**：`PluginDrivenScanNode.pinMvccSnapshot`〔context 空→`resolveSysTableSnapshotPin()` 委派源表 `loadSnapshot`〕。链路 `BindRelation:467-474`→`PhysicalPlanTranslator:802-805`→guard→pin→`IcebergScanPlanProvider.buildScan:338-342`。
- **连接器**：`IcebergTableHandle`/`IcebergConnectorMetadata`〔sys 分支 + `getTableSchema`/`getColumnHandles`〕/`IcebergScanPlanProvider`〔`planSystemTableScan`/`supportsSystemTableTimeTravel()=true`/`classifyColumn()`〕/`IcebergScanRange`。
- **fe-core**：`PluginDrivenScanNode`〔`checkSysTableScanConstraints` capability-aware / `pinMvccSnapshot` sys fallback / `classifyColumn` SPI 委派〕 + `PluginDrivenExternalTable.getEngine` iceberg case + `PluginDrivenExternalDatabase.buildTableInternal`〔SUPPORTS_MVCC_SNAPSHOT→`PluginDrivenMvccExternalTable`〕。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：`datasource/iceberg/source/IcebergScanNode`〔`classifyColumn:907-919` C2 移植源 / `createTableScan:569` sys TT〕 + `IcebergExternalTable.initSchema:297-301`〔GAP-B 隐藏列注入源〕 + `IcebergUtils.isIcebergRowLineageColumn:1756`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B` 即可。**fe-core -am 单类/多类 ~2.5min**。后台 task 通知的 exit code 是 echo 的非 maven 的，读 surefire XML。**注**：fe-core 构建尾常有 antlr `->`/`super::` "mismatched input" 行 = parser-gen 噪音（每次都有、exit 0、测全绿），非本改动。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `catalog|common|datasource|qe|analysis|nereids|planner`）。**故连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`）+ fe-core contract UT pin 值防漂移（C2 范式）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS` + stub 包私 seam（如 `classifyColumnByConnector`/`sysTableSupportsTimeTravel`），勿设 private final 字段〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow/verifyNoInteractions 测易 trivially-pass → 必变异验真（C2 已逐条做：连接器 M4/M5、fe-core M1+M2+M3）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。本 session = C2 改：
  - SPI：`fe/fe-connector/fe-connector-api/.../scan/ConnectorColumnCategory.java`（新）· `.../scan/ConnectorScanPlanProvider.java`
  - 连接器：`fe/fe-connector/fe-connector-iceberg/.../iceberg/IcebergScanPlanProvider.java` · `.../src/test/.../IcebergScanPlanProviderClassifyColumnTest.java`（新）
  - fe-core：`fe/fe-core/.../datasource/PluginDrivenScanNode.java` · `.../src/test/.../datasource/PluginDrivenScanNodeClassifyColumnTest.java`（新）
  - 文档：`plan-doc/tasks/designs/P6.6-C2-ws-synth-read-design.md`（新）· `plan-doc/tasks/designs/P6.6-flip-rfc.md`〔§6.WS-SYNTH-READ superseded + WS-1 GAP-A/B〕· `plan-doc/decisions-log.md`〔D-069〕· `plan-doc/connectors/iceberg.md`〔进度日志〕· `plan-doc/HANDOFF.md`
- message `[refactor](catalog) P6.6 iceberg: C2 — <subj>` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。P6.7 删 legacy 时尤其。
- **HANDOFF/设计/RFC/audit-spec 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep + 实证）再信文档。**C1 推 D4/D5；C2 推 RFC BE「DCHECK 崩」+ D7（问错方向）+ 挖出 GAP-A/GAP-B**——C3 起步**同样先对抗 recon 核 RFC §6.WS-WRITE** 再 TDD。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)`** —— iceberg 逻辑落连接器经 SPI（capability / 中立 default 方法）。C3 的 sink 合成列/分布须 connector-guard，不能 `instanceof Iceberg*`（大概率须新 SPI 钩子，仿 C2 `classifyColumn`）。
- **大文件用 subagent/workflow 总结**；`decisions-log.md`（>30k token）/巨行编辑前先 grep 定位精确子串再 surgical Edit（Edit 前必先 Read 一次该文件，哪怕只读锚点几行）。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗 + 先 code 独立判断、后交叉核历史结论（C2 即此范式：主 session 亲读 classifyColumn/LazyMaterializeTopN/MaterializeProbeVisitor/BE reader 链路，再用 8-agent wf 交叉核 + 3 adversarial verify）。
- **P6.6 进行中**：C1 ✅ / C2 ✅ DONE。下个 session 进 **C3 WS-WRITE**（先 recon + 核 C3↔GAP-B 依赖）。**大阶段分多 session**，每 C 完即更 HANDOFF + commit。翻闸（C5）是全有或全无不可逆点，C5 前须 C1–C4 全绿 + 用户二签。
