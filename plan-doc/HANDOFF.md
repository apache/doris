# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 翻闸 → 进 C1（WS-PIN）实现**（D1–D7 全签）

**P6.1–P6.5 = ✅ 全 DONE**。**P6.6 RFC 已起草 + D1–D7 全签**（`research/p6.6-flip-recon.md` + `tasks/designs/P6.6-flip-rfc.md`）：D1 分步多 commit、D2 rewrite 接 PluginDriven、D3 不建中立 scan-range SPI、D4 WS-PIN 用时序修非 SPI 重载、D5 sys MvccTable 委派源表、D6 GSON 同翻闸 commit、D7 paimon top-N 留 C2 查证。**起步至此 0 产品码、0 `SPI_READY_TYPES`。**

## P6.6 起步指引（先做这个）
1. **读 `tasks/designs/P6.6-flip-rfc.md`**（编排全集，已 supersede 旧「4 阻塞」框架）+ `research/p6.6-flip-recon.md`（code-grounded recon，含对 HANDOFF/T08-design §5 的 **5 处纠正**）。
2. **D1–D7 已签** → 直接进 **C1 WS-PIN**（地基）。先判 C1 是否要独立子设计 vs 直接 TDD（C1 触 live paimon 共享 seam，须 paimon-parity UT 先行）。
3. **翻闸 = 5 commit-stream 原子合流**（RFC §2，每 C dormant/parity、自带 UT+mutation、最后翻闸）：
   - **C1 WS-PIN**：buildColumnHandles 时序修（pinMvccSnapshot 提前）+ getColumnHandles 读 handle.schemaId（**非**新 SPI 重载）+ `PluginDrivenSysExternalTable implements MvccTable` 委派源表。
   - **C2 WS-SYNTH-READ**：`PluginDrivenScanNode.classifyColumn` SYNTHESIZED override（移植 `IcebergScanNode:907-919`，connector-guard）+ BE `iceberg_reader.cpp` 合成列 `add_not_exist_children`。
   - **C3 WS-WRITE**：`visitPhysicalConnectorTableSink` 合成列物化 + 分布（connector-guard）。
   - **C4 WS-REWRITE**：rewrite_data_files 接 PluginDriven——连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册（去连接器 `IcebergExecuteActionFactory:89` throw）；**唯一深水 = per-group 读端子设计**。
   - **C5 FLIP（原子、不可逆）**：`SPI_READY_TYPES`+iceberg / 删 `case:137` / `pluginCatalogTypeToEngine` iceberg / **GSON compat 8 catalog+db+table**（镜像 paimon）/ mvcc+partition-stats capability 核 / Show* parity。
4. **翻闸后**：P6.7 删 legacy（**~78** 处 `instanceof Iceberg*`/43 文件——非旧文「~49」+ `action/`+`rewrite/`+`IcebergScanNode`/`IcebergTableSink`/`IcebergTransaction`）→ P6.8 docker（sys time-travel e2e / 合成列 top-N / rewrite commit / 7-flavor / image replay / DV-048/049）。
5. **⚠️ C5 前切忌动 `SPI_READY_TYPES`**（现 = {jdbc,es,trino-connector,max_compute,paimon}）。C1–C4 全程 iceberg dormant、随时可停（零行为变更）；**C5 是唯一不可逆点**（GSON 身份变 → 不可降级回读 image）。

---

# 🔴🔴 开放问题 — P6.6 翻闸（详见 `tasks/designs/P6.6-flip-rfc.md`，本节仅留 recon 纠正 + DV 登记）

> 旧「4 阻塞」框架已被 RFC §2 的 **5 commit-stream（C1 WS-PIN / C2 WS-SYNTH-READ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）** supersede。recon（`research/p6.6-flip-recon.md`）对 HANDOFF/T08-design §5 的 **5 处纠正**（驱动 RFC）：

- **[DV-038 纠正]**「通用 `classifyColumn` 把 GLOBAL_ROWID 归 REGULAR」**误**——legacy `IcebergScanNode:907-919` 分类正确；真问题 = 通用 `PluginDrivenScanNode` **无** classifyColumn override，翻闸后 iceberg 走通用路丢 SYNTHESIZED + BE `iceberg_reader.cpp` field-id DCHECK（→ C2）。
- **[WS-PIN 纠正]**「query→handle pin threading 休眠」深挖：iceberg 连接器 `resolveTimeTravel:509-548`/`applySnapshot:591-603`/`getTableSchema(snapshot):192-218` **已实现**，normal 表 time-travel 经 paimon 模板已通；真缺口 = ①`buildColumnHandles`@688 在 `pinMvccSnapshot`@694 **之前**（时序 bug，修 = 提前 pin，**非**加 SPI 重载）②`PluginDrivenSysExternalTable` 非 MvccTable → `StatementContext.loadSnapshots:987` 跳过（→ C1）。
- **[DV-045 纠正]** rewrite **不能纯 defer**：`IcebergRewriteDataFilesAction:173,196` `(IcebergExternalTable) table` 翻闸即 ClassCastException；`instanceof PhysicalIcebergTableSink` 在 **`RewriteTableCommand:188`**（非 RewriteGroupTask）。用户裁 = 这次接 PluginDriven，中立 scan-range SPI **不需**（rewrite = per-group 内部 INSERT-SELECT）（→ C4）。
- **[GSON 纠正]** **8** catalog 类（非 7）+ db + table，现用 `registerSubtype`，须改 `registerCompatibleSubtype`（→ C5，与翻闸同 commit）。
- **[计数纠正]** `instanceof Iceberg*` 实测 **78**/43 文件（P6.7）。

**[pre-flip 行为偏差中央登记]**：P6.4 = DV-045/046/047；**P6.5 = DV-048〔correctness-bearing：F2 paimon priv loosening〔live〕+ serialized FileScanTask 字节潜伏（T05）〕/ DV-049〔perf-cosmetic：sys split-weight 丢 + 内部 TableType.PLUGIN_EXTERNAL_TABLE〔user-visible 已 parity〕+ position_deletes 文案〕**。**注**：T07 guard 拒 + hms 大小写 = 用户裁现修〔[D-067]〕非 DV；T07-续 test-6「sys 元数据列谓词非 FE 行裁」= BE-applied residual = legacy parity 非 DV（T08 faithfulness wf 经 iceberg 1.10.1 bytecode 复证）。

**⚠️ P6.6 才动 `SPI_READY_TYPES`**（`CatalogFactory:51`）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**P6.4 T07/T08/T09 + P6.5 T01–T08 + 本 session P6.6-RFC（doc-only）已 commit 未 push（13 commit）**。**用户未要求 push**——留用户裁量。工作树 tracked 干净。
- **P6.1–P6.5 = ✅ DONE**。**P6.6 翻闸 = 进行中（RFC 已起草、D1–D7 全签 → 下个 session 进 C1 WS-PIN）**。
- iceberg **不在** `SPI_READY_TYPES`（仍走 switch-case `:137 case "iceberg"`，pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 = P6.6 起步 recon + holistic RFC（**0 产品码、0 SPI_READY_TYPES**）

- **recon wf `wf_2e6efc57-e20`**（10 agent / 741k token：5 area Explore reader + 每 area 对抗 verify cross-connector 破坏论断）+ 2 深挖 recon（WS-5 rewrite dispatch / WS-PIN paimon 模板）+ 主 session 亲验 4 决策性 finding → `research/p6.6-flip-recon.md`。
- **holistic RFC** → `tasks/designs/P6.6-flip-rfc.md`（编排 altitude，仿 P6.3 write RFC）：翻闸 = **5 commit-stream**（C1 WS-PIN / C2 WS-SYNTH-READ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP），每 C dormant-for-iceberg / live-connector parity / 自带 UT+mutation，**最后翻闸**（唯一不可逆点）。
- **决策 D1–D7 全签**（2026-06-25）：D1 分步多 commit / D2 rewrite 接 PluginDriven / D3 不建中立 scan-range SPI / D4 WS-PIN 用时序修非 SPI 重载 / D5 sys MvccTable 委派 / D6 GSON 同翻闸 commit / D7 paimon top-N 留 C2 查证。
- **recon 对旧 HANDOFF/T08-design §5 的 5 处纠正**（见「🔴🔴 开放问题」）：classifyColumn 非现归错（真问题=通用路无 override）/ pin-threading 真缺口=时序+sys-MvccTable（连接器 resolveTimeTravel/applySnapshot 已实现）/ rewrite 不能纯 defer（ClassCast）/ GSON 8 类非 7 / instanceof 78 非 49。
- **验证**：全程 code-grounded（recon wf 对抗 verify + 亲验 `IcebergScanNode:907-919`/`StatementContext:987`/`GsonUtils:375-411`/`IcebergRewriteDataFilesAction:173,196`）。**0 产品码、0 测、0 `SPI_READY_TYPES`**；本 commit = doc-only（recon + RFC + HANDOFF）。**0 新 DV**；D1–D7 入 RFC（非 decisions-log，翻闸后随 C 落地再登记）。

---

# 🗺️ 代码脚手架（iceberg sys-table，全 DONE/dormant）

- **连接器**：`IcebergTableHandle`〔sys 变体 + 保留 pin〕/`IcebergConnectorMetadata`〔`listSupportedSysTables`/`getSysTableHandle`〔LAZY〕/`getTableSchema`·`getColumnHandles` sys 分支/`loadSysTable`/`buildTableDescriptor` hms↔iceberg fork〔`equalsIgnoreCase`〕〕/`IcebergScanPlanProvider`〔`planSystemTableScan`〔`buildScan` 应用 filter→FileScanTask `residual`〕/`resolveSysTable:1132`/`getScanNodeProperties` sys guard/`supportsSystemTableTimeTravel()`=true:196/`SYS_TABLE_DUMMY_PATH="/dummyPath":127〕/`IcebergScanRange`〔`serializedSplit` 早返〕。
- **fe-core**：`PluginDrivenScanNode.checkSysTableScanConstraints`〔capability-aware：`sysTableSupportsTimeTravel()` 委派 `connector.getScanPlanProvider().supportsSystemTableTimeTravel()`；scan-params 块**先于** snapshot 块〕 + `PluginDrivenExternalTable.getEngine:474/getEngineTableTypeName:505` iceberg case + `getSupportedSysTables:419`〔SPI list→bare-key〕 + `TableIf.TableType.toMysqlType:324` PLUGIN→"BASE TABLE" + `findSysTable:413`〔case-SENSITIVE Map.get〕 + `ShowCreateTableCommand.validate` 解包。零改动复用：`PluginDrivenSysExternalTable`〔无 `@SerializedName` 字段；`resolveConnectorTableHandle:85` 透 sys handle〕/`PluginDrivenSysTable`/`SysTableResolver`/`SysTable.getTableNameWithSysTableName`〔不 lower-case suffix〕。
- **新 SPI**：`ConnectorScanPlanProvider.supportsSystemTableTimeTravel()` default false——仅 iceberg override。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：`datasource/iceberg/IcebergSysExternalTable`〔`toThrift:116-131`〕 + `datasource/systable/IcebergSysTable`〔小写键 `SUPPORTED_SYS_TABLES` + `UNSUPPORTED_POSITION_DELETES_TABLE`〕 + `IcebergScanNode`〔`createTableScan:569` sys time-travel honor / `doGetSystemTableSplits`〕。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相）；单类/多类 `test -Dtest=A,B` 即可。**实测**：连接器单类 warm ~13s、全量 ~48s；**fe-core -am 单类/多类 ~2.5min**。后台 task 通知的 exit code 是 echo 的非 maven 的，读 surefire XML / `*_EXIT` 行。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（`org.apache.doris.thrift.*` + SDK `org.apache.iceberg.*` 允许；SPI 在 `connector.api.scan` 合法）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔guard 用 `CALLS_REAL_METHODS` + `guardOnlyNode()`〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow/guard 测易 trivially-pass → 必变异验真。**生产 0 改的 session 用 `git checkout -- <prod>` 复原最干净**（diff 必空）。多 assert-pin 一测用 `assertAll`（否则 `assertEquals` short-circuit 掩盖后续 pin）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕）。本 session = P6.6 起步 **0 产品 + 0 测 + doc-only**：
  - 文档：`plan-doc/research/p6.6-flip-recon.md`（新）· `plan-doc/tasks/designs/P6.6-flip-rfc.md`（新）· `plan-doc/HANDOFF.md`（decisions-log/deviations-log 无新条目；D1–D7 入 RFC）
- message `[refactor](catalog) P6.6 iceberg: RFC — <subj>` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。**注意 P6.4-T07..T09 + P6.5-T01..T08 + 本 RFC 已 commit 未 push（13 commit）**（rebase/force 须谨慎）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。P6.7 删 legacy 时尤其。
- **HANDOFF/设计/audit-spec 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep + 实证）再信文档。**P6.5 实证纠 audit test-6 spec 1 处**（「sys 元数据列谓词 FE 行裁」实为 BE-applied residual），写完即 run、红则 root-cause 不硬凑。
- **大文件用 subagent/workflow 总结**；PROGRESS.md 巨行（§一 6191 字符 + §二 board mega-row + §四 dynamics）编辑前先 grep 定位精确子串再 surgical Edit（Edit 前必先 Read 一次）。
- **文档同步五步**：tasks 表 + 实现记录 / PROGRESS〔§一 + §二 board + §四 dynamics〕 / connectors/iceberg.md〔完成度 + 进度日志〕 / decisions-log / deviations-log；HANDOFF 覆盖式。**本 session = RFC-draft 轮**：仅 recon + RFC + HANDOFF（无产品码 → PROGRESS/tasks/connectors 大同步留 C1 落地起）。
- **P6.6 进行中**：RFC `tasks/designs/P6.6-flip-rfc.md` 已起草、**D1–D7 全签**。下个 session 进 **C1 WS-PIN**（先看是否要 C1 子设计/直接 TDD）。**大阶段分多 session**，每 C 完即更 HANDOFF + commit。翻闸（C5）是全有或全无不可逆点，C5 前须 4 stream 全绿 + 用户二签。
