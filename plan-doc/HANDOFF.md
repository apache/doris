# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.5-T07 = parity 审计 + DV 中央登记（残留 3 项）+ 对抗 parity workflow**

**P6.5-T06 = ✅ DONE**（thrift 描述符 hms↔iceberg 分叉 `buildTableDescriptor`〔连接器，覆 base+sys〕 + `getScanNodeProperties` sys 收口〔[D-065]〕 + fe-core engine-name/SHOW-CREATE parity〔[D-066]，用户签字〕，TDD，**4 产品 + 2 测 + 1 新测类**，连接器 **532/0/1** + fe-core engine 测 **14/0/0**，3 mutation-check 红证，**未 push**）。recon workflow `wf_aefdfdd7-57e`〔8-agent〕**纠 HANDOFF 框定 2 处**（见下「给下一个 agent 的 meta」）。新决策 **[D-066]**（用户 AskUserQuestion 2026-06-25 双签字：descriptor 复现 fork + fe-core engine/show-create 现修）。

- **T07 内容**（设计 §10 节奏 = T06 → **T07 parity 审计 + DV 中央登记 + 对抗 parity wf** → T08 收口/汇总设计 + faithfulness 对抗验证 wf + gate 重跑 = **P6.5 DONE**）：
  - **parity-UT 审计 + gap-fill**（镜像 P6.3-T08 / P6.4-T08）：对抗 byte-parity 审计 workflow（多 area finder 覆 T02–T06 sys 路：handle/listSupportedSysTables/getSysTableHandle/getTableSchema/getColumnHandles/scan-split/buildTableDescriptor/getScanNodeProperties + DESCRIBE/SHOW/information_schema parity；每 finding refute-by-default skeptic + completeness critic）→ 补 UT gap。
  - **DV 中央登记**（`deviations-log.md`，镜像 P6.4 DV-045/046/047 三层）= **T06 消解 3 项后的残留**：
    - **[残留①] sys 表内部 `TableIf.TableType` = `PLUGIN_EXTERNAL_TABLE`**（legacy `IcebergSysExternalTable` 报 `ICEBERG_EXTERNAL_TABLE`，`:57`）——但 **user-visible 已 parity**（T06-F1 使 engine name = "iceberg"、getEngineTableTypeName = `ICEBERG_EXTERNAL_TABLE`；T06-C1 使 thrift 描述符 = HIVE/ICEBERG_TABLE）→ 残留**仅内部枚举**，perf-cosmetic 级或 note；T07 裁是否登记。
    - **[残留②] position_deletes 文案**（`listSupportedSysTables` 不含 → 通用 not-found vs legacy bespoke「not supported yet」，Q2 用户签字）。
    - **[残留③] serialized `FileScanTask` 字节潜伏**（T05）——FE deserialize-round-trip UT 已同进程核可消费，跨版本/classloader interop FE 不可及，P6.8 docker e2e 兜底。
  - **⚠️ T06 已消解（**非** DV，勿登记）**：thrift hms↔iceberg 分叉〔现复现〕/ engine name Plugin→iceberg〔现修〕/ SHOW CREATE 解包〔输出已 Env+UserAuth 处理 + authTableName 现修〕。
- **TDD + 测试基建**：复用连接器侧 fail-loud fake（`RecordingIcebergCatalogOps`/`RecordingConnectorContext`/真 `InMemoryCatalog`，**无 Mockito**）；fe-core 侧 Mockito（`TestablePluginCatalog`）。**mutation-check 必验**（dormant 路 + assertFalse guard 测易 trivially-pass，HANDOFF 坑①）。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.5 = T01–T06 done，T07–T08 待）。翻闸前必修下述阻塞（**同需读/写路径共享 fe-core seam 的 holistic 修**）：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。**P6.5 元数据列挂靠**：iceberg **元数据列**（`IcebergMetadataColumn`/`IcebergRowId`，P6.5 用户签字推迟）属此族——挂 nereids `instanceof IcebergExternalTable` + `getFullSchema()` 钩子、不受 `SPI_READY_TYPES` 控制、flip 后表变 `PluginDrivenExternalTable`→钩子失效→DML row-id 注入断，须在此 holistic 修一并长出（无 paimon 模板）。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-045]**（写路径，P6.4-T06 实证）= `rewrite_data_files` **执行半翻闸接线**（R-B，专门写路径 RFC）。① 事务半 + 规划半已建 dormant；②③④ 执行半留 fe-core，用户裁 Option 1 推后。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager`。

**⚠️ P6.5 新增翻闸激活点（T06-F1/F2 fe-core 改）**：fe-core `PluginDrivenExternalTable.getEngine/getEngineTableTypeName` 的 `case "iceberg"`（T06-F1）+ `ShowCreateTableCommand` 的 `PluginDrivenSysExternalTable` 解包（T06-F2）**对 iceberg pre-flip 不激活**（iceberg 表仍是 `IcebergExternalTable`），flip 后才生效——但 **F2 对 live paimon 立即生效**（修其潜伏 priv 过严，严格更宽松同向，破坏风险近零；已实测连接器 + fe-core engine 测无回归，但 `ShowCreateTableCommand.validate()` 无隔离 UT，P6.8 e2e 兜底）。

**[pre-flip 行为偏差中央登记]**：P6.4 = [DV-046]（correctness-bearing）+ [DV-047]（perf-cosmetic 批）。**P6.5 残留（T07 批量，T06 消解 3 项后）**：内部 `TableType.PLUGIN_EXTERNAL_TABLE`〔user-visible 已 parity〕/ position_deletes 文案 / serialized FileScanTask 字节潜伏（T05）。**注**：T02 偏差①（sys handle 保留 pin）+ T03 [D-063]（lazy）+ T04 [D-064]（@snapshot sys 短路）+ T05 [D-065]（scan split 范围）+ **T06 [D-066]（descriptor 复现 fork + engine/show-create 现修）均非** pre-flip 行为偏差——是 parity-保留/更忠实/消除偏差的内部设计选择，**不登记 DV**。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 四路 dormant + sys split 已发射 + sys 描述符 fork 已建；翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**T01–T06 + arg-move 已推**；**P6.4 T07/T08/T09 + P6.5 T01/T02/T03/T04/T05/T06 待 push**（T06 commit 后 `git rev-list --count origin..HEAD`≈9）。**用户未要求 push**——留用户裁量。
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，iceberg 389/0/1 + fe-core 30/0）。**P6.4 = ✅ DONE**（T01–T09，iceberg 494/0/1 + fe-core 14/0）。**P6.5 = 🔵 进行中**（T01 ✅ recon+设计+二签字；T02 ✅ `IcebergTableHandle` sys 变体；T03 ✅ 2 override〔LAZY [D-063]〕；T04 ✅ `getTableSchema`/`getColumnHandles` sys 分支〔[D-064]〕；T05 ✅ scan split 路〔[D-065]〕；**T06 ✅ 描述符 fork + getScanNodeProperties 收口 + fe-core engine/show-create parity〔[D-066]〕，连接器 532/0/1 + fe-core engine 测 14/0/0**；T07–T08 待）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.5-T06（描述符 fork + getScanNodeProperties 收口 + fe-core engine/show-create parity，TDD），待 commit + push

- **改动 = 4 产品 + 2 测 + 1 新测类**：
  - **连接器（dormant）**：`IcebergConnectorMetadata.java`〔C1 `buildTableDescriptor` 覆写 + 4 thrift import〕 + `IcebergScanPlanProvider.java`〔C2 `getScanNodeProperties` sys guard〕 + `IcebergScanPlanProviderTest.java`〔+2 测〕 + 新 `IcebergBuildTableDescriptorTest.java`〔3 测〕。
  - **fe-core（用户签字，flip 后激活；F2 对 paimon 立即生效）**：`PluginDrivenExternalTable.java`〔F1 getEngine/getEngineTableTypeName `case "iceberg"`〕 + `ShowCreateTableCommand.java`〔F2 authTableName 解包 + import〕 + `PluginDrivenExternalTableEngineTest.java`〔+2 测〕。
- **C1 描述符 fork**：`iceberg.catalog.type=="hms"` → `HIVE_TABLE`+`THiveTable` 否则 `ICEBERG_TABLE`+`TIcebergTable`（镜像 legacy `IcebergSysExternalTable.toThrift:116-131`，6-arg 描述符 + 空 props，null-safe）。SPI 签名无 handle → 单覆写覆 base+sys（仿 paimon）；**BE 无感**（recon G：sys JNI 读只看 scan-range，描述符表类型 HIVE/ICEBERG/SCHEMA 皆合法不崩）→ 纯 FE parity + 闭合 base SCHEMA_TABLE 缺口。
- **C2 getScanNodeProperties sys 收口（[D-065]）**：方法顶 `boolean systemTable=iceHandle.isSystemTable()`；`if(!systemTable)` 包 path_partition_keys + schema_evolution dict 两块。修 unpinned-sys 潜伏崩溃（`encodeSchemaEvolutionProp`→`buildCurrentSchema:194` 抛「requested column not found」，meta 列不在 base schema）+ pinned-sys 静默错 dict。保 jni + location 凭据（BE 读元数据文件需）。
- **F1/F2 fe-core parity**：F1 engine "iceberg"〔paimon 安全，仅 iceberg-typed catalog 命中〕；F2 SHOW CREATE authTableName 解包 `PluginDrivenSysExternalTable`→source〔输出已 Env.getDdlStmt:4915 + UserAuth:60 解包；**F2 含 live paimon 潜伏 priv 修**；⚠️ 无隔离 UT——`validate()` 全局单例耦合〕。
- **[D-066] 用户双签字（AskUserQuestion 2026-06-25）**：Q1 = 复现 legacy fork（连接器，覆 base+sys；否决「单一类型」「暂不做+DV」）；Q2 = fe-core engine/SHOW-CREATE 现修（非 DV 推迟）。
- **mutation-check（Rule 9/12，2 run，`cp` 复原 diff IDENTICAL）**：A fork 判别 `TYPE_HMS`→`TYPE_REST` swap → hms/rest 双红；B dict guard `if(true)` → unpinned 抛 + pinned dict-present 红；C ppk guard `if(true)`〔**保** dict guard〕 → unpinned 达 `assertFalse(ppk)` 红〔治 trivially-pass 坑〕。
- **验证（重跑 surefire 实证，Rule 12）**：连接器 **532/0/1**〔= 527 + 5；`IcebergBuildTableDescriptorTest` 3/0/0、`IcebergScanPlanProviderTest` 63/0/0〕；fe-core `PluginDrivenExternalTableEngineTest` **14/0/0**；checkstyle 0〔validate BUILD SUCCESS〕；import-gate exit 0〔`org.apache.doris.thrift.*` 允许〕；`CatalogFactory:51` 未改 → iceberg 仍**不在** `SPI_READY_TYPES`。**新 1 D（[D-066]）；无新 DV**。**live-e2e 未跑**（dormant + flip 后激活，P6.8 兜底）。
- **文档同步五步**：task 表（P6.5 phase 行 + T06 行 ✅ + T06 实现记录 + 下一步 T07）/ PROGRESS（§一 P6 行 + §二 board iceberg 行〔UT 527→532 双处〕 + §四 dynamics bullet）/ connectors/iceberg.md（完成度 ~90→~92% + 进度日志 P6.5-T06）/ decisions-log（**新 [D-066]**）/ deviations-log（**无新 DV**——T06 消解 3 项、残留延 T07）；HANDOFF 覆盖式。

---

# 🗺️ 代码脚手架（iceberg）

- **P6.5 sys-table 进度**：连接器增量全 dormant + fe-core 2 块 flip 后激活——
  - `IcebergTableHandle`（**✅ T02**）：sys 变体 = `sysTableName` + `forSystemTable(db,table,sys,snapshotId,ref,schemaId)`〔保留 pin〕+ `isSystemTable()`/`getSysTableName()`。
  - `IcebergConnectorMetadata`（**✅ T03+T04+T06**）：T03 = `listSupportedSysTables` + `getSysTableHandle`〔LAZY〕；T04 = sys `getTableSchema`/`getColumnHandles` + `loadSysTable`；**T06 = `buildTableDescriptor`〔hms↔iceberg fork，覆 base+sys〕**。
  - `IcebergScanPlanProvider`/`IcebergScanRange`（**✅ T05+T06**）：T05 = sys split 路；**T06 = `getScanNodeProperties` sys guard〔:633 顶 `if(!systemTable)` 包 path_partition_keys :644 + schema_evolution dict :679-687〕**。
  - **fe-core（T06 用户签字 2 改）**：`PluginDrivenExternalTable.getEngine:462`/`getEngineTableTypeName:492`〔加 `case "iceberg"`〕 + `ShowCreateTableCommand.validate:116`〔authTableName 解包 `PluginDrivenSysExternalTable`〕。其余 fe-core（`PluginDrivenSysExternalTable`/`PluginDrivenSysTable`/`SysTableResolver`/`PluginDrivenScanNode`/`Env.getDdlStmt:4915`/`UserAuthentication:60`）**零改动复用**（Env/UserAuth 已解包 PluginDrivenSysExternalTable）。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/IcebergSysExternalTable`(177)〔`toThrift` hms/iceberg 分叉 `:116-131` + 类型 `ICEBERG_EXTERNAL_TABLE` `:57`〕 + `datasource/systable/IcebergSysTable`(82) + `IcebergScanNode` sys 路（`doGetSystemTableSplits:974` / `setIcebergParams:285-295` / `getFileFormatType:1090`）+ `IcebergExternalTable.toThrift:116-132`〔base 表同 fork〕 + BE `IcebergSysTableJniScanner` + `iceberg_sys_table_jni_reader.cpp:37-42`。**现 iceberg sys-table 仍走 legacy**（连接器路 + fe-core engine/show-create 改对 iceberg dormant，直到 P6.6）。
- **元数据列（P6.5 推迟，DV-041 同族）**：`datasource/iceberg/{IcebergMetadataColumn,IcebergRowId}` + DML 消费方（挂 nereids `instanceof IcebergExternalTable` 钩子）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→假错**）；offline 加 `-o`；连接器 = `fe-connector-iceberg`、SPI = `fe-connector-api`、dispatch = `fe-core`。checkstyle 在 `validate` phase 跑（先于 compile/test）；验证读 surefire **XML**（聚合用 python ET）。**全量验证前 `rm -f target/surefire-reports/TEST-*.xml`** + **`-Dmaven.build.cache.enabled=false`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）；单类/单方法 `test` 即可。**T06 实测**：连接器 -am 单类 warm ~20s、全量 ~20s；**fe-core -am 单类 ~3min**（编译 fe-core main+test）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`；**`org.apache.doris.thrift.*` 显式允许**——T06 `THiveTable`/`TIcebergTable`/`TTableDescriptor`/`TTableType` OK；SDK `org.apache.iceberg.*` 允许）。测试：连接器侧**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`；`createMetadataTableInstance` 需真 `BaseTable`，`FakeIcebergTable` 不够）；fe-core 侧 Mockito（`TestablePluginCatalog(type)` 设 `getType()`）。live-e2e CI-gated（docker），勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + **assertFalse(containsKey) guard 测**（T06 C2 两测）易 trivially-pass → 必 mutation-check 验真（T06 实证：ppk guard 须**单独**变异〔保 dict guard〕才能让 unpinned 测达到 ppk 断言而非先抛）。**坑②**：变异行超 120 列 → checkstyle 先挂；**坑③**：iceberg `for(X ignored:it)` 触 `UnusedLocalVariable`。**`cp` 复原务必用绝对路径**（相对路径在错 cwd 静默失败）；复原后文件读-状态被重置 → Edit 前须重 Read。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·`fe/IcebergScanPlanProvider.java`〔仓根游离副本，勿混淆〕）。本 session 动 **4 产品 + 2 测 + 1 新测**〔`fe/fe-connector/fe-connector-iceberg/.../{IcebergConnectorMetadata,IcebergScanPlanProvider}.java` + `.../test/.../{IcebergBuildTableDescriptorTest,IcebergScanPlanProviderTest}.java` + `fe/fe-core/.../{PluginDrivenExternalTable,nereids/.../ShowCreateTableCommand}.java` + `fe/fe-core/.../test/.../PluginDrivenExternalTableEngineTest.java`〕 + **5 文档**〔tasks/P6-iceberg-migration.md · PROGRESS.md · connectors/iceberg.md · decisions-log.md · HANDOFF.md〕。
- message `[refactor](catalog) P6.5 iceberg: T06 — <subj>` + 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。**注意 T01–T06 + arg-move 已推 origin**（rebase/force-push 须谨慎；P6.4 T07/T08/T09 + P6.5 T01–T06 一并待 push）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + 实证）再信文档。**T06 实证纠 HANDOFF 框定 2 处**：① HANDOFF 称 T06 改「连接器 `buildTableDescriptor`」框定半误——`buildTableDescriptor` **是连接器级 SPI 钩子**（`ConnectorTableOps:187` 默认 null → fe-core `PluginDrivenExternalTable.toThrift:511` 回退 SCHEMA_TABLE），iceberg 连接器原**无**覆写（base+sys 都退化 SCHEMA_TABLE，P6.1/P6.2 遗留 base 缺口）；T06 加覆写**正确**，但「fe-core 零改动」边界被 fe-core engine/show-create 修打破（用户签字）。② recon agent F 初判「SHOW CREATE 输出渲染 sys 表 DDL」**错**——`Env.getDdlStmt:4915` + `UserAuthentication:60` **已**解包 `PluginDrivenSysExternalTable`→source（清 room 交叉核纠正）；唯 `ShowCreateTableCommand.validate():116` authTableName 残留。**教训**：recon agent 结论须主 session 独立读码交叉核（尤「未处理/缺失」类否定断言）。
- **大文件用 subagent/workflow 总结**（playbook §3.1）。PROGRESS.md 巨行（§一 P6 行 + §二 board 行〔UT 计数双处易遗漏〕），编辑前用 Read 取精确行再 surgical Edit（Edit 前必先 Read 该文件；`cp` 复原后亦须重 Read）。
- **文档同步五步**（playbook §5.1）：`tasks/P6-iceberg-migration.md` 状态 + 实现记录 + `PROGRESS.md`〔§一 + §二 board〔**UT 计数双处**〕 + §四 dynamics〕 + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF 覆盖式。
- **T07 起步**：先跑对抗 byte-parity 审计 wf（覆 T02–T06 sys 路 + DESCRIBE/SHOW/info_schema parity）→ gap-fill UT；DV 中央登记**仅残留 3 项**（内部 TableType / position_deletes 文案 / serialized 字节潜伏）——**勿**把 T06 已消解的 thrift 分叉/engine name/SHOW CREATE 登记为 DV。决策 A/B/[D-063]/[D-064]/[D-065]/[D-066] 已定，**无需再问**。
- **faithfulness 对抗 workflow 范式**：收口/汇总设计（T08）若引行号/commit/UT 计数必跑此 wf；**UT 计数 claim 必重跑 surefire 实证**（`@Test` 计数不能证绿，Rule 12）。**F2 `ShowCreateTableCommand` 无隔离 UT**（全局单例耦合）——勿 claim 已测，P6.8 e2e 兜底。
