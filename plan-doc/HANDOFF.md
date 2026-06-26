# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C4（rewrite_data_files 执行半接 PluginDriven）= 翻闸前最后一道大活**

**本 session（session 13）= S5d（translator dual-mode）实现 → commit-bridge 全闭 ✅**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ → commit-bridge：recon ✅ + S1 ✅ + S2/S3 ✅（option D）+ S4 part1=Fix B ✅ + S4 part2=supply ✅ + S5a ✅ + S5b ✅ + S5b2 ✅ + S5c ✅ + S5d ✅ → 全闭**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C4 待办（最重）→ C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（S5d translator dual-mode，详 design §11.7.9；1 impl commit）
> 1 main（`PhysicalPlanTranslator.java` +81/-8）+ 1 test（新 `PhysicalPlanTranslatorIcebergRowLevelDmlTest` 4/0）+ design §11.7.9。**1 对抗 review wf（`wf_c863af9a-01f`，3 lens〔parity-ironlaw / be-contract / test-quality〕+ 16 finding 逐个对抗 verify + synthesis）→ parity GO / be-contract GO / test-quality GO-WITH-NITS，0 blocker、0 产品缺陷**（12 产品正确性 claim 全 CONFIRMED；4 test-quality finding：2 should 已修、2 nit 接受）。

- **改动**：两 iceberg row-level DML visitor dual-mode：
  - `visitPhysicalIcebergDeleteSink`：`if (getTargetTable() instanceof PluginDrivenExternalTable)`→新 helper `buildPluginRowLevelDmlSink(sink, WriteOperation.DELETE)`；else→原 `IcebergDeleteSink`（verbatim 入 else）。**DELETE 不需 slot-loop / output_exprs**（BE `viceberg_delete_sink.cpp:269-278` 按 **block 列名** `__DORIS_ICEBERG_ROWID_COL__` 解 row_id）。
  - `visitPhysicalIcebergMergeSink`：slot-loop（operation/row_id materialized-name + outputExprs）+ `setOutputExprs` **上提至 native/plugin 分叉之上**；plugin arm→`buildPluginRowLevelDmlSink(sink, WriteOperation.MERGE)`；else→原 `IcebergMergeSink`（verbatim）。**根因**：BE `viceberg_merge_sink.cpp:309-343` 按 `expr_name` 解 operation/row_id，而 `expr_name`=帧级 `TPlanFragment.output_exprs`（`PlanFragment.toThrift:326`，**与 sink 类型无关、不在 TDataSink 内**）+ slot col_name（`DescriptorToThriftConverter:54` 取 materializedColumnName）→两 arm 都须跑 loop。
  - 新私有 helper `buildPluginRowLevelDmlSink`：镜像 INSERT 模板 `visitPhysicalConnectorTableSink`（catalog→connector→session→metadata→`getTableHandle`→`applyMvccSnapshotPin`〔Fix B〕），`connectorColumns` from `getCols()`，`writeSortInfo=null`〔MERGE 排序由连接器 `buildMergeSink` 出 `sort_fields` thrift field 6，`TIcebergMergeSink` 无 sort_info 字段〕，7-arg `PluginDrivenTableSink` 透传 `WriteOperation`。**INSERT 模板完全不动**。
- **验证**：新测 **4/0**（delete-plugin-arm / merge-plugin-arm〔output_exprs 内容断言 size==3+含 op/rowid/data〕/ merge-loop-lift〔plugin arm materialize op+rowid col_name〕/ mvcc-pin-wiring〔`mockStatic(MvccUtil)`→断言 sink.tableHandle==pinned sentinel，证 Fix B 接线〕）+ pre-flip parity `IcebergDDLAndDMLPlanTest` **14/0** + `ExplainIcebergDeleteCommandTest` **11/0** + INSERT `PhysicalConnectorTableSinkTest` **6/0** + `PhysicalIcebergMergeSinkTest` **9/0**；**8-mutation 全 KILLED**（delete/merge-routing→CCE、delete/merge-op→INSERT、loop-gate、merge-outputexprs、drop-pin、outputexprs-content）；checkstyle PASS（全量 build 未 skip）；`SPI_READY_TYPES` 未改。**0 新 pre-flip DV**。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。**legacy 豁免类**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支合法）：`IcebergRowLevelDmlTransform`/`Iceberg{Delete,Update,Merge}Command`/`Logical/PhysicalIceberg{Delete,Merge}Sink`/`IcebergNereidsUtils`/`Iceberg*Executor`/translator `visitPhysicalIceberg*` visitor/`IcebergTransaction`/`IcebergRewritableDeletePlanner`。**通用 fe-core 类**（`PluginDrivenTableSink`/`PluginDrivenInsertExecutor`/`PluginDrivenExternalTable`/translator 新 helper `buildPluginRowLevelDmlSink`）须全经中立 SPI——S5d helper 仅引中立 `PluginDriven*`/`Connector*`/`WriteOperation`/`MvccUtil`，已合规。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre+C3b-core+commit-bridge 全闭，C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE ✅ / C4 WS-REWRITE 待 / C5 FLIP 待）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。**[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。**[C3a ✅]** INSERT OVERWRITE @branch（[D-070]）。**[C3b-pre ✅]** partition_columns+parallel-write（[D-071]）。
- **[C3b-core ✅ + commit-bridge 全闭]**（[D-072]+[D-073]）：step1-3 ✅ + commit-bridge S1+S2/S3+S4part1+S4part2+S5a/b/b2/c/**d** ✅。详 §10+§11（§11.7 = commit-bridge 全程，§11.7.9 = S5d）。
- **[C4 待办，最重 = 下个 session]** rewrite_data_files（compaction procedure）执行半接 PluginDriven：连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 throw；**`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE**（pre-flip 表类变 PluginDrivenExternalTable）。动码前先对抗 recon（clean-room）+ 亲核真实代码。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / mvcc+partition-stats capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core+commit-bridge S1/S4/S5a/b/b2/c/**d** 无新 DV。**S2/S3=[DV-S2-rederive]**（post-flip 旧删源 commit-time manifest 读 + 升级表 legacy+DV 双删；S4 part2 supply 半边 + S5d translator 改道并入此族）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞，勿在 C4 增量做）
- **[FU-broker-write → 连接器/翻闸]** 连接器三 write builder（INSERT `buildSink`/DELETE `buildDeleteSink`/MERGE `buildMergeSink`）均未填 `setBrokerAddresses`（`fileType==FILE_BROKER`）——连接器既有空缺（非 S5d，INSERT 路也缺），broker-mode 写盘 iceberg 罕用；翻闸前若需支持 broker 写盘，三 builder 一并补。
- **[FU-flip-e2e]** commit-bridge 全程 pre-flip byte-identical + UT 锁，但真翻闸 DELETE/MERGE/UPDATE 端到端（旧删不复活、operation/row_id BE 解析、冲突 OCC、translator→PluginDrivenTableSink 改道）**未跑**（CI-gated，勿谎称）。
- **[FU-step1-nullconn → cutover]** `IcebergRowLevelDmlTransform.pluginConnectorSupportsRowLevelDml`/`checkPluginMode` 的 `getConnector()` unguarded（与 step-3 同名 helper 对齐）。
- **[FU-getRowIdColumn]** `IcebergMergeCommand.getRowIdColumn(562)` 仍 `IcebergExternalTable`（不在合成链，S5b2 未触）；翻闸/P6.7 时核是否 dead。
- **[FU-order/remap/dualdelete/supply-leak/Bnit]** 见 git log 历史 HANDOFF（design §11.7.5–7）。

---

# 🗺️ 代码脚手架（C4 起点；impl 期 re-grep 防漂移；S5d-impl=`81d11c515f1`）

- **C4 锚点（下个 session）**：`IcebergRewriteDataFilesAction:173,196`（翻闸即 CCE）；连接器 `IcebergProcedureOps`（注册去 throw）；rewrite_data_files 执行链 5 seam 泛化（动码前 recon 列全）。
- **commit-bridge 已建（全闭、休眠至 C5）**：S1 `PluginDrivenTableSink`(WriteOperation 透传)/S2-S3 `IcebergConnectorTransaction.collectRewrittenDeleteFiles`→`readExistingFileScopedDeletes`(option D)/S4 part1 Fix B(`PluginDrivenScanNode.applyMvccSnapshotPin`+`IcebergWriteContext.readSnapshotId`)/S4 part2 `IcebergRewritableDeleteStash`(scan stash→write 盖 rewritable_delete_file_sets)/S5a `validateRowLevelDmlMode`/S5c `PluginDrivenInsertExecutor.finalizeRowLevelDmlSink`/**S5d `PhysicalPlanTranslator.buildPluginRowLevelDmlSink`+两 visitor dual-mode**。
- **连接器 write 全链（休眠）**：`IcebergWritePlanProvider.planWrite`〔INSERT/OVERWRITE→`buildSink`、DELETE→`buildDeleteSink`、UPDATE/MERGE→`buildMergeSink`〕字段已齐（含 format_version + rewritable_delete_file_sets，缺 broker_addresses=FU）。BE `viceberg_merge_sink.cpp`〔expr_name 解 op/row_id `:309-343`、sort_fields `:233`〕/`viceberg_delete_sink.cpp`〔block-name 解 row_id `:269-278`〕。
- **fe-core legacy 豁免**：见上「铁律」清单。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）。验证读 surefire **XML**（python ET / grep）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **fe-core -am 单类首次 ~5-7min、热 cache ~2.5min**；**Bash 前台 120s 超时会杀构建** → 长构建用 `run_in_background:true` 再读 surefire XML（别前台等；foreground `sleep` 被 harness 阻断）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仓根 `tools/`）。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`/`RecordingIcebergCatalogOps`+`FakeIcebergTable`）；fe-core **Mockito**（`mockito-inline` 可 mock final + `mockStatic`；`Deencapsulation`〔jmockit〕读/写私有字段）。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 易 trivially-pass → 必变异验真。脚本范式 `scratchpad/mutate_s5d.py`：cp 备份→**「行为禁用形」`&& false` / `if (false) {...}`（非删引用形，避 UnusedImport 假阴）**→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire failures+errors>0（KILLED）→restore。**⚠️ exact-string mutate 锚点须唯一**（`str.replace` 子串匹配：8-space 缩进会撞 12-space 行、相同语句在 INSERT 模板与 helper 各一份→须带后续注释/上下文消歧）。**⚠️python3.6 无 `capture_output`/`text=`** → 用 `stdout=subprocess.DEVNULL`。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔真文件在 `fe/fe-connector/...`，勿提交〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **S5d commit = 1 改 main（`PhysicalPlanTranslator.java`）+ 1 新 test（`PhysicalPlanTranslatorIcebergRowLevelDmlTest.java`）+ design `§11.7.9`**；HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session S5d）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core + commit-bridge 全闭（S1–S5d）✅ → C4 待办 → C5 翻闸**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 全程 ~85-86G free，OK）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。**多次增量 build 后 .class 可能 stale→误判**；全量验证用 clean `package`。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **用户铁律：fe-core 不得新 `instanceof Iceberg*` seam** —— dual-mode 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法；通用类（含 translator 新 helper）须全经中立 SPI。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（本 session review `wf_c863af9a-01f` 3-lens+16-verify+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **commit-bridge 全闭**：S1–S5d ✅。**下一站 C4（rewrite_data_files 执行半接）= 翻闸前最后大活**，再 C5（FLIP 不可逆）。C4 fresh session 全 budget 做；逐子步 green+mutation；**C5 前切忌动 `SPI_READY_TYPES`**。
- **C4 起步**：先读 design §10（C4 设计）+ 本 HANDOFF「C4 待办」+「代码脚手架 C4 锚点」。动码前 re-grep `IcebergRewriteDataFilesAction`/`IcebergProcedureOps` + 列全 5 seam + clean-room 对抗 recon。
- **上下文超 30% 即交接**。本 session = S5d（commit-bridge 收官），在干净节点交接 C4。
