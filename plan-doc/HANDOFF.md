# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C4 步骤 R5（连接器 transaction 中立 SPI gap：registerRewriteSourceFiles + post-commit 统计提升，dormant）**

**本 session = C4 R4（sink-bind 中立化 + GATHER override）= R4a ✅ + R4b ✅**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core+commit-bridge 全闭 ✅ → C4 进行中（R1/R2/R3/R4 ✅，R5–R8 待）**。翻闸 = 5 commit-stream（C1/C2/C3 ✅ / C4 进行中 / C5 FLIP 不可逆待）。iceberg **仍不在** `SPI_READY_TYPES`。

## 📖 起步必读（动 R5 前）
1. **`plan-doc/tasks/designs/P6.6-C4-ws-rewrite-design.md`** §5（transaction 解）+ §7 R5 + §8 OPEN（`getFilesToAddCount`/统计 accessor 现可见性）。
2. 本 HANDOFF「C4 关键认知」+「R5 起步」。**动码前 re-grep 锚点**（设计行号可能漂移）。

## ✅ 本 session 完成（C4 R4，详设计 §5）
> 起步对抗 recon `wf_93bb76a7-6ec`（5 reader〔unbound-sink / bindsink-rowlineage / rewritecmd-executor / gather-override / planwrite〕+ synthesis）。synthesis 揪出**铁律陷阱**：finding 初稿要在通用连接器绑定路径塞 `IcebergUtils.isIcebergRowLineageColumn` = 违反铁律且无 live caller。code-first 独核交叉确认。
- **用户裁（2026-06-27 signed）= 去掉 row-lineage 绑定分支**：通用连接器绑定路径 `selectConnectorSinkBindColumns` 用 `getBaseSchema(true)`（**已含隐藏列**），rewrite 不丢 row-lineage（`ExternalTable.getBaseSchema(false)` 才过滤可见列）。设计「否则丢 row-lineage」前提是按 iceberg 专用路径（默认可见列）推断，对通用路径不成立。⇒ R4 只做开关传递 + GATHER override，**不**加 row-lineage 臂。
- **R4a impl（commit `a3d7210e892`，additive/dormant）**：中立 `rewrite` 标记一路穿连接器 sink 链。`UnboundConnectorTableSink`+rewrite 字段（10 参规范构造，保留 5/9 参重载默认 false，3 个 `UnboundTableSinkCreator` 调用方零改动）；`LogicalConnectorTableSink`+rewrite（构造参+5 with*+纳入 equals/hashCode/toString 身份，memo 不折叠 rewrite/非 rewrite）；`BindSink.bindConnectorTableSink:878` 传 `sink.isRewrite()`（**无** row-lineage 改动）；`PhysicalConnectorTableSink`+`isRewrite`（两构造+4 with*+`getRequirePhysicalProperties` 顶部 `if(isRewrite) return GATHER` 短路，赢过分区 shuffle/并行写臂；中立字段携带，**不**读 ConnectContext、**不** instanceof Iceberg，区别于 `PhysicalIcebergTableSink` 的 StatementContext 旧机制 `useGatherForIcebergRewrite`）；实现规则 `LogicalConnectorTableSinkToPhysicalConnectorTableSink:36` 透传。验证 clean build：`PhysicalConnectorTableSinkTest` 7/0（新增 `rewriteModeGathersEvenOnPartitionedTable`：分区表+rewrite→仍 GATHER）+ `BindConnectorSinkStaticPartitionTest` 5/0（live bind 路未回归）；GATHER override 1-mutation KILLED。
- **R4b impl（commit `12fe50ee88e`，additive/dormant）**：写入半。新 `ConnectorRewriteExecutor`（fe-core，仿 `IcebergRewriteExecutor` extends `BaseExternalTableInsertExecutor`，no-op beforeExec/doBeforeCommit〔事务由 rewrite 协调方外部持有〕，`transactionType` 返回中立 `UNKNOWN` 非 ICEBERG）；`RewriteTableCommand.selectInsertExecutorFactory:188` 加中立 `else if (physicalSink instanceof PhysicalConnectorTableSink)→ConnectorRewriteExecutor`（按 sink 类型路由不加 instanceof Iceberg；rewrite 标记走 sink 不走 InsertCommandContext 故无 setRewriting；throw 文案改 iceberg+connector）；连接器 `IcebergWritePlanProvider.planWrite` 加 `case REWRITE`+`buildRewriteSink`（复用 buildSink，仅两处 delta，byte-identical 于 legacy `planner.IcebergTableSink.bindDataSink` isRewriting 分支：`write_type=REWRITE` + fv≥3 追加 row-lineage schema；fail-loud：rewrite 与 overwrite 互斥→抛 `DorisConnectorException`）。验证：`IcebergWritePlanProviderTest` 36/0（3 新 REWRITE 用例：fv2 write_type+无 row-lineage / fv3 含 `_row_id`·`_last_updated_sequence_number` / overwrite 拒绝）；3-mutation KILLED；checkstyle 连接器+fe-core 通过。B1/B2（执行器+dispatch）= dormant fe-core，test-compile BUILD SUCCESS。

## 🔑 C4 关键认知（动码前必懂）
- **翻闸后 fe-core `IcebergRewriteDataFilesAction` 子树整条不可达**（`ExecuteActionFactory:61` instanceof PluginDriven **先**分流到 `ConnectorExecuteAction`）。该子树（action/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor`）= legacy-exempt，留 pre-flip 活路径，P6.7 删。
- **真 fix 落点** = `ConnectorExecuteAction` 按 `executionMode` 分派 → **新 fe-core 分布式 rewrite driver**（R6 建）。连接器 `IcebergExecuteActionFactory:88` 对 rewrite **保持 throw**（rewrite 不走 `execute` 单调用路；走 plan+scope+commit 中立 SPI）。
- **THE CRUX**：每组 INSERT-SELECT 须只扫自己 bin-pack 那批文件。Option B 解（R2 已建）= 连接器规划器出 N 组中立 path-set（String）→ handle 变体 `withRewriteFileScope` 穿边界 → `planScanInternal:332` 过滤。
- **GATHER 信号双轨（R4 后）**：pre-flip iceberg 走 `StatementContext.useGatherForIcebergRewrite`（`RewriteGroupTask:263` set，`PhysicalIcebergTableSink:120` 读）；post-flip 连接器走 **sink 字段** `PhysicalConnectorTableSink.isRewrite`（由 R6 driver 建 `UnboundConnectorTableSink(rewrite=true)` 起源，经 bind→logical→physical 链携带）。两轨独立，互不污染。

## 🚦 R5 起步（连接器 transaction 中立 SPI gap，dormant）= **连接器模块（build 快），先 recon `IcebergConnectorTransaction`**
> R5 = 补连接器 transaction 的中立 SPI 缺口，使翻闸后 fe-core 分布式 driver（R6）能经中立接口注册 rewrite 源文件 + 读 post-commit 统计。详设计 §5「transaction 解」。

R5 工作集（动码前逐一 re-grep 锚点防漂移）：
1. **`IcebergConnectorTransaction.updateRewriteFiles(List<DataFile>)`**（设计称 :286，**re-grep 确认行号+可见性**）：现 **package-visible 且收 iceberg `DataFile`**（注释自述「rewrite coordinator 在连接器，fe-core 不能 traffic iceberg DataFile」）→ post-flip fe-core driver **不能**调它。
2. **新中立 SPI** `ConnectorTransaction.registerRewriteSourceFiles(Set<String> rawPaths)`（连接器内部把 rawPaths 解析回 `DataFile`；路径基准对齐 [INV-M1] RAW `dataFile.path().toString()`）。
3. **post-commit 统计 accessor 提升进 SPI**：`getFilesToAddCount` 等。**[INV-stats-order]**：连接器 `getFilesToAddCount` 仅 `commit()` 后有效（legacy 是 `finishRewrite` 后 pre-commit 读）→ 统计读须移到 post-commit。**re-grep `getFilesToAddCount`/统计 accessor 现可见性 + 调用时序**。
4. 中立 SPI 放置：`fe-connector-api/.../handle/ConnectorTransaction`（与既有 verb 同接口）。连接器 `IcebergConnectorTransaction` 实现。
- 连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）。mutation 验真。**dormant**：无 live caller 直到 R6 driver。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（含 C4 dead 子树：`IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink`）保留 iceberg 引用合法。**通用 fe-core 类**（C4 新增：`ConnectorRewriteExecutor`/`UnboundConnectorTableSink`/`LogicalConnectorTableSink`/`PhysicalConnectorTableSink` isRewrite + `RewriteTableCommand` 按 `PhysicalConnectorTableSink` 中立键路由）须全经中立 SPI，**不**加 instanceof Iceberg。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 进行中，C5 待）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 进行中** / C5 FLIP 待）。

- **[C4 进行中 = 当前]** rewrite_data_files 翻闸就绪（Option B 全对等）。**R1/R2/R3/R4 ✅（executionMode SPI / scan path-set 作用域 / 连接器 planRewrite SPI / sink-bind 中立化+GATHER override）→ R5（transaction 中立 SPI gap：registerRewriteSourceFiles + post-commit 统计提升）→ R6（fe-core 分布式 driver）→ R7（WHERE lowering）→ R8（flip rehearsal，flip-gated）**。详设计 §7。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]。**C4 R1–R4 无新 DV**（dormant，行为零变更）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞，勿在 C4 增量做）
- **[FU-connector-bind-visibility]** 通用连接器绑定 `selectConnectorSinkBindColumns` 用 `getBaseSchema(true)`（含隐藏列）→ 翻闸后**普通**（非 rewrite）连接器 INSERT 也会把隐藏 row-lineage 列纳入 bindColumns（iceberg 专用路径默认只取可见列）。R4 范围外（R4 只管 rewrite）。翻闸前若需对齐普通插入语义，须引入**中立**「是否 row-lineage/隐藏写入列」表能力（设计 D1 选 A：`PluginDrivenExternalTable.isRowLineageColumn(Column)` 默认 false，iceberg 插件表 override），**禁** `IcebergUtils.isIcebergRowLineageColumn`（铁律）。
- **[FU-broker-write]** 连接器三 write builder 均未填 `setBrokerAddresses`；翻闸前若需 broker 写盘三 builder 一并补。
- **[FU-flip-e2e]** commit-bridge + C4 全程 pre-flip UT 锁，但真翻闸端到端（旧删不复活 / operation·row_id BE 解析 / OCC / rewrite 每组只扫自己文件 / GATHER 输出文件数）**未跑**（CI-gated/flip-gated，勿谎称）。
- **[FU-getRowIdColumn]** `IcebergMergeCommand.getRowIdColumn(562)` 仍 `IcebergExternalTable`；翻闸/P6.7 核是否 dead。
- **[FU-step1-nullconn / order/remap/dualdelete/...]** 见 git log 历史 HANDOFF。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）。**连接器模块**（`fe-connector-api`/`-iceberg`）build 快（无 fe-core），可前台或后台；**fe-core -am 单类首次 ~2-7min**（本 session 实测 fe-core 编译 ~1:49–2:14）→`run_in_background:true` 再读 surefire XML。验证读 surefire **XML**（python ET）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **⚠️ checkstyle 全量 build 跑（除非 `-Dcheckstyle.skip=true`）**：import 须同组无空行 + 组内字母序（ASCII 序，大写先于小写）。本 session 加 import 用单独 `mvn checkstyle:check -pl :<mod>` 快验通过。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core **Mockito**（`mockito-inline`+`mockStatic`；`Deencapsulation` 读写私有 final 字段——本 session R4a 测试用 `Deencapsulation.setField(sink,"isRewrite",true)` 注入物理 sink final 字段）。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路必变异验真。范式 `scratchpad/mutate_r4a.py`/`mutate_r4b.py`：cp 备份→「行为禁用形」`&& false`/`if(false)`/翻枚举值（非删引用，避 UnusedImport 假阴）→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire fail+err>0（KILLED）→restore + `os.utime(f,None)`（避 stale .class）。**⚠️ exact-string mutate 锚点须唯一**；**⚠️python3.6 无 `capture_output`/`text=`**→`stdout=subprocess.PIPE`。**⚠️ commit 前必核已 restore（无 `&& false`/无 `.bak`）**。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔真文件在 `fe/fe-connector/...`，勿提交〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **R1=`1bddf3426d6` / R2=`0a0d5b8de83` / R3=`a7c2732d984` / R4a=`a3d7210e892` / R4b=`12fe50ee88e`**；HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 R1/R2/R3/R4）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core+commit-bridge 全闭 ✅ → C4 R1/R2/R3/R4 ✅ / R5–R8 待 → C5 翻闸**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 起 ~86G free）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。多次增量 build 后 .class 可能 stale→误判；全量验证用 clean `package` 或重新 `-am test`。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式/可达性可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **铁律落地优先于照搬设计 finding**：R4 recon 中 reader 初稿要在通用绑定路径塞 iceberg 专用 row-lineage 判断 = 违反铁律；synthesis/code-first 独核推翻，改为「中立开关 + 既有 getBaseSchema(true) 已含隐藏列」。参 finding 前先核铁律 + 现有 Doris 行为契约（Rule 7 surface conflict）。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（R4 recon `wf_93bb76a7-6ec` 5-reader+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **C4 逐子步**：R5→R8，每步 additive/dormant + green + mutation + commit + HANDOFF。**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**。本 session 完成 C4 R4（R4a+R4b），在 R4 收官干净节点交接 R5。
