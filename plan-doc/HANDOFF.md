# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C4 步骤 R3（连接器 planRewrite SPI + wire 既有规划器，dormant）**

**本 session（session 14）= C4 起步对抗 recon + 全量设计（用户裁 Option B）+ R1（executionMode SPI）✅ + R2（连接器 scan path-set 作用域）✅**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core+commit-bridge 全闭 ✅ → C4 进行中（R1 ✅ / R2 ✅，R3–R8 待）**。翻闸 = 5 commit-stream（C1/C2/C3 ✅ / C4 进行中 / C5 FLIP 不可逆待）。iceberg **仍不在** `SPI_READY_TYPES`。

## 📖 起步必读（动 R2 前）
1. **`plan-doc/tasks/designs/P6.6-C4-ws-rewrite-design.md`**（C4 全量设计；本 session 新建）——含 10-seam 清单 / THE CRUX 解（Option B 中立 path-set）/ 正确性不变式 / R1–R8 实现顺序 / impl 期首核 OPEN。
2. 本 HANDOFF「C4 关键认知」+「R2 起步」。

## ✅ 本 session 完成（C4 recon+design+R1，详设计 §0–§9）
> 对抗 recon `wf_59515933-9dc`（4 reader〔seams/side-channel/txn-bind/Trino〕+ synthesis + adversarial critic）。critic **以代码证据推翻 synthesis 初稿**（Option A 阈值模型 = BLOCKER：丢失 Doris 已发布的多准则 compaction；defer-WHERE = MAJOR：现存活功能回退），加 3 正确性不变式。
- **用户已裁（2026-06-26 signed）**：① scan 作用域 = **Option B 全行为对等**（中立 path-set，复用已存在连接器 `RewriteDataFilePlanner`）；② 路由 = **executionMode flag**（非硬编码名字）；③ **WHERE 现在就做**（不 defer）。
- **R1 impl（commit `1bddf3426d6`，additive/dormant）**：新中立枚举 `ProcedureExecutionMode{SINGLE_CALL,DISTRIBUTED}`；`ConnectorProcedureOps` 加 `default getExecutionMode(name)=SINGLE_CALL`；`IcebergProcedureOps` override：rewrite_data_files→DISTRIBUTED（大小写不敏感）余 SINGLE_CALL。无 live caller（driver 未建）。验证 `ConnectorProcedureOpsDefaultsTest` 4/0 + `IcebergProcedureOpsTest` 11/0；2-mutation KILLED。
- **R2 impl（commit `0a0d5b8de83`，additive/dormant）**：`IcebergTableHandle` 加不可变 `rewriteFileScope`（`Set<String>` 原始路径，null=全扫）+ `withRewriteFileScope` 复制工厂（仿 `withSnapshot`）+ 纳入 equals/hashCode/toString 身份；`IcebergScanPlanProvider.planScanInternal` 枚举循环按 scope 过滤。**[INV-M1] 已亲核基准**：scope 与过滤两侧都用 RAW `dataFile.path().toString()`（`buildRange:518` originalPath=raw，`.path()`=normalized BE 路径；过滤须用 raw 否则扫错文件 over-read→重复行）。验证 clean build：`IcebergTableHandleTest` 20/0 + `IcebergScanPlanProviderTest` 72/0 + 全 `fe-connector-iceberg` 600/0 + `fe-connector-api` 39/0（无回归）；2-mutation KILLED（scan 过滤禁用 / equals 丢 scope）。

## 🔑 C4 关键认知（recon 推翻旧 HANDOFF 假设，动码前必懂）
- **翻闸后 fe-core `IcebergRewriteDataFilesAction` 子树整条不可达**（`ExecuteActionFactory:61` instanceof PluginDriven **先**分流到 `ConnectorExecuteAction`，:66 iceberg 分支永不取）。⇒ **旧 HANDOFF「修 `IcebergRewriteDataFilesAction:173/196` cast」= 修了白修**（不可达）。该子树（action/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor`）= legacy-exempt，留 pre-flip 活路径，P6.7 删。
- **真 fix 落点** = `ConnectorExecuteAction` 按 `executionMode` 分派 → **新 fe-core 分布式 rewrite driver**（R6 建）。连接器 `IcebergExecuteActionFactory:88` 对 rewrite **保持 throw**（rewrite 不走 `execute` 单调用路；走 plan+scope+commit 中立 SPI）。
- **THE CRUX**：每组 INSERT-SELECT 须只扫自己 bin-pack 那批文件。pre-flip 走 `StatementContext:322` 的 iceberg-typed `List<FileScanTask>` 侧信道→`IcebergScanNode:498` 消费；翻闸后 scan 走 `PluginDrivenScanNode`（**不读**该信道，已实证）→ 不处理则每组扫全表（重复读写/数据错乱）。**Option B 解** = 连接器规划器出 N 组中立 path-set（String）→ 新 handle 变体穿边界 → `planScanInternal:332` 过滤；stash 由 `List<FileScanTask>` 改 `List<String>`。

## 🚦 R3 起步（连接器 planRewrite SPI + wire 既有规划器，dormant）
- 目标：连接器出「N 个 rewrite 组」给 fe-core driver（R6）——每组 = 中立 `Set<String>` 原始数据文件路径（喂 R2 的 `withRewriteFileScope`）+ 每组统计元（total size / delete-file count / data-file count，算结果行用）。wire 既有 `connector/iceberg/rewrite/RewriteDataFilePlanner.planAndOrganizeTasks`（已存在、import-clean、含全部选择/分组/装箱逻辑）+ `RewriteDataGroup`。
- **[INV-M1] 基准已定**：组路径集用 `RewriteDataGroup.getDataFiles()→dataFile.path().toString()`（raw），与 R2 过滤侧同源。
- **动码前须核的 OPEN（R3 子决策）**：
  1. `planRewrite` 落 `ConnectorProcedureOps` 新方法 还是 新 `ConnectorRewriteOps` SPI？（executionMode 已在 ConnectorProcedureOps，加这里最省；但 rewrite-专属，分离更干净——次 session 裁，倾向加 ConnectorProcedureOps 省一个 SPI 面）。
  2. rewrite 10 参数（target-file-size/min-input-files/rewrite-all/max-file-group-size/delete-file-threshold/delete-ratio-threshold/min·max-file-size/output-spec-id）的中立 params DTO + 解析归属：连接器拥 arg 校验（`NamedArguments` fe-core 不可达），故 `planRewrite` 收**原始 properties Map + `ConnectorPredicate` WHERE + handle**，连接器内部解析/校验（复用连接器 arg infra）→ 出组。WHERE = DEC-C4-3 半边（连接器侧 `RewriteDataFilePlanner.Parameters.whereCondition` 已收，只缺 fe-core lowering=R7）。
  3. 返回 DTO 形（neutral）：`List<组{Set<String> paths, long totalSize, int deleteFileCount, int dataFileCount}>` + 是否带结果 schema。re-grep `RewriteDataFilePlanner.planAndOrganizeTasks` 签名 + `RewriteDataGroup` 的 `getDataFiles/getTotalSize/getDeleteFileCount/getTaskCount` 访问器。
- offline UT（连接器无 Mockito，`InMemoryCatalog` + appended DataFile，仿 `IcebergScanPlanProviderTest`/`RewriteDataFilePlannerTest` 若存在）。mutation：参数阈值改变 → 选中文件集变。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（含 C4 dead 子树：`IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单）保留 iceberg 引用合法。**通用 fe-core 类**（C4 新增：rewrite 特例按 `executionMode`/`PhysicalConnectorTableSink` 中立键路由，**不**加 instanceof Iceberg；driver/`ConnectorRewriteExecutor`/`UnboundConnectorTableSink` isRewrite 全中立）须全经中立 SPI。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 进行中，C5 待）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 进行中** / C5 FLIP 待）。

- **[C4 进行中 = 当前]** rewrite_data_files 翻闸就绪（Option B 全对等）。**R1 ✅（executionMode SPI）/ R2 ✅（scan path-set 作用域）→ R3（连接器 planRewrite SPI + wire 既有规划器）→ R4（sink-bind 中立化 + GATHER override）→ R5（transaction 中立 SPI gap：registerRewriteSourceFiles + post-commit 统计提升）→ R6（fe-core 分布式 driver）→ R7（WHERE lowering）→ R8（flip rehearsal，flip-gated）**。详设计 §7。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]。**C4 R1 无新 DV**（dormant）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞，勿在 C4 增量做）
- **[FU-broker-write]** 连接器三 write builder 均未填 `setBrokerAddresses`（broker-mode 写盘 iceberg 罕用）；翻闸前若需 broker 写盘三 builder 一并补。
- **[FU-flip-e2e]** commit-bridge + C4 全程 pre-flip UT 锁，但真翻闸端到端（旧删不复活 / operation·row_id BE 解析 / OCC / rewrite 每组只扫自己文件）**未跑**（CI-gated/flip-gated，勿谎称）。
- **[FU-getRowIdColumn]** `IcebergMergeCommand.getRowIdColumn(562)` 仍 `IcebergExternalTable`（不在合成链）；翻闸/P6.7 核是否 dead。
- **[FU-step1-nullconn / order/remap/dualdelete/...]** 见 git log 历史 HANDOFF。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）。**连接器模块**（`fe-connector-api`/`-iceberg`）build 快（无 fe-core），可前台或后台；**fe-core -am 单类首次 ~5-7min**→`run_in_background:true` 再读 surefire XML（别前台等；foreground `sleep` 被 harness 阻断）。验证读 surefire **XML**（python ET）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **⚠️ checkstyle 全量 build 跑（除非 `-Dcheckstyle.skip=true`）**：import 须同组无空行 + 组内字母序（本 session R1 踩过：新 import 用空行分组→`CustomImportOrderCheck` "Extra separation"）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core **Mockito**（`mockito-inline`+`mockStatic`；`Deencapsulation` 读写私有）。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路必变异验真。范式 `scratchpad/mutate_r1.py`/`mutate_r2.py`：cp 备份→「行为禁用形」`&& false`/`if(false)`（非删引用，避 UnusedImport 假阴）→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire fail+err>0（KILLED）→restore。**⚠️ exact-string mutate 锚点须唯一**；**⚠️python3.6 无 `capture_output`/`text=`**→`stdout=subprocess.PIPE`。**⚠️ mutation 跑时源文件处于变异态**——commit 前必核已 restore（无 `&& false`/无 `.bak`）。
- **⚠️ mutation 后 STALE .class 坑（本 session 踩，R2 full-run 假挂）**：脚本 `shutil.move(.bak)` restore 后 .java mtime **早于** mutation 编出的 .class → maven-compiler staleness 跳过重编 → **变异 .class 残留** → 之后任何 `-am`/全量 build 误用残留 .class 假挂（症状：改了/没改的源却测出变异行为）。**修=验证后续 green 用 `mvn clean test`**（HANDOFF 既有「全量用 clean」），或脚本 restore 后 `os.utime(f, None)`。源 git-clean 不代表 .class 干净。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔真文件在 `fe/fe-connector/...`，勿提交〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **R1 commit = `1bddf3426d6`**（新 `ProcedureExecutionMode` + `ConnectorProcedureOps`/`IcebergProcedureOps` + 2 test + design doc）；**R2 commit = `0a0d5b8de83`**（`IcebergTableHandle`/`IcebergScanPlanProvider` + 2 test）；HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 R1/R2）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core+commit-bridge 全闭 ✅ → C4 R1 ✅ / R2 ✅ / R3–R8 待 → C5 翻闸**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session ~85G free）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。**多次增量 build 后 .class 可能 stale→误判**；全量验证用 clean `package`。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（C4 recon 教训：旧 HANDOFF 的「修 :173/:196」基于错误的可达性假设——dispatch 在 `ExecuteActionFactory:61` 就分流走了）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/可达性可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **Trino-faithful ≠ 总是对**：C4 synthesis 初稿照搬 Trino 阈值模型被 critic 以「Doris 现有 feature 契约更富」推翻。参 Trino 前先核现有 Doris 行为契约（Rule 7 surface conflict 而非 average 向简化）。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（C4 recon `wf_59515933-9dc` 4-reader+synthesis+adversarial-critic）+ 先 code 独立判断、后交叉核历史结论。
- **C4 逐子步**：R2→R8，每步 additive/dormant + green + mutation + commit + HANDOFF。**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**。本 session 完成 C4 起步 recon+design + R1 + R2，在 R2 收官干净节点交接 R3。
