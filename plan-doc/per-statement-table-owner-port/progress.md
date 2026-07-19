# Progress Log —— 每语句表加载归属者 · 移植到其它连接器

> **Append-only**：只追加、不覆盖（覆盖式状态在 HANDOFF/tasklist）。

---

## 2026-07-19 — session 0：建任务空间

- 用户拍板：把 iceberg 的 PERF-07「每语句表加载归属者」整体性改动移植到其它连接器；**本 session 只建跟踪文档空间，实际处理留下一个 session**。
- 读准 iceberg 蓝本（PERF-07 summary）：可复用地基 = 中性 `ConnectorStatementScope`(fe-connector-api) + `ConnectorSession.getStatementScope()` + `ConnectorStatementScopeImpl`/`StatementContext`/`ConnectorSessionImpl` 构造期捕获/`ExecuteCommand` 重置(fe-core)，**已随 PERF-07 落地**（commit `97bdcd6bdbe`）。→ 关键结论：**移植是纯连接器侧工作，无需再改 fe-core**（不再触铁律 A）。连接器侧范式 = `IcebergStatementScope` helper + 四处 `resolveTable*` 共享 + (可选)拆胖句柄 + (可选)下沉跨臂暂存 + (可选)fail-loud。
- 初摸候选（grep）：只有 iceberg 用了该 SPI；`loadTable`/`getTable` 触点 paimon 4 / hive 3 / hudi 1 / maxcompute·es·jdbc·trino 0。→ 候选 = paimon(高)/hive-hms(中-高)/hudi(中)；读-only 四家大概率排除，待复核。
- 落地文件：`README.md`（用途 + 已就位地基 + 连接器侧 5 步模板 + 候选表 + 单连接器立项流程 + 铁律）、`tasklist.md`（PORT-01~04 全待 recon）、`HANDOFF.md`（下一步 = 确认范围 + 逐候选 recon）、`progress.md`（本文件）、`designs/`（空）。
- **未动任何产品代码**。**下一步**：见 HANDOFF —— 下个 session 起步先与用户确认范围/顺序，再对第一个连接器 recon。

## 2026-07-19 — session 1：逐连接器 recon + 架构统一性调研（结论：全部 🔬 + 定高度 L0）

- **逐连接器 recon（recon + 独立对抗复核，双签，多 agent workflow）**：结论 = **没有一个连接器现在值得移植**。
  - paimon → 🔬 **将来候选**：写未迁移（只读）、加载已被 transient 胖句柄 + SDK CachingCatalog 压到≈1；触发点=加行级 UPDATE/DELETE。
  - hive/hms → 🔬 不必做：仅 INSERT/OVERWRITE、跨查询 `CachingHmsClient.tableCache` 已兜（作用域超集）；网关只做 1 次探测加载后委派兄弟（兄弟自带作用域）。
  - hudi → 🔬 不必做：只读；唯一读侧成本=未缓存 metaClient 重建，形状不对/可变/鉴权错配。
  - maxcompute·es·jdbc·trino → 🔬 排除：均无行级 DML；trino 读侧重解析最贵=另立议题占位。
  - **核心洞察**：iceberg 独特在它是唯一迁移了行级写的连接器（行级写才有多臂重复加载 + 跨臂暂存）。
- **架构统一性专项调研（3 agent：placement / onboarding / Trino-altitude）**：
  - **统一接口已存在** = 中性 `getStatementScope()` + `ConnectorStatementScope`（新连接器天生继承）。
  - **Trino 模型不可直接移植**：Trino 靠"引擎自持每事务 metadata 生命周期"，Doris 连接器是共享单例、session 一条语句重建~26 次，唯一 span 宿主=`StatementContext`；现有 SPI 已是最可移植高度。
  - **高度分级 L0/L1/L2/L3**；**paimon-加写=L1 抽共享 helper 的正确触发点**（有 2 个真实用户）；共享 helper 落 `fe-connector-api`（不碰铁律 A）。
- **用户拍板**：先把结论记成**单独文档** → `designs/recon-findings-and-trino-refactor-groundwork.md`；**重构成 Trino 架构（L2/L3）留下个 session 专题讨论**（该文档 §7 已备预备材料）。
- **未动任何产品代码**（纯 plan-doc）。**下一步**：见 HANDOFF —— 下个 session = 讨论"重构成 Trino 架构"的可行性/分期/与铁律 A 的取舍。

## 2026-07-19 — session 2：Trino 式重构专题（用户豁免铁律）→ 目标架构定稿方向 + 红队

- **用户新指令**：抛开一切铁律/约束，可改任何模块，目标=至少和 Trino 一样合理、能更好则更好。
- **workflow ①（`wf_72d1e505-75c`）现状+Trino recon + 对抗验证**：更正旧文档——`ConnectorMetadata` 是**每调用即弃外壳**（`Connector` 才是单例），改每语句不用砸单例；句柄已 fact-carrying（Trino invariant 3 已满足）；`StatementContext` 已是被验证的 span 宿主（off-thread 靠构造期捕获）；外部写=每语句 autocommit（1 StatementContext≈1 外部事务）；Trino 四不变量 + 两弱点（iceberg 软 load-once / 冗余双 memo）源码核实。
- **定稿设计** → `designs/trino-parity-metadata-redesign-design.md`：目标架构=引擎自持每语句 metadata 实例 + 管道层唯一 memo + 下层按身份分片(拆凭证)缓存。
- **用户拍板**：终点 P1–P4（不含 P5）；直上 P1 键石；语句末确定性 close；先红队。
- **workflow ②（`wf_62cc379e-c6e`）4 架构师 × 4 评审对抗红队**：胜出=span 派（3/4 轴第一）；嫁接忠实-Trino 的 `CatalogStatementTransaction` + 缓存派"元数据/凭证拆分" + 最小面派若干硬化 + arch 门禁。**揪出五处公共盲区（两硬伤）**：①close 须走现有 `registerQueryFinishCallback` 而非 StatementContext.close；②HMS sibling 扇出须键含属主连接器 + sibling 路由进 funnel；③EXECUTE reset 先 closeAll；④close 栅栏在 SplitSourceManager 注销后；⑤P4 残留泄漏威胁模型待签字。已并入 §8/§9。
- **未动任何产品代码**（纯 plan-doc）。**下一步**：见 HANDOFF —— 等用户点头后细化 P1 seam-by-seam 实现设计或直接进入 P1 实现。

## 2026-07-19 — session 2（续）：P1 seam-by-seam 实现设计（grounding + 定稿）

- **workflow ③（`wf_7e537094-44f`）只读 grounding**（5 读者：seam 清单 / close 生命周期 / HMS sibling / scope-session SPI / 测试+门禁基建）。核实产出：**66 处真实 seam**（改道表）；close 钩子=`registerQueryFinishCallback`（须移注册点到 scope 创建、closeAll 幂等）；**HMS 网关已 LIVE**（2026-07-17 进 SPI_READY_TYPES，dormant 注释过期，三连接器共享 catalogId→key 加属主 label）；`ConnectorMetadata` 已 Closeable no-op（memoize 安全）；守门/门禁全有模板。
- **定稿** → `designs/P1-implementation-design.md`：§1 SPI/plumbing 签名、§2 66 缝改道、§3 扫描节点存字段、§4 close 接线、§5 HMS sibling funnel、§6 read-vs-write（写侧 7 缝留 P3）、§7 守门、§8 arch 门禁、§9 四个待确认点、§10 commit 切分 C1–C5。
- **未动任何产品代码**（纯 plan-doc）。**下一步**：见 HANDOFF —— 等用户确认 §9 四点后进入 P1 实现（C1→C5）。

## 2026-07-19 — session 3：扩展范围取证 + 用户二次拍板 → 定稿分期

- **用户就 §9 四点拍板，偏离两处原推荐**：写侧要一起做、后台加载点"传句柄到后台线程"、缓存隔离现在就做。
- **workflow（`wf_8b907b93-e9f`，18 agents，recon+对抗+综合）** 三块结论：
  - **读写共用**：可行且忠实 Trino（`CatalogTransaction` 单实例读写共用已核实）；机制=`CatalogStatementTransaction` 共持体 + 写臂 8 处改道；两闸门=按连接器保留起写刷新（hive 留 beginWrite getTable）+ 读写身份指纹相等。
  - **后台传递（用户方向被证伪）**：把实例传后台不安全（共享池 worker 复用 → 已关闭对象被无关查询误用）；纠正=语句派生异步"提交时捕获"+ 7 处跨语句 loader 显式强制 NONE 读穿；**查出并建议修 `fetchRowCount` 在 ANALYZE 线程被错误绑定的真实隐患**。
  - **缓存隔离**：影响面小（仅 iceberg 三投影缓存 + fe-core 表结构缓存）；两轴切分（授权敏感投影按身份分片、含凭证原始表永不跨语句缓存）+ 新 SPI `getIdentityShardKey()`；HMS 异构网关是最尖的洞。
  - **综合**：强烈建议**分期**（硬依赖 + 后台传递被证伪 + 缓存隔离是独立安全工程；一次性全做=不可二分回归面）。
- **用户二次拍板（动码前）**：①**分期推进**；②后台**采纳纠正做法 + 修隐患**；③**确认 list ≠ load** → 缓存隔离=真实越权修复，独立安全 track。
- **定稿** → `designs/expanded-scope-phasing-and-security-decisions.md`：决策表 + 三块取证 + 分期 STEP 1（读取键石，含后台纠正）/ STEP 2（HMS 兄弟）/ STEP 3（写入共用，拆 3a/3b）/ STEP 4（缓存隔离，独立安全 track）+ 对旧文档更正。
- **未动任何产品代码**（纯 plan-doc）。**下一步**：见 HANDOFF —— 等用户对 STEP 1 逐提交方案点头即进入实现（C1→C3 = 键石 + 后台纠正）。

## 2026-07-19 — session 3（续）：落地 STEP 1 的 C1 地基 + C2 关闭接线

- 用户逐提交确认后开写。**首次改动产品代码**（用户已豁免铁律 A）。
- **C1 地基**（commit `5b7312f9d1f`）：`ConnectorStatementScope` 加 `getOrCreateMetadata`/`closeAll` 默认方法；`ConnectorStatementScopeImpl.closeAll` 幂等 close-once + best-effort；新增静态漏斗 `PluginDrivenMetadata.get(session, connector)`（按 catalogId memoize `connector.getMetadata`）。fe-core 单测 memo-once/NONE-each/跨目录隔离/关一次。字节中性（无生产路径接进漏斗）。编译 + checkstyle 0 + 8 测全绿。
- **动手 C2 前的取证**（workflow `wf_9250330b-e81`）：发现 `captureStatementScope` 在**每次 on-thread 会话构建**就急切建 scope；原设计"在 scope 创建处注册关闭回调"**会泄漏**——DDL/`SHOW`/`DESCRIBE`/`EXPLAIN`/前台 `ANALYZE` 走 `Command.run` 建 scope 却永不到 `unregisterQuery`，回调连带 scope 永留无 TTL 注册表。已对抗复核。
- **C2 关闭接线**（commit `12f3e95239b`）：改为**两层关闭**——主关闭 `PluginDrivenScanNode.getSplits` 注册 `scope::closeAll`（对象捕获、跳 NONE、同 read-txn queryId 键，只对走协调器语句触发、pump 静默后）；兜底 `StatementContext.close()` 的 `isReturnResultFromLocal` 守卫 closeAll（直连 `executeQuery` finally + `proxyExecute` 新增 finally 覆盖转发主节点）；`resetConnectorStatementScope` 改 closeAll-before-null；`handleQueryWithRetry` 每重试重置。核实 `releasePlannerResources` 幂等故转发路径补 close 安全。单测 reset-先关/close-本地关/close-异步延后。P1 关闭仍 no-op，行为中性。编译 + checkstyle 0 + 11 测全绿。
- 更正 `P1-implementation-design.md` §4（旧"scope 创建处注册"→两层关闭）；残留风险（取消非硬栅栏、arrow-flight 断连、待确认的非-getSplits 外部查询、**TCCL 自钉扎硬前置**）记入分期定稿 §6。
- **下一步**：见 HANDOFF —— 下个 session 做 C3（读取侧改道 ~55 缝 + 扫描节点存字段 + 7 处后台读穿纠正 + 修 fetchRowCount 隐患），动手前先核行号列清单 + 配对抗复核。用户将在下个 session 开新任务。

## 2026-07-19 — session 4：落地读取侧改道 + 后台读穿纠正（读取键石收官主体）

- **动手前核对关（对抗复核 workflow `wf_6e2967a9-1a2`，10 agents：7 逐文件普查 + 3 对抗验证）**：把改道表逐条对当前代码行号重核。结论落 `designs/C3-read-reroute-verified-checklist.md`：fe-core 连接器 `getMetadata` 工厂缝**共 66 处**，完整切成四类，相加=66、零遗漏/零重叠/零未分类；设计命名行号零漂移（仅扫描节点尾部 +13 行位置漂移）。对抗复核 CONFIRM 写专用点 `resolveWriteTargetHandle` 唯一生产调用方是插入执行器开事务、读路径够不到，必须留后续。
- **两处偏差交用户拍板 + 拍定**：①名字映射两缝（`fromRemoteDatabaseName`/`fromRemoteTableName`）经复核跑在离请求线程的缓存加载路径 → 归入强制 NONE 读穿（读穿 9 处、改道 49 处）；②读穿 loader 走统一入口 + 传 NONE 会话（门禁零例外）。
- **落地（首次改产品代码，未提交前全绿）**：新增 `PluginDrivenExternalCatalog.buildCrossStatementSession()`（镜像 buildConnectorSession + 强制 NONE，保留凭证）；9 处后台 loader 改用它并走统一入口（含 `fetchRowCount` ANALYZE 隐患修复——不必单改统计模块）；49 处读/DDL/命令/多版本改道进统一入口；扫描节点加 `volatile cachedMetadata` + `metadata()` 访问器（静态 create 直调入口）。源码证实「强制 NONE 走统一入口」与裸直调**字节等价**（NONE 每次跑工厂、零留存）。
- **测试适配（workflow `wf_fbb60841-365`，11 agents 并行修各文件）**：改道后这些路径经统一入口调 `session.getStatementScope()`，Mockito mock 默认返 null → 漏斗 NPE；按 C1 约定（测试给 NONE 作用域）逐文件补 `getStatementScope()→NONE` stub + 给测试替身补 `buildCrossStatementSession` override（不弱化任何断言/verify）。新增守门 `ConnectorSessionImplTest.explicitNoneStatementScopeWinsOverLiveContext`（活 ctx 下显式 NONE 胜出，锁 ANALYZE 隐患修复的机制）。
- **验证**：目标测试 247 全绿（先红 130 error+30 fail → 修后 0/0）+ ConnectorSessionImplTest 18 绿 + fe-core checkstyle BUILD SUCCESS 零违规 + fe-core 主编译 BUILD SUCCESS。
- **提交口径**：4 个逻辑子步（助手/读穿/改道/扫描存字段）在 2 个共享文件里交织，且每提交须保持绿（测试适配须随产品改动同提），逐 hunk 拆分需交互式 git（本环境不支持）→ 按 C1/C2 先例=1 个绿代码提交（含产品+测试适配+守门）+ 1 个文档提交，代码提交体内枚举 4 子步。
- **下一步**：见 HANDOFF —— 读取键石剩防漂移门禁（形态=统一入口零例外）；下一大步 = HMS 异构网关兄弟扇出（键含属主 label + 兄弟 getMetadata/起事务进同一入口 + 异构网关 e2e）。

## 2026-07-19 — session 5：防漂移门禁落地（读取键石完全收官）

- **用户拍板（动码前，方案 A）**：现在就上门禁锁死读取侧、写入 8 处显式豁免（写入共用步骤再收）；否决"推迟整个门禁到写入步骤后一次落"。
- **动手前取证**：对当前树 grep `.getMetadata(` 全域 = 21 行 = 9 处无参（`TestExternalCatalog`×8 静态 Map + `RuntimeProfile.node.getMetadata()`，均 `src/main`）+ 3 处 funnel javadoc + 1 处 funnel 真调 + **8 处写入裸调**。读取侧已"零裸调"（49 处 C3 已改道走 `PluginDrivenMetadata.get`）。
- **落地（commit `b2d147998d1`）**：
  - 新增 `tools/check-fecore-metadata-funnel.sh`（bash grep 门禁，仿 `check-connector-imports.sh`）+ 自测 `.test.sh`。规则：扫 `fe/fe-core/src/main/java`，禁裸 `Connector#getMetadata(session)`；**放行**=①funnel 文件 `datasource/plugin/PluginDrivenMetadata.java`（含其 javadoc）②带 `getMetadata-funnel-exempt` 标记的行（**call 行或其上一行**，兼容长行）③无参 `getMetadata()`（异方法）④注释行。正则双形匹配（同行参数 + 换行到下一行的参数），锚定调用形不误伤 `getMetadataTableRows`/API 定义。
  - 8 处写入裸调各加一行上置标记注释（103 字，全 <120）：PhysicalPlanTranslator×2 / BindSink×2 / PluginDrivenInsertExecutor / IcebergRowLevelDmlTransform / PhysicalIcebergMergeSink / PluginDrivenExternalTable(resolveWriteTargetHandle)。删标记即自动收紧到该处。
  - 挂入 `fe/fe-core/pom.xml` validate 阶段 exec（`${project.basedir}/../../tools/...`，与 fe-connector 门禁同深度同范式）。
- **验证**：自测 PASS（含核心裸调、换行参数、funnel 白名单、同行/上行标记、无参跳过、`getMetadataTableRows` 边界、注释跳过、退出码、标记可承重 10 项）；门禁对真实树 exit 0，对 8 处未标记 exit 1（都证过）；fe-core checkstyle 0 违规；`mvn -pl fe-core validate` 实跑触发门禁 exec + BUILD SUCCESS。
- **读取键石（RD-1 / STEP 1）至此完全收官**：C1 地基 + C2 关闭 + C3 改道 + 扫描存字段 + 后台读穿 + 防漂移门禁全落地。
- **未提交的第三方无关文件不动**（stray untracked `fe/.mvn/maven.config` 等非本轮产物，只 stage 本轮 9 文件）。
- **下一步**：见 HANDOFF —— 下一大步 = HMS 异构网关兄弟扇出（RD-2 / STEP 2），动码前先按分期定稿 §1/§2 + P1-design §5 对当前代码做 grounding recon 并把方案用中文详述待用户确认。

## 2026-07-19 — session 6：HMS 异构网关兄弟元数据每语句去重落地（RD-2 主体）

- **动码前设计先行 + grounding**（workflow `wf_62fa5a7f-07a`，5 读者 + 1 对抗完整性核验，并自行 clean-room 读码交叉核对）。核实结论比文档预想**小很多、也更集中**：整个 fe-connector-hive 模块"取兄弟元数据"仅 **4 处**（3 个 helper `icebergSiblingMetadata`/`hudiSiblingMetadata` by-TYPE + `siblingMetadata` by-HANDLE，加 `getTableSchema` 旁路）；文档"~43 处 per-handle 改道"实为误导——那 40+ 处 per-handle 转发 + `beginTransaction(session,handle)` 全穿第三个 helper，**改动零行**。catalogId 经 `session.getCatalogId()` 可达（无需新布线）；连接器侧直用 `session.getStatementScope().getOrCreateMetadata`（不 import fe-core，且该 key 形态早在 SPI 注释预声明）。
- **中文方案交用户确认**（不引任务代号）：4 处收口 + 属主标签从解析器**命中臂**取（拒绝会 force-build 的 supplier 身份比对——否则 hudi-only 目录会平白建 iceberg 兄弟）+ 旁路收回 + beginTransaction 免费覆盖 + closeAll 免额外接线。用户确认整体方案；**e2e 时机拍板 = 随后续统一补**（本步只做连接器单测锁死机制，异构网关 e2e 留切换阶段统一补，对齐 `hms-iceberg-delegation-needs-e2e`）。
- **落地（commit `5fd55d0a32a`）**：新增 `SiblingOwner{connector,label}`（`ICEBERG_LABEL`/`HUDI_LABEL` 常量作单一真源）；`HiveConnector.resolveSiblingOwnerLabeled` 命中臂带标签、`resolveSiblingOwner` 委派它（3 个 provider seam 字节不变）；`HiveConnectorMetadata.memoizedSiblingMetadata` key=`metadata:<catalogId>:<label>`，3 helper + 旁路收口，beginTransaction 免费覆盖。NONE 下工厂每调 = 字节等价；仅用 fe-connector-api 类型；兄弟只作 `Connector`/`ConnectorMetadata` 持有、绝不 cast。
- **测试**：5 个新去重断言（多转发+写事务共 1 建 / NONE 每调重建 / by-TYPE==by-HANDLE 同 key / 跨 catalogId 隔离 / iceberg-hudi 按标签隔离）；既有兄弟套件改传 NONE 作用域会话（转发路径现解引用作用域），无断言弱化；复制 `TestStatementScope` + 新增 `ScopeSession` 到 hive 测试树（连接器测试不能 import fe-core/iceberg）。
- **验证**：全模块 **348 单测全过**；checkstyle **0 违规**；fe-core 漏斗门禁 + 连接器 import 门禁均 **exit 0**；改动后整个 hive 模块只剩 **1 处** `owner.getMetadata(session)`（就在漏斗工厂内）。**对抗复审（workflow `wf_e55f3a51-561`，correctness + tests 两视角 + 逐条对抗核验）零 finding**；唯一非阻塞观察 = `listFileSizes` 未列入 `EXPECTED_METHODS` 转发面锁（**既有、非本轮引入**，留作后续测试硬化的 carry-forward）。
- **下一步**：见 HANDOFF —— 下一大步 = 写入共用（RD-3：无状态写点改道进入口 + `ConnectorTransaction` 归属上移）。

## 2026-07-19 — session 7：写入共用落地（读写共享一实例 + 事务归属上移，RD-3 完成）

- **动码前设计先行 + grounding**（workflow `wf_1a053ce4-b22`，6 读者：写缝普查 / 事务生命周期 / 闸门1 hive 起写 / 闸门2 iceberg 身份 / 纠缠点 side-car / 兄弟写路由；末段 verify agent 因大 JSON 输入撞结构化输出重试上限失败，但 6 读者全产出，事务生命周期与身份闸门二区自读核实补齐）。核实结论：**8 写缝精确**(7 纯只读外壳 + 1 开事务)、事务本就从 metadata 铸出→共享 metadata 即共享事务父、**异构网关开事务上一步已免费覆盖**、闸门1 是前瞻约束(改道天然不违反)、side-car 与本步**正交**。**纠正闸门1 旧表述**：hive 起写取表走 `CachingHmsClient` 缓存(与读同对象)、非“更新鲜快照”，吃重的是写鉴权取 + 原始参数拒 ACID + 提交期唯一把关。
- **中文方案 + 高度决策交用户**（不引任务代号）：先讲清读写为何可共用一实例(对齐 Trino `CatalogTransaction`)、两闸门、3a/3b 拆分、side-car 处理；就“事务归属上移的高度”给三选项，**用户选“完整共持体”**（新增 fe-core `CatalogStatementTransaction` + 执行器经它开/收事务 + 统一 commit/rollback vs closeAll 顺序）。详细方案二次确认后开写。
- **第一步 改道 + 身份闸门**（commit `f208036f3c5`）：8 处写裸调 → `PluginDrivenMetadata.get` + 删 8 豁免标记(门禁自动 100% 无例外)；收口加 `getUser()` 身份一致 fail-loud 断言(一语句一用户恒成立、永不触发、守铁律不解析令牌)；hive 起写未触。三写路径套件补 NONE 作用域桩 + 2 新身份断言测试。
- **第二步 事务归属上移**（commit `a03b88b0d80`）：新增 `CatalogStatementTransaction`(co-hold 共享 writeOps+session+懒建事务；`begin` 从共享实例铸事务+全局注册；`finalizeAtStatementEnd` 仅 `isActive`(孤儿)才回滚)；执行器 `beginTransaction` 经 `scope.computeIfAbsent("txn:"+catalogId,...)` 取共持体开事务；`ConnectorStatementScopeImpl.closeAll` 改**两趟**(先 finalize 事务、再关 metadata)；`PluginDrivenTransactionManager` 加 `isActive`(map 即已了结状态源→兜底幂等、绝不撤已提交)。NONE 下共持体瞬态、字节等价。5 新共持体测试 + 1 两趟顺序测试。
- **验证**：**103 目标单测全绿**(7 新)；checkstyle **0 违规**；fe-core 收口门禁 exit 0(写入零裸调)+ 自测 PASS；连接器 import 门禁 exit 0。异构网关 e2e 沿用“择机统一补”。
- **iceberg 表 side-car 未动**（正交、保持现状；删除留远期连接器内部重构）。定稿 → `designs/P3-write-sharing-implementation-design.md`。
- **下一步**：见 HANDOFF —— 下一大步 = 缓存隔离（RD-4 / STEP 4，独立安全 track：`getIdentityShardKey()` SPI + iceberg 三投影缓存 Key 分片 + fe-core 表结构缓存 bypass/分片 + 防漂移门禁 + 越权 e2e；随属主键覆盖异构网关；威胁模型签字）。
