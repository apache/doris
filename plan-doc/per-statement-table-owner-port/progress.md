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
