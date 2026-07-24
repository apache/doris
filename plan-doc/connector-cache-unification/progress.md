# Progress Log — 连接器缓存框架统一 (connector cache unification)

> **append-only**：每 session 追加一条（日期 / 做了什么 / 结论 / 下一步）；不覆盖、不删旧条。
> 滚动上下文在 [`HANDOFF.md`](./HANDOFF.md)，进度总览在 [`tasklist.md`](./tasklist.md)。

---

## 2026-07-23 — 搭建伞形追踪空间（未启动执行）

- **做了什么**：读 `00-research-report.md` + `data/connector-audits.json`（hive/hudi/paimon 全文）+ 参考空间 `perf-hotpath-iceberg/`（README/HANDOFF/tasklist）；据此在本目录建 `HANDOFF.md` + `tasklist.md` + 本 `progress.md`。
- **结论**：
  - 本空间定位为**伞形协调空间**——追踪 workstream 级（WS-HUDI / WS-MC / WS-ES / WS-DOC / WS-P2）+ 4 个 owner 决策（D1–D4）；逐连接器执行各自另开 `perf-hotpath-<c>/` 兄弟空间（报告 §9）。
  - 未改 `README.md`（它是完成态的调研交付物）；稳定流程/铁律/build 坑放进 HANDOFF 底部稳定区。
  - **未做任何拍板、未启动任何执行、未改产品代码。**
- **下一步（交下个 session）**：先向用户讲清 D1–D4 拿签字（中文，见 HANDOFF「下一步」），默认推荐先启动 WS-HUDI（唯一 P1 真缺口）并新开 `plan-doc/perf-hotpath-hudi/`。动码前按 HEAD 重侦察（行号信 grep）。

---

## 2026-07-23 (2) — owner 4 决策签字 + 设计定稿（仍 0 产品代码改动）

- **做了什么**：
  1. 向 owner 讲清 D1–D4 并拿签字：D1 hudi+mc+es 都做；D2 先建底座；D3 现在就通用化门禁；D4 原选"提炼进 fe-core"。
  2. 跑 11-agent 只读设计调研 workflow（6 路 HEAD 侦察 + Trino 参考 → 设计综合 → 3 路对抗评审 → 终稿）。产物存 `designs/`（`foundation-design-FINAL.md` + draft + review-1..3）。
     - 注：首跑综合步因"大嵌套结构化输出被截断"失败；改成 prose 输出 + 断点续跑（6 侦察命中缓存），成功。
  3. 设计定稿后向 owner 二次确认，两处更正/确认拍板。
- **结论（关键）**：
  - **D4 前提被侦察推翻**：per-statement memo 底座**早已在 `fe-connector-api`**（`ConnectorStatementScope.computeIfAbsent`/`ConnectorSession.getStatementScope`；iceberg/hudi 均依赖该层、均不依赖 fe-core）→ helper 只是该层一个小静态方法 → **fe-core 0 行、铁律 A 不碰**。owner 二次确认接受、删掉"往 fe-core 塞"备选（含 mc 的 B2）。又一次"侦察推翻已签字蓝图"（memory `execution-blueprint-overestimates-recon-first`）。
  - **底座几乎现成**：通用缓存封装 = 升格已存在的 `ConnectorPartitionViewCache[V]`（iceberg/hive/paimon 已用）→ `ConnectorMetadataCache[V]`；key `PartitionViewCacheKey`→`ConnectorTableKey` 四元组。**整套 A/B/C + hudi/mc/es = 0 行 fe-core 改动。**
  - **门禁原方案有 BLOCKER**：iceberg 缓存字段也在 metadata 类上、hive 网关无标记缓存 → 评审重设计为"模块内扫缓存构造点 + 断言凭证置空 + 网关纳入 + 零声明者硬失败"。
  - owner 确认：**iceberg 本轮就改挂**（真正先建底座、证 parity）。
  - 评审强制若干安全约束：hudi 记忆"不可关闭投影"、instant 每语句重取只缓存分区列表、iceberg `invalidateDb` parity 测试、es 分片路由拆开保持每语句。
- **下一步（交下个 session）**：进入实施，按 HANDOFF「实施路线」8-PR（PR-0 先验 → PR-1 封装 → PR-2 helper+iceberg 改挂 → PR-3 iceberg 收敛 → PR-4 hudi → PR-5 mc → PR-6 es → PR-7 门禁）。**动每个文件前按 HEAD 重侦察**；全程守 fe-core 0 行，遇"不得不碰 fe-core"停手交 review。

---

## 2026-07-23 (3) — PR-0 完成：预编译执行的连接器作用域重置回归守门（含外表可达性侦察）

- **做了什么**：
  1. 进实施前按 HEAD 重侦察 PR-0/1/2 承重事实（6 路只读 workflow + 综合 drift 报告）：**设计成立、有更正、更正均缩小工作量**。
  2. 用户质疑"预编译语句是否支持外表"→ 专项 Explore 侦察**查实：外表确经预编译执行**——走普通 `executor.execute()` 全量规划路、每次执行重新经连接器解析外表；**永进不了 OLAP 短路点查快路**（短路规则只匹配 `logicalOlapScan()`，`LogicalResultSinkToShortCircuitPointQuery.java:88,97`）。故 `ExecuteCommand.java:95` 的 `resetConnectorStatementScope()` 真实可达、承重，只是此前**零测试守门**（现有测试只覆盖 reset 原语，不证 `ExecuteCommand` 调它）。
  3. 加 FE 单测 `ConnectorStatementScopeTest.executeCommandResetsConnectorScopePerExecution`：往复用的语句上下文放哨兵值 → 驱动 `ExecuteCommand.run()`（执行器 stub 空操作、`enableGroupCommitFullPrepare=false` 走普通路）→ 断言执行后作用域被替换、哨兵不残留。
  4. 改正 FINAL 设计里被证伪的机制描述（"每次执行拿新 `StatementContext`" → "复用上下文 + 每次执行显式 `resetConnectorStatementScope()`"，并记录外表可达性侦察结论）。
- **验证**：`mvn test -pl fe-core -am -Dtest=ConnectorStatementScopeTest -Dmaven.build.cache.enabled=false`：`Tests run: 9, Failures: 0`，BUILD SUCCESS，checkstyle 过。**变异验证**（注释掉 `ExecuteCommand.java:95` 的 reset）：`Failures: 1` 且仅新测试变红（`expected: not same`），其余 8 测试仍绿 → 证明测试能失败且精确针对该 reset（Rule 9）。变异后已**逐字节还原** `ExecuteCommand.java`（`git diff` 空）。
- **侦察更正（供后续 PR，动码前仍须再 grep 确认）**：
  - ①**无"兼容子类"可删**——连接器直接构造 `ConnectorPartitionViewCache`（iceberg 构造两次 `:281/:284`，hive `:134`，paimon `:158`），`git grep "extends ConnectorPartitionViewCache"` 空 → PR-1 删掉"删兼容子类"这一步；勿把 `IcebergPartitionCache`（独立 PERF-02 层）误当子类。
  - ②`ttl≤0→CACHE_TTL_DISABLE_CACHE` 映射复制在 **6** 处（设计写 5）：`IcebergComment/Format/LatestSnapshot/Partition/Table` + `PaimonLatestSnapshotCache` → PR-1 收进 `CacheSpec` 动 6 处。
  - ③`formatCache` 挂在 `IcebergScanPlanProvider`（`IcebergConnector.java:782` 注入）**非** metadata 对象 → PR-3 触 format 缓存须对扫描规划器。
  - ④iceberg 5 缓存**全独立 `final class`**、均建于 `MetaCacheEntry`、**无一** extends `ConnectorPartitionViewCache`；entry 名 hyphen（`iceberg-table` 等，非 `iceberg.table`）→ PR-3 钉死 legacy 名。
  - ⑤`ConnectorStatementScopeImpl` 在 **fe-core**（`org.apache.doris.connector`，引用 fe-core `CatalogStatementTransaction`），interface 在 fe-connector-api；iceberg 经 fe-connector-spi **传递**依赖 api、hudi 直接依赖 → PR-2 的 `ConnectorStatementScopes` helper 放 fe-connector-api 仍**0 行 fe-core**。
- **下一步**：PR-1 通用缓存封装升格（`ConnectorPartitionViewCache[V]`→`ConnectorMetadataCache[V]`、`PartitionViewCacheKey`→`ConnectorTableKey`、6 处 ttl 映射收进 `CacheSpec`、修 stale javadoc "no consumers yet"、iceberg/hive/paimon 改挂）；纯加+改名+删，反应堆 test-compile + 现有 partition-view 测试证零变化。**动每个文件前按 HEAD 重侦察**。

---

## 2026-07-23 (4) — PR-1 完成：通用缓存封装升格为 ConnectorMetadataCache（纯重命名，行为不变）

- **做了什么**：
  1. 动码前按 HEAD 重侦察全部改名点（`ConnectorPartitionViewCache` / `PartitionViewCacheKey` 的所有引用，15 文件 4 模块），确认无外部脚本/配置引用、新名无冲突。
  2. 把已经通用的缓存封装正式升格：`ConnectorPartitionViewCache<V>`→`ConnectorMetadataCache<V>`、`PartitionViewCacheKey`→`ConnectorTableKey`（含文件改名，`git mv` 保留历史）；构造器由硬编码 `"partition_view"` 改为显式传 `(engine, entryName, props)`，供后续连接器注册独立命名的缓存条目。
  3. hive/iceberg/paimon（生产+测试）共 12 文件改挂新名；三连接器构造点显式传 `"partition_view"` → 条目名、`meta.cache.<engine>.partition_view.*` 配置项、缓存键**逐字节不变**。修 stale "no consumers yet" javadoc。
  4. **收窄设计原 bundling**（Rule 2/3）：TTL≤0 禁用映射去重（6 处复制）+ 预解析 CacheSpec 构造器**推迟到 iceberg 收敛那步**做（那批 6 处里 5 个是 iceberg 手写缓存类，下一步本就重写它们，避免二次翻动）。
- **验证**：`mvn install -pl cache,hive,iceberg,paimon -am`（**install 非 test**——hive/iceberg/paimon 经 fe-connector-hms 依赖 hive-shade jar，`-am test` 不产 shade jar 会在 hms 编译期挂，见 build 坑 1）：BUILD SUCCESS，四模块全过；7 个分区视图缓存测试类共 **66 测试 0 失败**（ConnectorMetadataCacheTest 11 + hive 5+4 + paimon 7+7 + iceberg 25+7）。
- **踩坑记录（供后续机械改名复用）**：`sed 's/ConnectorPartitionViewCache/ConnectorMetadataCache/g'` **子串过匹配**——把测试类名 `HiveConnectorPartitionViewCacheTest` 也改成 `HiveConnectorMetadataCacheTest`（但文件名没改）→ checkstyle `OuterTypeFilename` 报错。教训：跨文件类名机械改名用**词边界** `\b`（`Hive`+`ConnectorPartitionViewCache` 间无边界，`\b` 可避免误伤）；或改后用"文件名 vs public 类名"扫描兜底（本轮已用该扫描定位唯一误伤）。
- **下一步**：PR-2 语句作用域通用 helper（`ConnectorStatementScopes.resolveInStatement` + namespace 注册表，放 `fe-connector-api`，**0 行 fe-core**）+ iceberg 私有 `IcebergStatementScope.sharedTable` 改委派（key 逐字节不变，须 byte-identical parity 测试）。动码前按 HEAD 重侦察。

---

## 2026-07-24 — PR-2 完成：语句作用域通用 helper `ConnectorStatementScopes`（0 行 fe-core，iceberg 改挂 byte-identical）

- **做了什么**（commit `ae8c925074d`，严格 4 文件、零 fe-core 源码）：
  1. 动码前按 HEAD 重侦察全部承重事实：`ConnectorStatementScopes`(复数)不存在须新建；iceberg 现键 `"iceberg.table:" + catalogId + ":" + db + ":" + table + ":" + queryId`；`ConnectorStatementScope.computeIfAbsent(String,Supplier<T>)`/`ConnectorSession.getCatalogId():long`/`getQueryId():String`/`getStatementScope():default NONE` 逐一核对；`rewritableDeleteSupply` 是 `(catalogId,queryId)`-keyed scan→write 累加器（非表解析）→留 iceberg 私有；4 个 `sharedTable` 调用方（metadata/scan/write/transaction）签名不变、仅 body 委派。
  2. 新增 `fe-connector-api` 的 `ConnectorStatementScopes.resolveInStatement(session, keyNamespace, db, table, loader)`：复用已存在的 `ConnectorStatementScope.computeIfAbsent` 原语，统一"每语句解析一次 db.table"的**安全关键键约定**（丢 queryId=跨执行泄漏、丢 catalogId=跨目录 MERGE 撞车、丢 namespace=异构网关下值类型撞车→ClassCastException）；null session / NONE scope 每次跑 loader（load-every-time 不变）。namespace 注册表以 `ICEBERG_TABLE="iceberg.table"` 落地（hudi/mc/es 保留、各自 PR 接入时声明）。
  3. iceberg `IcebergStatementScope.sharedTable` 改为委派该 helper，用 `ICEBERG_TABLE` 命名空间**逐字节复现**历史键前缀 → 4 resolver 命中/未命中/NONE 回落全等。
- **验证**：
  - `install -pl fe-connector-api,fe-connector-iceberg -am` BUILD SUCCESS，全模块 0 checkstyle。
  - 新 `ConnectorStatementScopesTest` **8 测试**（memo-once / 5 轴逐一隔离 / namespace 值类型隔离(否则 CCE) / null+NONE load-every-time / 键逐字节断言）；`IcebergStatementScopeTest` **7 测试**（+ byte-key parity `"iceberg.table:7:db1:t:q1"` + iceberg 级 null-session）；**iceberg 全模块 1133 测试 0 失败**（4 个 sharedTable 调用方测试类全绿：Transaction 66/Metadata 51/Scan 114/Write 42）。
  - **铁律 A 核实**：`git diff --numstat -- 'fe/fe-core/**'` 空 → 0 行 fe-core。
  - **4 路对抗净室复审**（byte-parity / callers / iron-rules-leak / test-quality）全判 **PARITY_HOLDS**、无一 refutes_parity；据其两条反馈**加固测试**：①测试替身 `getSessionId()` 改为 ≠ `getQueryId()`（否则 queryId→sessionId 误改会跨查询泄漏却无测试能红）②补 iceberg 级 null-session 测试。
  - **Rule 9 变异验证**：把 helper 键 `getQueryId()`→`getSessionId()`，**恰好两个 byte-key 测试变红**（api 1/8、iceberg 1/7）、其余全绿；已逐字节还原（`git diff` 该行回 `getQueryId()`）。
- **一条 surface 给 owner（非阻塞）**：`ICEBERG_TABLE` 常量放在中立 SPI 层 `fe-connector-api` 上，两名评审标为 minor 层次瑕疵（中立 SPI 里出现连接器名），但同时判定"可接受的命名空间注册表、非有害泄漏"——这是设计 §B 修订#7 owner 已签的**中心化 uniqueness 注册表**（防 R9 跨连接器 namespace 撞车的唯一审计点）；若移到各连接器自持则失去中心审计点。保持设计原样，如 owner 更偏好各连接器自持常量可轻量改。
- **下一步**：iceberg 5 缓存收敛（原计划下一步），动码前重侦察 + 向 owner 讲清成本后由 owner 重新定范围。

---

## 2026-07-24 (2) — owner 重定范围：只做安全 ttl 去重，全量收敛延后（行为字节级不变）

- **背景/决策**：原计划下一步是把 iceberg 5 个手写缓存全量收敛到通用 `ConnectorMetadataCache`。动码前重侦察后向 owner 如实讲清成本：该收敛**零功能收益**、改动面宽（~19 文件 / 重写 ~37 测试 / 6 个重载构造函数签名波及 / 收敛后调用点从强类型 `TableIdentifier` 退化成字符串四元组键 / 每缓存独立注释退化成字段注释），且通用框架**已被证明可用**（已在 hive/iceberg/paimon 三连接器分区视图缓存跑着）、有性能收益的 hudi/mc/es **不依赖**它。**参考 Trino**（共享底层原语 + 各连接器自持缓存、不强制统一封装）→ iceberg 这 5 个缓存已是 Trino 式。**owner 拍板：只做安全 DRY、全量收敛延后。**
- **做了什么**（严格 8 文件、0 行 fe-core、纯 `[refactor]` 行为不变）：
  1. 新增 `CacheSpec.ofConnectorTtl(ttlSecond, capacity)`：把连接器"`ttl≤0` 禁用"契约折叠进 `CacheSpec` 的禁用哨兵（0）——负 ttl 走禁用而非被 `CacheSpec` 读成 `-1`「不过期(启用)」。逐字节等于原三元式 `ttl>0 ? of(true,ttl,cap) : of(true,DISABLE,cap)`。
  2. **6 处**复制的三元式改调该工厂：`IcebergTable/LatestSnapshot/Comment/Format/Partition` + `PaimonLatestSnapshot`（PR-1 侦察更正②点名的 6 处）。
  3. 补 `CacheSpecTest.ofConnectorTtlFoldsNonPositiveToDisabled`（Rule 9）：钉死承重的负-ttl-必禁用——`-1`/`-2` 折叠成禁用哨兵、`isCacheEnabled` 恒 false（否则负值静默变永不过期缓存）。
- **验证**：`mvn install -pl :fe-connector-cache,:fe-connector-iceberg,:fe-connector-paimon -am -Dmaven.build.cache.enabled=false`（install 非 test，见 build 坑）：**BUILD SUCCESS**。`CacheSpecTest` 15（+1 新）、`ConnectorMetadataCacheTest` 11、6 个受影响缓存的既有测试**原样全绿**（Iceberg Table 7 / Comment 8 / Partition 8 / Format 8 / LatestSnapshot 6 + Paimon LatestSnapshot 6）、iceberg 全模块 1134、paimon 379，**0 失败 / 0 错误**。checkstyle 过。fe-core `git diff` 空。
- **未做（明确延后）**：5 个 iceberg 缓存类**未收敛**、其类型/键/`*ForTest` 访问器/5 个测试文件**原样保留**；通用缓存的 pre-resolved-名/loadCount 访问器、`invalidateDb` Namespace→String parity 等收敛相关改动**一并延后**（框架已被证明可用、消费者不依赖；出现真实需要再上收）。
- **下一步**：转入有性能收益的连接器工作——旗舰 **hudi**（新开 `plan-doc/perf-hotpath-hudi/`，镜像 iceberg 布局；前置改 pom 引 `fe-connector-cache` 工具箱）或先做 **mc / es** 两个小 PR。动码前按 HEAD 重侦察。

---

## 2026-07-24 (3) — WS-MC maxcompute round-1：每语句表句柄记忆化（commit `58daadd10e0`）

> 承 hudi round-1 完成（见 `plan-doc/perf-hotpath-hudi/`，commit `26690775c81`）后，转入 mc/es 两个小 PR 中的 **maxcompute**。详见兄弟空间 `plan-doc/perf-hotpath-maxcompute/`。

- **病灶**：`MaxComputeConnectorMetadata.getTableHandle` 每次解析都发一次冗余 ODPS `tables().exists()` 远程探测 + 新建惰性 `Table`；funnel 只 memo metadata 不 memo handle → 一条语句 ~17 个解析点（fe-core `resolveConnectorTableHandle` 13 + translator 2 + BindSink 2）各付一次探测（冷统计逐列放大 O(列数)），各自 Table 首访各触发一次 reload。= 审计 MC-1（P1）+ MC-2（P2）。
- **做了什么**（1 连接器文件、+28/-9、**0 fe-core**）：per-statement `MaxComputeConnectorMetadata` 实例加 `Map<List<String>, MaxComputeTableHandle> tableHandleMemo`（`ConcurrentHashMap`），`getTableHandle` 经 `computeIfAbsent` 解析。**present-only**（mapping 返回 null → 不记录 → 缺表每次重探、`Optional.empty()` 逐字节不变）；**保留 exists() 只去重不删**（删会把干净 not-found 退化成后续 `OdpsException`）；handle 无 equals/hashCode → 按 `(db,table)` 值 key（`List.of`）；CHM 因 off-thread scan 池复用同 per-statement session/instance。`createTable`/`tableExists` 直调 helper 不经 memo；跨查询 `MaxComputePartitionCache` 正交。
- **验证**：全 `MaxCompute*` **120 测试绿**、checkstyle **0 违规**。守门单测 `MaxComputeConnectorMetadataHandleMemoTest`（仿 `DropDbTest` 手写记录 fake、无 Mockito、null odps 离线）4 例：同表 2 解析→探测/build 各 1 + handle `assertSame`；不同表独立；**同名跨 db 不撞**；缺表每次重探不 memo。**Rule 9 变异两处**：去 memo（`memo.clear()`）→ 同表用例红（探测 1→2）；db-blind key（`List.of(tableName)`）→ 跨-db 用例红（`assertNotSame` 失败）。
- **净室对抗复审**（4 lens + 2 verify workflow）：parity **PARITY_HOLDS**、staleness **PARITY_HOLDS**；concurrency CONCERN（共享 Table 并发首次 reload）经 verify **REFUTED**——off-thread scan 任务用 scan-node ctor 一次解析的 `currentHandle`、**不调** getTableHandle，共享 Table 是 baseline 既有属性，memo 反而让单线程分析先暖 schema、**降** reload 争用；test-quality CONCERN（无跨-db 用例，db-blind key 变异能漏网）经 verify **CONFIRMED_REAL** → **已补**跨-db 守门用例并变异验证。
- **未做/延后**：PERF-MC02 陈旧注释（doc-only，随手/并 WS-DOC）、PERF-MC03 可选跨查询 `Table` 缓存（热点触发）。**e2e 需集群本地未跑**（异构 + 独立 max_compute catalog 的 SELECT/分区裁剪/写路径解析计数），留标注。
- **下一步**：WS-ES（es 连接器 `fetchMetadataState` per-scan hoist 2×→1× + raw mapping 承载决策；**shard routing 绝不 cross-query 缓存**）。
