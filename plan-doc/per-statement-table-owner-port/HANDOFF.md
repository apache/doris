# 🤝 Session Handoff —— 每语句表加载归属者 · 移植/重构

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`progress.md`](./progress.md)；状态见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本轮（2026-07-19 session 7）已完成：**写入共用一个每语句元数据实例 + 事务归属上移（读写共用步骤完全落地）**

## 一句话结果
- **背景**：一条写语句（INSERT/DELETE/MERGE）此前在多个环节各自向连接器“要一次元数据外壳”（8 处），与读取路径的“每语句一实例”割裂。本轮把写入也纳入同一收口，让**读和写在同一条语句里共用同一个元数据实例**（对齐 Trino `CatalogTransaction`：一 metadata + 一事务/语句/目录），并把写事务上移成语句级共持体。
- **grounding 纠偏**：8 写缝精确（7 纯只读外壳 + 1 开事务）；事务本就**从 metadata 实例上铸出**→共享 metadata 即共享事务父；**异构网关开事务上一步已免费覆盖**；side-car 与本步**正交**；**闸门1 旧表述纠正**——hive 起写取表走 `CachingHmsClient` 缓存(与读同对象)、非“更新鲜快照”，吃重的是写鉴权取 + 原始参数拒 ACID + 提交期唯一把关。
- **已落地（全绿）**：
  - **3a 改道 + 身份闸门**（commit `f208036f3c5`）：8 处写裸调 → `PluginDrivenMetadata.get` + 删 8 `getMetadata-funnel-exempt` 标记（门禁自动 100% 无例外）；收口加 `session.getUser()` 身份一致 **fail-loud 断言**（一语句一用户恒成立、永不触发、守铁律不解析令牌）。
  - **3b 事务共持体**（commit `a03b88b0d80`）：新增 `CatalogStatementTransaction`（co-hold 共享 writeOps+session+懒建事务；`begin` 铸事务+全局注册；`finalizeAtStatementEnd` 仅 `isActive`(孤儿)才回滚）；执行器经 `scope.computeIfAbsent("txn:"+catalogId,…)` 开事务；`ConnectorStatementScopeImpl.closeAll` 改**两趟**(先 finalize 事务、再关 metadata)；`PluginDrivenTransactionManager` 加 `isActive`（map 即已了结状态源 → 兜底幂等、绝不撤已提交）。NONE 下瞬态、字节等价。
  - **保留**：闸门1(hive 起写自取表) 天然满足、闸门2(身份) 断言守、tx↔session 绑定、全局事务注册(BE RPC)、执行器 onComplete/onFail 提交/回滚时机。
- **验证**：**103 目标单测全绿**（7 新）；checkstyle 0；fe-core 收口门禁 exit 0（写入零裸调）+ 自测 PASS；连接器 import 门禁 exit 0。异构网关 e2e 沿用“择机统一补”。
- **未动**：iceberg 表 side-car（正交，保持现状；删除留远期连接器内部重构）。

---

# ➡️ 下一个 session = **缓存隔离（RD-4 / STEP 4，独立安全 track）**

## 第一件事（先读）
1. 读 `designs/expanded-scope-phasing-and-security-decisions.md` **§3**（缓存隔离：确认 list≠load=真实越权、两轴切分、`getIdentityShardKey()` SPI、iceberg 三投影缓存、fe-core 表结构缓存 bypass/分片、异构网关最尖洞、防漂移门禁、Trino 对齐、待签字点）+ **§4 表 STEP 4**。
2. 读架构记忆 `iceberg-table-resolution-cache-scoping`（缓存作用域五条纪律）、`catalog-spi-plugin-tccl-classloader-gotcha`、`hms-iceberg-delegation-needs-e2e`、`catalog-spi-history-schema-info-lowercase-nested-names`。
3. 读本轮 `designs/P3-write-sharing-implementation-design.md`（读写共用 as-built，STEP 4 与之正交）。

## A. 动码前先 grounding + 出中文方案待确认（本任务铁律：设计先行 + 安全签字）
- **STEP 4 是真实越权修复**（用户已确认 list ≠ load）：“能列举但无权加载”的用户会经缓存命中读到别人授权才见的表结构/快照/分区。**须威胁模型签字**（授权新鲜度：TTL 内远端撤权仍命中，Trino 同性质，可接受但须签字；manifestCache 默认 OFF，开启须分片或声明与 session=user 不兼容）。
- **影响面小**：只涉及 **iceberg 三投影缓存 + fe-core 表结构缓存**；hive/paimon/hudi 无 SUPPORTS_USER_SESSION、无按用户授权轴，不动。
- 出中文方案（不引任务代号）：为何 list≠load 是越权、两轴切分（授权敏感投影按身份分片 vs 含凭证原始表永不跨语句缓存）、`getIdentityShardKey()` 放哪、异构网关怎么随属主键覆盖、防漂移门禁形态。**待用户确认（含威胁模型签字）后再改代码。**

## B. 实现要点（§3 + §4 STEP 4）
1. **新 SPI `ConnectorSession.getIdentityShardKey()`**：默认取 `getUser()`（Doris 主体非令牌字节 → fe-core 不解析凭证，守铁律）；off-thread loader 也读它防指纹漂移。
2. **iceberg 三投影缓存 Key 分片**：`latestSnapshotCache`/`partitionCache`/`formatCache`（`IcebergConnector` cache 块）Key 加 shard key，**仅 `isUserSessionEnabled()` 时填充**（否则常量→非 session=user 目录字节不变）；关残留泄漏（`beginQuerySnapshot` 命中跳过 per-user loadTable）。
3. **fe-core 表结构缓存**（`SchemaCacheKey` 仅按 nameMapping）：**分层**——(a) 先 `shouldBypassSchemaCache(SessionContext)`，session=user + 委派凭证下绕缓存现读（镜像已安全的库/表名缓存 bypass）；(b) 后续按需再把 identityShardKey 织入 Key。
4. **异构网关是最尖的洞**：hive 前门（无 SUPPORTS_USER_SESSION → fe-core 名字缓存 bypass 不触发）委派给 session=user 的 iceberg 兄弟时会泄漏；shard key 与 Key 选择须按**有效属主连接器身份**、经兄弟 getMetadata 路由传播（复用 STEP 2 已建的属主键收口）。
5. **防漂移门禁**：授权敏感的跨语句缓存 Key 在 session=user 下**必须**含 `getIdentityShardKey()`，把“加了新缓存忘分片”从静默泄漏变构建失败。
6. **原始表缓存保持 OFF**（`tableCache` 两条件 null 门维持）；语句内复用仍靠 iceberg 表 side-car（本步不删）。

## 后续 / 关联
- **越权 e2e**：can-list-cannot-load 用户命中不泄漏 + 异构网关 e2e（落 `external_table_p2/refactor_catalog_param`，与读写共用步骤欠的异构网关 e2e 一并统一补）。
- 读写共用 as-built：`designs/P3-write-sharing-implementation-design.md`。

## 铁律 / 闸门提醒
- 用户 2026-07-19 已豁免铁律 A（fe-core 只减不增）。仍守：连接器 connector-agnostic（作用域值 Object）；作用域/缓存跨用户即泄漏；**fe-connector-* 不得 import fe-core**（门禁）；**新 SPI 用 `getUser()`/主体字节，不解析凭证令牌**。
- **动码前探测并发活动**（git log/status + maven 进程 + 近 90s mtime），发现活跃即停手（`concurrent-sessions-shared-worktree-hazard`）。
- 本任务设计先行 + 安全签字：STEP 4 调研设计结束、正式改代码前，先把方案用中文详述 + **威胁模型签字**待用户确认；e2e 沿用“择机统一补”。

---

# 🗂 遗留 / 关联
- 分期定稿（现行主线）：`designs/expanded-scope-phasing-and-security-decisions.md`（§3 缓存隔离、§4 分期 STEP 4、§6 残留风险）。
- 读写共用 as-built：`designs/P3-write-sharing-implementation-design.md`（3a 改道+身份闸门、3b 共持体+两趟 closeAll、side-car 正交）。
- 门禁：`tools/check-fecore-metadata-funnel.sh`（读写侧现均无例外）、`tools/check-connector-imports.sh`；STEP 4 新增缓存分片防漂移门禁。
- 架构记忆：`iceberg-table-resolution-cache-scoping`、`catalog-spi-plugin-tccl-classloader-gotcha`、`hms-iceberg-delegation-needs-e2e`。
- e2e：异构网关 e2e（INSERT/DELETE/MERGE + can-list-cannot-load 越权）落 `regression-test/suites/external_table_p2/refactor_catalog_param`，随 STEP 4 统一补。
