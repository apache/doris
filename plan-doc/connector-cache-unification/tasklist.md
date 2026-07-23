# Task List — 连接器缓存框架统一 (connector cache unification)

> **伞形进度总览（program 级）**。本文件只追踪 **workstream 状态 + owner 决策**，**不复制分析**。
> 权威分析：[`00-research-report.md`](./00-research-report.md)（§3 矩阵 / §5 路线图 / §6 授权 / §7 反指 / §8 决策 / §9 workspaces）
> · [`connectors/*.md`](./connectors/)（7 份 per-connector 审计 + 对抗复核 verdict）
> · [`data/connector-audits.json`](./data/connector-audits.json)（结构化审计 + `verify.corrections`）。
> 逐连接器的 `PERF-NN` 细粒度勾选在**各自兄弟空间** `plan-doc/perf-hotpath-<connector>/tasklist.md`，不在本文件。
> **ID 一旦分配永不复用**；删除标 `[deleted YYYY-MM-DD <原因>]` 保留占位。
> 状态图例：⏳ 待启动 · 🚧 进行中 · ✅ 完成 · ❌ 阻塞 · 🔬 复核中 · 🅿 待用户拍板 · 🚫 反指/不立项

---

## 0. 待拍板决策（程序启动前置 — 见 HANDOFF「下一步」）

> ✅ **已于 2026-07-23 拿到 owner 签字**（下方「用户拍板」列），**并在设计定稿后二次确认**（见下）。
> **设计已定稿** → [`designs/foundation-design-FINAL.md`](./designs/foundation-design-FINAL.md)（11-agent 只读设计调研 + 3 路对抗评审，全程逐符号核对 HEAD）。
> ⚠ **D4 重大更正**：原选"提炼进 fe-core"，但设计侦察**推翻前提**——per-statement memo 底座**早已存在于 `fe-connector-api`**（`ConnectorStatementScope.computeIfAbsent` / `ConnectorSession.getStatementScope`；iceberg、hudi 均依赖该层、均**不**依赖 fe-core）。故 helper 只是该层**一个小静态方法**（`ConnectorStatementScopes.resolveInStatement`，统一 key 约定）→ **fe-core 零改动、铁律 A 根本不碰**。owner 2026-07-23 **二次确认接受此复读**、并**删掉"往 fe-core 塞"的备选**（三方评审一致：签字了但可避免的 fe-core 改动正是不必要改动混入主干的方式）。
> **结论：本轮整套底座（A 通用缓存封装 + B 语句作用域 helper + C 门禁）+ hudi/mc/es 消费 = 全程 0 行 fe-core 改动。** iceberg 本轮就改挂（owner 确认，真正"先建底座"）。

| ID | 决策 | 报告建议（默认） | 用户拍板（2026-07-23，含设计后二次确认） | 状态 |
|---|---|---|---|---|
| **D1** | **范围**：本轮只打 hudi，还是把 mc `MC-1`、es `ES-F1/F2` 一并纳入? | hudi 单独立项 + mc/es 各一小 PR；jdbc/paimon/trino P2 进 backlog | ✅ **采纳默认**：hudi + mc + es 都做（各自独立 PR），jdbc/paimon/trino 延后 | ✅ |
| **D2** | **排序**：先建共享底座泛化 vs per-connector-first? | per-connector-first（避免投机抽象，出现第 2 个消费者再上收） | ✅ **否决默认 → 先建共享底座**：把 iceberg 5 个手写缓存上收为通用封装（升格已存在的 `ConnectorPartitionViewCache`）；**iceberg 本轮就改挂** | ✅ |
| **D3** | **门禁通用化**：`check-authz-cache-sharding.sh` 现只盯 iceberg，现在就改成扫描任意 `SUPPORTS_USER_SESSION` 连接器吗? | (b) 延后无当下风险 / (a) 前瞻更稳 —— 交用户选 | ✅ **选 (a) 现在就通用化**，且**评审重设计**：从"扫主类带标记字段"改为"**模块内扫缓存构造点 + 断言按用户凭证置空**"（原方案有 BLOCKER 漏洞：iceberg 缓存字段也在 metadata 类上、hive 网关无标记缓存） | ✅ |
| **D4** | **共享 helper**：投入 fe-core 共享的"经 statement scope 解析表" helper 吗? | 不提炼；让 hudi 在自己模块内 memo（碰铁律 A，Rule 2） | ✅ **设计后二次确认：不动 fe-core** —— helper 放 `fe-connector-api`（memo 底座已在该层），iceberg + hudi 共用；**铁律 A 不碰**。原"提炼进 fe-core"作废；maxcompute 的"往 fe-core funnel 包一层"备选(B2)一并**删除**（评审三方否决：改到本轮不测的 jdbc/paimon/trino/hive 调用次数契约） | ✅ |

---

## 1. Workstream 总览（at-a-glance）

> 优先级 = 报告 §5/§9。**唯一 loop-amplified（iceberg-式）P1 真缺口 = hudi**；mc/es 是 constant-factor（2–4x）P1 收尾；其余结构上反指再加重缓存。

| ID | 优先级 | 覆盖发现 | 主题（一句话） | 目标兄弟空间 | 依赖 | 状态 |
|---|---|---|---|---|---|---|
| **WS-HUDI** | **P1 旗舰** | HD-P01/02/03/04/05 | 缓存最薄连接器：metaClient 每 pass 重建 5–6x、schema 4x、裸 `ThriftHmsClient` → 全套 memo + 采纳工具箱 | `plan-doc/perf-hotpath-hudi/` | D1 | ⏳ |
| **WS-MC** | P1（小） | MC-1（+MC-2） | 每次 handle 解析冗余 ODPS `tables().exists()` 远程探测 → metadata 内 `Map<(db,table),Handle>` + 去冗余探测 | `plan-doc/perf-hotpath-maxcompute/` | D1 | ⏳ |
| **WS-ES** | P1（小） | ES-F1/F2（+ES-F3） | `fetchMetadataState` 2x/查询 + mapping 重取 2x → per-scan hoist + mapping/field-context 承载决策 | `plan-doc/perf-hotpath-es/` | D1 | ⏳ |
| **WS-DOC** | 低（doc-only） | 陈旧注释一批 | 修 `ConnectorPartitionViewCache` "no consumers yet" + hive/hudi/mc 的 "dormant / 未在 SPI_READY_TYPES / never called" 陈旧注释 | `plan-doc/cleanup-stale-connector-docs/`（或随手做） | — | ⏳ |
| **WS-P2** | P2（可延后） | PA-1 · HP-1/HP-2 · TRINO-H1/H2/H3 · hive 写后读一致性 | 各连接器常数倍/CPU/一致性收尾，按热点 profile 触发 | `plan-doc/perf-hotpath-backlog-p2/` | 热点触发 | ⏳ |

> **共享底座泛化（deferred，绑 D2/D3）**：iceberg 3 个格式中立缓存（table-handle / comment / file-format）上收为 `ConnectorXViewCache` 泛型封装、`check-authz-cache-sharding.sh` 通用化 —— **仅在出现第 2 个消费者时启动**，当前不投机抽象。

---

## 2. Workstream 详情

> 详情只记「交付概要 + 约束 + 依赖 + 权威指针」，不复制病灶分析（在报告/审计/JSON 里）。

### [ ] WS-HUDI — P1 旗舰（HD-P01/02/03/04/05）
- **权威**：报告 §5.1 + §9；[`connectors/hudi.md`](./connectors/hudi.md)；JSON hudi 节（含 3 条 `verify.corrections`）。
- **交付概要**（报告 §9）：
  1. **per-statement resolved-metaClient + schema memo**，由 `HudiConnectorMetadata` 与 `HudiScanPlanProvider` 经同一 `ConnectorStatementScope` 共享 → 杀 `HD-P01`（metaClient 5–6x→1x）+ `HD-P02`（schema 4x→1x）。**旗舰**。
  2. **包 `CachingHmsClient`**（一行 parity，仿 `HiveConnector.wrapWithCache`）→ 修 `HD-P04`（裸 `ThriftHmsClient`）。
  3. **cross-query metaClient/table 缓存 + `(table,instant)`-keyed 分区缓存**（用 `ConnectorPartitionViewCache`）→ 修 `HD-P03`（MTMV/SHOW PARTITIONS 重枚举）。
  4. **per-scan hoist**（metaClient/schema/storage-config/uri-normalizer 各一次）→ 修 `HD-P05`。
- **前置**：`fe-connector-hudi` 目前**零 `fe-connector-cache` 依赖** → 先改 pom 引入工具箱（见 HANDOFF build 坑 5）。
- **约束**：**无 authz 工作**（hudi 单一 catalog 身份、非 session=user，名字为 key 缓存安全）；**无写事务工作**（只读，`beginTransaction` throws）。cross-query metaClient/分区缓存须 **TTL + REFRESH 有界**（freshness）。
- **⚠ 复核先行**：`HD-P03` 对**过滤查询已去重到 ~1 次**（真放大在 fe-core 多次独立调 `listPartitions*` / 无过滤扫 / 一次 MTMV refresh 4–6 次）；`HD-P01` 无条件下界 ~3–4x（含条件站点达 5–6x）—— 动码前按 HEAD 重侦察确认乘数。
- **依赖**：D1（是否本轮）。旗舰、先做、收益最大。

### [ ] WS-MC — P1 小（MC-1，+MC-2）
- **权威**：报告 §5.1 + §9；[`connectors/maxcompute.md`](./connectors/maxcompute.md)；JSON maxcompute 节。
- **交付概要**：在**已经是每语句一个**的 `MaxComputeConnectorMetadata` 实例内加 `Map<(db,table),Handle>`，并对已解析读路径去掉多余 ODPS `tables().exists()` 远程探测 → `MC-1` k×/语句 → 1×/语句；顺带收 `MC-2`（lazy `Table` reload）。低风险高杠杆。
- **约束**：连接器侧改动；无 authz（静态 AK/SK 单身份）、无写事务共享需求（`MaxComputeConnectorTransaction` 非-MVCC）。
- **依赖**：D1。可与 WS-ES 并行。

### [ ] WS-ES — P1 小（ES-F1/F2，+ES-F3）
- **权威**：报告 §5.1 + §9；[`connectors/es.md`](./connectors/es.md)；JSON es 节（含 ES-F2 的 ADJUSTED 更正）。
- **交付概要**：
  - `ES-F1`：per-scan hoist `EsMetadataState`（`fetchMetadataState` 每查询 2 次 → 1 次），provider 已是每 scan-node 单例，加 `(index,columnNames)` 字段 memo，零 staleness（同语句）。
  - `ES-F2`：**注意不是"零新增缓存直接复用 schema"**——fe-core `ExternalSchemaCache` 只存解析后列，`EsConnectorMetadata.getTableSchema` 丢弃了原始 mapping，而 `resolveFieldContext` 需要它。消除它须**让 `ConnectorTableSchema` 携带 field-context** 或**加 per-statement/cross-query mapping 缓存**——立项时定承载方式。
  - `ES-F3`（P2）：在每语句一个的 metadata 上 memo schema。
- **⚠ 约束（硬）**：**ES shard routing 必须留 per-statement，绝不能 cross-query 缓存**（ES rebalance/refresh 模型）。
- **依赖**：D1。可与 WS-MC 并行。

### [ ] WS-DOC — doc-only（随手可做）
- **权威**：报告 §4(4) + §9；JSON 各连接器 `migrationStatus.notes` / `verify.corrections`。
- **交付概要**（调研时快照行号，执行时以 grep 为准）：
  - `ConnectorPartitionViewCache` 类 javadoc "NO consumers yet" → 已有 ≥2 消费者（hive、paimon）。
  - hive：`CachingHmsClient.java:85-88`、`HiveFileListingCache.java:72-74`、`HiveConnector.wrapWithCache:667-668`、`HiveConnector.java:118,126-127`（sibling 字段注释）的 "Dormant / hms is not in SPI_READY_TYPES"。
  - hudi：`HudiConnectorMetadata.java:391`、`HiveConnectorMetadata.java:410,1802,1942,1972,2007` 的 "dormant until hms enters SPI_READY_TYPES / today getTableHandle is never called"。
  - maxcompute：`beginTransaction:332`、`MaxComputeWritePlanProvider:74` 的陈旧注释。
  - 起因：`#65473`/`6e521aa64b2`（2026-07-16）把 hms 翻为 live 但没更新这批 class 注释。**纯 doc，运行时无害**。
- **依赖**：无。可任何时候随手做（甚至可并进 WS-HUDI 的 hudi 部分）。

### [ ] WS-P2 — 热点触发的收尾（记录，勿抢跑）
- **权威**：报告 §5.2 + §7 + §9。
- **条目**：
  - **PA-1**（paimon，P2/CPU）：`listPartitionNames/Values` 绕过 `partitionViewCache` → 路由进去；仅省 CPU 重渲染（底层远程已被 SDK partitionCache 挡）。见 [`connectors/paimon.md`](./connectors/paimon.md)。
  - **HP-1/HP-2**（jdbc，P2）：`HP-1` scan 期 `getColumnHandles` 冗余远程取列；`HP-2` 写路径 `buildInsertSql` 新建实例绕开 funnel（正确性中立，BE 逐行 auto-commit）。见 [`connectors/jdbc.md`](./connectors/jdbc.md)。
  - **TRINO-H1/H2/H3**（trino，P2 / **仅 CPU 清理**）：cold 3x `getTableHandle` / 2N `getColumnMetadata` / 2x `applyFilter`。**禁加 L1 缓存**（反指，见下）。见 [`connectors/trino.md`](./connectors/trino.md)。
  - **hive 写后读一致性**（P2，仅 hive）：Doris 对 hive 走粗 REFRESH+TTL，存在 TTL 有界的 read-your-write 窗口 → 给 `CachingHmsClient` 加写路径失效，或写后读走 live/bypass。优先级低（TTL 有界）。
- **依赖**：按真实热点 profile 触发，不预抢。

---

## 3. 反指 / 不立项（记录，勿重开 — 报告 §7）

> 这些**结构上不该**再加 iceberg-式重缓存；记在此防止后续 session 按包名/惯性重新提案。

- **hive** 🚫（重缓存）：已 framework-aligned —— `CachingHmsClient`（表/分区名/分区/列统计 4 缓存）+ `HiveFileListingCache` + `ConnectorPartitionViewCache` + sibling memo + 写事务快照，无 P0/P1 重复远程加载缺口。`HMS-H4`（ACID `getAcidState` 未缓存）是 snapshot/write-id 依赖、legacy-parity 正确，**应保持不缓存**；`HMS-H6` belt-and-suspenders per-statement memo **不划算**（HMS `getTable` 远比 iceberg `loadTable` 便宜）。hive 唯一实活 = doc 修（→ WS-DOC）。
- **trino** 🚫（**主动反指**）：bridge，元数据缓存/失效/split 枚举全委托嵌入式 Trino 连接器（自带 `CachingHiveMetastore` 等）。加 Doris 侧 table/handle 缓存会**双重缓存**并把 schema 跨外部 ALTER 冻结，重引 Trino 已解决的 stale-metadata bug 类；`TrinoTableHandle` 事务派生、transient/不可序列化。只做 P2 CPU 清理（H1/H2/H3）。
- **paimon** 🚫（连接器级 table 缓存）：SDK `CachingCatalog`（默认开）已提供 tableCache/partitionCache/manifestCache，连接器又已恢复 3 个 legacy 语义缓存。加连接器级 table 缓存与 SDK 缓存重复。只有 `PA-1` 一个 P2 一致性清理值得做。
- **jdbc** 🚫（连接器侧 table 缓存）：关系型透传，`planScan` 零远程 IO、恒一个 scan range，无 split/file/manifest/partition/snapshot 层；昂贵元数据（schema/columns/row-count/名录）已被 fe-core cross-query 缓存前置，连接池 per-catalog 单例。加连接器侧 table 缓存**无对象可缓存**。只有 `HP-1/HP-2` 两个 P2。

---

## 4. 已知全局事实（供各 workstream 复用，勿再各自重查）

- **Layer-2 已通用、已承重、无需再做**：per-statement `ConnectorMetadata` funnel（`PluginDrivenMetadata.get`，key `"metadata:"+catalogId`，身份钉 fail-loud）对全部 7 个 `SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute, paimon, iceberg, hms}` 一体适用，由 `tools/check-fecore-metadata-funnel.sh` 强制（0 exempt）。hudi 作为 hms sibling 骑同一 scope。
- **共享底座 `fe-connector-cache` = 统一的天然归宿**：`MetaCacheEntry` / `CacheSpec`（`meta.cache.<engine>.<entry>.*`）/ `CacheFactory` / 泛型 `ConnectorPartitionViewCache`；已被 hive/hms/iceberg/maxcompute/paimon 采用，**hudi 尚未采用**（WS-HUDI 前置）。
- **authz 现状**：8 连接器中**只有 iceberg-REST 声明 `SUPPORTS_USER_SESSION`** 并做 per-user 凭证 vending；其余 7 个均单一 catalog 身份 → 今天加名字为 key 的 cross-query 缓存都 authz-安全（但见铁律 4 的"结构性 vs 当前"警告 + D3）。
