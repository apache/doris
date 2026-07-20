# 每语句表加载归属者 · 跨连接器复核结论 + Trino 重构议题预备

> **本文件定位**：把"把 iceberg 的每语句表加载归属者移植到其它连接器"这件事的**全部调研结论**固化到一处，供后续 session 直接接手。
> **本轮不动任何生产代码**。用户拍板：先把结论记成文档；**重构成 Trino 架构的问题留到下一个 session 专题讨论**（见 §7）。
> 蓝本/地基/铁律见同目录 [`README.md`](../README.md)；进度见 [`progress.md`](../progress.md) / 状态见 [`tasklist.md`](../tasklist.md)。

---

## 0. 一句话结论

- **逐连接器复核（recon + 独立对抗复核，双签）：没有一个连接器现在值得移植。** iceberg 之所以值得，唯一原因是它是本仓库里**唯一迁移了行级写（DELETE/MERGE）**的连接器——行级写让同一张表在一条语句里被"读+扫描+写成形+开事务"反复远端加载 3~5 次，这才是那套改造的靶子。别的连接器要么在 Doris 侧只读、要么仅 INSERT 追加写，没有这个重复加载风暴；现有的跨查询缓存又已把加载压到 ≈1 次。
- **"统一接口标准"其实已经存在**：中性 SPI `ConnectorSession.getStatementScope()` + `ConnectorStatementScope`(带 `NONE` 默认)。每个连接器天生继承，离线自动退化成安全的逐次加载。新连接器**不是没有标准可循**。
- **真正的将来触发点 = paimon 加行级写**。paimon 现在已经是"iceberg 改造前"的形状（有胖句柄 + 多加载 seam），只差写臂；一旦加 UPDATE/DELETE 就会复现风暴，届时移植才有价值，且那是抽取共享 helper 的正确时机（有 iceberg + paimon 两个真实用户）。
- **推荐高度 = L0**（写下约定 + 登记触发点，生产逻辑零改动）；**L1**（抽共享 helper）留到 paimon 加写时；**L2/L3（Trino 式重构）** 是远期、跨切、与删旧代码期铁律 A 冲突的独立项目——**下个 session 专门讨论**。

---

## 1. 背景（给未来读者的最小上下文）

新 SPI 框架下，一条 DML（尤其 DELETE/MERGE）里同一张表被反复远端 `loadTable`：读元数据、扫描规划、写成形、开事务各自加载，最糟一条语句 3~5 次。连接器 session 一条语句内被重建约 26 次，缓存挂它即死。唯一贯穿整条语句的对象是 fe-core 的 `StatementContext`。

iceberg 已通过 **PERF-07** 修掉（commit `97bdcd6bdbe` 建地基 + `ea7fd1f6e7a` 连接器侧）：
- **中性地基（已就位、勿再改）**：`ConnectorStatementScope`(fe-connector-api) + `ConnectorSession.getStatementScope()` + `ConnectorStatementScopeImpl`/`StatementContext` 懒建/重置 + `ConnectorSessionImpl` 构造期捕获 + `ExecuteCommand` 重置(fe-core)。
- **连接器侧范式**：`IcebergStatementScope` helper（`sharedTable` 键 `iceberg.table:catalogId:db:tbl:queryId` + `rewritableDeleteSupply`）+ 四处表解析共享 + 拆胖句柄（删 `IcebergTableHandle.resolvedTable`）+ 下沉跨臂删除暂存（删 `IcebergRewritableDeleteStash` 单例）+ v3 DML under NONE fail-loud。

**移植一个连接器 = 纯连接器侧工作，不再碰 fe-core**（不触铁律 A）。

---

## 2. 逐连接器复核结论（recon + 对抗复核，双签，2026-07-19）

> 方法：每个候选一个深度 recon agent（数一条 DML 真实加载几次 / 现有缓存边界 / 胖句柄 / 跨臂暂存 / 鉴权凭证语义），再配一个**独立对抗复核 agent**（专挑"加载被现有缓存兜住""共享泄漏"两类刺）。以下均为双签、置信=高。

| 连接器 | 有写入面？ | 一条 DML 真实远端加载 | 胖句柄 | 跨臂暂存 | 凭证泄漏 | 结论 |
|---|---|---|---|---|---|---|
| **paimon** | ❌ 未迁移（只读） | DML=0；SELECT≈1 | ✅ 有 `PaimonTableHandle.paimonTable`（载荷型，非纯 memo） | ❌ | 无（目录级单一身份；vend 仅用于下游存储） | **将来候选**（触发点=加行级写） |
| **hive / hms 网关** | ⚠️ 仅 INSERT/OVERWRITE | DML=N/A；写≈0~1 | ❌（纯坐标+扁平标量） | ❌ | 无（连接器级单一身份） | **不必做** |
| **hudi** | ❌ 只读 | DML=0；读侧 ~4-6 次未缓存 metaClient 构建 | ❌ | ❌ | 无 | **不必做** |
| **maxcompute** | ⚠️ 仅 INSERT 追加 | 低（句柄带惰性表代理，写复用不重载） | ❌ | ❌ | 无 | **排除** |
| **es** | ❌ 只读 | 读侧 mapping 有重取（已被 fe-core schema 缓存兜） | ❌ | ❌ | 无 | **排除** |
| **jdbc** | ⚠️ 仅 INSERT 追加 | 低（纯坐标句柄，写复用坐标） | ❌ | ❌ | 无 | **排除** |
| **trino** | ❌ 只读 | 读侧 `getTableHandle` 每 seam 重解析、**最贵且未缓存** | 同句柄内 memo | ❌ | 无 | **排除**（读侧优化候选，另立议题） |

### 关键证据（file:method，便于将来核验）

**hive / hms 网关（不必做，置信高）**
- 无行级写：`HiveWritePlanProvider.supportedOperations()` = `EnumSet.of(INSERT, OVERWRITE)`；full-ACID 写在 `HiveConnectorTransaction.rejectTransactionalWrite`(beginWrite:210) 硬拒。DML 风暴结构性不存在。
- 加载已被**跨查询**缓存兜住：读/写/beginWrite 三处 `getTable` 全走 `CachingHmsClient.tableCache`（跨查询、TTL 24h、按 (db,table) 键）→ 实际远端 ≈0~1 次，是"每语句作用域"的**超集**。已核实缓存**确实接线**（`HiveConnector.createClient:588` = `wrapWithCache(new ThriftHmsClient(...))`；`hms` 在 `CatalogFactory.SPI_READY_TYPES`）——代码里多处 "dormant/未进 SPI_READY_TYPES" 的 javadoc 是 cutover 后的**过期注释**（doc-hygiene 待清）。
- 无胖句柄（`HiveTableHandle`=纯坐标+扁平标量）；无跨臂暂存（`HiveReadTransactionManager` 是 ACID 读锁生命周期管理器，非扫描→写桥）。
- **网关委派已查清**：网关只对普通 hive 表自己加载；遇 iceberg/hudi-on-HMS 表只做 **1 次廉价格式探测加载**（`getTableHandle:343`）后委派兄弟连接器，兄弟自带作用域——**网关自己不需要任何作用域**，也不得重复包裹委派加载。

**paimon（将来候选，置信高）**
- **写未迁移**：`PaimonConnector` 不 override `getWritePlanProvider`（→ null → `supportedWriteOperations()` 空 → INSERT/DELETE/MERGE 全拒）；代码注释 `PaimonConnector.java:294` 明写 "paimon write is not migrated"。
- 加载已被句柄 memo + SDK 缓存压到 ≈1：`getTableHandle:185` 单次 `catalogOps.getTable` 后 `setPaimonTable` 存到 transient 句柄；后续 ~8 处 `resolveTable`/`resolveScanTable` 走快路径。跨查询由 paimon SDK `CachingCatalog` 去重。
- **胖句柄存在**：`PaimonTableHandle.paimonTable`(transient, PaimonTableHandle.java:89) 是 `IcebergTableHandle.resolvedTable` 的直接同款。但**是载荷型非纯 memo**：`withBranch` 故意置 null 让分支重载不同表、`withScanOptions` 拷贝它、承担查询内 Table 实例一致性——现在删它换作用域 map 是**平移（等价功能、无减载）+ 有回归面**。
- 无跨臂暂存（无写臂）。鉴权=目录级单一身份；vended 凭证只用于下游 per-scan-range 存储读、不烘进 Table 对象，故共享**不泄漏**。

**hudi（不必做，置信高）**
- 只读：不 override 写入面，`beginTransaction`(HudiConnectorMetadata.java:397) 抛 "Hudi tables are read-only"；INSERT/DELETE/MERGE 在准入门被拒。
- 5 步模板 4 步空操作。唯一真实重复成本=读路径 ~4-6 次**未缓存的 `HoodieTableMetaClient` 构建**（每 seam 从 basePath 重建）——但**形状不对**（不是"表加载归属者"）、**metaClient 可变**（`getSchemaFromMetaClient` 调 `reloadActiveTimeline()`）、**鉴权/TCCL 上下文错配**（metadata 臂在 `metaClientExecutor.execute` 的 doAs 里、planScan 在 fe-core 扫描线程内联无 doAs），且主成本（FileSystemView / MDT `getAllPartitionPaths`）压根不是表对象加载。

**maxcompute·es·jdbc·trino（排除，对抗复核确认非橡皮图章）**
- 均无行级 DML：es/trino 只读、jdbc/maxcompute 仅 INSERT-append 且写成形复用句柄/坐标不二次加载。无跨臂暂存、无 v3 复活风险、凭证目录级。
- 诚实指出：**读路径确有"每 seam 重解析"未缓存**——trino 最重（`getTableHandle`+`getColumnHandles`+N×`getColumnMetadata` 每 seam 重跑）、es 次之（mapping 重取）。但这正是蓝本明确打折的"读侧、收益有限、已被 `SchemaCacheValue` 兜一部分"那类，且缺写侧/暂存/胖句柄的架构收益。**trino 标记为最弱排除**——若将来单独立"读侧去重"新任务，它是这组里唯一有实打实重解析成本的。

### 一个不改结论的开放问题
- paimon/hive 的句柄是否跨 fe-core 边界被重建（使加载数从 1 涨到 2-3）尚未 trace 确认。但即便最坏情况，那些多出来的加载**也全部命中现有跨查询缓存**，不改变任何"不必做"的结论。

---

## 3. 核心洞察：iceberg 为什么独特

那套改造 5 步（① sharedTable ② 四处 seam ③ 拆胖句柄 ④ 下沉暂存 ⑤ fail-loud）的价值，几乎全押在 **③④⑤ 依赖"行级写"** 上：

- **只有 iceberg 迁移了行级写（DELETE/MERGE）**，它同时读 + 写同一张表，才有"写成形×N + beginWrite"这些绕过读缓存的加载臂，以及"扫描填、写读"的跨臂删除暂存。
- 其它连接器：paimon/hudi/es/trino **只读**、hive/hms **仅 INSERT/OVERWRITE**、jdbc/maxcompute **仅 INSERT-append**。对它们 ①② 要么冗余（现有缓存已兜）、③④⑤ 基本是空操作。

因此"逐连接器复核判不必做"不是保守，而是**iceberg 本就是特例**。

---

## 4. 架构统一性分析（回答用户"是否有必要改造和统一"）

### 4.1 统一接口其实已存在
Doris 版的统一契约 = 中性 SPI `ConnectorSession.getStatementScope()` + `ConnectorStatementScope`（`computeIfAbsent(key, loader)` + `NONE` 默认）。**每个连接器天生继承，离线安全退化。** `IcebergStatementScope` 只是 40 行的连接器私有便利包装。所以"新连接器没有统一标准"这个担心其实不成立——标准已发布，缺的只是"更强的强制/便利外壳"。

### 4.2 Trino 参照模型：统一靠"引擎自持的每事务生命周期"，不是一个缓存类
- Trino 每条语句都是一个事务；引擎为每个被访问的 catalog **懒造一个"每事务 `ConnectorMetadata`"实例并 memoize 到事务上**（`IcebergTransactionManager`/`HiveTransactionManager` 内的 per-catalog `MemoizedMetadata`），commit/rollback 时销毁。
- 连接器把加载到的表**缓存在这个实例上**（iceberg `IcebergMetadata.tableMetadataCache`、hive per-transaction metastore 的 `tableMetadataCache`）。`getTableHandle`/`getTableMetadata`/`getColumnHandles`/统计/`beginInsert`/`finishInsert` 都重入**同一个** metadata → 命中缓存 → 远端 `loadTable` 只发一次。
- **统一的是"共享生命周期"（去重的 span=事务、活满 span 的对象=metadata 都由引擎供给和销毁），"cache-on-metadata" 是自然掉出来的约定**；新连接器实现 `ConnectorMetadata` 即免费获得。

### 4.3 为什么 Trino 模型不可直接移植到 Doris
Doris 缺 Trino 的每一个结构前提：
- **无每事务 `ConnectorMetadata`**：Doris 连接器/`*ConnectorMetadata` 是**长期共享单例**（每 catalog 一个、从不按语句造），挂每语句可变缓存会跨语句/跨用户泄漏（正因如此跨查询 `IcebergTableCache` 对 `session=user`/vended 关闭）。
- `ConnectorSession` **不是稳定 span**（一条语句重建约 26 次，挂它即死）。
- **DML 事务对象出现得晚、且只在写臂**（`*ConnectorTransaction` 在 beginWrite 才建），无法承载"读+扫描+写成形"整条 span。
- **唯一活满整条语句、连接器够得到的对象 = fe-core `StatementContext`**，只能经中性 `getStatementScope()` 够到。所以 Doris 必须把 Trino 模型**反过来**：不是"引擎把每事务 metadata 发给连接器"，而是"连接器向上够 fe-core 的 span"。
- 全盘照搬会撞铁律 A：给 14 个连接器引入每事务 metadata + TransactionManager 生命周期 + 改 planner 每语句取 metadata = 删旧代码期的 fe-core 大净增。

**结论：现有 `ConnectorStatementScope` + `getStatementScope()` + `NONE` 已是 Trino 思想在 Doris 铁律下能落地的最高、最可移植高度。**

### 4.4 高度分级（L0–L3）
| 高度 | 内容 | 现在值不值 |
|---|---|---|
| **L0** | 只把统一**约定写成文**（键格式 `<类型>.table:catalogId:db:tbl:queryId`、NONE=逐次加载、扫描→写跨臂在 NONE 下 fail-loud）+ 登记触发点；生产逻辑零改动 | ✅ **推荐（现在做）** |
| **L1** | 把 iceberg 私有 helper 提升为 `fe-connector-api` 共享 util、迁移 iceberg 6 处调用（纯连接器侧、fe-core 不动、不碰铁律 A） | ⚠️ 留到 paimon 加写（第 2 个真实用户）时 |
| **L2** | 每语句基类 `ConnectorMetadata` / 契约方法连接器 override | ❌ 需引入每语句 metadata=迷你 Trino 重构 or 只是更重的 L1；碰铁律 A、影响刚稳定的读热路 |
| **L3** | Trino 式每事务 metadata 全重构 | ❌ 远期、跨切、明确碰铁律 A、需独立立项 + 单独签字 |

---

## 5. 将来触发点：paimon 加写（证据）

- **paimon 的"写未迁移"是真实的、被推迟的路线项**：老的 JNI-writer INSERT 写栈（commit `6c6c9a4b6cd`：`datasource/paimon/PaimonTransaction`、`planner/PaimonTableSink`、`nereids/.../PaimonInsertExecutor`、`transaction/PaimonTransactionManager` + BE `paimon_table_sink_operator`/`PaimonJniWriter`）在**删旧代码期被整体删除、未搬进新 SPI 连接器**。将来的 paimon 写会在写 SPI 上**全新构建**，且 UPDATE/DELETE 是真正新增（老实现只有 INSERT）。
- **paimon 现在就是"iceberg 改造前"的形状**：胖句柄 `PaimonTableHandle.paimonTable` + 多加载 seam（4 文件，所有连接器最多）。**唯一缺写臂。**
- **一旦加行级 UPDATE/DELETE**：长出"写成形 + beginWrite"两条加载臂 + （若 merge-on-read）"扫描填、写读"的删除暂存 → **完整复现 iceberg 3~5 次加载风暴 + 跨臂暂存需求**。届时移植对 paimon**真正有价值**（架构连贯 + 溶掉胖句柄 + 给暂存一个自然的家），且有 iceberg + paimon 两个真实用户来把共享 helper 的接口形状定对。

---

## 6. 共享 helper 落点（若将来做 L1）

- **家 = `fe-connector-api`**（`ConnectorStatementScope`/`ConnectorSession` 已在此；所有连接器都依赖它；是依赖图最低公共祖先；**连接器侧、不碰铁律 A**）。`fe-connector-spi`/`fe-connector-metastore-api` 在其之上，只依赖 api 的连接器看不见，排除。
- **签名**（一个真原语 + 一个键约定便利）：
  ```java
  static <T> T computeIfAbsent(ConnectorSession s, String key, Supplier<T> loader) {
      return s == null ? loader.get() : s.getStatementScope().computeIfAbsent(key, loader);
  }
  static <T> T sharedTable(ConnectorSession s, String typePrefix, String db, String tbl, Supplier<T> loader) {
      if (s == null) return loader.get();
      String key = typePrefix + ":" + s.getCatalogId() + ":" + db + ":" + tbl + ":" + s.getQueryId();
      return s.getStatementScope().computeIfAbsent(key, loader);
  }
  ```
- **iceberg 迁移代价 = 6 处调用 + 删 `IcebergStatementScope`(85 行)**：`sharedTable` 4 处（`IcebergConnectorMetadata:613` 读 / `IcebergConnectorTransaction:211` beginWrite / `IcebergWritePlanProvider:697` 写成形 / `IcebergScanPlanProvider:2274` 扫描）；`rewritableDeleteSupply` 2 处（`IcebergScanPlanProvider:674` 填 / `IcebergWritePlanProvider:193` 读）。
- **注意**：删除暂存那半只能**结构性**泛化（值类型 `Map<String,List<TIcebergDeleteFileDesc>>` 是 iceberg thrift、"NONE 不桥接→v3 fail-loud"语义是 iceberg 专属）——泛化时把 iceberg 专属键串 + fail-loud 守卫留在 iceberg 调用点即可。**只有 table-share 那半是真连接器中性。**

---

## 7. 下一步：下个 session 讨论"重构成 Trino 架构"（L2/L3）—— 预备材料

> 用户 2026-07-19 指示：本轮先记文档；**重构成 Trino 架构的问题下个 session 专题讨论**。以下是给那次讨论的种子，避免从零起。

### 7.1 Trino 架构到底要引入什么（对照 §4.2/§4.3）
1. **每语句/每事务 metadata 实例**：把 `*ConnectorMetadata` 从"每 catalog 共享单例"改成"每语句（或每事务）新建、由引擎 memoize+销毁"。
2. **一个 span 生命周期管理器**（对应 Trino `TransactionManager`）：即便裸 SELECT 也要有一个活满整条语句的 metadata 宿主；Doris 现成的 span 宿主是 `StatementContext`。
3. **planner 改造**：Nereids/规划各阶段改成"经 span 取当前语句的 metadata 实例"，而非直接调共享连接器。
4. **连接器改造**：把表/schema/统计缓存从"跨查询共享缓存 + transient 胖句柄"迁到"挂 per-statement metadata 实例"。

### 7.2 必须先摆平的硬冲突（讨论要点）
- **铁律 A（删旧代码期 fe-core 只减不增）**：上述 1/2/3 几乎全是 fe-core 净增 + planner 改造。→ 讨论：是否等删旧代码期结束再做？是否需要用户单独签字放行？
- **读热路径刚稳定**（PERF-01~06、PERF-11）：per-statement metadata 会重排读热缝，回归面大。
- **凭证/多租户语义**：per-statement 实例天然按语句隔离（=按用户），能顺带解决"跨查询缓存对 session=user/vended 关闭"的历史包袱——这是 L3 相对现状的**真正架构收益**，值得作为讨论的正面理由。
- **收益 vs 成本的诚实定位**：现状"连接器向上够 span"已拿到 90% 的去重收益；L3 的增量主要是**架构连贯 + 多租户缓存归一 + 为将来大量写连接器铺路**，而非单点性能。要不要为这些付"跨切重构 + 独立项目"的价，是用户的战略决定。

### 7.3 建议的讨论产出
- 一份"Doris 每语句/每事务 metadata 重构"的**可行性 + 分期**设计（可否分"先 span 宿主、后逐连接器迁移、最后退役共享单例"三期落地，避免一次性大爆炸）。
- 明确触发条件（如：删旧代码期结束 / 写连接器数量到某阈值）。

---

## 8. 参考

- 蓝本权威设计：`plan-doc/perf-hotpath-iceberg/designs/FIX-PERF-07-unified-per-statement-table-owner-{design,summary}.md`。
- 代码范本：`fe-connector-iceberg/.../IcebergStatementScope.java`（commit `ea7fd1f6e7a`）；地基 commit `97bdcd6bdbe`。
- 架构记忆：`iceberg-table-resolution-cache-scoping`（缓存作用域纪律 + Trino 协同 + "全高度留远期"）。
- 本轮调研原始返回（每 agent 完整结论）：workflow journal
  `.../subagents/workflows/wf_e89cf92e-ff3/journal.jsonl`（逐连接器 recon+对抗复核）、
  `.../subagents/workflows/wf_4802a3d2-1c9/journal.jsonl`（placement / onboarding / Trino-altitude 三专项）。
- 公开 tracking issue：apache/doris#65185。
