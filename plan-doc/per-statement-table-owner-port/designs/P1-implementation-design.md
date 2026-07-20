# P1 实现设计（seam-by-seam）—— 引擎自持每语句 ConnectorMetadata 实例

> **范围 = 仅 P1 键石**：让一条语句、一个 catalog(一个属主连接器)在读/扫描/DDL/MVCC 路径上**恰好复用一个 memoized `ConnectorMetadata` 实例**,`connector.getMetadata` 保持纯工厂、唯一 memo 在 fe-core 管道,语句末**确定性 close**。写路径(7 缝)与工作集下沉、按身份分片缓存分别留 P3/P2/P4。
> **本文仍不动代码**:是"可动工蓝图 + commit 切分"。目标架构全景见 [`trino-parity-metadata-redesign-design.md`](./trino-parity-metadata-redesign-design.md)。
> 全部 `file:method:line` 来自本轮只读 grounding(workflow `wf_7e537094-44f`,已核实)。

---

## 0. P1 成功判据

- 一条查询里,对同一 `(catalogId, 属主连接器)`,`connector.getMetadata(session)` 只被真正调用**一次**;读/扫描/DDL/MVCC 的所有 seam 复用该实例。
- 该实例在**查询结束、BE/pump 静默之后**被确定性 `close()`(P1 里 close 仍是 no-op,只验证接线正确);预编译 EXECUTE 重执行不泄漏;异构 HMS 网关下每 sibling 一实例。
- **P1 字节中性**:`NONE`(离线/测试/无 ConnectContext)下逐次工厂=今日行为;perf delta≈0(语句内去重本就有,收益=单漏斗 + 无 per-connector memo + 确定性生命周期)。
- 守门:见 §7。

---

## 1. 新增 / 改动的 SPI 与 fe-core plumbing（精确签名）

### 1.1 `ConnectorStatementScope`（fe-connector-api,加一个默认方法)
现状:`<T> T computeIfAbsent(String key, Supplier<T> loader)` + `NONE`(`connector/api/ConnectorStatementScope.java:45,48`)。
**加**(默认方法,`NONE` 天然逐次不缓存):
```
// 便利入口:类型化 metadata memo；默认经 computeIfAbsent,故 NONE 下逐次跑 factory
default ConnectorMetadata getOrCreateMetadata(String key, Supplier<ConnectorMetadata> factory) {
    return computeIfAbsent(key, factory);
}
// 语句末确定性关闭；默认 no-op,故 NONE 恒惰性
default void closeAll() { }
```
> 值仍以 `Object` 存(fe-core 不认连接器类型,守 connector-agnostic 铁律)。

### 1.2 `ConnectorStatementScopeImpl`（fe-core)
现状:`ConcurrentHashMap<String,Object>` + `computeIfAbsent`(`connector/ConnectorStatementScopeImpl.java:38-44`)。
**加** `closeAll()`:遍历 values,对 `Closeable`/`AutoCloseable` 者 best-effort 关闭(log-and-continue,仿 `QueryFinishCallbackRegistry` 隔离);**幂等**(关一次后清标记/清表,重复调用无害——因 close 会从多处触发,见 §4)。

### 1.3 静态 funnel（fe-core,新类 `PluginDrivenMetadata`)
```
public static ConnectorMetadata get(ConnectorSession session, Connector connector) {
    ConnectorStatementScope scope = session.getStatementScope();          // 默认 NONE
    String key = "metadata:" + session.getCatalogId();                    // 普通连接器
    return scope.getOrCreateMetadata(key, () -> connector.getMetadata(session));
}
```
- `NONE`(无 ConnectContext/StatementContext)→ `getOrCreateMetadata` 走 `computeIfAbsent` 的 NONE 实现 → **逐次 `connector.getMetadata(session)`,零跨语句缓存**(off-thread loader 即便误路由也安全)。
- `session` 仍要照常构造(后续 `metadata.xxx(session, …)` 要它);**只 memoize `getMetadata` 的结果**。
- 证据:`session.getCatalogId()` 恒有(`buildConnectorSession` 一律 `withCatalogId(getId())`,`PluginDrivenExternalCatalog.java:1118`);`ConnectorSession.getCatalogId():69`、`getQueryId():31`。

### 1.4 `ConnectorMetadata.close()`（已存在,无需新增)
`ConnectorMetadata extends 6 Ops + Closeable`,默认 `close() throws IOException` 为 no-op;**无连接器 override、无 fe-core 调用点**(`connector/api/ConnectorMetadata.java:248`)→ 开始 memoize + 在 closeAll 里调它是**生命周期安全**的。P1 不改各连接器 close(仍 no-op),只验证接线。

### 1.5 close 回调注册(见 §4;不新开 `StatementContext.close` 步)

---

## 2. 66 处 seam 的改道规则

| 组 | 位置 | 数量 | P1 处置 | 说明 |
|---|---|---|---|---|
| **A 读** | `PluginDrivenExternalTable`(13)+`ExternalDatabase.getLocation`(1) | 14 | **改道 funnel** | 均 on-thread;connector=`getConnector()`,session=`buildConnectorSession()` |
| **A/排除** | `PluginDrivenExternalTable.getChunkSizes:1087` | 1 | **保持直调(fallback)** | 统计分析线程,常无 ConnectContext |
| **B DDL** | `PluginDrivenExternalCatalog` DDL(19)+`tableExist`(1)+name-mapping(2) | 22 | **改道 funnel** | on-thread;connector=字段 |
| **B/排除** | `listDatabaseNames:295`、`listTableNamesFromRemote:311` | 2 | **保持直调** | name-cache loader,off-ConnectContext |
| **C 扫描** | `PluginDrivenScanNode`(create:205 + 8 处 per-method) | 9 | **改道 + 存字段** | 见 §3 |
| **D 写** | PhysicalPlanTranslator×2 / InsertExecutor / BindSink×2 / IcebergMergeSink / IcebergRowLevelDmlTransform | 7 | **P1 不动,留 P3** | 见 §6(read-vs-write 复用) |
| **E MVCC** | `PluginDrivenMvccExternalTable`(143/358/725) | 3 | **改道 funnel**(带 null-ctx fallback) | 主 on-thread,少数后台 metadata-table 扫描 |
| **F 排除** | `initSchema:460`、`getColumnStatistic:1052`、`fetchRowCount:1153` | 3 | **保持直调** | 跨语句缓存 loader,off-ConnectContext |
| **misc** | ShowPartitions/ConnectorExecuteAction/CallExecuteStmt/JdbcQueryTVF | 4 | **改道 funnel** | on-thread 命令/TVF |
| **misc/排除** | `MetadataGenerator.dealPluginDrivenCatalog:1329` | 1 | **保持直调** | BE 驱动 fetch,可能无 ConnectContext |

- **改道机械动作**:`ConnectorMetadata m = connector.getMetadata(session);` → `ConnectorMetadata m = PluginDrivenMetadata.get(session, connector);`(connector/session 每处都在作用域)。
- **排除项本质安全**:funnel 在 `NONE`/null-ctx 下本就 fallback 直调;但排除项**保留显式直调**更清晰、且避免误把跨语句 loader 的结果塞进某个残留 scope。
- **假阳性**:`datasource/test/TestExternalCatalog.java` 的 8 处 `catalogProvider.getMetadata()` 是测试用静态 Map,非 SPI,勿动。
- **negative**:`planner/PluginDrivenTableSink.java` 无 `getMetadata` 调用(sink 从 translator 收 session+handle),D 组工作全在 PhysicalPlanTranslator。

---

## 3. 扫描节点（组 C）：存字段去 per-method 重取

`PluginDrivenScanNode` 已把 `connector`(:147)、`connectorSession`(:148)、`currentHandle`(:151) 存字段(构造期赋值 190-192,create 建 203-205)。
- **加**懒字段:`private ConnectorMetadata cachedMetadata;` + `private ConnectorMetadata metadata(){ if(cachedMetadata==null) cachedMetadata = PluginDrivenMetadata.get(connectorSession, connector); return cachedMetadata; }`。
- 8 处 per-method 重取(`convertPredicate:805`/`tryPushDownLimit:845`/`tryPushDownProjection:864`/`pinMvccSnapshot:909`/`pinRewriteFileScope:1054`/`pinTopnLazyMaterialize:1073`/`buildColumnHandles:1889`/`buildRemainingFilter:1926`)一律改调 `metadata()`。
- **懒**(而非 create 传入):`JdbcQueryTableValueFunction.getScanNode` 直构造节点、手里无 metadata。
- 8 处都跑在**单规划线程**(在并发 `appendBatch` 之前),但镜像现有 `volatile resolvedScanProvider` 模式加 `volatile` 更稳(off-path 若调 `metadata()` 防竞态)。

---

## 4. 确定性 close 接线（P1 最关键、含红队盲区①②的确切修法）

> **⚠ 已更正（2026-07-19，workflow `wf_9250330b-e81` 取证 + 已实现于 commit `12f3e95239b`）**：本节原方案"注册点 = scope 首次创建时（`getOrCreateConnectorStatementScope`）"**会泄漏**——`captureStatementScope` 在**每次 on-thread 会话构建**就急切建 scope，而 DDL/`SHOW`/`DESCRIBE`/`EXPLAIN`/前台 `ANALYZE` 走 `Command.run` 建了 scope 却**永不到 `unregisterQuery`**，回调（连带 scope）永留无 TTL 的注册表。已改为**两层关闭**：
> - **主关闭**：注册点 = `PluginDrivenScanNode.getSplits`（挨着现有 read-txn 回调、同 queryId 键、对象捕获 `scope::closeAll`、跳过 `NONE`）。getSplits 只对**走协调器**的扫描/写/游标取数/内部查询触发，全部到 `unregisterQuery` → 无注册表泄漏、pump 静默后触发。
> - **兜底关闭**：`StatementContext.close()` 加 `isReturnResultFromLocal` 守卫的 closeAll，覆盖不走协调器的 `Command` 语句（直连经 `ConnectProcessor.executeQuery` finally 已调 close；转发主节点在 `proxyExecute` 补 finally 调 close）。arrow-flight（异步返回）经守卫延后到其自身 query-finish close，不被提前关。
> - **重置**：`resetConnectorStatementScope()` 改"先 closeAll 再置空"；`handleQueryWithRetry` 每次重试前重置 → 预编译 EXECUTE / 重试复用的 StatementContext 不会 memoize 进已关闭 scope。
> 幂等（closeAll close-once）保证主+兜底双触发时恰好关一次。下面 §4.1/§4.2 保留原始分析（含为何不用 `StatementContext.close` 作**唯一**钩子的红队盲区①），但**注册点以本更正为准**。残留风险见 expanded-scope 文档。

### 4.1 钩子 = 现有 query-finish 回调,**不是** `StatementContext.close()`
- 注册:`QeProcessorImpl.registerQueryFinishCallback(String queryId, Runnable)`(`QeProcessorImpl.java:216`)。
- 触发:`unregisterQuery` → `QueryFinishCallbackRegistry.runAndClear(DebugUtil.printId(queryId))`(`:212`),在 `StmtExecutor.finalizeQuery`(`:1034`,`updateProfile(true)` 之后)里调;而 `coordBase.close()`(`handleQueryStmt:1582`)已先驱动 `Coordinator.close:817`→`FileQueryScanNode.stop:695`→`splitAssignment.stop()` **栅栏住 off-thread pump**。⇒ 回调**严格在 BE drain + pump 栅栏 + profile 等待之后**触发,每 queryId 一次。
- key 对齐:`connectorSession.getQueryId()` = `DebugUtil.printId(ctx.queryId())`(`ConnectorSessionBuilder.from:75`),与 `runAndClear` 同 key。
- 先例:hive 读事务释放 `buildReadTransactionReleaseCallback`(`PluginDrivenScanNode.java:687`,在 `getSplits:1207` 无条件注册)——`scope.closeAll()` **完全镜像它**(含 `onPluginClassLoader` TCCL pin,若 closeAll 触及连接器加载对象)。
- 为何 `StatementContext.close()` 错(盲区①):它只 `releasePlannerResources()`(`StatementContext.java:979`,故意不碰 scope,注释 :205);它从 `ConnectProcessor.executeQuery:410` 在 execute() 之后跑、无对 BE/pump 的栅栏;SQL-cache 路径 `parseFromSqlCache:469` 只 `releasePlannerResources` **不 close**;**EXECUTE 复用一个 StatementContext 跨多次执行、只在最后 close** → 挂 close() 会漏掉每次中间执行的 scope。

### 4.2 注册点 = **scope 首次创建时**(修红队未点透的覆盖漏洞)
- grounding 指出:读事务回调注册**只在 `getSplits`**,而 `getSplits` **只有扫描语句到达**;写/纯元数据/DDL/EXPLAIN 语句在 `captureStatementScope`(`:202`)创建了 scope 却从不扫描 → 若照抄"getSplits 注册",这些语句的 `closeAll` **永不注册**。
- **修**:在 `StatementContext.getOrCreateConnectorStatementScope()`(`:656`,scope 懒建的唯一入口)里,首次建 scope 时**向 `QeProcessorImpl` 注册一次** `queryId → scope.closeAll()`。⇒ 任何触连接器的语句都恰好注册一个 closeAll。
- **幂等**:多张表/多 session 共享一个 scope、重试(`handleQueryWithRetry` 换 queryId 重跑 `getSplits`)都可能多次触发 → `closeAll()` 必须 **close-once**。

### 4.3 EXECUTE 复用(盲区③)
- `ExecuteCommand.run`(`:95`)每次执行调 `resetConnectorStatementScope()`(`StatementContext.java:669`,当前**只置空不 close**)。**修:改成先 `scope.closeAll()` 再置空**——作为兜底(某次执行的 scope 若在 analysis 阶段建、但计划短路没走到注册/触发)。因主 query-finish 回调可能已关同一实例,再次印证 closeAll 必须幂等。

### 4.4 取消/超时(盲区④)
- 反 pump 由 `Coordinator.close()`(取消/超时走 `:1396`)→`scanNode.stop()`→`splitAssignment.stop()` 栅栏;`SplitSourceManager` 另有 GC-based WeakReference reaper 兜底(`:72`)。close 回调走 query-finish、在 coord.close 之后,故**已在 pump 栅栏之后**,无需额外机制;仅需保证异常路径也走到 `unregisterQuery`(现有 finally 已覆盖)。

---

## 5. HMS 异构网关 sibling 扇出（组内特例,gateway 现已 LIVE）

> **事实纠偏**:hms 已在 `SPI_READY_TYPES`(commit `83585fd5097`,2026-07-17,`CatalogFactory.java:56`),网关**服务真实查询**;代码里 ~20 处 "dormant until hms enters SPI_READY_TYPES" 注释是**过期**的。

- 网关行为:`HiveConnectorMetadata.getTableHandle:344` 按格式探测,ICEBERG/HUDI 表**转发 sibling**、返回 sibling 原生 handle;之后每个方法按 `instanceof HiveTableHandle` 判别,非 hive handle 经 `siblingMetadata(session,handle)=siblingOwnerResolver.apply(handle).getMetadata(session)`(`:292`)转发,**~43 处 per-handle 点每次新建 sibling metadata**。
- owner 解析:`HiveConnector.resolveSiblingOwner(handle)`(`:190`)按 `ownsHandle` 三路派发**已建的** volatile sibling 字段(iceberg 先于 hudi),孤儿 handle fail-loud。每网关**恰好一个** iceberg + 一个 hudi sibling(`getOrCreateIcebergSibling:488` DCL)→ sibling Connector **对象身份稳定**。
- **坑**:三连接器**共享一个 `catalogId`**(`createSiblingConnector` 传 `this` context,`DefaultConnectorContext.java:174`)→ **catalogId 单独做 key 会把三连接器塌成一个 metadata、派错**。
- **修**:
  1. **funnel key 加属主 label**:`"metadata:" + catalogId + ":" + owner`,`owner ∈ {hive,iceberg,hudi}`,**在 `resolveSiblingOwner` 返回后按命中臂取 label**(非预解析、非 identityHashCode)。
  2. **sibling getMetadata 也进 funnel**:`HiveConnectorMetadata.siblingMetadata`/`icebergSiblingMetadata`/`hudiSiblingMetadata` 里,先解析属主 Connector(照旧、可 fail-loud),再 `scope.getOrCreateMetadata(key(catalogId, ownerLabel), () -> owner.getMetadata(session))` → getTableHandle 的 by-TYPE 转发与后续 per-handle 转发**共享一个** sibling metadata/语句。
  3. **只存/返回 `ConnectorMetadata` 接口**,绝不 cast 具体类型(跨 loader CCE)。
  4. 补异构网关 e2e(`hms-iceberg-delegation-needs-e2e`,e2e 在 `external_table_p2/refactor_catalog_param`)。
- 说明:scan/write/procedure provider 是 Connector 级(不在 getMetadata funnel),scan provider 已 per-handle memoize(PERF-11/C14);它们只共用 `resolveSiblingOwner` 的身份解析。
- **注意**:sibling funnel 调用在**连接器内部**(fe-connector-hive),fe-core 的 arch 门禁看不见 → 门禁只管 fe-core 两文件(§8);sibling 一实例由连接器侧单测守(§7)。

---

## 6. read-vs-write 复用决策：P1 只做读侧,写侧留 P3

- 风险:部分连接器期望**每事务 metadata**(trino-connector 已用 `getMetadata(session, txn)` 2 参重载,`TrinoConnectorDorisMetadata.java:104`);把一个 metadata 实例跨读(扫描)+写(sink/insert)共享,可能违反事务语义。
- **P1 决策:组 D(7 处写 seam)不改道,留 P3**。理由:
  - P1 里 metadata 仍是**无状态外壳**(工作集 P2 才下沉),故"读侧共享一个实例"对写侧无影响;写侧维持现状(`PluginDrivenInsertExecutor` 已把 `writeOps` 按写语句自缓存,`ensureConnectorSetup:206`)。
  - 把"读+写同一实例/事务"的语义留给 P3 与事务折叠**一起**设计(那时才把 `ConnectorTransaction` 并入实例),避免 P1 就踩事务语义。
- ⇒ **P1 的"一语句一实例"覆盖读/扫描/DDL/MVCC;写臂在 P3 汇入。** 这是干净的增量。

---

## 7. 守门与测试（全有现成模板）

- **加载计数守门(连接器侧,iceberg)**:仿 `IcebergStatementScopeTest`(`:51-103`)——loader 内 `AtomicInteger`,共享=1、`NONE`=2;远端 loadTable 计数用 `RecordingIcebergCatalogOps.log`。连接器侧**须 copy `TestStatementScope`**(不能 import fe-core,bash 门禁会挂)。
- **跨 catalog / 跨 queryId 隔离**:同文件两个 `ScopeSession` 仅 catalogId / queryId 不同,断言非同实例(已有 `differentCatalogIdIsolatesTheLoad`/`differentQueryIdIsolatesTheLoad`)。
- **fe-core 侧 funnel + 预编译无泄漏**:仿 `ConnectorStatementScopeTest`(`:35-94`),真 `StatementContext` + `ConnectorStatementScopeImpl`;`resetConnectorStatementScope` 后断言换实例且旧值不存;**加断言 closeAll 被调**(用可数 close 的假值)。
- **HMS sibling 一实例 + 扇出不重载**:仿 `HiveConnectorSiblingTest`(`:67-107`,`buildCount==1`/close-once/fail-loud-not-memoized);驱动 getTableHandle→scan/write 转发,断言 sibling 只 loadTable 一次(`RecordingIcebergCatalogOps.log` size)。
- **close-once / 游标取数 / 转发 / 重试**:P1 净新。可数 close 的 scope 值,经批式 pump(`PluginDrivenScanNodeBatchModeTest` 风格)驱动,断言早终止/异常路径 close 恰一次。
- **NONE 离线**:`ConnectorSessionImpl` ctor 默认 NONE、`captureStatementScope` 无 ctx 返回 NONE → 离线测试自动落 NONE。

---

## 8. arch 门禁（enforce invariant 2,非仅约定）

- 机制 = **bash grep 门禁**(仿 `tools/check-connector-imports.sh`,同风格 + 自测),**非 ArchUnit**(classpath 无,勿引)。
- 规则:grep fe-core 的 `connector.getMetadata(` 调用形,**只允许** `PluginDrivenExternalCatalog.java` 与 `PluginDrivenExternalTable.java`(fe-core 里唯二合法 funnel 载体;funnel 自身 `PluginDrivenMetadata` 内部那一处 factory lambda 也在允许名单)。
- 排除项(§2 的 7 处直调 loader)要么走 funnel 的 null-ctx fallback、要么在门禁里显式白名单(建议白名单,保留显式直调语义)。
- 落点:`fe/fe-connector/pom.xml` 现有 exec 只扫 fe-connector 根;fe-core 域的新规则需**自己的 exec execution**,或用 checkstyle `Regexp`(checkstyle.xml 已用 Regexp 模块)限定 `org.apache.doris.datasource.plugin`。
- 正则须锚定调用形,别误伤 API 定义处与 `getMetadataTableRows`。

---

## 9. 未决 / 需你确认点

1. **写侧留 P3**(§6):P1 不改 7 处写 seam,读侧先"一语句一实例"。认可否?(替代:P1 也把写 seam 改道,但要先答 read-vs-write 事务语义——我不建议在 P1 冒这个险。)
2. **注册点 = scope 创建时**(§4.2):在 `getOrCreateConnectorStatementScope` 里向 query-finish 注册 closeAll(覆盖写/DDL/EXPLAIN)。认可否?(替代:只在 getSplits 注册=会漏非扫描语句。)
3. **排除项处置**(§2/§8):7 处 off-ctx loader **保留显式直调 + 门禁白名单**,而非依赖 funnel 的 null fallback。认可否?
4. **P4 残留泄漏威胁模型**(总设计 §8.3⑤):属独立安全签字,不阻塞 P1。确认 P1 期间**不动** `SchemaCacheValue`/`latestSnapshotCache` 等缓存门禁。

---

## 10. P1 落地顺序（建议 commit 切分）

1. **C1 地基**:`ConnectorStatementScope.getOrCreateMetadata/closeAll` 默认方法 + `ConnectorStatementScopeImpl.closeAll`(幂等)+ `PluginDrivenMetadata` funnel。含 fe-core 单测(memo/NONE/隔离)。
2. **C2 close 接线**:`getOrCreateConnectorStatementScope` 注册 query-finish closeAll + `resetConnectorStatementScope` closeAll-before-null。含 close-once / EXECUTE / 取消 用例。
3. **C3 读侧改道**:组 A/B/E/misc(排除 §2 标注项)+ 扫描节点组 C 存字段。含加载计数守门。
4. **C4 HMS sibling**:key 加属主 label + sibling getMetadata 进 funnel。含 sibling 一实例守门 + 异构网关 e2e。
5. **C5 arch 门禁**:bash/checkstyle 规则 + 自测。
- 每个 commit 独立可编译、可回退;C1 后即"每语句一实例"(读侧),C4 后覆盖异构网关。**纯连接器无关地基 + fe-core 改道**,用户已豁免铁律 A。
