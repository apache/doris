# 扩展范围定稿：分期 + 后台读穿纠正 + 缓存隔离升级为安全修复

> 本文承接 `P1-implementation-design.md` §9 的四个待确认点。用户 2026-07-19（session 3）就"是否把写入共用 / 后台线程传递 / 缓存隔离一并纳入当前工作"做了拍板，并触发一轮取证 workflow（`wf_8b907b93-e9f`，18 agents，recon + 对抗验证 + 综合）。本文记录**用户决策、取证结论、定稿分期**，并更正 `P1-implementation-design.md` §9 点 3 与总设计里"后台传递"的隐含框架。
> **本文仍不动代码**。

---

## 0. 用户决策（2026-07-19 session 3，动码前）

`P1-implementation-design.md` §9 四点的实际裁决（部分偏离原推荐）：

| 点 | 原推荐 | 用户裁决 | 影响 |
|---|---|---|---|
| ① 写侧留后续 | 只做读侧 | **读写一起做**（但经取证 → 排在读取键石之后、独立成步） | P3 提前进入范围，但**分期**、不与键石混提交 |
| ② close 注册点 | scope 创建时 | **认可**（scope 创建时） | 不变 |
| ③ 后台加载点 | 显式直调 + 白名单 | **要求"把统一句柄传到后台线程"** → 取证判定该方向**不安全**，纠正为"显式读穿 + 修隐患" | 见 §2 |
| ④ 缓存隔离 | 本阶段不动、留后续签字 | **现在就处理** + 确认 **list ≠ load** | 缓存隔离升级为**真实安全修复**，独立安全 track，见 §3 |

**综合裁决**：**分期推进**（非一次性全做）。三块都做，但排序 + 独立提交 + 独立验证。

---

## 1. 取证结论 A —— 读写共用一个元数据实例：可行且忠实 Trino

- **Trino 已核实**：`CatalogTransaction` 每（事务, catalog）memoize **恰好一个** `ConnectorMetadata`，读规划与写入**共用它**；写入经共享事务句柄绑定，提交 = `connector.commit(txnHandle)`。我们的目标形状与之**完全一致**。
- **机制**：把统一入口存的"裸 metadata"升级成 `CatalogStatementTransaction` 共持体（持 memoized metadata + 首次写时懒建的 `ConnectorTransaction`）。现成范本 = `ConnectorRewriteDriver`（co-hold metadata/session + `beginTransaction` + commit/rollback）。
- **写臂 8 处 getMetadata 改道进同一入口**（均已核实为无状态薄壳的只读用法 + 一次起事务）：`PhysicalPlanTranslator.visitPhysicalConnectorTableSink:660`(INSERT) / `buildPluginRowLevelDmlSink:612`(DELETE/MERGE)、`PluginDrivenInsertExecutor.ensureConnectorSetup:206`、`PluginDrivenExternalTable.resolveWriteTargetHandle:133`（藏在"读"文件里的写专用点）、`BindSink.checkConnectorStaticPartitions:673`/`checkConnectorWritePartitionNames:712`、`IcebergRowLevelDmlTransform.checkPluginMode:112`、`PhysicalIcebergMergeSink.buildInsertPartitionFieldsFromConnector:303`。
- **两条必守的正确性闸门**（非性能）：
  1. **按连接器保留"起写刷新"**，不一刀切。iceberg 复用语句内共享表（新鲜度靠 `newTransaction()` refresh）；**hive 起写必须保留其在鉴权上下文里当场 `hmsClient.getTable`**（`HiveConnectorTransaction.beginWrite:199/208-211`）——负责 ACID 事务表拒绝 + 权威起始快照。误换成扫描缓存表 = 正确性/授权 bug。
  2. **身份一致性闸门**：iceberg 接 REST、session=user 时 metadata 烤进 per-user 委派操作（`IcebergConnector.getMetadata:256` `newCatalogBackedOps`）。读/写会话在同一语句本是同一用户，但**须显式断言二者身份指纹相等**再共用，否则理论上"拿 A 的对象执行 B 的写"。
- **不做的事**：不统一两个 `ConnectorSession`（保留 `setCurrentTransaction` 绑定，更大且无正确性必要）；不引入事务协调器（外部写仍每语句 autocommit，1 StatementContext ≈ 1 外部事务）。
- **HMS 兄弟**：sibling 入口必须覆盖 **beginTransaction** 路由（`HiveConnectorMetadata:1854`），不仅 getMetadata；键含属主 label，写事务从与读同一个 funnel-memoized 兄弟实例上 mint。
- **风险**：iceberg session=user 身份错配（首要正确性闸门）；hive 起写刷新丢失；兄弟 downcast；提交/收尾**不得**再调 getMetadata 建第二个实例；maxcompute/jdbc 写壳仅旁证，接入前须确认无状态 + 事务可从共享 metadata mint；scope-map 生命周期须在语句末确定性 close/rollback 未提交事务，且幂等。

## 2. 取证结论 B —— "后台线程传递"被证伪，纠正为"读穿 + 修隐患"

**用户原要求"把统一元数据对象传到后台线程"，取证判定不安全，应反向做。**

- **为何不安全**：那几个后台加载点填的是**跨语句共享缓存**（活得比语句久）；把每语句实例塞进去、语句末 close → 共享缓存里留**已关闭对象**。后台线程是**全进程复用固定池**（`ExternalMetaCacheMgr` scheduleExecutor），一个查询关掉的资源可能被**无关查询**在同一池线程上误用 → 崩溃/串数据。**明确拒绝 InheritableThreadLocal / 在 worker 上 set**。
- **正确模型（两分支，也正是 Trino）**：
  - **语句派生的异步**（扫描 pump、将来写侧异步）→ **提交时捕获**：复用请求线程上建好的 session（`PluginDrivenScanNode.connectorSession` final 字段，`ConnectorSessionImpl.statementScope` 是 final，脱线程可达）。规则：新异步一律把请求线程 session/scope 传进闭包，**绝不在池 worker 上 `buildConnectorSession()`**（那里 `ConnectContext.get()==null` → 落 NONE）。
  - **真正跨语句的后台缓存加载器**（7 处）→ **一律读穿**：每次临时新建、**显式声明 NONE**（`ConnectorSessionBuilder.withStatementScope(NONE)` 或新增 `buildCrossStatementSession()`），绝不绑定语句实例。7 处 = `PluginDrivenExternalCatalog.listDatabaseNames:295`/`listTableNamesFromRemote:311`、`PluginDrivenExternalTable.initSchema:460`/`getColumnStatistic:1052`/`getChunkSizes:1087`/`fetchRowCount:1153`、`MetadataGenerator.dealPluginDrivenCatalog:1329`。
- **真实隐患（顺手修）**：`fetchRowCount` **并非**只跑在后台——`AnalysisManager.buildAnalysisJobInfo:415/417` 在 **ANALYZE 语句的执行线程**上直调 `table.fetchRowCount()`，该线程有活的 StatementContext → 会**错误绑定到 ANALYZE 语句的作用域**。今天无害（值是 `Optional<Long>`），但一旦实例挂可关闭资源即 use-after-close。→ **对 7 处一律强制 NONE**，把"读穿"从"碰巧落在哪个线程池"变成**强制契约**。
- **Trino 对齐**：Trino 后台走跨事务缓存（`CachingHiveMetastore`）读穿，不复用事务 metadata；我们**匹配**，并**额外加**一道显式-NONE 守门（因我们的捕获是 thread-driven，Trino 不需要）。

## 3. 取证结论 C —— 缓存隔离：确认 list ≠ load，升级为真实安全修复

**用户确认 list ≠ load** → "能列举但无权加载"的用户会经缓存命中读到别人授权才见的表结构/快照/分区。**这是真实越权，非纵深防御。**

- **影响面小**：只涉及 **iceberg 的投影缓存 + fe-core 表结构缓存**；hive/paimon/hudi 无 SUPPORTS_USER_SESSION、无按用户授权轴，**不动**。
- **两轴切分（对齐 Trino 的切线）**：**只对"不含凭证、授权敏感"的投影按身份分片**（表结构/快照 id/分区规格）；**含凭证的原始表 + FileIO 一律不跨语句缓存**（凭证会过期；身份轴 ≠ 令牌过期轴）。
- **SPI 原语**：新增连接器无关的 `ConnectorSession.getIdentityShardKey()`（默认取 `getUser()`，源自 Doris 主体非令牌字节 → fe-core 不解析凭证，守铁律）。off-thread loader 也读它，防指纹在请求线程与异步线程间漂移。
- **iceberg**：把 shard key 放进当前**未门控**的三个投影缓存的 Key——`latestSnapshotCache`/`partitionCache`/`formatCache`（`IcebergConnector` cache 块 ~200-232），仅 `isUserSessionEnabled()` 时填充（否则常量 → 非 session=user 目录字节不变）。关掉残留泄漏（总设计 §8.3⑤：`beginQuerySnapshot` 命中跳过 per-user loadTable）。
- **fe-core 表结构缓存**（唯一引擎侧泄漏，`SchemaCacheKey` 仅按 nameMapping）：**分层**——(a) 先加 `shouldBypassSchemaCache(SessionContext)`，session=user + 委派凭证下**绕缓存现读**（镜像已安全的库/表名缓存 bypass），最小正确修；(b) 后续若性能需要再把 identityShardKey 织入 Key（触及广泛调用的 `getSchema` 签名 + off-thread loader，更侵入）。
- **原始表缓存保持 OFF**（`tableCache` 两条件 null 门维持）；语句内复用仍靠 `IcebergStatementScope.sharedTable`（及 P2 后的实例字段）。**P4 恢复的是投影 RPC 的跨语句复用，不恢复 scan 期 loadTable**（那是 P2）。
- **HMS 异构网关是最尖的洞**：hive 前门（无 SUPPORTS_USER_SESSION → fe-core 名字缓存 bypass 不触发）委派给 session=user 的 iceberg 兄弟时会泄漏；shard key 与 Key 选择须按**有效属主连接器身份**、且经兄弟 getMetadata 路由传播。**必须与 P1 的"键含属主连接器"一并落 + 异构网关 e2e。**
- **防漂移门禁**：加 arch/checkstyle 规则——授权敏感的跨语句缓存的 Key 在 session=user 下**必须**含 `getIdentityShardKey()`，把"加了新缓存忘了分片"从静默泄漏变成构建失败。
- **Trino 对齐**：切线一致（只缓存无凭证投影、FileIO 每请求现挂）；Trino 的身份分片是 opt-in（impersonation），默认靠缓存之上的 access-control 层，故 **Doris 的"session=user 恒分片"比 Trino 默认更严**（需签字确认这是有意选择）。放置差异：Trino 分在**工厂层**（per-user 缓存实例），我们放**Key 里**（贴合 Doris 扁平有界缓存 + 总设计措辞），同等隔离、靠防漂移门禁补工厂模型的免费防漂移。
- **待签字/待答**：授权新鲜度（TTL 内远端撤权仍命中，Trino 同性质，可接受但须签字）；manifestCache（默认 OFF，开启则须分片或声明与 session=user 不兼容）。

---

## 4. 定稿分期（不砍任何一块，只排序 + 独立提交/验证）

> **为何不一次性全做**：①硬依赖——写入无入口可改道，直到读取键石落地（写严格下游）；②"后台传递"被证伪，非独立第三块，是键石 close 接线的一条正确性约束；③缓存隔离是独立安全工程（未决威胁模型 + 缺 SPI 原语 + 与重构正交），捆进 ~64 处机械改道会给签字施压、且越权回归无法二分定位。约六十多处改道 + 写 + 授权缓存同落刚稳定的性能热路径 = **不可二分的回归面**。

| 步 | 内容 | 依赖 | 验证 |
|---|---|---|---|
| **STEP 1 · 读取键石**（原 P1，含后台纠正） | 统一入口 `PluginDrivenMetadata.get` + `getOrCreateMetadata`/`closeAll`（幂等）+ 读/扫描/DDL/MVCC 改道 + 扫描节点存字段 + close 走现有 query-finish、注册在 scope 创建点 + `resetConnectorStatementScope` 先 closeAll 再置空。**7 处后台加载器显式强制 NONE 读穿 + 修 `fetchRowCount` ANALYZE 隐患**。 | — | 加载计数=1(NONE=N)；跨 catalog/queryId 隔离；游标/转发/重试/SQL-cache close 恰一次 |
| **STEP 2 · HMS 兄弟扇出**（P1-design §5） | 键 =(catalogId, ownerLabel)；兄弟 getMetadata **及 beginTransaction** 经同一入口；异构网关 e2e | STEP 1 | 每 sibling 一实例、加载计数=1 |
| **STEP 3 · 写入共用**（P3，拆两小步） | **3a** 无状态写点改道进入口（translator×2 / executor / resolveWriteTargetHandle / BindSink×2 / IcebergMergeSink / RowLevelDml）；**3b** `ConnectorTransaction` 归属上移到 `CatalogStatementTransaction`，定 commit/rollback vs closeAll 顺序 | STEP 1+2 | 3a 门：读/写会话身份等价（iceberg session=user）；保留 hive 起写刷新；保留 tx↔session 绑定 |
| **STEP 4 · 缓存隔离**（P4，独立安全 track，可与 1–3 并行启动） | `getIdentityShardKey()` SPI → iceberg 三投影缓存 Key 分片 + fe-core 表结构缓存 bypass(先)/分片(后) + 防漂移门禁；随 STEP 2 的属主键一并覆盖异构网关 | 威胁模型签字 + STEP 2（属主键） | 越权 e2e：can-list-cannot-load 用户命中不泄漏；异构网关 e2e |

**纠缠点（STEP 3 时再定，现在不决）**：iceberg"起写复用已解析表"目前读 `IcebergStatementScope.sharedTable`，而该 side-car 在 P2 计划里要删。到 STEP 3b 请用户定：先接旧 side-car、P2 再搬 vs 先做个 iceberg 最小 P2 前置。

---

## 5. 对既有文档的更正

- `P1-implementation-design.md` §9 点 3：从"显式直调 + 白名单"**更正为"显式强制 NONE 读穿 + 修 fetchRowCount ANALYZE 隐患"**。§2 排除项组（A/排除、B/排除、F 排除、misc/排除）处置随之从"保持直调"细化为"显式 NONE session"。
- 总设计"后台传递/off-thread 构造期捕获"表述保留正确（捕获机制没错），但**"把实例传给后台线程"这一用户方向被证伪**——后台跨语句 loader 必须读穿，仅语句派生异步捕获。
- `P1-implementation-design.md` §9 点 4 与总设计 §8.3⑤：缓存隔离**不再是"本阶段不动 + 远期签字"**，而是**已确认 list≠load 的真实安全修复**，作为 STEP 4 独立 track，威胁模型签字仍需，但不再推迟到"某个远期"。

---

## 6. STEP 1 实现进展（2026-07-19 session 3 续）

- **C1 地基**（commit `5b7312f9d1f`）：`ConnectorStatementScope.getOrCreateMetadata/closeAll` 默认方法 + `ConnectorStatementScopeImpl.closeAll`(幂等 close-once) + 静态漏斗 `PluginDrivenMetadata.get`。fe-core 单测：memo-once / NONE-each-call / 跨目录隔离 / closeAll 关一次。**字节中性**（无生产路径接进漏斗）。
- **C2 关闭接线**（commit `12f3e95239b`）：**两层关闭**（见 `P1-implementation-design.md` §4 更正块）。主关闭=`PluginDrivenScanNode.getSplits` 注册 `scope::closeAll`（对象捕获、跳 NONE、同 read-txn queryId 键）；兜底=`StatementContext.close()` 的 `isReturnResultFromLocal` 守卫 closeAll（直连 `executeQuery` finally + `proxyExecute` 新增 finally）；`resetConnectorStatementScope` 改 closeAll-before-null；`handleQueryWithRetry` 每重试重置。单测：reset-先关 / close-本地关 / close-异步延后。**P1 关闭仍 no-op，本 commit 行为中性**。
- 取证 workflow：`wf_9250330b-e81`（scope 创建普查 / unregisterQuery 覆盖 / 边界路径，各带对抗复核）。

### 关闭接线的残留风险（carry-forward，多为既有/共担）
1. **取消/超时非硬栅栏**：`SplitAssignment.stop()` 只置 `isStopped` 标志、不 join `scheduleExecutor` 上的批读 future；慢的在途 `planScanForPartitionBatch` 可能在 closeAll 清表后再 `computeIfAbsent` 塞值（不崩，但那值不被关）。**既有、与 read-txn 回调共担**，非本改动引入。P1 no-op 关闭下无实害；**关闭做实事前须硬化**（join pump 或 closed-guard computeIfAbsent）。
2. **arrow-flight 异常断连**：`FlightSqlConnectProcessor.close` 不跑 → 注册表条目留存（无 TTL）。既有、共担。
3. **待确认**：走协调器的 arrow-flight/内部查询若碰外部目录却**从不走 getSplits**（纯 information_schema / 某些元数据 TVF）→ 无主关闭 + arrow-flight 跳兜底 → 可能泄漏。需在 STEP 3/后续确认这类路径必走 getSplits 或本地兜底。
4. **🔴 TCCL 自钉扎（硬前置，给"关闭做实事"的那一步）**：本 commit 关闭是 no-op 故无需；一旦某连接器把 `ConnectorMetadata.close()`（或 P2 的 FileIO/Table）做成实事，**主关闭（getSplits 注册的回调）与兜底（StatementContext.close/proxyExecute）两处都必须把 TCCL 钉到 provider 的插件 classloader**（同 read-txn 释放回调 + `TcclPinningConnectorContext` 的 locus 纪律），否则跨类加载器 split-brain。**连接器 `close()` 自钉扎是首选**（fe-core 保持 connector-agnostic、不持 classloader）。
