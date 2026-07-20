# 目标架构设计：把外部表元数据层重构成"每语句/每事务 metadata 实例"（对齐并超越 Trino）

> **本文定位**：这是"是否把 Doris 表解析/元数据层重构成 Trino 式架构"专题讨论的**设计草案**。
> **前提（用户 2026-07-19 拍板）**：本设计**不受任何现有铁律/约束限制**——可改任何模块（fe-core / planner / 连接器）。唯一目标：设计一个**至少和 Trino 一样合理、能更好则更好**的架构，不被当前实现绑住。
> **本轮仍不动任何生产代码**：先出设计、用户确认方向后再实现。
> 现状事实全部经一轮只读并行 recon + 对抗验证取证（workflow `wf_72d1e505-75c`），下引 `file:method:line` 均来自该轮核实。

---

## 0. 结论先行

1. **可行，且比上一轮结论文档判断的容易得多。** 上一轮 §4.3 的关键前提"Doris 的 `*ConnectorMetadata` 是每 catalog 长期共享单例"**是错的**（见 §1 更正）。真相是:`ConnectorMetadata` 现在是**每次调用新建、即用即弃的无状态外壳**,生命周期比"每语句"还短。要把它变成"每语句一个",不是要拆一个顽固单例,而是**给一个本就无根的临时对象钉一个语句级的家**——这恰恰是 Trino 的做法。
2. **Doris 现状已经具备 Trino 四条不变量里的两条半**:句柄不可变且携带事实(不变量③,已满足);语句级宿主 `StatementContext` 已存在且已托管 MVCC pin 与连接器作用域(不变量①的宿主已就位);缺的是"引擎在管道层 memoize 唯一 metadata 实例"(不变量②)和"生命周期即事务、按用户天然隔离"(不变量④)。
3. **目标架构 = 引擎自持的"每语句(每事务)metadata 实例" + 其下一层"跨语句、按身份分片的共享缓存"**。前者给结构化的"一语句一次加载"(比 Trino 的 iceberg 更硬),后者给按用户的跨语句复用(补上今天 session=user 下缓存被全关的窟窿)。
4. **诚实的收益定位**(必须带着走的老结论):"每语句实例"本身**不解决**按用户的**跨语句**性能——那要靠"按身份分片的缓存"(分期里的 Phase 4)。每语句实例真正的价值是**架构自洽 + 结构性正确**(把今天散落的 6 处"这个值是不是跨用户敏感"的门禁,收敛成一条结构性质:实例是每语句、天然单用户)。

---

## 1. 现状的真实形状（经核实，含一处对上一轮文档的更正）

### 1.1 三层生命周期（这是理解一切的钥匙）

| 对象 | 生命周期 | 持有什么 | 对应 Trino |
|---|---|---|---|
| **`Connector`** | **每 catalog 一个长期单例**(`transient volatile` 字段,仅 ALTER/重启/close 重建) | **全部状态**:各类跨查询缓存(iceberg 的 snapshot/table/partition/format/comment/manifest;paimon 的 snapshot+schemaAt;hive 的 fileListing;maxcompute 的 partition)、`ConnectorContext`、鉴权 | ≈ Trino 的 `Connector`(每 catalog 单例) |
| **`ConnectorMetadata`** | **每次 `getMetadata(session)` 调用新建、即弃** | **几乎无状态**,是把连接器单例的缓存注进来的**薄委派外壳** | ≈ Trino 的 `ConnectorMetadata`——但 Trino 是**每事务一个并被引擎 memoize**,Doris 是**每次调用一个、从不 memoize** |
| **`ConnectorSession`** | **每次 `buildConnectorSession()` 新建**(一条语句 ~26–63 次),且**贵**(每次 `VariableMgr.toMap()` 全量反射 dump + 全局读锁) | queryId、委派凭证、捕获的语句作用域引用 | ≈ Trino 的 `ConnectorSession`——但 Trino 的是**廉价不可变请求上下文**,Doris 的是**昂贵且被反复重建** |

证据:`PluginDrivenExternalCatalog.java:102`(connector 字段)、`IcebergConnector.getMetadata:256`(`return new IcebergConnectorMetadata(...)` 每次新建,7 个连接器皆然,grep 无任何 `ConnectorMetadata` 类型字段)、`ConnectorSessionBuilder.java:158-179` + `VariableMgr.toMap:940-959`(每次 build 的反射开销)。

> **⚠ 对上一轮结论文档的更正**:`designs/recon-findings-and-trino-refactor-groundwork.md` §4.3 与 `HANDOFF.md` 写的"Doris 连接器/`*ConnectorMetadata` 是长期共享单例(每 catalog 一个、从不按语句造)"——**把 `Connector`(确是单例)和 `ConnectorMetadata`(其实是每调用即弃)混为一谈了**。对抗验证判定该表述 **REFUTED**(证据:`IcebergConnector.getMetadata:256` 等 7 处)。这个更正是**利好**:让 metadata 变成每语句,不需要打破单例,只需给一个"生命周期本就是自由变量"的临时对象钉个语句级的家。

### 1.2 规划器怎么拿元数据(每处都在重造)

从 SQL 到执行,每个 seam 都在重复同一个三元组:**长期 catalog 单例 → 长期 Connector 单例 → 新建 `ConnectorSession` → `connector.getMetadata(session)`(每调用新 metadata)→ 从 metadata 拿一个不可变 `ConnectorTableHandle`**。**没有任何"每语句 metadata 对象"被引擎穿针引线地传下去。**

需要改道的 seam(recon 已枚举,择要):
- **读**:`PluginDrivenExternalTable` 里 ~17 处(`initSchema`/`getColumnStatistic`/`fetchRowCount`/`getNameToPartitionItems`/`toThrift`/`getComment`/`listPartitions`…),每处都 `buildConnectorSession`+`getMetadata`。
- **扫描**:`PluginDrivenScanNode` 在 `create()` 把 session/connector/handle 捕获成**字段**(单扫描节点内共享),但之后每个方法(`applyFilter`/`applySnapshot`/`planScan`/`getColumnHandles`…)仍 `connector.getMetadata(session)` 重取。
- **写**:`PhysicalPlanTranslator.visitPhysicalConnectorTableSink`(自建 session)、`PluginDrivenTableSink.bindDataSink`(planWrite)、`PluginDrivenInsertExecutor`(**第二个** session + `writeOps` 字段 + `beginTransaction`)。写路径**两个独立 session**,仅靠把 `ConnectorTransaction` 绑到 sink 的 session 上勉强串起来。
- **异步缓存 loader**(schema/rowCount/columnStats):**在没有 `ConnectContext` thread-local 的线程上跑**——任何"每语句对象"必须能不经 thread-local 够到(现有作用域用**构造期捕获**已解决此问题,可复用)。

### 1.3 已就位的"半个 Trino 形状"(利好)

- **句柄已是 Trino 模型**:`ConnectorTableHandle` 不可变,由 `applyFilter/applyProjection/applyLimit/applySnapshot/applyRewriteFileScope/…` 返回**新句柄**逐步精化——这正是 Trino 的 `ConnectorTableHandle`-update 模式。**Trino 不变量③(事实随句柄前向流动、规划期不重解析)Doris 已满足。**
- **语句级宿主已存在**:`StatementContext` 每语句在 `StmtExecutor` 构造时新建、挂到 `ConnectContext`,已托管 MVCC 快照 pin(`StatementContext.snapshots`,一语句一次解析、读写共享)与连接器作用域,随语句 GC。`ConnectorStatementScope`(挂在它上的 `ConcurrentHashMap<String,Object>` memo 竞技场)+ iceberg 的 `IcebergStatementScope` 已用它做到"一语句一张表只加载一次"。
- **但作用域只是"半成品"**:它是**无类型的 String→Object 竞技场**,不是**有类型的每语句 `ConnectorMetadata`**;**规划器不经它取表**(规划器走的是跨语句的 `ExternalCatalog` 元数据缓存);只有 iceberg 一个连接器手写键去消费它。也就是说"读规划的取表路径"和"每语句加载归属者"**今天还不是同一条缝**——而 Trino 把它们合成了一条。

### 1.4 事务模型(现状割裂)

- **外部无统一事务管理器**:每个 `ExternalCatalog` 自持一个 `TransactionManager`;plugin 目录一律用 `PluginDrivenTransactionManager`,把 commit/rollback/close 委派给**连接器在 `beginTransaction` 才晚建**的 `ConnectorTransaction`(`session.allocateTransactionId()`),绑到 session、`onComplete` 提交/`onFail` 回滚。
- **`ConnectorTransaction` 是糟糕的 span 宿主**:出生太晚(仅 beginWrite)、只在写臂、读/规划期根本看不见。
- **内部 OLAP 是另一条路**:`GlobalTransactionMgr` + 每连接 `TransactionEntry`;`GlobalExternalTransactionInfoMgr` 只是个 id→Transaction 的注册表(供 BE→FE RPC 查),**不是协调器**。内外无共享抽象。
- **外部写实际上是"每语句 autocommit"**:`ConnectorTransaction` 在一条语句内建、在该语句 `onComplete` 提交 → **一个 `StatementContext` ≈ 一个外部事务**。这让 `StatementContext` 成为 span 宿主是**自然且正确**的。

---

## 2. Trino 为什么优雅（四条不变量）+ 两处弱点

经源码核实(`trinodb/trino` master,下引类名/行号来自该轮 web recon):

**四条不变量:**
1. **事务 = 元数据的身份与生命周期单位**。无显式 BEGIN 的语句跑在 autocommit 事务里;每个被触达的 catalog,`TransactionManager` 懒建并 memoize **一个** `CatalogMetadata`(`activeCatalogs` map,`CatalogHandle` 为键);其内 `CatalogTransaction` 用 `@GuardedBy(this)` 字段 memoize **恰好一个** `ConnectorMetadata`(`connector.getMetadata(session, txnHandle)` 只调一次)。commit/rollback → `connector.commit/rollback` → 事务移除。**无人工失效、无跨语句泄漏。**
2. **单一漏斗、单一实例**。所有元数据调用走 `Session → MetadataManager → TransactionManager → CatalogMetadata → CatalogTransaction`。**memoize 发生在管道层,不在连接器里** → 一语句一实例由**引擎**保证,与连接器自觉性无关。
3. **事实随句柄前向流动、不靠重载**。`getTableHandle` 一次解析,快照 id/schema/分区 spec 装进不可变 `ConnectorTableHandle`,穿过 analyze/optimize/execute 直到 `beginInsert/beginMerge`。规划期从不重解析。
4. **身份是事务级的 → 按用户隔离白拿**。`ConnectorMetadata` 用 `session.getIdentity()` 构建;`ConnectorSecurityContext = txnHandle+identity+queryId`;事务是每 session 的,故 metadata **从不跨用户共享**——隔离是事务模型的副产品。

**两处弱点(Doris 应超越):**
- **弱点 A**:每事务 `ConnectorMetadata` 被 memoize 了,但**加载到的 Table 对象没有**(iceberg)。`IcebergMetadata` 只缓存统计;`getTableHandle/getTableMetadata/getTableProperties` 各自重调 `catalog.loadTable()`,把去重下推到**每事务 `TrinoCatalog` 上有界可淘汰的** `tableMetadataCache`(maxSize ~1000)。**多表语句会淘汰重载 → "一语句一次加载"是软的、非结构性的**(直接坐实了"一条语句 loadTable 3–4 次靠下层缓存兜")。
- **弱点 B**:core `CatalogTransaction` 已保证 `getMetadata` 只调一次,Hive 连接器却**又在 `HiveTransactionManager.MemoizedMetadata` 里重复实现一遍**每事务 memo。冗余耦合。

---

## 3. 目标架构（Doris 版：对齐 Trino 的不变量，并针对性超越其弱点）

### 3.1 核心：引擎自持的"每语句/每事务 metadata 实例"，memoize 在管道层

把今天"每次调用新建即弃"的 `ConnectorMetadata`,改成**每(语句, catalog)一个、由引擎懒建并 memoize、语句/事务结束时确定性销毁**。

- **宿主 = `StatementContext`**(外部写=每语句 autocommit,一 `StatementContext` ≈ 一外部事务;宿主已在,已托管 MVCC pin 与作用域,已有正确 GC/reset)。
- **管道层单一漏斗**:在语句 span 上加 `ConnectorMetadata getOrCreateMetadata(catalogId)`,懒建、memoize、语句内所有 seam 复用同一实例——**对应 Trino 不变量②,且 memo 在引擎侧,连接器零自觉性要求**(直接规避 Trino 弱点 B:连接器不再各自手写 memo)。
- **off-thread 可达**:实例引用像作用域一样在 `ConnectorSession` **构造期捕获**,异步 scan pump / 缓存 loader 无 thread-local 也够得到。
- **确定性销毁**:`StatementContext.close()` 逐 catalog `metadata.close()`(对齐 Trino 的 commit/rollback 即销毁;比今天纯靠 GC 更干净)。

因为 `ConnectorMetadata` 今天已是"无根的临时外壳"(designNotes 原话:"除了缓存,没有任何东西把它钉在每调用"),且句柄已满足不变量③,这一步在结构上**主要是引擎侧改道 + 把缓存归属从单例挪到实例**,SPI 方法签名基本不用动(方法本就吃 `(session, handle)`)。

### 3.2 超越点①：结构性的"一语句一次加载"(打败 Trino 弱点 A)

把**每语句工作集**(一次加载的 raw Table、扫描→写的删除清单桥、列句柄)从"连接器单例上的缓存 / 无类型 String→Object 作用域",**上移成每语句 metadata 实例上的字段**。

- iceberg 的 `IcebergStatementScope.sharedTable`/`rewritableDeleteSupply` 从"外挂 side-car"变成"实例的字段" → **加载一次成为硬的结构性质**(实例在,表就在;不是有界可淘汰缓存)。**这比 Trino 的 iceberg 更硬。**
- paimon 的胖句柄 `PaimonTableHandle.paimonTable` 溶进实例字段,句柄回归纯坐标(iceberg 已做过同款)。

### 3.3 超越点②：两级缓存 —— 每语句工作集 之上/之下 各司其职

| 层 | 位置 | 作用 | 对应 Trino |
|---|---|---|---|
| **每语句工作集** | 每语句 metadata 实例的字段 | 语句内一次加载、读写共享、扫描→写桥;**天然单用户单语句** | Trino 每事务 metadata 的缓存(但 Doris 做成硬字段) |
| **跨语句共享缓存** | Connector 单例(或专门的 caching 层),**按身份分片** | 跨语句复用;**值敏感处把 identity 放进 key** | Trino 的 `CachingHiveMetastore` / `TrinoCatalog` 缓存 |

关键洞察(对齐 Trino 不变量④,并补 Trino 未系统化之处):

- **正确性坍缩成一条性质**:每语句实例**天然单用户**(一语句一用户),故其内部一切缓存**永不跨用户** → 今天散落的 ~6 处门禁(`SUPPORTS_USER_SESSION` 分支、`shouldBypassTableNameCache/DbNameCache` 两钩子、`IcebergTableCache` 置 null、`IcebergCommentCache` 门禁、`IcebergStatementScope` side-car)**收敛成"实例是每语句/单用户"这一条结构性质**,删掉一整类"新加了个缓存忘了 gate"的 bug。
- **性能补窟窿在下层**:今天 session=user/凭证目录下,跨语句缓存被**整个关掉**(`IcebergTableCache=null` 等),每条语句都为每张表多付 1 次 `loadTable` + 1–2 次 list RPC。**把下层跨语句缓存改成按身份分片(identity 进 key)** → 按用户的跨语句复用安全恢复。**这才是按用户场景真正的性能收益所在。**

### 3.4 超越点③：把写事务并进同一个 span

- `beginWrite` 从"重新 `loadTable`"改成"**从每语句实例取已解析的表**"(保留 openTransaction 的 refresh 兜新鲜 OCC 基底)。
- `ConnectorTransaction` 不再是"晚建 + 绑 session"的孤儿,而是**由每语句 metadata 实例/span 持有**;commit/rollback 由 span 在语句末驱动。写路径两个 session 的割裂被收编。对齐 Trino"metadata 即事务、写方法吃句柄、commit=connector.commit(txnHandle)"。

### 3.5 超越点④(远期/可选)：统一内外事务协调器

- Trino 只有外部连接器,没有"内部表"二元性;Doris 有 `GlobalTransactionMgr`(OLAP) 与 `PluginDrivenTransactionManager`(外部)两套。**把二者统一到一个事务协调面**,让外部写能参与多语句 `BEGIN..COMMIT`(span 挂 `ConnectContext`、`StatementContext` 持每语句视图,即"两级 span")——这是 Doris **特有**的自洽升级(不是"打败 Trino",是补 Doris 的割裂)。**最深、风险最高、对"达到 Trino 平价"非必需**,列为远期。

### 3.6 附带清理(与主线解耦，可先落)

- **`ConnectorSession` 变廉价 + 不可变**:每语句 memoize 一次 var-map 快照,消灭 ~26 次 `VariableMgr.toMap` 反射 dump;把唯一可变字段(transaction 槽)移到 metadata/事务对象上,恢复 session 全不可变。**纯性能 + 简化,不依赖 metadata 生命周期改造,可最先落。**

---

## 4. 可行性与风险

### 4.1 为什么可行(且比原判断容易)
- `ConnectorMetadata` 本就是每调用即弃的无根外壳 → **给它钉个语句级的家,不需打破任何单例**(最大的"想象中的拦路虎"不存在)。
- 句柄已不可变、已 fact-carrying → **Trino 不变量③已满足**,不用重写。
- `StatementContext` 已是被验证的 span 宿主(MVCC pin + 作用域已在其上),off-thread 可达已由"构造期捕获"解决 → **不变量①的宿主与最难的 off-thread 问题都已就位**。
- seam 虽多但**窄而齐**:一切走 `getMetadata / getScanPlanProvider / getWritePlanProvider / beginTransaction`,fe-core 从不跨调用持有 metadata 引用(唯一例外是写执行器的 `writeOps` 字段)→ 改道是机械但可枚举的工作。

### 4.2 风险(诚实列出)
1. **回归面**:~40 处读热路 seam 改道,踩在刚稳定的读热缝上(PERF 系列)。→ 缓解:分期、每期加"加载计数守门"回归。
2. **确定性销毁 vs off-thread**:实例不能在异步 scan pump 还在用时提前 close。→ 缓解:沿用作用域的"随语句 GC + 不在 close() 里清工作集"纪律,只在确证无 off-thread 引用处做确定性 close。
3. **异步缓存 loader 无 thread-local**:schema/rowCount/stats loader 跑在别的线程、且填的是**跨语句**缓存。→ 这些本就该留在"下层跨语句缓存",每语句实例**读穿**到它们(对齐 Trino 每事务 metadata 读穿 `CachingHiveMetastore`),不强行纳入每语句实例。
4. **Phase 4 的身份分片需威胁建模**:哪些值是用户敏感(凭证/可见性/授权)要 identity 进 key,哪些是纯元数据可共享(snapshot/format)。recon 已留两个 open 问题(session=user 下 `SchemaCacheValue` 仍开、latest-snapshot 仍跨用户共享——是否可接受需签字)。
5. **一语句多事务?**(open Q):多目标 MERGE、或 HMS 异构网关委派兄弟连接器,是否一条语句会 mint 多个 `ConnectorTransaction`?若是,"一 StatementContext = 一事务"需放宽成"一实例/catalog"。→ 设计上按 (语句, catalog[, 目标表]) 建键即可容纳。

---

## 5. 分期（避免一次性大爆炸；每期独立可验证）

| 阶段 | 内容 | 独立价值 | 打败 Trino? | 风险 |
|---|---|---|---|---|
| **P0 · 解耦的廉价前置** | `ConnectorSession` 每语句 memoize var-map(灭 ~26× 反射 dump);为后续把 transaction 槽移出 session 铺垫 | 纯性能 + 简化 | — | 低 |
| **P1 · 键石:引擎自持每语句 metadata 实例** | span 上加 `getOrCreateMetadata(catalogId)`,管道层 memoize,读/规划/DDL/list 全改道走它;确定性 close;off-thread 构造期捕获 | 一语句一实例(不变量①②) | 弱点 B(单漏斗、连接器零自觉) | 中(改道面广) |
| **P2 · 工作集上移 + 删 side-car** | 一次加载的 Table/扫描→写桥/列句柄 变成实例字段;删 `IcebergStatementScope` 的 String-key 用法;paimon 胖句柄溶进实例 | 结构性一次加载 | **弱点 A(硬 load-once)** | 中 |
| **P3 · 写事务并入 span** | `beginWrite` 取实例已解析表(非重载);`ConnectorTransaction` 由实例/span 持有;收编两 session 割裂 | 读写同一 span、写路径自洽 | 对齐 Trino metadata=事务 | 中-高 |
| **P4 · 按身份分片的跨语句缓存** | 下层缓存 identity 进 key;删 `SUPPORTS_USER_SESSION` 全关门禁,改成"缓存按用户分片";补 session=user 跨语句性能窟窿 | **按用户跨语句性能**(真正性能收益) | 超越 Trino 的 all-or-nothing gate | 中(需威胁建模) |
| **P5 · 统一内外事务协调器(远期)** | 一个事务面桥接 OLAP + 外部;外部写参与多语句 BEGIN..COMMIT;两级 span | 内外自洽、多语句外部事务 | Doris 特有自洽(非平价必需) | 高 |

- **达到 Trino 平价 = P1–P3**;**超越 Trino = P2(硬 load-once)+ P4(按用户缓存)**;**P5 是远期战略,非平价必需**。
- 建议落地顺序:P0(热身)→ P1(键石)→ P2 →(P4 与 P3 可并行/择序)→ P5 视产品需要。

---

## 6. 用户已拍板的点（2026-07-19）

1. **终点 = 平价 + 超越(P1–P4)**;不含 P5(统一内外事务)。
2. **首个落地单元 = 直上 P1 键石**(跳过 P0 热身)。
3. **销毁 = 语句末确定性 close**(对齐 Trino,接受 off-thread 约束)——**但见 §8 盲区①:实际须挂在现有 query-finish 钩子,而非 `StatementContext.close`。**
4. **先跑一轮多架构师对抗红队再定稿**——已完成,结论见 §8。

---

## 7. 参考

- 本轮 recon+对抗验证 workflow:`wf_72d1e505-75c`(journal 在 `.../subagents/workflows/wf_72d1e505-75c/`),含 metadata 生命周期 / planner 取数路径 / 多租户缓存 / span+txn 模型 / Trino 精确机制 五组核实结论 + 4 条支撑事实的对抗判定。
- 上一轮结论(部分被本轮 §1 更正):`recon-findings-and-trino-refactor-groundwork.md`。
- 架构记忆:`iceberg-table-resolution-cache-scoping`(缓存作用域五纪律 + StatementContext=每语句 owner + Trino 协同)。
- Trino 源:`InMemoryTransactionManager` / `CatalogTransaction` / `CatalogMetadata` / `MetadataManager` / `HiveMetadataFactory` / `IcebergMetadata`(master)。

---

## 8. 红队结论与定稿（多架构师对抗,workflow `wf_62cc379e-c6e`,2026-07-19）

4 位架构师(span 中心 / 缓存中心 / 忠实 Trino / 最小面)各出一版目标架构,4 位评审(Trino 忠实度 / 迁移风险+off-thread / 多租户正确性 / 完整性)横向打分 + 找致命伤 + 挑最佳点。

### 8.1 排名
| 评审轴 | 第1 | 第2 | 第3 | 第4 |
|---|---|---|---|---|
| Trino 忠实+超越 | **span (85)** | 忠实Trino (83) | 最小面 (77) | 缓存 (58) |
| 迁移+off-thread | **span (82)** | 最小面 (76) | 忠实Trino (66) | 缓存 (58) |
| 多租户正确性 | 缓存 (84) | **span (80)** | 最小面 (71) | 忠实Trino (64) |
| 完整性 | **span (64)** | 忠实Trino (61) | 最小面 (55) | 缓存 (45) |

**胜出 = span 中心**(3/4 轴第一):复用**已验证 off-thread 可达**的现有 `ConnectorStatementScope` + `getStatementScope()` 到达路径;`connector.getMetadata` 保持纯工厂;**唯一 memo 在 fe-core 管道**(invariant 2 最纯);P1 近字节中性、可回退、风险最低。

> 一处被评审核实的关键点:现 `ConnectorSessionImpl` 捕获的是 **`ConnectorStatementScope` 接口**、不是 `StatementContext` 本身。故"经 getStatementScope 到达"是**被证明的**路径;而"把新管理器直接挂 StatementContext"(忠实 Trino 派原案)**不在**这条已证 off-thread 路径上,须额外补捕获——这也印证应以 span 派为骨架。

### 8.2 嫁接进定稿的各家最佳点
1. **忠实 Trino 派的 `CatalogStatementTransaction`**:把"每(语句,catalog)metadata 实例"与"写事务"做成**同一个持有者**(invariant 1 最紧、P3 折叠最干净)。**弃用**其 `ConnectorSession.getMetadata(connector)` SPI(分层异味 + fallback 少测)。
2. **缓存派的"元数据/凭证拆分"(P4 唯一正确的威胁模型)**:缓存**不含凭证的投影**(schema/snapshotId/分区 spec/授权名单)按 catalog 共享、**每请求现挂新 FileIO**;只对授权敏感投影按身份分片;**vended raw Table 一律不跨语句缓存**。身份轴 ≠ 令牌过期轴。
3. **最小面派的**:`ConnectorSession.getIdentityShardKey()` 集中指纹(off-thread loader 复用防漂移)+ `ConnectorMetadata.close()` 默认 no-op + 扫描节点把 memoized metadata 存字段去掉 per-method 重取;`statementSession()` 消灭 ~26 次 `VariableMgr.toMap` 反射 dump(**列为独立性能项,不进 P1** 以保 P1 字节中性)。
4. **arch/checkstyle 门禁**:禁止在 funnel 之外直接调 `connector.getMetadata`(把 invariant 2 从"约定"变"结构强制";须对 HMS sibling 布线显式放行/改道)。

### 8.3 五处四家都漏的公共盲区(定稿必修,①②为硬伤)
1. **🔴 确定性 close 挂错生命周期**。四家都想挂 `StatementContext.close()`/StmtExecutor finally。但每查询连接器资源释放本走 **`registerQueryFinishCallback`**(`unregisterQuery`→`finalizeQuery`,profile 等待之后=pump/BE 静默之后触发,现已用于提交 hive 读事务+放锁)。二者只在简单 autocommit 单语句下重合;**游标取数 / 转发 master / 重试 / SQL-cache 复用(只 releasePlannerResources 不 close)** 下会**过早/重复/永不**触发 → 关掉 off-thread pump 仍用的 Table/FileIO。**修:close 走现有 query-finish 钩子。**
2. **🔴 HMS 异构网关 sibling 扇出**。`HiveConnectorMetadata` 在 ~25 处 per-handle 点调 `siblingOwnerResolver.apply(handle).getMetadata(session)`,每次**在连接器内部新建 sibling metadata、funnel 看不见** → 网关一开:invariant 2 破、P2 一次加载破(sibling Table 重建 ~25 次)、catalogId 单键装不下三连接器、lint 门误报/漏保证。**修:memo 键改 `(catalogId, 属主连接器身份)`;sibling getMetadata 也路由进同一 funnel;补异构网关 e2e(记忆 `hms-iceberg-delegation-needs-e2e`)。**
3. **预编译 EXECUTE 复用泄漏**:`resetConnectorStatementScope()` 先置空不 close → P2/P3 后跨执行泄漏 FileIO/事务。**修:reset 先 closeAll 再置空。**
4. **取消/超时 reaper**:close 须栅栏在 `SplitSourceManager` 注销之后(reaper 可能 close 后才懒调 planScan),不能靠含糊"pump join"。
5. **P4 残留泄漏(威胁模型待定)**:`latestSnapshotCache`/`partitionCache`/`formatCache` + fe-core `SchemaCacheValue` 在 session=user 下**仍开**,`beginQuerySnapshot` 命中不走 per-user loadTable → 能 list 不能 load 的用户或有 schema/snapshot 泄漏。**定 P4 前须证明无 per-user 授权态,否则按身份/快照分片或走 per-user 委派目录。**

---

## 9. 定稿蓝图（P1 键石，可动工粒度）

**骨架 = span 派;值 = 忠实 Trino 派的 `CatalogStatementTransaction`;close = 走 query-finish 钩子;键含属主连接器。**

### 9.1 新增/改动(P1)
- **`ConnectorStatementScope`(fe-connector-api,加类型化方法)**:`ConnectorMetadata getOrCreateMetadata(MetaKey key, Supplier<ConnectorMetadata> factory)`;`MetaKey = (catalogId, 属主连接器身份)`。`NONE` 每次跑 factory(离线/测试字节不变)。旧 `computeIfAbsent` 迁移期共存。
- **`ConnectorStatementScopeImpl`(fe-core)**:背 `Map<MetaKey, CatalogStatementTransaction>`;`CatalogStatementTransaction` 持 memoized `ConnectorMetadata`(+ P3 持写 `ConnectorTransaction`);提供 `closeAll()`。
- **funnel(fe-core 静态)**:`PluginDrivenMetadata.get(connector, session)` = `session.getStatementScope().getOrCreateMetadata(key(session,connector), () -> connector.getMetadata(session))`。**~40(实测 ~64)缝**一律改调它替 `connector.getMetadata(session)`(connector+session 每处都在作用域内;扫描节点已把二者存字段,再存 memoized metadata 一字段去掉 per-method 重取)。
- **`ConnectorMetadata.close()`**:默认 no-op(P2/P3 起连接器 override 释放 Table/FileIO/事务)。
- **确定性 close**:在**现有 `registerQueryFinishCallback`/`unregisterQuery`** 注册 `scope.closeAll()`(pump 静默后);`resetConnectorStatementScope()` 改为**先 closeAll 再置空**;close 栅栏在 SplitSourceManager 注销之后。
- **HMS sibling 改道**:`HiveConnectorMetadata` 的 sibling `getMetadata` 经属主连接器身份走同一 funnel(键含属主)→ 每 sibling 一实例。
- **arch/checkstyle 门禁**:禁 funnel 外直调 `connector.getMetadata`(白名单:funnel 自身 + 明确排除的 off-thread 跨语句 loader)。

### 9.2 P1 终态与验证
- 一条语句一 catalog(一属主连接器)**恰好一个 memoized `ConnectorMetadata`**,读/扫描/写/DDL 全缝复用,pump 静默后确定性 close。连接器内部零改(iceberg 的 `IcebergStatementScope` 共存于 impl,P2 再删)。
- **P1 字节中性**:NONE 下逐次 factory=今日行为;perf delta≈0(语句内去重本就有)。收益=单漏斗 + 无 per-connector memo + 确定性生命周期。
- **守门**:①每(语句,catalog)加载计数=1(对照 NONE=N);②跨 catalog/跨 queryId 隔离;③预编译重执行不泄漏(closeAll 被调);④异构 HMS 网关下每 sibling 一实例、加载计数=1;⑤游标取数/转发/重试/SQL-cache 路径 close 恰好一次、不早不晚(针对盲区①的回归)。

### 9.3 后续期(定稿方向,细化留各自立项)
- **P2**:once-loaded Table/删除桥/列句柄 → 实例字段(硬 load-once,超越弱点 A);删 `IcebergStatementScope` string-key + paimon 胖句柄。
- **P3**:写事务并入 `CatalogStatementTransaction`;beginWrite 取实例已解析表;收编两 session。
- **P4**:元数据/凭证拆分 + 按身份分片授权敏感投影 + 删 `SUPPORTS_USER_SESSION` 全关门禁;先解决 §8.3⑤ 残留泄漏威胁模型。
- **独立性能项**:`statementSession()` 每语句 memoize session(灭 ~26× 反射 dump);审计所有 build 点确认 session 属性语句内恒定。

### 9.4 待你拍板才动代码
本文件是设计定稿方向。**下一步(若你点头):把 §9.1 细化成 seam-by-seam 的 P1 实现设计(逐文件逐方法),仍不动代码;或直接进入 P1 实现。** 另:P4 的 §8.3⑤ 残留泄漏属安全威胁模型,建议单独走一次签字(涉及"能 list 不能 load"的元数据披露)。
