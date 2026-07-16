# Hive 连接器 metastore-event 管道下沉 — 本步设计稿（2026-07-10）

> 权威设计 = 本文件。基于一轮 HEAD-grounded 多维侦察（`wf_0c686fb4-d9d`：6 维盲评 + 完整性对抗 critic）+ lead 独立核对最载重事实 + 用户签字决策。**行号信 HEAD 不信文档**（每处实现前在 HEAD 复核）。
> 上位：`tasks/hms-cutover-execution-plan-2026-07-10.md` §2.1（本步）。样板：`tasks/hive-connector-cache-step-design-2026-07-10.md`（上一步"连接器自持缓存"，本步在其之上加分区失效）。

---

## 0. 一句话 & 用户签字

把 HMS 通知事件管道从 fe-core 下沉到 hive 插件：fe-core 保留**连接器无关、角色感知**的薄驱动（主/从 + 编辑日志 + 游标 + 转发），插件只接管**抓取 + 解析**，通过新 SPI 回传**中立变更描述**。全休眠落地（hms 仍是 legacy 时完全惰性；paimon/iceberg/jdbc/hudi 字节不变）。对齐 Trino：引擎持有 HA/复制，插件持有取数/解析。

- **签字 A（2026-07-10，本步）** = 结构性事件（建/删/改名 库和表）用**即时重建（平价）**：事件到达即在内存挂库/表壳，从节点即时可见；不采"作废+惰性重建"。理由：即时重建成本低（`PluginDrivenExternalDatabase.buildTableInternal` 只 `new` 一个懒壳，不访问 metastore），零可观察行为变化，且顺带修一个通用目录上"未实现即抛异常"的隐患（`registerDatabase`）。
- **默认（无需签字，本步一并落地）**：
  - 分区事件在连接器侧只能做到**整表级**缓存失效（D2 的分区缓存按"整批名字列表"和目录 location 建键，无单名谓词）。因连接器缓存是**拉取式**（失效后下次 `listPartitionNames` 自动重列，见 §2 实证），正确性不受影响，仅略粗；**不为此重构刚落地的 D2 缓存**。描述符仍带分区名，供编辑日志与未来精确化。
  - 能力探测用**可空 provider 访问器** `Connector.getEventSource()`（非 `instanceof`、非布尔能力位）——pollOnce 带参且返数据，形状贴近 `getScanPlanProvider/getProcedureOps`，且把事件 API 从 99% 无此能力的连接器基座上移开。
  - **编辑日志留 fe-core**：驱动从描述符自建 `MetaIdMappingsLog` 并写盘；插件绝不触碰 `EditLog`。
  - **合并/去重（mergeEvents）放插件侧**：其依赖 HMS 类型的分组语义（`TableKey`/`canBeBatched`/跨事件 `removePartition`），pollOnce 返回**已合并**的描述符。
  - 每步 opt-in 开关（`isHmsEventsIncrementalSyncEnabled`）随连接器：`HiveConnector.getEventSource()` 在该目录未启用增量同步时返回 null（连接器读自己的 `HmsProperties`，fe-core 不解析属性）。

---

## 1. 关键实证（HEAD 核对，决定设计）

1. **拉取式缓存 ⇒ 失效即可表达"新增分区"**。`CachingHmsClient.listPartitionNames`（:143-145）= `partitionNamesCache.get(key, key -> delegate.listPartitionNames(...))`：失效后下次 `get` miss → 重跑 loader → 新 Thrift RPC → 新分区出现。故**只需失效、无需 eager 插名**即可表达 `ADD_PARTITION`。`flush(db,table)`（:164-168）按 `invalidateIf(key.matches(db,table))` 丢该表全部条目 ⇒ 分区失效退化整表级，但拉取式保证正确。
2. **游标写盘在工厂、仅 master**：`MetastoreEventFactory.getMetastoreEvents → logMetaIdMappings`（:86-102）构 `MetaIdMappingsLog(fromHmsEvent=true, lastSyncedEventId)` + 每事件 `transferToMetaIdMappings()`，`replayMetaIdMappingsLog`（本地）+ `editLog.logMetaIdMappingsLog`（复制）。**注意**：编辑日志写入当前**埋在 Model B 要下沉的 parse/merge 步里** — 拆分时必须把它**回提到 fe-core 驱动**，否则 master→follower 游标传播悄然中断（follower 永卡 masterLastSyncedEventId==-1）。
3. **游标传播回放**：`ExternalMetaIdMgr.replayMetaIdMappingsLog`（:109-127）已 catalogId-keyed（无 HMS cast，a6dc782d816），`fromHmsEvent` 分支调 `getMetastoreEventsProcessor().updateMasterLastSyncedEventId(catalogId, lastSyncedEventId)`。id 映射仅喂**死 getter**（`getDbId/getTblId/getPartitionId` 零生产调用）；**活用途只剩游标**。opcode `OP_ADD_META_ID_MAPPINGS=470`（`OperationType:397`，回放 `EditLog:1414`，写 `EditLog:2580`）+ 中立 GSON 回放**必须存活**（旧 journal 兼容）。
4. **fe-core mutator 现状**（8 个事件调用点；只被事件路径调用，泛化无旁支）：
   - 已通用（翻转即用）：`CatalogMgr.unregisterExternalTable`（:679，走 `ExternalDatabase.unregisterTable`）；`RefreshManager.refreshExternalTableFromEvent/refreshTableInternal`（:224/:245，已含 D2 的 `PluginDrivenExternalCatalog.getConnector().invalidateTable` 钩子 :254-257）；`replayRefreshTable` rename 分支（:192-198）。
   - **多余 cast**（删即可，无行为变化）：`unregisterExternalDatabase`（:769 → 用通用 `ExternalCatalog.unregisterDatabase`，:1193 是唯一实现）；`refreshPartitions` 末尾 `setUpdateTime` cast（:297 → `ExternalTable.setUpdateTime` 通用 :311）。
   - **真阻塞**：`registerExternalDatabaseFromEvent`（:772）→ `catalog.registerDatabase`，通用 `ExternalCatalog.registerDatabase`（:1203）体是 `throw NotImplementedException`，只 `HMSExternalCatalog`（:205-215）override；`PluginDrivenExternalCatalog` **未 override** ⇒ 翻转后建/改名库事件**运行时抛异常**（编译通过、prod 才炸）。override 体（`buildDbForInit`+`metaCache.updateCache`+`Util.genIdByName`）全通用 ⇒ **上提到 `PluginDrivenExternalCatalog`**。
   - `registerExternalTableFromEvent`（:723）硬 cast `(HMSExternalCatalog)`(:742)+`(HMSExternalDatabase)`(:751) ⇒ 翻转 CCE。`buildTableForInit` 是 `ExternalDatabase:260` 通用 `T`，`PluginDrivenExternalDatabase.buildTableInternal`(:44-61) 返回正确子类（只 `new` 懒壳，**不访问 metastore**）⇒ 传通用 catalog/db、返回捕为 `ExternalTable`。
   - **分区缓存耦合**：`addExternalPartitions`（:792，`(HMSExternalTable)` :822 + `getPartitionColumnTypes` + `ExtMetaCacheMgr.hive(id).addPartitionsCache`）/ `dropExternalPartitions`（:835，`(HMSExternalTable)` :861 + `dropPartitionsCache`）/ `RefreshManager.refreshPartitions`（:263，`invalidatePartitionCache`）/ `replayRefreshTable` 分区分支（:200-221，gated `instanceof HMSExternalCatalog` + `refreshAffectedPartitionsCache((HMSExternalTable)...)`）—— 全戳 **D2 正在退休的 fe-core hive 缓存**。翻转后既 CCE 又丢失效 ⇒ 加 `PluginDrivenExternalCatalog` 分支路由到新 `Connector.invalidatePartition`。
5. **SPI 现状**：`Connector` 已有 `invalidateTable/invalidateAll/invalidateDb`（:311/315/323，D2，活）；**无** `invalidatePartition`、**无** pollOnce/事件方法。`ConnectorCapability` 无事件位。旧 `ConnectorMetaInvalidator`（5 动词，`invalidatePartition` 带 VALUES 退化整表）**仍死**（零生产调用）——**不复用它**，走 D2 活 SPI 族。插件 `HmsClient` **无** `getNextNotification/getCurrentNotificationEventId`（旧 fe-core `HMSCachedClient:74-78` 是模板；底层 vendored `HiveMetaStoreClient:3012/3043` 已实现 Thrift）。
6. **TCCL/R-010**：poller 路径**当前零 pin**（跨 `event/` grep 无 classloader）。样板 = `PluginDrivenScanNode.onPluginClassLoader`（:517-525，pin 到 `provider.getClass().getClassLoader()`）。驱动是 `MasterDaemon` 后台线程（不继承调用方 pin），须**显式**在每次 `pollOnce` 外 try/finally pin 到 `getEventSource().getClass().getClassLoader()`（=插件加载器），覆盖 RPC + JSON/GZIP 反序列化。`ThriftHmsClient.doAs` 只 pin SYSTEM 加载器且即刻还原，**不够**。
7. **无连接器现在轮询事件**（iceberg/paimon-native 仅手动 REFRESH）⇒ 本步是全新插件+SPI 管道，非镜像。

---

## 2. 目标架构（Model B，全休眠）

### 2.1 新 SPI（fe-connector-api / -spi，全 default，其余连接器零感知）
- **`MetastoreChangeDescriptor`**（中立 POJO，仅原语/枚举/名字，**无任何 HMS 类型**——R-010 依赖此）：
  - `op`：`REGISTER_DATABASE / UNREGISTER_DATABASE / RENAME_DATABASE / REGISTER_TABLE / UNREGISTER_TABLE / RENAME_TABLE(含视图重建，after 可==before) / REFRESH_TABLE / INVALIDATE_PARTITIONS(kind∈ADD/DROP/REFRESH)`
  - 载荷：`dbName, tableName`（remote），`dbNameAfter/tableNameAfter`（rename），`partitionNames: List<String>`（**规范名 `col=val/...`，非 values**），`updateTime`（ms），`eventId`（long）
- **`ConnectorEventSource`**：`long getCurrentEventId()` + `EventPollResult pollOnce(EventPollRequest req)`
  - `EventPollRequest{ long lastSyncedEventId, boolean isMaster, long masterUpperBound }`（follower 用 upperBound 上界；batchSize 连接器自读配置）
  - `EventPollResult{ long newCursor, List<MetastoreChangeDescriptor> descriptors, boolean needsFullRefresh }`（覆盖 master 首拉/`REPL_EVENTS_MISSING`、follower 首拉的"推进游标+无事件+需全刷"分支）
- **`Connector.getEventSource()`** → `ConnectorEventSource`（default `null`；探测=非空）
- **`Connector.invalidatePartition(String dbName, String tableName, List<String> partitionNames)`**（default no-op，名字键；D2 活族新成员）

### 2.2 插件侧（fe-connector-hms / -hive；只接管抓取+解析）
- `HmsClient` 加 `getCurrentNotificationEventId()` + `getNextNotification(lastEventId, maxEvents, filter)`（default 抛/空，避免测试替身破裂）；`ThriftHmsClient` 委派 vendored `HiveMetaStoreClient`；`CachingHmsClient` 透传。
- 反序列化器（`GzipJSONMessageDeserializer` + JSON 选择器）+ 事件→描述符映射 + `mergeEvents` 合并 **移入插件**。
- `HmsEventSource implements ConnectorEventSource`：master 分支 `getCurrentEventId` 比较+拉取；follower 分支按 upperBound 拉取；解析→合并→中立描述符；`REPL_EVENTS_MISSING` 检测→`needsFullRefresh=true`。
- `HiveConnector.getEventSource()`：增量同步启用时返回 `HmsEventSource`，否则 null（读自身 `HmsProperties`，try/catch→null 保"未初始化即跳过"语义）。覆盖**整个 HMS 目录所有格式**（hive+iceberg-on-HMS+hudi-on-HMS，名字键，无 DLAType）。
- `HiveConnector.invalidatePartition(...)`：镜像 `invalidateTable` 三段式 —— `CachingHmsClient.flush`（整表级）+ `HiveFileListingCache.invalidateTable`（location 键无法按名精确，退整表）+ `forEachBuiltSibling` 转发。

### 2.3 fe-core 侧
- **mutator 泛化（§1.4，全向后兼容 legacy，休眠安全）**：删 `registerExternalTableFromEvent` 两处 cast；`registerDatabase` 上提 `PluginDrivenExternalCatalog`；删 `unregisterExternalDatabase` 多余 cast；`addExternalPartitions/dropExternalPartitions/refreshPartitions/replayRefreshTable 分区分支`加 `if(PluginDrivenExternalCatalog) connector.invalidatePartition(...)` 分支（与现有 `instanceof HMSExternalTable` 分支互斥；pre-flip 走 HMS 分支不变）。
- **新驱动 `MetastoreEventSyncDriver extends MasterDaemon`**（**存活包** `datasource/`，非将删的 `datasource/hive/event/`）：
  - 迭代 `getCatalogIds()`；`catalog instanceof PluginDrivenExternalCatalog && getConnector().getEventSource()!=null` 才处理（能力探测，非 `instanceof HMS*`）。
  - 自持两 `Map<catalogId,Long>`（lastSynced + masterLastSynced）；`Env.isMaster()` 分角色。
  - `onPluginClassLoader` pin 外包 `pollOnce`；据 `EventPollResult`：`needsFullRefresh` → master `replayRefreshCatalog` / follower 转发 `REFRESH CATALOG`；否则应用描述符（走泛化 mutator + `connector.invalidatePartition`）；master 从描述符自建 `MetaIdMappingsLog`（op→ADD/DELETE×objType×names，**修 drop-partition 误标 DATABASE 的旧 bug 为 PARTITION**，喂 `nextMetaId` 保编辑日志形状）+ `replayMetaIdMappingsLog` + `editLog.logMetaIdMappingsLog`；存 `newCursor`。
  - 构造+启动**休眠**（`Env` 内，仿老 poller）：pre-flip 无匹配目录 → 惰性、不写编辑日志、游标空。

### 2.4 翻转时接线（Phase 2，非本步；本步只留清单）
- `MetastoreEventsProcessor:116` 老 gate 去除；`ExternalMetaIdMgr.replayMetaIdMappingsLog` 的游标回放**改指新驱动**（或双写，二选一，翻转时定）；启用新驱动对 flipped-hms 生效。老 poller + `datasource/hive/event/` 整目录 **Phase 3 删**（新驱动替换入口后）。
- **（复审新增，finding #2）主节点须初始化 flipped 事件源目录**：新驱动只处理已初始化的目录（避免 force-init 空闲的 paimon/iceberg/jdbc，保 pre-flip 字节不变）。但老 poller 曾在主节点强制初始化每个 HMS 目录。故翻转时须有一处让主节点**初始化 flipped 事件源目录**（否则"主从只读分离、某目录仅被从节点查询"时，主不 seed 游标 → 该从节点静默停更）。若不做则须签字接受该 HA 退化 + e2e 断言。

---

## 3. 提交序列（各休眠、独立、可复审）

| # | 提交 | 内容 | 休眠性 |
|---|---|---|---|
| E-a | feat SPI | `MetastoreChangeDescriptor`(+op enum) / `ConnectorEventSource` / `EventPollRequest/Result` / `Connector.getEventSource()` default null / `Connector.invalidatePartition` default no-op | 全 default，零连接器感知 |
| E-b | feat 插件取数 | `HmsClient.getCurrentNotificationEventId/getNextNotification` + `ThriftHmsClient` 委派 + `CachingHmsClient` 透传 | 无调用者 |
| E-c | feat 插件事件源 | 反序列化器+event→descriptor+merge 移入插件；`HmsEventSource`；`HiveConnector.getEventSource/invalidatePartition` | hms 未 SPI_READY |
| E-d | feat fe-core mutator 泛化 | de-cast + `registerDatabase` 上提 + 分区 PluginDriven 分支（互斥、加性） | PluginDriven 分支 pre-flip 不可达 |
| E-e | feat fe-core 驱动 | `MetastoreEventSyncDriver` + Env 构造/启动 | pre-flip 无匹配目录 |
| E-f | test | 描述符映射、`registerDatabase` on PluginDriven 不抛、驱动能力探测跳过非事件连接器等休眠单测 | — |

设计稿 + HANDOFF 单独 commit（与 code 分开）。每步 `mvn -pl <mod> -am test` + `check-connector-imports.sh` + checkstyle。

---

## 4. e2e 欠账（翻转后，勿静默丢，Rule 12）
异构 HMS docker：CREATE/DROP/ALTER(rename)/INSERT + ADD/DROP/ALTER PARTITION 事件到达后 FE 元数据同步正确（master + follower 双跑）；跨类加载器 pin 无 CCE（child-first fixture，单测同加载器测不出 R-010）；`HmsGsonCompatReplayTest` 旁的 opcode-470 旧 journal 回放；heterogeneous 事件覆盖 iceberg-on-HMS/hudi-on-HMS。

## 5. 铁律核对
- fe-core 无新增 `instanceof HMS*`/`switch(dlaType)`（新驱动用 `PluginDrivenExternalCatalog`+能力探测）✓
- fe-core 不解析属性（enable 开关随连接器）✓
- 跨边界 pin TCCL（驱动线程包 pollOnce）✓
- `history_schema_info` 逐层小写：本步不涉 schema 字典 ✓
- `PluginDrivenMvccExternalTable` 字节+成本双不变：本步不改其共享方法（只经 `buildTableInternal` 新建，走既有路径）✓

---

## 6. 落地 + 净室复审记录（2026-07-10 晚，DONE）
6 步全部落地（休眠、独立提交、各 test-compile SUCCESS + checkstyle 0 + import gate ok）：
E-a `0214f04`（SPI）→ E-b `b13ed79`（插件取数）→ E-c `902546d`（插件事件源）→ E-d `3552554`（fe-core mutator 泛化）→ E-e `6a96820`（fe-core 驱动）→ E-f `9113c51`（休眠 parser 单测，6 绿）。

**净室对抗复审**（`wf_0d49c409-a86`：6 维盲评 → 逐条 refute-by-default 对抗验证）= 14 疑点、4 坐实（其余 10 条含大小写/空分区/enable-gate 等经验证为误报或休眠+翻闸自owed）。4 坐实收敛为 3 个真回归（均休眠 pre-flip、须翻闸前修）：
- **自愈丢失（poison event 死锁，#1≡#3，#4 的根因）** → 已修 `fb21498`：老 poller 遇处理异常做 `onRefreshCache(true)+游标归 -1` 自愈跳过毒事件；新驱动之前无限重试。修法=`HmsEventSource` 把**瞬时 fetch 错误**原地重试(`ofNothing`，不 reset/不invalidate)，只让**确定性 parse/apply 错误**上抛；驱动 `realRun` catch 把游标归 -1 → 下轮 first-pull 全刷跳过毒事件；`applyDescriptors` 不再回退一格。
- **eager gzip 解压（#4）** → 已修 `134907b`：`prepareBody` 按 `needsBody(type)` 惰性——db/insert/ignored 事件不解压（省 CPU + 损坏体不抛；死锁本身已由自愈兜底）。
- **`isInitialized()` gate（#2，未改代码，留翻闸项）**：跳过主节点空闲目录会漏 seed 游标 → 从节点静默停更；但去掉 gate 会 force-init 空闲 paimon/iceberg/jdbc（破坏 pre-flip 字节不变）——**两难**。裁决=保 gate（字节不变优先）+ 改诚实注释 + 列为翻闸项（§2.4 复审新增：翻转时主节点初始化 flipped 事件源目录）。

**⚠ 复审确认的 e2e 欠账（补 §4）**：毒事件自愈（构造确定性抛错的 descriptor，断言下轮全刷自愈非死锁）；主从只读分离下 flipped 目录游标传播（断言从节点不停更）；gzip/plain 各事件类型的表/分区体解析忠实度（本步单测只覆盖 body-free 中立路径）。
