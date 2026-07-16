# Apache Doris Catalog SPI 迁移 — 完整评审

> **范围**:Issue [#65185](https://github.com/apache/doris/issues/65185) 伞形跟踪的全部 9 个 PR(P0–P6),分支 `branch-catalog-spi`
> **基线**:所有结论对齐分支最新 tip `3ba75b7cf8a`,并与当前 `apache/master`(legacy 代码)逐一对照
> **方法**:20 个独立 finder(按角度+子系统分片)→ 对 tip 复核 → 6 个对抗验证器(CONFIRMED/PLAUSIBLE/REFUTED)→ live 集群实测(paimon filesystem + iceberg hadoop catalog)
> **注**:9 个 PR 均已 MERGED。本文既是对已合入代码的记录性评审,也是给 P7(hive)/P8(清理)的风险清单。

---

## 目录

1. [执行摘要](#1-执行摘要)
2. [迁移做了什么(架构概览)](#2-迁移做了什么架构概览)
3. [架构评估](#3-架构评估)
4. [抽象评估](#4-抽象评估)
5. [扩展性评估](#5-扩展性评估)
6. [贯穿性正确性主题](#6-贯穿性正确性主题)
7. [安全观察](#7-安全观察)
8. [逐 PR 详细发现](#8-逐-pr-详细发现)
9. [验证方法与统计](#9-验证方法与统计)
10. [优先级建议](#10-优先级建议)
- [附录 A:被证伪的候选](#附录-a被证伪的候选)
- [附录 B:live 集群实测证据](#附录-b-live-集群实测证据)

---

## 1. 执行摘要

这是一次**规矩、低单点风险、但把大量"期票"推给 P7/P8 兑现**的重构。

**整体判断**:SPI 形状直接对标 Trino,分层清晰,对已迁移连接器(jdbc/es)真正零影响,GSON 升级兼容(invariant #2)在 iceberg/paimon/maxcompute 上都验证干净,写路径/事务提交协议/10 个 iceberg procedure 逐字节忠实移植。**架构本身没有硬伤。**

**真正的风险全是行为细节和结构债**,分三类:

- **已确认的正确性回归**(对 master):P4 batch-mode 闸门塌陷(高)、P4 分区值缓存删除(每查询往返 ODPS)、P6 iceberg 缓存偏斜→BE DCHECK 崩溃(高,需特定 TTL 时序)、P6 equality-delete 行数不 gate、P6 s3tables 默认凭证链回归、P6 region 别名收窄。
- **休眠的 P7 地雷**:P3 hudi plugin 路径全套 4 个高危 bug(分区值不 unescape、datetime ISO 匹配不上、HMS 名当存储路径、混大小写 Avro 崩 JNI)——今天不触发(hudi 不在 `SPI_READY_TYPES`),但 P3 建立的正是 P7 要信赖的"测试基线",而 parity 测试写法抓不到这些。
- **运营/升级坑**:P6 升级只换 `lib/` 不部署 `plugins/connector` → 所有存量 iceberg catalog 首次访问即抛异常(invariant #2 的"透明迁移"只在完整部署时成立)。

**贯穿性设计问题**(比单个 bug 更值得 PMC 关注):
- EXPLAIN 节点名 `VPluginDrivenScanNode` 系统性违反 invariant #3("EXPLAIN output preserved"),且 `deviations-log` 无记录。
- 三处 per-connector 字符串 switch(引擎名/表类型名)必须手改同步,无编译期保护。
- SPI 表面正在"每迁移一个连接器就长出该连接器私有词汇"(MVCC 微秒/毫秒、iceberg 的 branch/tag 方法、odps 的 block 分配),偏离"中立 SPI"承诺。
- `SUPPORTS_SHOW_CREATE_DDL` 无引擎侧脱敏——靠连接器作者自觉不泄密码。

---

## 2. 迁移做了什么(架构概览)

**目标**:把 FE 里硬编码的外部数据源(hive/iceberg/paimon/hudi/trino-connector/maxcompute/jdbc/es)从 `fe-core` 解耦成 `fe/fe-connector/*` 下可独立加载的插件,`fe-core` 只保留通用设施和稳定的 catalog SPI 桥。终态:每个连接器一个自包含 zip;`fe-core` 无任何连接器的编译期知识。

**三条不变量(验收标准)**:
- **I1 — 单向依赖** `fe-connector → fe-core`;连接器禁止 `import org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`,CI grep 守门。
- **I2 — 元数据向后兼容**:旧镜像里持久化的 `IcebergExternalCatalog` 等 GSON 类型必须透明反序列化+迁移到 `PluginDrivenExternalCatalog`。
- **I3 — 用户可见行为零回归**:`SHOW CREATE CATALOG`、`information_schema`、EXPLAIN 输出、错误信息、catalog type/engine 名全保留。

**阶段**:P0(SPI 基线)→ P1(scan-node 路由收口)→ P2(trino,首个端到端样板)→ P3(hudi 加固,cutover 推迟到 P7)→ P3b(kerberos 收拢进 fe-kerberos)→ P4(maxcompute,首个删 legacy)→ P5+P5b(paimon 迁移+删 legacy)→ P6(iceberg,最大,7 flavor+MVCC+写路径+procedures)。P7(hive)/P8(删 allowlist)未做。

**核心桥接类**(全在 fe-core):
| 类 | 作用 |
|---|---|
| `PluginDrivenExternalCatalog extends ExternalCatalog` | catalog 层桥,路由 DDL/元数据到 `connector.getMetadata(session)` |
| `PluginDrivenExternalTable` / `PluginDrivenMvccExternalTable` | 表层桥,后者实现 `MvccTable`/`MTMVRelatedTableIf` |
| `PluginDrivenScanNode extends FileQueryScanNode` | 扫描节点,统一承载所有插件连接器的 scan |
| `ConnectorMvccSnapshotAdapter implements MvccSnapshot` | 把 SPI 快照套进引擎既有 MVCC pin 管线 |
| `PluginDrivenTransactionManager` | 双入口事务记账(legacy auto-commit + SPI ConnectorTransaction) |
| `CatalogFactory.SPI_READY_TYPES` | allowlist,决定哪些 catalog type 走插件路径(当前:jdbc/es/trino-connector/max_compute/paimon/iceberg) |

---

## 3. 架构评估

### 3.1 SPI 分层与依赖方向 —— ✅ 做对了

- **血统清晰**:`ConnectorSession` / `ConnectorMetadata`(聚合 6 个细粒度 Ops 接口:Schema/Table/Pushdown/Statistics/Write/Identifier)/ `ConnectorTableHandle` / `applyFilter`/`applyProjection` handle-update 模式,直接对标 Trino SPI。行业验证过的形状,后来者好上手,增量迁移成本最低。
- **全 default 方法**:连接器只 override 自己支持的能力,对已迁移的 jdbc/es 真正零影响(FakeConnectorPlugin 测试锁死了每个 default 的行为)。
- **反向通道做对了**:`ConnectorMetaInvalidator` 经 `ConnectorContext` 下发,连接器回调引擎丢缓存而不 import 引擎;`ConnectorMvccSnapshotAdapter` 用适配器把 SPI 类型包进引擎既有 `MvccSnapshot`,fe-core 类型不泄漏进 SPI。
- **守门存在但不完备**(见 [6.5](#65-升级兼容-invariant-2) 与 P0 发现):grep 黑名单,漏 static import、漏 `persist/transaction/fs/statistics` 等包、只扫 `src/main/java`。是"聊胜于无"而非"机器保证 I1"。

### 3.2 桥接层 —— ✅ 结构合理,但 `PluginDrivenScanNode` 在膨胀

`PluginDrivenScanNode` 从 P4(~200 行)到 P6(~1000+ 行)承载了所有连接器的两套 batch-scan 模型、MVCC pin、field-id dict、分区展示……它是"统一收口"的收益点,也是复杂度积聚点。P6 里 `hasSnapshotPin` 分支的 field-id dict 构建 + 独立 schema 缓存,正是 [6.4](#64-缓存一致性) 那个 BE 崩溃隐患的所在。**建议**:随着 P7 加入,考虑把 MVCC/dict 逻辑拆成可组合的 helper,别让这个类继续成为所有连接器 scan 复杂度的垃圾场。

### 3.3 类加载隔离模型 —— ⚠️ 有一处真实的锁失效

每个插件 zip 走 `ChildFirstClassLoader`,`FS_PARENT_FIRST_PREFIXES` 声明哪些包父加载优先,`TcclPinningConnectorContext` 在调用连接器 SDK 前 pin 线程上下文 classloader。

**隐患**(P3b-C5,CONFIRMED medium):`fe-filesystem-hdfs` 现在编译依赖 `fe-kerberos`,而 plugin-zip 的 dependencySet 没排除它(违反自己"fe-core classpath 已有的 jar 要排除"的策略,fe-core 也 ship fe-kerberos)。`FS_PARENT_FIRST_PREFIXES` 列了 `org.apache.hadoop.` 但**没列 `org.apache.doris.kerberos.`** → 目录加载的 HDFS 插件拿到 child-first 的第二份 `HadoopKerberosAuthenticator`,其 `synchronized (HadoopKerberosAuthenticator.class)` 类锁与 fe-core 那份**不互斥**,而两份都在改同一个父加载的 `UserGroupInformation` 全局状态。修复一行:把 fe-kerberos 从 plugin lib/ 排除,或把前缀加进父加载优先列表。

---

## 4. 抽象评估

### 4.1 抽象得好的地方

- **数据面演进友好**:请求/结果全用不可变 POJO + builder(`ConnectorCreateTableRequest` 等),加字段=加 setter 不破签名;`ConnectorPartitionInfo` 加统计字段用"哨兵 UNKNOWN + 保留旧构造器"。
- **向后兼容内建在默认实现里**:新旧 `createTable` 的降级链、`beginTransaction` 默认 throw = 引擎按 auto-commit 处理。

### 4.2 Stringly-typed 契约 —— ⚠️ 零编译期保护

- partition transform 是字符串(`"year"`/`"bucket"`,不认识的**静默当 CUSTOM**);
- bucket 算法是字符串(`"doris_default"`/`"hive_hash"` 占位);
- `ConnectorPartitionField.transformArgs` 只能 `List<Integer>`(paimon 的非整数 transform 参数放不进);
- 契约活在 javadoc 和 RFC 附录里,typo 直接变语义。

### 4.3 SPI 表面正在"连接器化" —— ⚠️ 这是最该管的抽象趋势

**核心观察:每迁移一个连接器,就往"中立" SPI 上长出该连接器的私有词汇。**

| SPI 成员 | 归属连接器 | 问题 |
|---|---|---|
| `ConnectorTransaction.supportsWriteBlockAllocation()` / `allocateWriteBlockRange()` | maxcompute(ODPS tunnel block-id) | 其他连接器全无用,javadoc 自认"e.g. maxcompute" |
| `Connector.deriveStorageProperties()` | iceberg(hadoop-catalog warehouse→fs.defaultFS) | 唯一 override 是 iceberg |
| `ConnectorContext.cleanupEmptyManagedLocation(location, tableChildDirs)` | iceberg | 硬编码 iceberg `["data","metadata"]` 目录布局 |
| `ConnectorTableOps` 的 7 个 branch/tag/partition-field 方法 | iceberg | `PartitionFieldChange` 建模 iceberg 的 `transformName + Integer transformArg`,hive/jdbc/es 永远实现不了 |
| `ConnectorMvccPartitionView.getNewestUpdateTimeMillis()` | iceberg | 名字说毫秒,javadoc 说是**微秒**(源定义,只依赖单调性)——下一个 MVCC 连接器按名字返回真毫秒就混单位 |

**历史教训已经发生**:P0 定义的 MVCC 三方法 `beginQuerySnapshot`/`getSnapshotAt`/`getSnapshotById`,到 P6 iceberg 真接入时被推翻重塑成 `resolveTimeTravel(spec)`/`applySnapshot(handle, snapshot)`——三分之二没活过第一个真实用户。这印证了 foundation-first 的固有风险:**没有 consumer 的 API 设计活不过第一个真实用户**,所以 P0 定义的每个 SPI 面都得到对应 consumer PR 里回头验证。

**建议**:把连接器专属能力收进可选 facet 接口(像 `getProcedureOps()` 那样能力发现),而不是堆在根接口当 default 方法。否则到 P8,"中立" SPI 会是每个 format 私有词汇的并集,实现者分不清 ~40 个 default 里哪些对自己是 load-bearing。

### 4.4 Magic-key 通道 —— ⚠️ 把 Doris SQL 方言塞进字符串 map

SHOW CREATE TABLE 的子句通过 table-properties map 的保留魔法键传递:`show.location` / `show.partition-clause` / `show.sort-clause`——**连接器预渲染 Doris SQL 文本**(`PARTITION BY ... BUCKET(8, \`c\`)`),fe-core 逐字 append 后再按名字 strip。外加 `partition_columns` / `primary_keys` 两个 undeclared 魔法键。

问题:①每个连接器都得内嵌 Doris 的 SQL 方言/引号规则(altitude 倒置);②真名为 `show.location` 的用户属性会被误 strip;③连接器 jar 与 fe-core 现在是独立版本化的构件,Doris 改 SHOW CREATE 语法时,每个连接器预渲染的字符串就与引擎版本漂移。**更正确**:一个结构化的 `ConnectorTableDdl` 载体(partition transform + sort key 作为数据),由 fe-core 渲染,方言知识留在一个 altitude。

---

## 5. 扩展性评估

### 5.1 P7 hive 能零改动接入吗? —— ❌ 不能

发现若干"plugin-first 路由默认不转发,只 legacy hive 分支转发"的能力,P7 切换即静默失效:

- **TABLESAMPLE**:`PluginDrivenScanNode` 完全无 table-sample 管线,SPI 无表达。`SELECT ... TABLESAMPLE(10 PERCENT)` 切换后静默全表扫(P1 发现)。
- **SQL cache**:`CacheAnalyzer` 用 `instanceof HiveScanNode` 统计可缓存性(:305-325),hive 变 `PluginDrivenScanNode` 后 SQL cache 静默不匹配,`COUNTER_QUERY_HIVE_TABLE` 也停止递增。
- **kerberos**:hive 是最重的 kerberos 用户(HMS + HDFS + metastore event poller),会把 [3.3](#33-类加载隔离模型--️-有一处真实的锁失效) 的锁分裂 + N 份独立登录/续期计时器放大。
- **hudi-on-HMS 的 DLA dispatch**(P3 设计):一个 catalog 服务两种表格式,dispatch 是否泛化到 iceberg-on-HMS、P7 full hive,还是 hudi-shaped,值得在 P7 前定型。

### 5.2 P8(删 allowlist)不是纯删除 —— ⚠️

P6 把 flavor 知识从 `instanceof` 搬进了通用类里的**字符串 switch**,而非让它消失:

- `PluginDrivenExternalTable.getEngine()` 硬编码 `case "iceberg"`;
- `FileQueryScanNode` 硬编码数据缓存 allowlist `Arrays.asList("hms","iceberg","paimon")`;
- `PlanNode.printNestedColumns` 仍 `instanceof IcebergScanNode`(cutover 后死代码);
- `CatalogFactory.SPI_READY_TYPES` 仍按字符串 gate 插件路径;
- `TableType.ICEBERG_EXTERNAL_TABLE` 还有 5 处编译期引用。

**后果**:一个全新连接器插件在 tip 上仍会拿到错误引擎名、被数据缓存排除、根本加载不了——直到有人改 fe-core。**连接器在 tip 上还不是真正可插拔的**,flavor 知识只是从 instanceof 挪进了字符串 switch。P8 要真删 allowlist,得先把这些 switch 换成 Connector SPI 声明的属性。

### 5.3 N-连接器 × N-编辑 的 switch —— ⚠️ 无编译期保护

引擎名/表类型名现在是**三处**必须手改同步的 fe-core switch(`CreateTableInfo.pluginCatalogTypeToEngine` + `PluginDrivenExternalTable.getEngine()` + `getEngineTableTypeName()`),javadoc 自认"the two switches must stay in sync"。P6 已经给三处都加了 iceberg case,证明了 N×N 轨迹。漏改一处 → `SHOW TABLE STATUS`/`information_schema` 里静默显示通用 `Plugin` 引擎名——正是这些 switch 存在要防的 I3 回归,却没有编译期或测试把它们绑在一起。**建议**:`Connector.getLegacyEngineName()`,每个插件声明一次。

### 5.4 跨连接器复制 —— ⚠️

| 复制物 | 副本 | 相似度 |
|---|---|---|
| `TcclPinningConnectorContext` | iceberg / paimon | ~97%(paimon javadoc:"the paimon analogue of the iceberg connector's") |
| `buildHadoopConfiguration` / `assembleHiveConf` | PaimonCatalogFactory / IcebergCatalogFactory | iceberg javadoc:"mirror PaimonCatalogFactory" |
| flavor-provider wrapper | dlf/hms/jdbc/rest × iceberg/paimon | 4 对,~80-92% |
| EQ/IN 分区裁剪块(~110 行)+ FakeHmsClient stub | hive / hudi | 3 份 stub |

TCCL decorator 守的是 classloader 隔离正确性——一份修了另一份漏,产生连接器专属的 classloader bug。一个共享 `fe-connector-support` 模块能止住这种线性增长。

---

## 6. 贯穿性正确性主题

### 6.1 EXPLAIN / 行为 parity(invariant #3)—— 系统性违约

- **EXPLAIN 节点名**:legacy 打印 `VPAIMON_SCAN_NODE`/`VICEBERG_SCAN_NODE`(master `PaimonScanNode.java:159` 传 `"PAIMON_SCAN_NODE"`),插件路径打印 `VPluginDrivenScanNode` + `CONNECTOR: x`。live 实测坐实。回归测试是**改期望值接受**而非保留行为。`deviations-log` 无记录。修复廉价:`planNodeName = connectorType.toUpperCase() + "_SCAN_NODE"`。
- **错误信息 drift**:P0 的 "CREATE TABLE not supported"(丢 catalog 名)、P4 的 DB DDL errno(丢 1007/1008)、P6 的 `position_deletes`/FOR TIME 越界错误文本。
- **SHOW PARTITIONS**:P4 现在 admit 所有 `PluginDrivenExternalCatalog`(jdbc/es/trino 原本被定向拒绝),错误类别对非分区连接器改变;MC 的 LIMIT/OFFSET 语义从远端分页变成全取-排序-分页。
- **selectedPartitionNum 语义**(P5,CONFIRMED medium):从"SDK 规划 split 里的 distinct 分区"变成"Nereids FE 剪枝后分区数",影响 EXPLAIN `partition=N/M` 和 `sql_block_rule` 的 `partition_num` 检查——legacy 通过的规则现在可能 block 查询。

### 6.2 休眠的 P7 地雷(P3 hudi,全部 CONFIRMED,当前 dormant)

hudi plugin 路径今天不触发(不在 `SPI_READY_TYPES`),但 P3 是 P7 的"测试基线",且 parity 测试写法抓不到这些:

- **分区值不 unescape**(high):HMS 存 `ts=2024-01-01 10%3A00%3A00`,`parsePartitionName` 拆原始文本直接比,含 `空格/:/%/=` 的分区值永远匹配不上→静默剪掉→丢行。legacy 用 `FileUtils.unescapePathName`。
- **datetime ISO 匹配不上**(high):fe-core 把 datetime 字面量包成 `LocalDateTime`,`String.valueOf` 出 `2024-01-01T10:00`(T 分隔、秒省略),永远不等于 Hive 路径文本→整表分区剪光→0 行。
- **HMS 名当 Hudi 存储路径**(high):`prunedPartitionPaths` 存 HMS 名 `year=2024/month=01`,喂给 `fsView.getLatestBaseFilesBeforeOrOn` 却期望 Hudi 相对存储路径;非 hive-style 分区(hudi 默认)存储布局是 `2024/01`→带 filter 0 split、不带 filter 有行。legacy 对分区 LOCATION 做相对化。
- **混大小写 Avro 崩 JNI**(high):`getTableSchema` 小写化,但 planScan 送原始 `Schema.Field::name`;BE `HadoopHudiJniScanner` 精确大小写查找→`IllegalArgumentException` 崩每个 MOR/JNI split。该 PR 自己的 `HudiSchemaParityTest` 用的就是 `Id`/`Name`/`Addr`。
- **COW/MOR 3-态 `"UNKNOWN"` 静默当 MOR**(medium):违背该 PR 自己的 fail-loud 设计;丢了 legacy 的 `skip_merge`/`flink.table.type` COW 信号;`spark.sql.sources.provider→COW` 是发明的启发式(无 legacy 对应),把 spark 注册的 MOR 表误读成 COW→跳 delta log→陈旧行。

### 6.3 谓词下推分歧

- **P4 maxcompute 变全有全无**(CONFIRMED low/perf):一个不可转换的 conjunct 丢掉整个 ODPS filter(legacy 逐 conjunct 保留可转的)。correctness 安全(BE 重过滤),但选择性查询扫描量暴涨。
- **P4 `==` vs `=`**(PLAUSIBLE medium):EQ 发 `==`,ODPS SDK 描述是 `=`,ODPS 解析器是否容忍 `==` 静态无法确定,需 live A/B。**注:同一处的 IN 方向改动其实修了 legacy 的一个反向 bug**(legacy 发的是反的 IN 谓词),这是 bugfix,建议加回归测试。
- **P5 paimon 三个"更激进"候选被 cast-guard 证伪**(REFUTED):`value.toString()` VARCHAR、Number 截断、LIKE 不查类型——都因 `supportsCastPredicatePushdown()=false` 在转换前 strip 掉含 cast 的 conjunct 而不可达。**这个 guard 是 load-bearing 的**,建议加测试/注释 pin 住,免得未来"启用 cast 下推"静默放出这些 arm。
- **P5 `convertAnd` 部分合取**(CONFIRMED low/sound):OR 下的部分 AND 保留可转子集(legacy 要求两侧都可转否则 null)。验证为 sound(部分 AND 严格更弱 + BE 重过滤),只是 plan/profile 可见差异。

### 6.4 缓存一致性

- **P6 快照缓存 vs schema 缓存偏斜 → BE DCHECK 崩溃**(PLAUSIBLE high):`beginQuerySnapshot` 从 `IcebergLatestSnapshotCache` pin `snapshotId+schemaId`,但 slot schema 从独立的 name-keyed fe-core schema 缓存绑定,两缓存独立 TTL 过期。外部 ALTER 后两者错时重载→pinned schemaId 与 bound slots 偏斜→BE field-id dict 缺 scan slot→`IcebergScanPlanProvider` 自己注释说会触发 BE StructNode DCHECK 崩溃。legacy 结构免疫(schema 缓存 keyed BY 快照的 schemaId,单一原子源)。无单查询复现(需独立 TTL 时序),但崩溃模式严重。**建议**:schema 缓存 key off pinned schemaId,照 legacy。
- **P6 同表双引用 version-blind**(PLAUSIBLE medium):`t JOIN t FOR VERSION AS OF old`,schema 绑定用 version-blind 取快照(偏好 latest)而 scan 用 version-aware pin old→同类偏斜。legacy 是"differently-unsound"(table-only key,两引用共享一快照),javadoc 承认此限。
- **P4 分区值缓存删除**(CONFIRMED medium):`getNameToPartitionItems` 每查询做一次完整 ODPS `listPartitions` 往返(legacy 从 `MaxComputeExternalMetaCache` 供)。数万分区的表每次规划多秒 + API 限流风险。

### 6.5 升级兼容(invariant #2)

- **✅ GSON shim 干净**:iceberg 8 flavor + database + table、paimon 5 flavor、maxcompute 全有 `registerCompatibleSubtype` shim + 回放测试;logType 保留;`gsonPostProcess` 从 logType 回填 `type` 且不改 catalog 属性。
- **⚠️ 运营坑**(P6,high operational):只换 `lib/doris-fe.jar` 不部署 `plugins/connector/iceberg`(经典升级手法)→镜像加载 OK,但首次访问任何存量 iceberg catalog 抛 `RuntimeException("No ConnectorProvider found...")`(`PluginDrivenExternalCatalog.java:132`),因为 legacy fe-core `iceberg` fallback 已删。"透明迁移"只在完整部署新 plugins 目录时成立。**需响亮的 release note + 升级文档步骤**(build.sh 部署到 `plugins/connector` 而非 `lib/`)。
- **⚠️ 守门有洞**(P0):grep 黑名单漏 `import static`、漏 `persist/transaction/fs/statistics/mysql/service` 包、只扫 `src/main/java`。今天无 live 违规,但这是 I1 的机器执行——建议反转为 allowlist(只许 `connector/thrift/filesystem/extension`)+ 匹配 `^import (static )?` + 扫测试源。

---

## 7. 安全观察

- **`SUPPORTS_SHOW_CREATE_DDL` 无引擎侧脱敏**:引擎把 `getTableProperties()` 逐字渲染进 SHOW CREATE TABLE(`Env.java:~4891`),仅靠 capability gate。capability 的 javadoc 自认:唯一防止密码泄露的是连接器作者记得在属性含凭证时不声明该 flag。一个未来的 JDBC 系 lakehouse 连接器若声明了 flag 而属性含密码,任何有 SHOW 权限的用户都能看到明文。**建议**:DDL 渲染路径加敏感键过滤(defense in depth)。
- **敏感键 masking 硬编码 per-connector 进 fe-core**:`DatasourcePrintableMap` 的 `SENSITIVE_KEY` 加 `MCProperties.SECRET_KEY`、iceberg REST 键块(注释"Keep in sync with the connector's sensitive REST keys")——每个新连接器要么改 fe-core,要么在 SHOW CREATE CATALOG 里静默泄密。

---

## 8. 逐 PR 详细发现

> 严重度:🔴 高 / 🟠 中 / 🟡 低。验证:**C**=CONFIRMED / **P**=PLAUSIBLE。dormant=当前不触发(P7 切换后触发)。

### P0 #63582 — SPI 基线 + DDL/Partition + import gate

*29 文件。无独立验证器;finder 对 tip 自校验,6 条全部 still_at_tip。已在 tip 修掉的 2 条(CTAS 已存在语义、建表后缓存失效)未报。*

| # | 严重度 | 文件:行 | 发现 | 失败场景 |
|---|---|---|---|---|
| P0-1 | 🟠 I3 | `PluginDrivenExternalCatalog.java:407` + `ConnectorTableOps.java:152` | 不支持的 CREATE TABLE 报 "CREATE TABLE not supported"(丢 catalog 名),legacy 报 "Create table is not supported for catalog: `<name>`";DROP/RENAME/ADD COLUMN 同样 drift | 客户端错误断言/日志解析失配,用户丢失 catalog 名 |
| P0-2 | 🟠 | `CreateTableInfoToConnectorRequestConverter.java:212` | `readBucketNum` catch-all 吞异常返回 0 桶 | `translateToCatalogStyle()` 抛异常时,连接器拿 `numBuckets=0` 建出错误分布的表;违反 AGENTS.md "report errors or crash" |
| P0-3 | 🟠 | `...Converter.java:159` | `convertFields` 静默丢不认识的分区表达式形状 | 不支持的 partition transform → 建出无分区/错分区表且报成功;丢失发生在 fe-core,连接器无法察觉 |
| P0-4 | 🟠 | `ConnectorTableOps.java:167` | SPI 默认 `createTable(request)` 降级到旧签名,静默丢 partition/bucket/external/ifNotExists | 只实现旧签名的连接器接受 `PARTITION BY` 建出无分区表报成功 |
| P0-5 | 🟡 I1 | `tools/check-connector-imports.sh:48` | 守门三洞:漏 `import static`、漏 persist/transaction/fs/... 包、只扫 main | 后续 phase 加 `import static org.apache.doris.catalog...` 或 `import org.apache.doris.persist.EditLog` 时守门放行,静默重耦合 |
| P0-6 | 🟡 | `ExternalMetaCacheInvalidator.java:72` | `invalidateStatistics` 静默 no-op;`invalidatePartition` 退化整表 | 未来连接器(HMS 事件)调 invalidateStatistics 期望丢陈旧行数,什么都没发生,优化器继续用陈旧统计;告诫只在 fe-core 实现注释里,SPI 侧不可见 |

### P1 #63641 — plugin-first 路由收口

*5 文件。3 条候选在 tip 已被后续 phase 修掉(FileScan 分支 setSelectedPartitions、hudi 增量/时旅 fail-loud、嵌套列裁剪 capability 化)——未报,验证了"对 tip 复核"的价值。*

| # | 严重度 | 文件:行 | 发现 |
|---|---|---|---|
| P1-1 | 🟠 dormant | `PhysicalPlanTranslator.java:836/911` | `visitPhysicalHudiScan` plugin 分支缺 `setSelectedPartitions` + `setDistributeExprLists`(FileScan 兄弟分支已修,这条漏了)→ P7 hudi 切换后分区不裁剪 + bucket-shuffle 计划退化。根因:dispatch 块两个 visitor 复制粘贴已漂移 |
| P1-2 | 🟠 dormant | `PhysicalPlanTranslator.java:740` | plugin 分支不转发 `getTableSample()`,`PluginDrivenScanNode` 无 table-sample 管线 → P7 hive 切换后 TABLESAMPLE 静默全表扫 |
| P1-3 | 🟡 | `CacheAnalyzer.java:305` / `PlanNode.java:949` | `instanceof HiveScanNode`/`IcebergScanNode` 残留:iceberg 已由 P6 切换使 PlanNode 那处 arm 死代码;hive 待 P7 切换后 SQL cache 静默失效 |

### P2 #64096 — trino(首个端到端样板)

*33 文件。验证器:C1/C7/C8 REFUTED(见附录 A)。首个迁移样板,设计模式被 P4/P5/P6 复制。*

| # | 严重度 | 验证 | 文件:行 | 发现 | 失败场景 |
|---|---|---|---|---|---|
| P2-1 | 🟠 | C | `TrinoConnectorDorisMetadata.java:243` | 每个元数据方法开 Trino 事务从不 commit/rollback/close;legacy 每次 schema load 缓存一个复用 | 有状态 Trino 连接器(hive TransactionManager)每查询泄漏 3+ 未关事务→FE 内存随查询数无界增长 |
| P2-2 | 🟠 | C | `TrinoBootstrap.java:136` | `getInstance(pluginDir)` 首胜单例,但 `trino.plugin.dir` 宣传为 per-catalog | 第二个 catalog 用不同 dir 静默用第一个的→"Cannot find Trino ConnectorFactory" |
| P2-3 | 🟡 I3 | C | `TrinoDorisConnector.java:78` | `preCreateValidation` 在 CREATE CATALOG 时急切加载插件+构造连接器;legacy 惰性 | 先建 catalog 后铺插件目录的脚本现在 CREATE 就失败;replay 跳过校验→create-vs-replay 分歧 |
| P2-4 | 🟡 | P | `TrinoConnectorDorisMetadata.java:107` | `listTableNames` 丢了 legacy 的 LinkedHashSet 去重 + prefix 过滤 | listTables 返回重复 SchemaTableName 的连接器→SHOW TABLES 重复行 |
| P2-5 | 🟡 | P | `TrinoDorisConnector.java:176` | DCL 发布顺序:guard 字段 `trinoConnector` 先于 `trinoSession` 赋值,fast-path 只查前者 | 并发首次访问落入两次 store 之间→`trinoSession.toConnectorSession` 瞬时 NPE(volatile 自愈) |

### P3 #64143 — hudi 加固(cutover 推迟 P7)

*28 文件。验证器:**8/8 CONFIRMED**。全部 dormant(hudi 未进 allowlist),但 P3 是 P7 基线。详见 [6.2](#62-休眠的-p7-地雷p3-hudi全部-confirmed当前-dormant)。*

4 高危(unescape / datetime ISO / HMS 名 vs 存储路径 / 混大小写 Avro JNI)+ 2 中(UNKNOWN 静默 MOR / 检测器丢 legacy COW 信号)+ 2 低(ENUM→STRING 误标 parity / 三份复制)。

### P3b #64655 — kerberos 收拢进 fe-kerberos

*82 文件。验证器分类 master-parity-break / intra-branch / new-code。C3/C6/C7/C8 REFUTED(master 同病=parity,见附录 A)。*

| # | 严重度 | 验证 | 分类 | 发现 |
|---|---|---|---|---|
| P3b-1 | 🟠 | C | parity-break | simple-auth 无 `hadoop.username` 时身份从"FE 进程用户"翻转为 remote user `"hadoop"`+doAs;按 FE 进程用户授权 ACL 的 HDFS 访问升级后 `AccessControlException` |
| P3b-2 | 🟠 | C | parity-break | `HadoopKerberosAuthenticator` 每次 login/refresh 无条件 `UGI.setConfiguration` + 强设 `authorization=true`;删了 legacy 的 first-writer-wins guard;混 simple+kerberos 多 catalog 更频繁抢写 JVM 全局 UGI |
| P3b-3 | 🟠 | C | new-code | plugin zip 双载 fe-kerberos,类锁分裂(见 [3.3](#33-类加载隔离模型--️-有一处真实的锁失效)) |
| P3b-4 | 🟡 | C | parity-break | `doAs` 把 `InterruptedException` 转 `IOException` 不 `Thread.currentThread().interrupt()`;查询取消对 filesystem 路径的重试/drain 循环不可见 |

### P4 #64300 — maxcompute(首个删 legacy)

*203 文件。验证器:C4 REFUTED(page cache parity,见附录 A)。首个删 legacy,模式被 P5/P6 复制。*

| # | 严重度 | 验证 | 文件:行 | 发现 | 失败场景 |
|---|---|---|---|---|---|
| P4-1 | 🔴 | C | `PluginDrivenScanNode.java:173` | batch-mode 闸门从 `!= NOT_PRUNED` 塌成 `!isPruned`;无 filter 的分区表 `initSelectedPartitions` 返回非哨兵 `isPruned=false` | ≥1024 分区表的无谓词全扫失去异步 batch split→FE 规划延迟/内存暴涨,EXPLAIN 变化 |
| P4-2 | 🟠 | C | `PluginDrivenExternalTable.java:266` | 分区值缓存删除,每查询完整 ODPS `listPartitions` 往返 | 数万分区表每次规划多秒 + API 限流 |
| P4-3 | 🟠 | P | `MaxComputePredicateConverter.java:146` | EQ 发 `==`(ODPS SDK 描述是 `=`),容忍性需 live A/B。**IN 方向改动是 bugfix**(legacy 发反向 IN),建议加回归测试 |
| P4-4 | 🟡 | C | `MaxComputePredicateConverter.java:117` | 谓词下推全有全无(一个 leaf 抛异常丢整个 filter);perf-only(BE 重过滤) |
| P4-5 | 🟡 | C | `PluginDrivenInsertExecutor.java:159` | `doAfterCommit` 吞 post-commit refresh 失败(DV-018);INSERT 报成功而 legacy 传播 DdlException。数据已提交,arguably 更安全,但 success-vs-error 用户可见 |
| P4-6 | 🟡 | C | `PluginDrivenExternalCatalog.java:493` | DROP DATABASE 丢 ERR_DB_DROP_EXISTS(1008),CREATE DATABASE 丢 ERR_DB_CREATE_EXISTS(1007)prechecK;createTable 的 ERR_TABLE_EXISTS 已恢复,说明这些码属契约 |
| P4-7 | 🟡 | C | `ShowPartitionsCommand.java:203` | SHOW PARTITIONS admit 所有 plugin catalog(jdbc/es/trino 原被定向拒绝);MC LIMIT/OFFSET 从远端分页变全取分页 |
| P4-D | 设计 | — | `ConnectorTransaction`/`Transaction`/`CreateTableInfo`/`MCConnectorClientFactory` | ODPS block 分配爬上通用事务接口(见 [4.3](#43-spi-表面正在连接器化--️-这是最该管的抽象趋势));第三处引擎名 switch(见 [5.3](#53-n-连接器--n-编辑-的-switch--️-无编译期保护));MC 凭证/客户端构建 FE 侧(MCConnectorClientFactory)vs BE-JNI 侧(MCUtils)复制,auth 修一处漏一处→split-brain |

### P5 #64446 — paimon 迁移 + cutover

*283 文件。验证器:C1/C2/C3 REFUTED(cast-guard,见附录 A);L2(SHOW CREATE 无分区)DISPROVEN(master 同样不渲染)。*

| # | 严重度 | 验证 | 文件:行 | 发现 |
|---|---|---|---|---|
| P5-1 | 🟠 I3 | C | `CatalogFactory.java:51`(加 paimon) | EXPLAIN 节点名 `VPAIMON_SCAN_NODE`→`VPluginDrivenScanNode`;deviations-log 无记录(live 实测坐实) |
| P5-2 | 🟠 | P | `PaimonScanPlanProvider.java:787` | JNI/COUNT range 的 `file_format` 取表级 `file.format`(默认 parquet),legacy 从首个数据文件后缀推;native 路径仍推后缀。ORC 数据+无 option→paimon-cpp 误读 |
| P5-3 | 🟠 | C | `PluginDrivenScanNode`(selectedPartitionNum) | 语义从"SDK split distinct 分区"变"FE 剪枝分区数",影响 EXPLAIN + sql_block_rule partition_num |
| P5-4 | 🟡 | C | `PaimonTypeMapping.java:254` | to-Paimon(CREATE TABLE)丢嵌套 nullability + struct 字段注释;非 SPI 限制(ConnectorColumnConverter 有 childrenNullable/Comments,iceberg 用了,paimon 没读) |
| P5-5 | 🟡 | C | `PaimonScanPlanProvider.java:457` | `ignore_split_type` session 变量插件路径无人消费(legacy 4 处);变量仍存在→`SET` 静默 no-op |
| P5-6 | 🟡 | C | `PaimonPredicateConverter.java:121` | `convertAnd` OR 下部分合取(sound,plan/profile 差异) |
| P5-7 | 设计 | — | `PaimonSchemaBuilder.java:127` | `getInitialValues()` 从不读,`VALUES IN` 静默吞(live 实测;master 同样吞=parity,但死 API 面 + 接受做不到的 DDL) |
| P5-D | 设计 | — | `PaimonCatalogFactory` | `buildHadoopConfiguration`/`assembleHiveConf` 共享设施放连接器里(iceberg 已复制,见 [5.4](#54-跨连接器复制--️)) |

### P5b #64653 — paimon 删 legacy

*75 文件,-8422。删除对账干净(GSON shim、sys 表、SHOW PARTITIONS 列、属性 passthrough 全有插件对应物)。*

| # | 严重度 | 文件:行 | 发现 |
|---|---|---|---|
| P5b-1 | 🟡 | `SummaryProfile.java:158` | FE 侧 paimon scan-plan 剖面指标(`Paimon Scan Metrics` 段:manifest scan 时间、skip split 数)随 `PaimonScanMetricsReporter` 删除无替代;`PAIMON_SCAN_METRICS` 常量零 writer 悬空。慢 split 规划诊断能力回归;plan-doc 登记为已知回归。建议:连接器无关的 scan-metrics 钩子(也服务 iceberg/hudi),或删悬空常量 |

### P6 #64688 — iceberg(最大,7 flavor + MVCC + 写路径 + procedures)

*685 文件。5 个子系统 finder + 独立验证器。写路径/事务/10 procedure/CTAS 回滚验证忠实(未报)。*

| # | 严重度 | 验证 | 文件:行 | 发现 |
|---|---|---|---|---|
| P6-1 | 🔴 | P | `IcebergConnectorMetadata` + `PluginDrivenMvccExternalTable:482` + `IcebergScanPlanProvider:1072` | 快照缓存 vs schema 缓存独立 TTL 偏斜→BE field-id dict 缺 scan slot→StructNode DCHECK 崩溃(见 [6.4](#64-缓存一致性)) |
| P6-2 | 🔴 | C(operational) | `PluginDrivenExternalCatalog.java:132` | 只换 lib/ 不部署 plugins/connector→所有存量 iceberg catalog 首次访问抛"No ConnectorProvider found"(见 [6.5](#65-升级兼容-invariant-2)) |
| P6-3 | 🟠 | C | `IcebergConnectorMetadata.java:645` | `computeRowCount` 丢 equality-delete gate:`totalRecords - positionDeletes` 无条件,legacy 有 equality-delete!=0→UNKNOWN;COUNT(*) 下推路径保留了 gate(不对称)。MOR/CDC 表统计膨胀,误导 CBO |
| P6-4 | 🟠 | C | `IcebergConnector.java:611` | s3tables 无显式凭证即硬失败;`S3FileSystemProvider.supports()` 要求 AK/SK/roleARN。只有 region+warehouse ARN 的 EC2 instance-profile catalog 无法创建;legacy 走 PROVIDER_CHAIN 默认链 |
| P6-5 | 🟠 | C | `IcebergCatalogFactory.java:83` | region 别名收窄成 4 个,丢 `AWS_REGION`/`iceberg.rest.signing-region`/`rest.signing-region`;REST vended-cred catalog 用被丢别名→S3FileIO "Unable to load region";注释声称"mirror" legacy 不实 |
| P6-6 | 🟠 | P | `PluginDrivenMvccExternalTable.java:475` | 同表双引用 version-blind 取快照(见 [6.4](#64-缓存一致性)) |
| P6-7 | 🟡 | C | `IcebergConnectorMetadata.java:626` | `FOR TIME AS OF '<数字串>'` 现读作 epoch millis(legacy 拒绝);benign superset |
| P6-8 | 🟡 | P | `IcebergTimeUtils.java:76` | 无上下文线程 `resolveSessionZone` 回退 UTC,legacy 回退 FE 默认 session tz;无具体可达的无上下文时间解析路径 |
| P6-9 | 🟡 | C | `IcebergTypeMapping.java:143` | 未知/v3 类型(TIMESTAMP_NANO/GEOMETRY)静默降级 UNSUPPORTED,legacy schema load 抛异常;TIME 是 parity(master 也 UNSUPPORTED) |
| P6-10 | 🟡 | C | `IcebergConnectorMetadata.java:1292` | `$position_deletes` 丢定向错误信息("not supported yet"),变通用 unknown-table |
| P6-S | 安全 | — | `ConnectorCapability` + `Env.java:4891` | `SUPPORTS_SHOW_CREATE_DDL` 无脱敏(见 [7](#7-安全观察));P6 还破坏性重写 capability enum(~14 值删除,强迫同 PR 重写 jdbc/mc/paimon) |
| P6-D1 | 设计 | — | `RowLevelDmlRegistry.java:38` | 能力门控通用,身体 iceberg-shaped(`IcebergDeleteCommand` 绑 `__DORIS_ICEBERG_ROWID_COL__`);下一个声明 DELETE 的连接器被路由进 iceberg transform→unresolvable-slot |
| P6-D2 | 设计 | — | `ConnectorMvccPartitionView.java:113` | `getNewestUpdateTimeMillis` 名说毫秒实微秒(见 [4.3](#43-spi-表面正在连接器化--️-这是最该管的抽象趋势)) |
| P6-D3 | 设计 | — | `ConnectorMetadata.java:142` | `applyRewriteFileScope`/`applyTopnLazyMaterialization` 契约靠散文 javadoc:raw path 字符串相等匹配"两侧都不归一化",违反→静默数据损坏(rewrite 重复行/OCC)非报错 |
| P6-D4 | 设计 | — | `TcclPinningConnectorContext` ×2 + flavor provider ×4 | 跨连接器复制(见 [5.4](#54-跨连接器复制--️)) |
| P6-D5 | 设计 | — | 多处 | P8 blocker(见 [5.2](#52-p8删-allowlist不是纯删除--️)) |

---

## 9. 验证方法与统计

**流水线**:每个 PR 按角度(逐行正确性 / 删除行为审计 / 跨文件 / 设计·抽象·扩展 / 复用·简化·效率 / conventions)+ 子系统(P6 切 5 刀:catalog flavor / MVCC / 写路径 / 删除+GSON / SPI 设计)派 finder → 每个候选**强制对最新 tip 复核**(后续 phase 修掉的不算)→ 对抗验证器逐条 CONFIRMED/PLAUSIBLE/REFUTED,主动找 guard、不可达状态、master parity。

**"对 tip 复核"救回的误报**:P0 有 2 条、P1 有 3 条候选在最新 tip 已被后续 phase 修掉——只读 PR diff 会误报。

**对抗验证证伪的**(保护信誉,不发掺水账):
- P2:3 条(trino-spi 自己小写列名 / create_time 建库必写 / executor 是 legacy 自带)
- P3b:4 条(全 master 同病=parity 非回归)
- P4:1 条(page cache preprocessor master 同样不可达)
- P5:3 条(cast-guard 拦截)+ 1 条(SHOW CREATE 分区 master 同样不渲染)

**finder 措辞纠错**:P4 finder 说 IN 方向反转是 bug,验证器查出**恰恰相反——tip 修了 legacy 的反向 bug**。据此把评论改成给作者 credit + 建议加测试。这就是验证不能省的原因。

**已发**:9 篇 GitHub review,26 条 inline 评论 + 每篇 body 汇总。

| PR | inline | 净发现(证伪后) |
|---|---|---|
| P0 #63582 | 6 | 6 |
| P1 #63641 | 2 | 2 + 1 body |
| P2 #64096 | 2 | 5(3 证伪) |
| P3 #64143 | 4 | 8(8/8 CONFIRMED)|
| P3b #64655 | 0 (body) | 4(4 证伪) |
| P4 #64300 | 2 | 7 + 设计(1 证伪) |
| P5 #64446 | 6 | 7 + 设计(4 证伪) |
| P5b #64653 | 0 (body) | 1 |
| P6 #64688 | 4 | 10 + 设计/安全 |

---

## 10. 优先级建议

**合 master 前应处理(高)**:
1. **P6 缓存偏斜**(P6-1):schema 缓存 key off pinned schemaId,消除 BE DCHECK 崩溃窗口。
2. **P6 升级坑**(P6-2):升级文档明确"必须部署 plugins/connector",或加 fe-core 侧 degraded 兜底。
3. **P4 batch-mode**(P4-1):恢复 `!= NOT_PRUNED` 判据,避免大分区表 FE 规划暴涨。
4. **安全**(P6-S):SHOW CREATE DDL 渲染路径加敏感键过滤。

**P7 前必查(中,dormant)**:
5. **P3 hudi 4 高危**(6.2):unescape / datetime / HMS-name-vs-storage-path / 混大小写 Avro——P7 切换即静默丢行或崩。P3 的 parity 测试要能抓到这些。
6. **P7 hive 适配缺口**:TABLESAMPLE SPI 表达、CacheAnalyzer instanceof、kerberos 锁分裂。

**贯穿性(应有跟踪 issue)**:
7. **EXPLAIN 节点名**(6.1):要么保留 per-connector 名,要么在 #65185 登记为明示偏差。
8. **三处引擎名 switch → Connector SPI 声明**(5.3)。
9. **SPI 表面连接器化**(4.3):连接器专属能力收进 facet,别堆根接口。
10. **跨连接器复制 → fe-connector-support**(5.4)。
11. **守门反转为 allowlist**(6.5)。

**P4/P5 待验证(中)**:
12. **P4 `==` EQ**(P4-3):live ODPS A/B。
13. **P5 file_format**(P5-2):option-unset ORC 表 paimon-cpp e2e。
14. **P5 cast-guard**(6.3):加测试 pin 住 `supportsCastPredicatePushdown()=false`,免得未来放出激进 arm。

---

## 附录 A:被证伪的候选(体现覆盖面)

| PR | 候选 | 判决 | 理由 |
|---|---|---|---|
| P2 | 列名大小写失配(columnHandleMap 小写 vs columnMetadataMap 原样) | REFUTED | trino-spi `ColumnMetadata` 构造器自身小写化名字;master 同样不对称=parity |
| P2 | create_time 每 planScan 新铸→BE 缓存不命中 | REFUTED | create_time 自 #18778(2023)每 catalog 建时必写 |
| P2 | 每 planScan 新建线程池不 shutdown | REFUTED | legacy `TrinoConnectorScanNode` 逐字自带;daemon 线程 60s 后 GC |
| P3b | 惰性 vs 急切 kerberos 登录 | REFUTED | master 同样惰性;bad-keytab 都在首次 IO/DDL 校验时报 |
| P3b | buildAuthenticator 判据分歧 | REFUTED | `HdfsConfigBuilder` 逐字 master 相同 |
| P3b | 空 username → createRemoteUser("") IAE | REFUTED | master fe-common 逐字相同,pre-existing |
| P3b | fe-kerberos 混中立类型+hadoop 机器 | REFUTED | 中立类型(AuthType/KerberosAuthSpec)machinery-free,JVM 惰性加载不可达 NoClassDefFoundError |
| P4 | INSERT INTO SELECT 不再关 page cache | REFUTED | master 的 `visitLogicalMaxComputeTableSink` 对 INSERT-SELECT 同样不可达(preprocess 早于 BindSink)=parity |
| P5 | VARCHAR `value.toString()` datetime/bool | REFUTED | `supportsCastPredicatePushdown()=false` 在转换前 strip 含 cast conjunct;VARCHAR arm 只见 String |
| P5 | 整数 Number 截断把谓词推强 | REFUTED | 同 cast-guard;cast-free 只交型匹配整型字面量 |
| P5 | LIKE 不查类型→ClassCastException | REFUTED | 同 cast-guard;CHAR 残留无 ClassCastException(CHAR stats 是 BinaryString) |
| P5 | SHOW CREATE TABLE 无 PARTITION BY(L2) | DISPROVEN | master paimon arm 同样只渲染 comment+LOCATION+PROPERTIES,无分区子句=parity |
| P6 | txn-id 双命名空间碰撞(P0 遗留) | 已缓解 | 连接器统一从 `session.allocateTransactionId()→Env.getNextId()` 取号 |

---

## 附录 B:live 集群实测证据

worktree `/Users/lanhuajian/github/doris-catalog-spi`(tip `3ba75b7cf8a`),单 FE(JDWP :35005)+ 单 BE,端口偏移 +30000。

- **paimon filesystem catalog**(`paimon_debug.spi_db`):CREATE CATALOG/DB/TABLE(踩 P0 DDL 转换器)/ SELECT / EXPLAIN 全通;谓词成功下推(`predicatesFromPaimon: GreaterThan(id, 1)`)。
- **实测坐实**:①EXPLAIN 打 `VPluginDrivenScanNode / CONNECTOR: paimon`(master 是 `VPAIMON_SCAN_NODE`)→ P5-1;②`PARTITION BY LIST (region) (PARTITION p VALUES IN ('x'))` 静默吞值定义 → P5-7;③paimon INSERT 被拒(supportsInsert=false)——但 master 也不支持 paimon 写=parity,非回归。
- **数据注入**:paimon 无 Doris 写路径,写了 `PaimonSeeder.java` 直接用插件目录里的 paimon 1.3.1 SDK Batch Write API 灌数据(绕过 Doris);t_orders(4 行含 NULL)、t_part(3 分区)、t_list(4 分区,含 `__HIVE_DEFAULT_PARTITION__`)。外部写入即时可见(无需 REFRESH)→ 快照查询时现 pin;SHOW PARTITIONS 统计列有值 → P0 的 `ConnectorPartitionInfo` 统计字段生效;分区裁剪穿透 SPI(`partition=1/3`)。
- **iceberg hadoop catalog**(`iceberg_legacy.ice_db`,legacy 对照):INSERT/SELECT 可用;EXPLAIN 打 `VICEBERG_SCAN_NODE`。坑:file:// warehouse 首次 INSERT 前需手工 `mkdir -p <warehouse>/<db>/<table>/data`(BE 本地写不建目录)。

---

*本评审综合了对 9 个 PR 的系统性多智能体审查(20 finder + 6 对抗验证器,全部对 tip 复核)与 live 集群实测。每条发现均带 master 对照的 file:line 和具体失败场景,可直接定位。*
