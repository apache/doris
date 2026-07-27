# 05. 修复 hudi 在「最后修改时间」字段里填时刻串导致查询缓存永久失效

> **优先级**：第一优先级（公共契约违约；表现为性能缺陷，不产生错误结果） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe/fe-connector/fe-connector-hudi`（主改动 + 单元测试）；`fe/fe-connector/fe-connector-api`（**仅补一段 javadoc**，不改签名）；`regression-test`（新增一个端到端用例）。**不动** `fe-core`。
> **预计改动规模**：生产代码 2 个文件、约 40 行；单元测试 1 个文件、约 60 行；端到端用例 1 个 groovy 文件、约 70 行。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

公共接口 `ConnectorPartitionInfo.getLastModifiedMillis()` 约定的单位是「epoch 毫秒」（1970 年以来的毫秒数，当前约 1.7×10¹²），hudi 连接器往这个字段里填的却是 Hudi 自己的时刻串 `yyyyMMddHHmmssSSS` 当成数字（约 2.0×10¹⁶，比墙上时钟大四个数量级）。引擎会拿这个值与当前时间相减来判断「这张表最近有没有在写」，减出来的值恒为 0，于是**分区 hudi 表（以及任何与它一起扫的查询）的 SQL 结果缓存永远不会启用**。本任务在连接器侧把时刻转成真正的 epoch 毫秒，引擎零改动。

## 二、背景：现在的代码是怎么写的

**（1）契约方。** `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorPartitionInfo.java:166-169`：

```java
/** @return last-modified epoch millis, or {@link #UNKNOWN}. */
public long getLastModifiedMillis() {
    return lastModifiedMillis;
}
```

契约就这一句：epoch 毫秒，或者 `UNKNOWN`（`-1`，见 `:32`）。

**（2）hudi 连接器怎么填的。** 分区列举的公共收集点是 `HudiConnectorMetadata.collectPartitions`（`fe/fe-connector/fe-connector-hudi/src/main/java/org/apache/doris/connector/hudi/HudiConnectorMetadata.java:716`），它有两条支路，都把「最新已完成 instant」当作最后修改时间往下传：

- 走 HMS 同步分区时（`:733`）：`return buildPartitionInfos(hmsNames, partKeyNames, latestInstant(handle));`
- 走 hudi 自己的元数据列举时（`:742-749`）：`HudiScanPlanProvider.latestCompletedInstant(metaClient)` 的结果作为 `Map.Entry` 的 key 传下去。

落笔的地方是 `buildPartitionInfos`（`:759-778`），第 775 行就是那个参数：

```java
result.add(new ConnectorPartitionInfo(name, values, Collections.emptyMap(),
        ConnectorPartitionInfo.UNKNOWN, ConnectorPartitionInfo.UNKNOWN,
        instant, ConnectorPartitionInfo.UNKNOWN,          // <- 第 775 行，这里应该是 epoch 毫秒
        orderedValues, Collections.emptyList()));
```

而 `instant` 是什么，`HudiScanPlanProvider.java:715-717` 与 `:725-727` 写得很清楚：从 timeline 取最新已完成 instant 的 `requestedTime()`（形如 `20240101120000000`），再 `Long.parseLong` 成 `long`；空 timeline 返回 `0L`。**它是一个把年月日时分秒毫秒直接拼起来的数字，不是时间戳。**

**（3）引擎怎么用这个值。** 只有一个消费点：`fe/fe-core/src/main/java/org/apache/doris/datasource/mvcc/PluginDrivenMvccExternalTable.java:279` 把每个分区的值收进 `nameToLastModifiedMillis`，随后两个方法读它（hudi 既不声明 last-modified 新鲜度、也不提供 range 视图，因此两个方法都落到最后那条兜底分支）：

- `getNewestUpdateVersionOrTime()`（`:803`，兜底分支 `:829`）——数据版本令牌，用于判断「表变没变」；
- `getNewestUpdateTimeMillisForCache()`（`:834`，兜底分支 `:848`）——**只**给 SQL 缓存的「安静窗口」门禁用，方法 javadoc（`MTMVRelatedTableIf.java:119-131`）明确写了它必须是「genuine WALL-CLOCK epoch-millis」。

门禁本体在 `fe/fe-core/src/main/java/org/apache/doris/qe/cache/CacheAnalyzer.java:263-277`：

```java
long newestUpdateMillis = 0;
for (CacheTable cacheTable : tblTimeList) {                       // 所有被扫的表取最大值
    newestUpdateMillis = Math.max(newestUpdateMillis, cacheTable.latestPartitionUpdateMillis);
}
if (now == 0) {
    now = nowtime();                                              // System.currentTimeMillis()
    now = Math.max(now, newestUpdateMillis);                       // :273
}
if (enableSqlCache()
        && (now - newestUpdateMillis) >= Config.cache_last_version_interval_second * 1000L) {
```

`cacheTable.latestPartitionUpdateMillis` 正是 `getNewestUpdateTimeMillisForCache()` 的返回值（`:512`）。`cache_last_version_interval_second` 默认 30（`fe/fe-common/src/main/java/org/apache/doris/common/Config.java:1426`）。

顺带核实两件事，避免把影响面说过宽：

- 外部表进 SQL 缓存要先打开会话变量 `enable_hive_sql_cache`（默认 false，门在 `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/analysis/BindRelation.java:855-857`）。**没打开这个开关时本缺陷不可见。**
- 无分区 hudi 表根本走不到这里：它的分区列举返回空（`HudiConnectorMetadata.java:718-720`），版本令牌算出来是 0，`SqlCacheContext.addUsedTable` 的 `version <= 0` 兜底（`fe/fe-core/src/main/java/org/apache/doris/nereids/SqlCacheContext.java:201-206`）直接把它标成不可缓存。**所以本任务修的是分区 hudi 表。**

**（4）别的连接器没有这个问题**（已逐个核对 `new ConnectorPartitionInfo(` 的调用点）：paimon 填 `partition.lastFileCreationTime()`（毫秒，`PaimonConnectorMetadata.java:1290`），hive / iceberg / maxcompute 走不带统计字段的构造器，全是 `UNKNOWN`。**hudi 是唯一一个填了非毫秒值的连接器。**

## 三、为什么这是个问题

违反的原则是：**公共接口写明了单位的字段，实现方必须按那个单位填**。这不是风格问题——引擎拿它跟墙上时钟做减法，单位错了减出来的数没有任何意义，而且错的方向让缺陷完全隐形。

推演一遍第二节那段门禁代码（假设一张 hudi 表最新 instant 是 `20240101120000000`）：

1. `newestUpdateMillis = 20240101120000000`（≈ 2.0×10¹⁶）；
2. `now = System.currentTimeMillis()` ≈ 1.7×10¹²，接着 `now = Math.max(now, newestUpdateMillis)` 把 `now` 拉到 20240101120000000；
3. `now - newestUpdateMillis == 0`，永远 `< 30000` → 门禁永不放行。

注意第 2 步那个 `Math.max`（它本来是为云上 FE 与元数据服务时钟不一致准备的）把这个缺陷变成**永久性**的：不是「等 30 秒就好」，而是无论过多久都为 0。

用户能观察到什么：打开 `enable_hive_sql_cache` 后，`EXPLAIN PHYSICAL PLAN` 里 hudi 查询永远看不到 `PhysicalSqlCache`，重复执行同一条 SQL 每次都完整重扫；更隐蔽的是**一张 hudi 表会污染整条查询**——门禁取所有被扫表的最大值，所以「olap 表 join hudi 表」的查询也一起失去 SQL 缓存。不会算错数据，纯粹是性能损失，但影响面是所有分区 hudi 表的查询。

顺带说明这个值的另一个身份：它同时被当作 hudi 的数据版本令牌（`getNewestUpdateVersionOrTime`）与每分区的物化视图新鲜度快照（`MTMVTimestampSnapshot`）。这两处只要求「变了就不同、单调不减」，instant 和 epoch 毫秒都满足，所以那两个功能今天是好的——本任务只把单位改对，不改这两处的语义。

## 四、用一个最小例子说明

```sql
-- 会话开关：外部表 SQL 缓存的总开关（默认关）
SET enable_sql_cache = true;
SET enable_hive_sql_cache = true;

-- 一张分区 hudi 表，最后一次写入发生在很久以前（远超 30 秒的安静窗口）
EXPLAIN PHYSICAL PLAN SELECT count(*) FROM hudi_catalog.db.one_partition_tb;
```

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
| --- | --- | --- |
| 上面这条查询，反复执行 | 门禁算出 `now - 20240101120000000 = 0`，永远不满 30 秒 → 计划里没有 `PhysicalSqlCache`，每次都重扫 | 表已安静很久 → 计划里出现 `PhysicalSqlCache`，第二次执行命中缓存 |
| `SELECT * FROM olap_tbl JOIN hudi_tbl ...` | hudi 表的值参与取最大值，把整条查询的门禁也顶死 | 两张表都安静 → 整条查询可缓存 |
| 刚往 hudi 表写完就查 | 不缓存（碰巧是对的，但理由是错的） | 不缓存（因为真的在 30 秒安静窗口内） |

单位换算本身就一行事：

```
现在填的：      20240101120000000     （2024-01-01 12:00:00.000 这串数字本身）
应该填的：       1704110400000        （同一时刻的 epoch 毫秒，1e12 量级）
判据：          |填的值 - System.currentTimeMillis()| 应该是「这张表多久没写」的量级，
                而不是四个数量级的差
```

## 五、解决方案

### 5.1 目标状态

hudi 连接器在把分区信息交给引擎之前，用 Hudi 自己的工具把 instant 转成 epoch 毫秒；raw instant 继续留在它本来该在的地方（查询快照 `beginQuerySnapshot` 的 `snapshotId`、时间旅行的 handle 属性），不再冒充「最后修改时间」。公共接口只补文档。

在 `HudiScanPlanProvider` 里新增两个方法（放在既有 `requestedTimeToInstant`，`:725` 旁边，保持「纯静态、可离线单测」的既有风格）：

```java
/** 表的 instant 是按哪个时区生成的（hoodie.table.timeline.timezone，默认 LOCAL）。 */
static ZoneId timelineZone(HoodieTableMetaClient metaClient);

/**
 * Hudi instant（yyyyMMddHHmmssSSS 数字）-> epoch millis，即
 * ConnectorPartitionInfo#getLastModifiedMillis 契约要求的单位。instant <= 0（空 timeline）返回 0。
 */
static long instantToEpochMillis(long instant, ZoneId zone);
```

实现建议（这四个 API 已用 `javap` 在 `hudi-common:1.0.2` 的 jar 里核实存在）：`HoodieInstantTimeGenerator.fixInstantTimeCompatibility(String)` 把 14 位的秒级老 instant 补齐成 17 位，`HoodieInstantTimeGenerator.MILLIS_INSTANT_TIMESTAMP_FORMAT`（值为 `"yyyyMMddHHmmssSSS"`）作为格式，`HoodieTableMetaClient.getTableConfig().getTimelineTimezone().getZoneId()` 拿时区。**不要**改用 `HoodieInstantTimeGenerator.parseDateFromInstantTime`：它一行就能用，但时区取自一个全局静态开关，对显式配了 `timeline.timezone=UTC` 的表会引入一个时区偏移量级的偏差（详见第七节）。

解析失败时 **log warn + 返回 0**，不要抛：这个值只喂缓存与新鲜度启发式，而它所在的分区列举是查询热路径，为一个统计字段炸掉整张表的查询不划算。返回 0 等于「无可靠变更信号」，引擎侧已有兜底（`SqlCacheContext` 的 `version <= 0` 分支）。

### 5.2 改动清单

| 文件 | 做什么 |
| --- | --- |
| `fe/fe-connector/fe-connector-hudi/.../HudiScanPlanProvider.java` | 新增 `timelineZone` 与 `instantToEpochMillis`（见 5.1）。javadoc 写清：这是给公共 `lastModifiedMillis` 字段用的单位转换，raw instant 只用于 timeline 定位与快照 id，两者不要混用。 |
| `fe/fe-connector/fe-connector-hudi/.../HudiConnectorMetadata.java` | ① `:742-749` 那个 `metaClientExecutor.execute` 里，把 `Map.Entry` 的 key 从 `latestCompletedInstant(metaClient)` 换成 `instantToEpochMillis(latestCompletedInstant(metaClient), timelineZone(metaClient))`——转换在**同一个已建好的 metaClient 上**完成，零额外远程调用。② 新增 `long latestInstantMillis(HudiTableHandle handle)`（镜像既有 `latestInstant`，`:785-789`），供 HMS 同步支路 `:733` 使用。③ `buildPartitionInfos`（`:759-760`）第三个参数改名 `instant` → `lastModifiedMillis`，并同步修正它的 javadoc（`:752-758` 现在写的是「= the pinned instant」）。 |
| `fe/fe-connector/fe-connector-api/.../ConnectorPartitionInfo.java` | 只改 `getLastModifiedMillis()` 的 javadoc（`:166`）：补一句「引擎会拿它与墙上时钟相减做 SQL 缓存的安静窗口门禁（`CacheAnalyzer`），填非 epoch 毫秒的值会**静默关闭**该表及与它同查询的所有表的 SQL 缓存」。签名与字段不动。 |
| `fe/fe-connector/fe-connector-hudi/src/test/.../HudiConnectorPartitionListingTest.java` | 在「item 1: instant string → long」那组（`:58-70`）后面加一组单位转换用例；同时修 `buildPartitionInfosStampsInstantAndValues`（`:139-156`）——它现在断言 instant 原样透传（`:149`），改成传一个毫秒值并断言透传。`:221` 与 `:234` 那两处传 `5L` / `88L` 的用例不受影响（转换已挪到调用点之前，`buildPartitionInfos` 只负责透传）。 |
| `regression-test/suites/external_table_p2/hudi/test_hudi_sqlcache.groovy`（新增） | 端到端：对一张分区 hudi 表断言 `PhysicalSqlCache` 出现。骨架直接抄 `regression-test/suites/external_table_p0/iceberg/test_iceberg_sqlcache.groovy`（那份是同一个门禁上 iceberg 微秒单位问题的回归，`:18-30` 的 WHY 注释、`:45-52` 读 `cache_last_version_interval_second` 自适应等待、`:54-66` 的 `assertHasCache` 断言助手都可以照用）；catalog 建法与表名抄 `regression-test/suites/external_table_p2/hudi/test_hudi_partition_prune.groovy:18-52`（`regression_hudi.one_partition_tb`）。 |

### 5.3 明确不要顺手做的事

- **不要给 `ConnectorPartitionInfo` 加「instant 原值」属性键。** 已核实 fe-core 里没有任何地方读 `ConnectorPartitionInfo.getProperties()`（唯一消费 `lastModifiedMillis` 的是 `PluginDrivenMvccExternalTable:279`；`ShowPartitionsCommand:299-312` 只读名字/行数/大小/文件数）。表级 instant 已经由 `beginQuerySnapshot` 的 `snapshotId` 承载（`HudiConnectorMetadata.java:437-449`）。加一个没有消费方的键属于投机。
- **不要动 fe-core 的任何文件。** 引擎侧已经把「版本令牌」与「安静窗口门禁值」拆成两个方法了（`CacheAnalyzer.java:259-262`、`:505-513` 的注释就是这次拆分留下的），修 hudi 不需要引擎配合。当前阶段 fe-core 只出不进。
- **不要顺手让无分区 hudi 表也能进 SQL 缓存。** 那要给 hudi 补一条表级新鲜度通道（`getTableFreshness` / range 视图之一），是独立的能力扩展，且无分区 iceberg 表今天同样不可缓存（见 iceberg 那份用例的注释），行为一致，不构成缺陷。
- **不要改 `beginQuerySnapshot` 里的 `snapshotId`。** 它必须保持 raw instant：物化视图的表级快照与时间旅行都靠它，换成毫秒会打断与 timeline 的对应关系。
- **不要顺手统一「其它连接器的分区统计字段」**（例如给 hive 补分区最后修改时间）。那是能力增强，与本次单位修复无关。
- **不要为这类单位约束加静态门禁**（shell/正则）。「某个 long 是不是 epoch 毫秒」不是文本可判定的，本仓库已有结论：这类门禁只适合存在性/前缀类不变量。约束靠 javadoc + 单测钉住。

## 六、怎么验证

**第一步：单元测试（不需要集群，是本任务的主要证据）。** 加在 `HudiConnectorPartitionListingTest`：

| 用例 | 断言 |
| --- | --- |
| `instantToEpochMillis(20240101120000000L, ZoneOffset.UTC)` | 等于 `1704110400000L`（2024-01-01T12:00:00Z），并断言落在 `[1e12, 1e13)` 区间——**量级断言就是这次的核心判据** |
| 14 位秒级老 instant（如 `20240101120000L`） | 转换成功且与上面同一天同一小时的量级一致。**不要硬编码猜测毫秒补位**，先跑出来看 hudi 的 `fixInstantTimeCompatibility` 实际补什么（jar 里的常量 `DEFAULT_MILLIS_EXT = "999"`），再把实际值钉进断言 |
| `instantToEpochMillis(0L, zone)` | 等于 `0L`（空 timeline 语义不变，仍是 `>= 0`，能过 `getNewestUpdateVersionOrTime` 的 `v >= 0` 过滤） |
| 不可解析的数字（如 `5L`） | 返回 `0L`，不抛异常 |
| **意图用例（最重要的一条）** | 用「一小时前」的本地时间拼出 instant 串再转换，断言 `System.currentTimeMillis() - 转换值` 落在约 1 小时 ± 5 分钟内。这条用例直接编码了 Rule 9 要求的「为什么」：这个值必须能被「当前时间减去它 = 这张表安静了多久」这个门禁正确解读。**回退到 raw instant 会让这个差值变成大负数，用例转红。** |

**变异验证（要做，成本极低）**：把 `HudiConnectorMetadata` 里的转换调用改回直接传 instant，确认上面那条意图用例转红；恢复。证明这组测试不是永远绿的空壳。

**第二步：端到端回归（需要 hudi 外部环境，`enableHudiTest=true`）**。新增用例 `test_hudi_sqlcache`：开 `enable_sql_cache` 与 `enable_hive_sql_cache`，对分区表 `regression_hudi.one_partition_tb` 断言 `EXPLAIN PHYSICAL PLAN` 里出现 `PhysicalSqlCache`。测试环境的 hudi 数据是预置的静态数据，早已超出 30 秒安静窗口，因此**修复前必然红、修复后必然绿**，判据干净。`use_hive_sync_partition` 建议像 `test_hudi_partition_prune` 那样两个取值各跑一遍，因为两条支路的转换点是分开改的（5.2 的 ① 与 ②）。

同时回跑既有的 hudi 物化视图相关用例，确认新鲜度语义没被打断（换单位后首次比较会判定「变了」，触发一次多余刷新，属预期）：`test_hudi_mtmv`、`test_hudi_rewrite_mtmv`、`test_hudi_olap_rewrite_mtmv`、`test_hudi_partition_prune`。

**第三步：编译门禁。** 全反应堆含测试源的 test-compile 是最强单一信号：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -Dmaven.build.cache.enabled=false
```

不得使用任何跳过测试编译的参数。跑单测：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test -pl fe-connector/fe-connector-hudi -am \
    -Dtest=HudiConnectorPartitionListingTest -DfailIfNoTests=false -Dmaven.build.cache.enabled=false
```

`-Dmaven.build.cache.enabled=false` 不是可选项：不加它 surefire 会被 build cache 静默跳过（日志出现 `Skipping plugin execution (cached): surefire:test`），此时 `BUILD SUCCESS` 是空的。另外 `mvn ... | tail` 之后的 `$?` 是 `tail` 的退出码，要读日志里的 `BUILD SUCCESS` / `BUILD FAILURE` 行。

## 七、风险与回退

- **正确性风险：无。** 这个字段不参与读数据、不参与分区裁剪，只喂缓存门禁与新鲜度比较。改错了最坏也只是缓存启用早晚，不会出错行。
- **数据版本令牌一次性变小。** 同一个值也是 hudi 的版本令牌，修复后从 ~2.0×10¹⁶ 掉到 ~1.7×10¹²。唯一会因此报错的地方是 SQL 字典：`Dictionary.hasNewerSourceVersion`（`fe/fe-core/src/main/java/org/apache/doris/dictionary/Dictionary.java:282-286`）在新版本小于已记录版本时抛异常。已核实 `srcVersion`（`:107`）**没有** `@SerializedName`，即不持久化、FE 重启归零，而部署这个改动本身就要重启 FE，所以不存在跨重启的比较。**结论：不构成实际风险，但要在 PR 说明里写明这条推理，别让评审自己去猜。**
- **物化视图会多刷一次。** 每分区的 `MTMVTimestampSnapshot` 值换了单位，物化视图元数据里持久化的上次刷新快照与新值不等，会触发一次刷新。之后恢复稳定。
- **时区偏差（已按 5.1 的方案规避，这里记录残余）。** instant 串本身不带时区，按 `hoodie.table.timeline.timezone` 生成（默认 `LOCAL`）。5.1 的方案从表配置读时区，所以显式配 `UTC` 的表是准的；配 `LOCAL` 的表按 FE 本地时区解析，若写入方与 FE 不在同一时区，转换值会有一个时区偏移量级的误差。这只会让缓存提前或推迟启用（最坏推迟约一个时区偏移的时长，因为 `now = Math.max(now, newest)` 会把未来值顶住），不影响正确性，且 `LOCAL` 语义本身就是「按写入方本地时区」，Doris 无从得知写入方时区——不要试图猜。
- **单调性。** instant 到毫秒的映射是同序的（hudi 自己比较 instant 时也把 14 位按补 `999` 处理），因此版本令牌仍然单调不减，字典与物化视图的单调性要求不被破坏。
- **回退**：改动集中在 hudi 连接器两个文件，`git revert` 即可。端到端用例可以留着（回退后它会转红，这正是它该做的事）。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md` 附录 A.6 第 120 条 —— 时刻串冒充 epoch 毫秒：本任务的出处，给出了「契约是 epoch 毫秒 / hudi 填 instant / 把 SqlCache 安静窗口门禁算坏」的判定与符号定位。
- 同一份报告第十节 10.2（主题七「语义与契约不清」里数值单位那一小节）—— 单位与未知值无统一约定，以及第十五节整治路线表第 6 项 —— 修四个真实缺陷的排期：把它与 trino 三路 OR 丢行、paimon 两处谓词收窄归为同一批，理由是有用户可见后果、不应排在设计整治之后。
- 同一门禁上已经修过的先例（**动手前建议先读**）：`regression-test/suites/external_table_p0/iceberg/test_iceberg_sqlcache.groovy:18-30` 的注释完整记录了 iceberg「微秒喂进毫秒门禁」的同类问题及其修法，`MTMVRelatedTableIf.getNewestUpdateTimeMillisForCache`（`fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVRelatedTableIf.java:119-131`）与 `ConnectorMvccPartitionView.getNewestUpdateWallClockMillis`（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/mvcc/ConnectorMvccPartitionView.java:126-137`）就是那次拆分留下的接口。iceberg 走的是 range 视图分支，hudi 走兜底分支，所以不能直接复用它的通道——但两者是同一个坑的两种表现。
- `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanRange.java:56` 附近是**同类的第三处**单位违约（契约写字节数、maxcompute 在行偏移模式返回行数，报告条目 121）。是另一份任务，不要塞进本任务，但两者可以共享同一条经验：公共接口凡写了单位的字段，都该有一条量级单测钉住。
