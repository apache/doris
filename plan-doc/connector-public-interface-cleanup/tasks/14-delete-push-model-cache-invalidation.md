# 14. 删除推模型的缓存失效接口，让「失效」只剩一个方向

> **优先级**：第三优先级（删死面） ｜ **风险**：低 ｜ **前置依赖**：无硬前置；与《补齐上下文包装类的转发缺口》（本任务集编号 06）改同两个文件，需约定先后，见 5.3
> **影响模块**：`fe-connector-spi`、`fe-core`、`fe-connector-iceberg`、`fe-connector-paimon`
> **预计改动规模**：删 3 个文件（约 246 行）+ 改 7 个文件（约 60 行），净减约 280 行，无新增代码
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

「丢弃元数据缓存」这件事现在有两套方向相反的接口：引擎通知连接器的那一套是活的，连接器通知引擎的那一套（`ConnectorMetaInvalidator`）没有任何连接器调用、而且引擎侧根本履行不了它承诺的语义——把后者整套删掉，让失效只剩「引擎 → 连接器」一个方向。

## 二、背景：现在的代码是怎么写的

**方向一（活的）：引擎通知连接器丢弃连接器自己的缓存。**
定义在 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/Connector.java`：`invalidateTable`（312 行）、`invalidateAll`（316 行）、`invalidateDb`（324 行）、`invalidatePartition`（336 行）。引擎侧实测 **17 处**调用（调研报告里写的 16 处漏了一处跨行书写的调用），分布在三个文件：

| 调用方文件 | 处数 |
|---|---|
| `fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalCatalog.java` | 10（289、464、562、616、642、686、687、766、779、792 行） |
| `fe/fe-core/src/main/java/org/apache/doris/catalog/RefreshManager.java` | 5（124、202、203、248、288 行） |
| `fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogMgr.java` | 2（818、853 行） |

这一套的分区参数是**分区名**。`Connector.java:333-334` 的契约写得很明确：`canonical partition names ("col=val/.../colN=valN")`；连接器侧有对应测试，`fe/fe-connector/fe-connector-hive/src/test/java/org/apache/doris/connector/hive/HiveConnectorPartitionViewCacheTest.java:124` 传的就是 `"dt=2024-01-01"`。

**方向二（死的）：连接器通知引擎丢弃引擎的缓存。**
`fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorMetaInvalidator.java:32` 定义了一个 5 方法接口（`invalidateAll` / `invalidateDatabase` / `invalidateTable` / `invalidatePartition` / `invalidateStatistics`，全是空 default，并带一个 `NOOP` 常量）。入口是 `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorContext.java:109` 的 `getMetaInvalidator()`，默认返回 `NOOP`。引擎侧实现是 `fe/fe-core/src/main/java/org/apache/doris/connector/ExternalMetaCacheInvalidator.java:34`，由 `fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:168-171` 返回。

这一套的分区参数是**分区列值**（`ConnectorMetaInvalidator.java:48-50` 的注释：`["2024", "01"]`）。

**实测调用方情况**：全仓库提到 `MetaInvalidator` 的只有 9 个文件——接口自身、`ConnectorContext`、`DefaultConnectorContext`、fe-core 的桥实现 `ExternalMetaCacheInvalidator`、fe-core 的 `FakeConnectorPluginTest`（断言默认返回 `NOOP`）、iceberg/paimon 两个上下文包装类里的一行转发、iceberg 的测试替身 `RecordingConnectorContext`、以及 `IcebergProcedureOpsTest:241` 注释里的一次提及。**零个连接器生产代码调用它。**

**真正在跑的是拉模型。** 外部元存储的变更由引擎轮询：`fe/fe-core/src/main/java/org/apache/doris/datasource/MetastoreEventSyncDriver.java:164` 调连接器的 `pollOnce` 拿一批中立的变更描述，`applyDescriptors`（202 行）由引擎自己作用到对象图和缓存上；需要丢连接器缓存时再走上面方向一的 17 处调用。连接器只负责「取事件 + 解析」，不负责通知谁去丢缓存。

iceberg 的测试已经把这段历史钉住了。`fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/IcebergProcedureOpsTest.java:56-61` 的类注释写：失效是引擎的责任，dispatch 不得失效任何缓存，`ctx.invalidatedTables` 在每次分派后都必须是空的，「非空就意味着**已被移除的**连接器侧通知又被加回来了」；对应断言有 7 处（167、242、271、290、324、349、370 行）。

## 三、为什么这是个问题

**第一，引擎侧履行不了这个接口承诺的语义**，两个方法名不副实，且这一点是写在代码注释里的既知事实：

- `ExternalMetaCacheInvalidator.java:60-69`：SPI 传来的是分区**值**，而引擎的分区缓存按分区**名**索引，从值还原名需要分区列名而 SPI 没有携带 —— 于是降级成整表失效，注释自认 `correct but over-broad`。
- `ExternalMetaCacheInvalidator.java:71-77`：`invalidateStatistics` 是**空方法**，因为引擎没有「只丢统计不丢 schema」的入口（行数缓存按 id 而非名索引），调 `invalidateTable` 会违反接口注释里「without dropping schema cache」的承诺。

也就是说：5 个方法里有 1 个是骗人的空操作、1 个的作用域比声明的粗一整个数量级。这两处的行为还各被一个单测钉住（`ExternalMetaCacheInvalidatorTest.java:76` 与 `:91`），于是「已知做不到」被固化成了「受保护的既定行为」。

**第二，同一件事的两套词汇互相冲突，是给下一个连接器作者埋的坑。** 名字撞、参数语义还相反：

| | 引擎 → 连接器（活的） | 连接器 → 引擎（死的） |
|---|---|---|
| 按库失效 | `Connector.invalidateDb(dbName)` | `ConnectorMetaInvalidator.invalidateDatabase(dbName)` |
| 按分区失效的第三个参数 | 分区**名**列表 `["dt=2024-01-01"]` | 分区**值**列表 `["2024", "01"]` |

一个新连接器作者看到 `ConnectorContext` 上挂着 `getMetaInvalidator()`，很自然会以为「我发现远端变了就该调它」，然后写出一段编译通过、运行不报错、但按分区失效实际把整张表的缓存都掀掉、按统计失效什么都不做的代码。

**第三，这是公共接口上的死面积。** `ConnectorContext` 是每个连接器都要面对的引擎服务门面（今天 19 个方法），其中一个方法通向一个完全没人用的方向。删掉它对任何在跑的功能都是零影响。

用户能不能观察到错误行为？今天不能——因为没人调用。这不是正确性缺陷，是「留着就会变成正确性缺陷」的陷阱。

## 四、用一个最小例子说明

场景：有人在远端 Hive 元存储上执行

```sql
ALTER TABLE sales.orders ADD PARTITION (year='2024', month='01');
```

Doris 侧需要让缓存反映这个变化。同一个需求，两条路的实际结果：

| 连接器想表达的意思 | 走死掉的推模型今天实际发生什么 | 走活的拉模型（保留的那一套）实际发生什么 |
|---|---|---|
| 「`sales.orders` 多了一个分区 `year=2024/month=01`，只丢这个分区」 | 连接器调 `context.getMetaInvalidator().invalidatePartition("sales", "orders", ["2024","01"])` → 引擎拿到的是列值、缓存按分区名索引 → **整张表的缓存全丢** | 引擎轮询到变更后自己调 `connector.invalidatePartition("sales", "orders", ["year=2024/month=01"])` → **按分区名精确失效** |
| 「这张表的统计过期了，只丢统计、别丢 schema」 | 连接器调 `invalidateStatistics("sales","orders")` → **什么都不发生**（空方法），连接器却以为已经生效 | 这条路今天不存在；需要时走正常的刷新路径 |

删掉左边一列，「失效」就只剩右边一套词汇：分区一律用分区名，方向一律是引擎调连接器。

## 五、解决方案

### 5.1 目标状态

- `fe-connector-spi` 里不再有 `ConnectorMetaInvalidator` 这个类型。
- `ConnectorContext` 的方法数从 19 降到 18，不再有 `getMetaInvalidator()`：

  ```java
  // 删除下面整块（含其上方 6 行 javadoc）
  // default ConnectorMetaInvalidator getMetaInvalidator() {
  //     return ConnectorMetaInvalidator.NOOP;
  // }
  ```

- `fe-core` 少一个类 `ExternalMetaCacheInvalidator` 与它的单测（fe-core 只减不增，符合当前阶段纪律）。
- 失效相关的公共接口只剩 `Connector` 上那 4 个方法（签名不动）：

  ```java
  default void invalidateTable(String dbName, String tableName) { }
  default void invalidateAll() { }
  default void invalidateDb(String dbName) { }
  default void invalidatePartition(String dbName, String tableName, List<String> partitionNames) { }
  ```

### 5.2 改动清单

| 文件 | 动作 |
|---|---|
| `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorMetaInvalidator.java` | **删除整个文件**（57 行） |
| `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorContext.java` | 删除 102-111 行（6 行 javadoc + `getMetaInvalidator()` default 方法） |
| `fe/fe-core/src/main/java/org/apache/doris/connector/ExternalMetaCacheInvalidator.java` | **删除整个文件**（82 行） |
| `fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java` | 删除 168-171 行的覆写 + 33 行的 import（这是 `ExternalMetaCacheInvalidator` 在生产代码里唯一的构造点） |
| `fe/fe-core/src/test/java/org/apache/doris/connector/ExternalMetaCacheInvalidatorTest.java` | **删除整个文件**（107 行，5 个 `@Test`） |
| `fe/fe-core/src/test/java/org/apache/doris/connector/fake/FakeConnectorPluginTest.java` | 删除 63-76 行的 `contextMetaInvalidatorDefaultsToNoop` + 28 行的 import（`Collections` 的 import 仍被其它测试用到，别删） |
| `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/TcclPinningConnectorContext.java` | 删除 143-146 行的转发覆写 + 24 行的 import |
| `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/TcclPinningConnectorContext.java` | 删除 121-124 行的转发覆写 + 24 行的 import |
| `fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/RecordingConnectorContext.java` | 删除 79-80 行的 `invalidatedTables` 字段（含其上方注释）+ 82-90 行的覆写 + 23 行的 import（`ArrayList` / `List` 的 import 还有别的字段在用，别删） |
| `fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/IcebergProcedureOpsTest.java` | 删除 7 处 `ctx.invalidatedTables` 断言（167、242、271、290、324、349、370 行）及其上方解释注释；**保留** 56-61 行类注释里「失效由引擎负责、分派后由引擎走标准刷新路径」这段设计说明，只删掉提到 `ctx.invalidatedTables` 这个已消失机制的句子 |

关于最后一条的取舍：这 7 处断言原本证明的是「连接器没有走推模型通知引擎」。删掉这套 SPI 之后，连接器**在类型层面就不存在**这条通道了，断言的失败条件不可能再出现——留一个永远不会红的断言比删掉它更糟（不可能失败的测试没有意义）。它保护的意图改由「代码里没有这个接口」结构性保证，设计意图则留在类注释里。

### 5.3 明确不要顺手做的事

1. **不要把 `Connector.invalidateDb` 改名成 `invalidateDatabase`。** 推模型删掉后名字冲突自动消失，剩下的单套名字叫什么已经不重要；改名要动 17 处调用点，纯搅动，且属于另一个「命名统一」议题。
2. **不要动 `ExternalMetaCacheMgr`。** 它的 `invalidateCatalog` / `invalidateDb` / `invalidateTable` 都另有引擎自身的调用方（例如 `fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalCatalog.java:690` 与 `.../ExternalDatabase.java:131`），删掉桥不会留下孤儿方法，也就不需要连带清理。
3. **不要顺手补「只丢统计不丢 schema」的入口。** 那是一个独立的功能缺口，而且要往 fe-core 加数据源相关代码，违反当前阶段 fe-core 只出不进的纪律。谁真需要它，另开任务。
4. **不要顺手给 iceberg/paimon 的包装类补别的缺失转发。** 实测 paimon 的包装类少转发 `newStorageUriNormalizer` 与 `getFileSystem`，这是编号 06 那项任务的范围。
5. **不要顺手给 `fe-connector-api` / `fe-connector-spi` 写模块边界文档。** 那是同一章调研里的另一条建议，与本任务解耦。
6. **不要在 iceberg 测试里保留「改写版」断言去模拟推模型**（比如自己造一个记录器再断言它是空的）。那是给已删除的机制立纪念碑。

**与编号 06 的顺序**：两项都改 iceberg/paimon 的 `TcclPinningConnectorContext.java`，但改的是不同方法块，文本不相邻。建议**先做本任务**（纯删，让包装类需要转发的方法先少一个），再做 06 补齐剩余转发；若已先做了 06，本任务照删本方法块即可，不需要回滚 06 的改动。合批也可以，但要在同一个提交里说明两件事。

## 六、怎么验证

1. **编译门禁（本任务最强的单一信号）**：删类型的验证本质上是符号级验证——任何漏改的引用都编译失败。跑全反应堆、**含测试源**：

   ```bash
   mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T1C test-compile
   ```

   禁止使用跳过测试编译的参数。要求 `BUILD SUCCESS`。

2. **删净反查**：

   ```bash
   grep -rn "MetaInvalidator" /mnt/disk1/yy/git/wt-catalog-spi --include=*.java
   ```

   期望零命中（`plan-doc/` 下的历史记录文档命中属正常，不要去改历史记录）。

3. **受影响单测（必须禁用 maven build cache，否则 surefire 会被静默跳过、`BUILD SUCCESS` 是空的）**：
   - `FakeConnectorPluginTest`（fe-core，删掉一个 `@Test`，其余必须仍全绿）
   - iceberg 的 `IcebergProcedureOpsTest`、`TcclPinningConnectorContextTest`
   - paimon 的 `TcclPinningConnectorContextTest`

4. **确认活的那一套没被牵连**：`HiveConnectorPartitionViewCacheTest.invalidatePartitionDropsTheWholeTablesCachedView`（`HiveConnectorPartitionViewCacheTest.java:105`）必须仍绿——它验证的是保留下来的方向（引擎调连接器、参数是分区名）。

5. **不需要变异验证，也不需要端到端回归**：删的是零生产调用方的接口面，运行时行为不变；没有任何 SQL 路径的行为会改变，因此不新增 groovy 回归。checkstyle 会扫测试源，注意删 import 后不要留下未使用 import。

## 七、风险与回退

风险低。三条可能的意外与应对：

- **担心「以后 HMS 事件管线搬进连接器时还需要它」**：不成立。事件管线已经按拉模型落地（`MetastoreEventSyncDriver` + 连接器的 `pollOnce` + 中立变更描述），而且 iceberg 测试注释明确记载连接器侧通知是**被有意移除**的。真需要推模型时，那时的需求会带着「分区名 vs 分区值」「统计缓存入口」这些今天缺失的信息一起来，重新设计比留一个错的空壳更好。早期计划文档（`plan-doc/00-connector-migration-master-plan.md`、`plan-doc/01-spi-extensions-rfc.md`、`plan-doc/decisions-log.md` 的相关决策条目）里还写着「事件管线通过这个接口回调」，那是已被实现推翻的旧决策；本任务落地后应在这些文档里补一句作废说明，但**不要**改写历史进度记录。
- **担心外部实现者**：这是内部接口，仓库外没有实现者；不做过时标注、直接删（与本轮整治的既定节奏一致）。
- **回退**：改动全在一个提交内且是纯删除，`git revert` 即可完整恢复，无数据/持久化影响（这些类型不参与 Gson 持久化、也不参与 thrift 有线格式）。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`
  - 第十三节「`api` 与 `spi` 两个模块的边界说不清」——该节末尾「缓存失效这件事被这个边界切成了两半，而且方向相反」那段给出了两套方向的对照，同节建议第 2 条就是删掉这套死的推模型；
  - 第 7.3 节「需要连带改连接器的删除」——本条在表格里，连带改动写的是「删 iceberg / paimon 两个包装类的转发与相关测试替身」；
  - 附录 A 第 78、79 两条原始发现——失效有两套并存机制、且两套方向相反的词汇，其中 78 被复核收窄为「构成零生产调用方的死 SPI 表面」；
  - 附录 C.1「两轮独立结论高度重叠」——在「本文更准的地方」清单里确认结论是「应删而不是改名」。
- 相关任务：编号 06《补齐上下文包装类的转发缺口》（改同两个文件，顺序见 5.3）。
- 旧决策出处（本任务作废其结论）：`plan-doc/decisions-log.md` 的「HMS event pipeline 放 fe-connector-hms，通过 ConnectorMetaInvalidator 回调」条目、`plan-doc/01-spi-extensions-rfc.md` 第 6 节。
