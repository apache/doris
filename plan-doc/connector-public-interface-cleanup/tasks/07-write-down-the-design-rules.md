# 07. 把公共模块的设计规则写下来（两个模块各一份包级说明）

> **优先级**：第二优先级（零风险，建议第一个合入） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-api`、`fe-connector-spi`（只动注释与 pom 的 `<description>`，零 Java 逻辑改动）
> **预计改动规模**：新增 1 个文件、改写 1 个文件、修 2 处 pom 描述；约 200～260 行注释文本。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

---

## 一、一句话说明这个任务要解决什么

两个公共模块今天没有任何地方写下「一项能力该声明在哪里、异常该怎么抛、thrift 允许出现在哪、什么该放
`fe-connector-api` 什么该放 `fe-connector-spi`、`getMetadata` 返回的对象活多久、哪些方法会在后台线程上被调用」，
所以新增一个连接器的人只能靠读既有连接器的实现去猜规则；这个任务就是把这些规则写成两份包级说明文档
（`package-info.java`），并顺手把两处已经与代码不符的模块描述改对。

它不改任何运行时行为，但**后面每一批整治的判据都从这份规则来**（第 10、17 号任务在依赖表里直接依赖它），
所以应最先合入。

---

## 二、背景：现在的代码是怎么写的

**（a）`fe-connector-api` 完全没有包级文档。** 实测：

```
find fe/fe-connector/fe-connector-api -name package-info.java   # 零命中
find fe/fe-connector/fe-connector-api/src/main -name '*.java' | wc -l   # 95
```

95 个源文件、9 个子包（`api`、`api/ddl`、`api/event`、`api/handle`、`api/mvcc`、`api/procedure`、
`api/pushdown`、`api/scan`、`api/write`），没有任何一份文档说明这些包的分工与设计约束。整个
`fe-connector` 目录下只有两份包级文档：`fe-connector-cache` 一份、`fe-connector-spi` 一份。

**（b）`fe-connector-spi` 那份包级文档只说了一句话，而且不完整。**
`fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/package-info.java:18-28` 全文只讲：
本包定义连接器必须履行的 SPI 契约，主入口是 `ConnectorProvider`，连接器实现「应当依赖本模块
（`fe-connector-spi`）并在 `META-INF/services` 里注册」。它既没说连接器同时也要用到
`fe-connector-api`（实际上 `fe-connector-spi/pom.xml:43-47` 声明了对 `fe-connector-api` 的依赖，所以是传递带进来的，
但读文档的人看不出来），也没有任何一句说明「什么该放 api、什么该放 spi」。

**（c）能力声明今天有三种形态并存，规则没写下来。** 三层形态本身在代码里是存在的：

- `Connector` 上的取得器，缺席返回 `null`：`Connector.java:66`（`getScanPlanProvider`）、`:93`
  （`getWritePlanProvider`）、`:192`（`getProcedureOps`）；
- 各 provider 自己的开关：例如 `ConnectorWritePlanProvider` 上的 `supportsWriteBranch()` /
  `requiresParallelWrite()` / `requiresFullSchemaWriteOrder()` / `requiresPartitionLocalSort()` /
  `requiresPartitionHashWrite()` / `requiresMaterializeStaticPartitionValues()`；
- 能力枚举 `ConnectorCapability`（`ConnectorCapability.java:31-182`，共 13 个常量）。

但**哪一层放什么，只有 `ConnectorCapability` 的类文档写了一条边界**（`:20-29`：写操作与 sink 特性不进枚举、
放 provider）。这条边界看上去有一处例外：建表期的 ORDER BY 子句门 `SUPPORTS_SORT_ORDER` 在枚举里
（`:165`），写路径的行序 `requiresFullSchemaWriteOrder` 在 provider 上，两者都能被笼统地归为「写相关能力」。
**但这不是疏漏，代码里已经写明了区分理由**：`SUPPORTS_SORT_ORDER` 的 javadoc（`ConnectorCapability.java:153-163`）
明确写了它是「建表 DDL 子句门」（`CREATE TABLE ... ORDER BY` 这个子句是否被接受，引擎在静态规划期就要判定，
此时还拿不到写计划提供者），并显式声明它**不同于**运行期 sink 特性
`ConnectorWritePlanProvider.requiresFullSchemaWriteOrder()`（管的是写路径上行怎么排序）。
所以规则文档要做的是**把这条既有理由收录成判据**（「DDL 子句门进枚举、运行期 sink 特性进 provider」），
**不要**把它当成需要被纠正的不一致去搬动 `SUPPORTS_SORT_ORDER`。

**（d）按表能力走的是逗号分隔字符串，且只对 13 个能力中的 5 个生效。**
`ConnectorTableSchema.java:98` 定义键 `__internal.connector.per-table-capabilities`；引擎唯一读它的地方是
`PluginDrivenExternalTable.java:302` 的私有方法 `hasScanCapability`，调用方 5 处
（`:239`、`:251`、`:264`、`:276`、`:289`）。其余 8 个能力只读连接器级集合。

**（e）异常契约完全缺失。** `DorisConnectorException.java` 全文只有一个 `RuntimeException` 子类和两个构造器，
类文档一句「Base runtime exception for all connector-related errors.」，没有任何分类、没有任何该抛/不该抛的判据。
连接器实际混用多个异常族（在 `fe/fe-connector` 下 grep `throw new`：`DorisConnectorException` 395 处、
`UnsupportedOperationException` 330 处、`IllegalArgumentException` 111 处、`RuntimeException` 102 处、
`IllegalStateException` 30 处），而引擎侧只翻译 `DorisConnectorException` 这一族
（`fe-core` 里 `catch (DorisConnectorException` 共 32 处，典型如
`PluginDrivenExternalCatalog.java:816` 把它包成 `DdlException`）。其余族一路冒泡成 FE 内部错误。

**（f）thrift 在两个模块上是两套相反的规则。** `fe-connector-api` 直接用 thrift 生成类，完整清单（实测）：

| 位置 | thrift 类型 |
|---|---|
| `api/scan/ConnectorScanPlanProvider.java:24-26` | `TFileCompressType`、`TFileScanRangeParams`、`TTableFormatFileDesc` |
| `api/scan/ConnectorScanRange.java:20-21` | `TFileRangeDesc`、`TTableFormatFileDesc` |
| `api/handle/ConnectorWriteHandle.java:21` | `TSortInfo` |
| `api/write/ConnectorSinkPlan.java:20` | `TDataSink` |
| `api/ConnectorTableOps.java:464` | `org.apache.doris.thrift.TTableDescriptor`（**内联全限定名**，不是 import） |

（另有一处只出现在注释里：`api/write/ConnectorWritePlanProvider.java:33` 的 javadoc 提到 `TDataSink`，
不构成依赖。`fe-thrift` 在 `fe-connector-api/pom.xml` 里是 `provided` 作用域、不传递。）
而 `fe-connector-spi` 刻意绕开 thrift：`ConnectorContext.java:255` 的 `getBackendFileType` 返回**枚举名字符串**、
`:291` 的 `getBrokerAddresses` 返回中立的 `ConnectorBrokerAddress`、`:337` 的
`testBackendStorageConnectivity(int storageBackendTypeValue, ...)` 把一个 thrift 枚举的**整数值**当参数传。

**（g）生命周期与线程模型一字未写。**
`Connector.java:45` 的 `getMetadata` 全部文档就是「Returns the metadata interface for the given session.」。
实际契约在引擎侧：`PluginDrivenMetadata.java:28-42` 明确写了「一条语句每个 catalog 恰好用一个
`ConnectorMetadata` 实例，语句结束时确定性关闭」，而且这是 fe-core 里唯一允许直接调 `getMetadata` 的地方。
`ConnectorMetadata.java:233` 的 `close()` 默认空实现，无任何幂等/线程要求。
统计接口 `ConnectorStatisticsOps.java:30` 的类文档只有一句「Operations for retrieving table-level statistics
from a connector.」——**没写它会在引擎的后台统计线程上被调用，也没写引擎不会钉线程上下文类加载器**。
证据：`PluginDrivenExternalTable.java:1026` 的 `getColumnStatistic` 与 `:1061` 的 `getChunkSizes` 全程不钉
类加载器，`:1057` 的注释甚至明写「No TCCL pin here; the hive `listFileSizes` impl pins internally」；对应的连接器侧
`HiveConnectorMetadata.java:939-954` 自己动手 `setContextClassLoader` 兜的正是这件事。

**（h）两处模块描述已经与代码不符。**
`fe-connector-spi/pom.xml:38` 写自己「包含 `ConnectorProvider`、`ConnectorContext` 和 `ConnectorTypeMapper`」——
`ConnectorTypeMapper` 这个类**全仓不存在**（Java 源里零命中；命中只有这行描述本身、调研报告引用它的那一行，
以及构建生成物 `fe/fe-connector/fe-connector-spi/.flattened-pom.xml:29` 里这行描述的副本——
第六节自检那条带 `--include='*.xml'` 的 grep 会命中生成物，不要误判成「没删干净」）。
`fe-connector-api/pom.xml:35-38` 自称「Consumer-facing API」（面向消费方的 API），而它实际装的是**连接器要实现**的
95 个接口与值对象。

---

## 三、为什么这是个问题

**（1）没有规则，新增连接器的第一个动作就是猜。** 想声明一项能力，作者要在 `Connector`（取得器返回 null）、
provider 的 `supportsXxx()`、`ConnectorCapability` 枚举、按表能力字符串这四种形态里选一个，而四种形态并存的
理由没有任何地方写。选错了不会有编译错误，也不会有测试失败——只会「实现了但没生效」，然后花半天排查。

**（2）文档写在错的地方比没写更贵。** 现在唯一写下来的两句都不准：`fe-connector-spi` 的包文档说自己是
「连接器实现必须履行的契约」（实际上连接器要实现的东西 95% 在 `fe-connector-api`），`fe-connector-api` 的 pom
说自己是「面向消费方」（实际相反）。照这两句去理解模块分工，会得到完全反的结论。

**（3）异常没有分类，用户看到的报错就没有分层。** 连接器抛 `IllegalArgumentException` 时引擎不翻译，
用户拿到的是一条内部异常；抛 `DorisConnectorException` 才会被翻译成 `DdlException` 这类可读错误。
今天这个区别没写在任何契约里，等于让「用户能不能看懂报错」取决于连接器作者随手选了哪个异常类。

**（4）线程与类加载器这条契约「踩了就炸」，而它一字未提。** 本项目已知的高危坑就是跨插件类加载器的按名反射：
统计方法跑在引擎后台线程上、引擎不钉线程上下文类加载器，连接器如果不自己钉，捆绑库按名反射会撞上 fe-core 里的
重复副本，表现为 `ClassCastException` / `NoClassDefFoundError`，而且是偶发的（只在后台 ANALYZE 触发时炸）。
hive 连接器是自己摸出来后补的救；下一个连接器作者没有任何线索知道要补。

**（5）后面每一批整治都需要判据。** 第 10 号（拆 `ConnectorTableOps`）与第 17 号（按表能力类型化 + 删镜像方法）
都要回答「这个开关该落在哪一层」。规则不先落地，那两批就是逐项拍脑袋。

---

## 四、用一个最小例子说明

假设我要新增一个连接器 X，只想做一件很小的事：**告诉引擎「我这个连接器支持写入分支（write branch）」**。

| 我想做什么 | 今天我实际会遇到什么 | 规则写下来之后应该是什么 |
|---|---|---|
| 找一份文档看规则 | `fe-connector-api` 没有包级文档；`fe-connector-spi` 的包级文档只说「注册 `ConnectorProvider`」 | `fe-connector-api/package-info.java` 第一段就说明能力声明分三层，以及每层的判据 |
| 挑一个地方声明 | 看到 `Connector.supportsWriteBranch()`（`Connector.java:132`）和 `ConnectorWritePlanProvider.supportsWriteBranch()` 两个同名方法，不知道该覆写哪个 | 规则明确：**覆写 provider 上的那个**；`Connector` 上的同名方法是引擎用的空安全读取口，连接器不得覆写 |
| 覆写 `Connector.supportsWriteBranch()` | 能编译、能运行，但如果同时没改 provider，两个答案就分叉；**没有任何测试会失败** | 规则把「唯一真源在 provider」写成契约，第 17 号任务再把这些方法改成语言层面无法覆写的形式 |

再举第二个更小的：我的连接器发现远端表不存在，该抛什么异常？

```
throw new IllegalArgumentException("table not found: " + name);   // 今天：引擎不翻译，用户看到内部异常栈
throw new DorisConnectorException("table not found: " + name);    // 今天：引擎翻译成 DdlException，用户看到可读错误
```

两行都能编译、都能跑，差别只在用户看到的报错长什么样，而**没有任何文档告诉作者该选哪一行**。

---

## 五、解决方案

### 5.1 目标状态

新增 `fe-connector-api` 的包级说明，改写 `fe-connector-spi` 的包级说明，两份互相引用。
**文档正文用英文**（与仓库既有 javadoc 一致，`fe-connector-cache/package-info.java` 是可照抄的格式模板：
ASF 头 + 一段 `<p>` 段落，不写任何 import）。下面用中文写清每条规则要表达什么。

**规则一：能力声明分三层，且只有三层。**

1. **「整块子系统有没有」** → `Connector` 上的 provider / ops 取得器，缺席返回 `null`
   （`getScanPlanProvider` / `getWritePlanProvider` / `getProcedureOps` / `getEventSource`）。
   保持 `null` 而不是改 `Optional`：引擎已按 `null` 判定，换掉是纯变更噪音。
2. **「某块子系统内部的开关」** → 该 provider 自己的 `supportsXxx()` / `requiresXxx()`，一律默认 `false`
   （opt-in），引擎必须先拿到 provider 再问。**唯一真源在 provider**。
   `Connector` 上现存的 11 个同名方法（`Connector.java:115/126/132/138/144/150/156/162/168/177/183`）
   是引擎侧的空安全读取口（方法体一律「取 provider，为空返 false，否则转发」），
   **连接器不得覆写它们**——今天 0 个连接器覆写（已核实：三个连接器只在各自的
   `*WritePlanProvider` 上覆写），规则要把这件事从「运气」变成「契约」。
3. **「引擎拿不到 provider 也必须问的静态规划开关」** → `ConnectorCapability` 枚举，且一律按
   「连接器级 ∪ 按表级」加法解析。凡是与某个 provider 一一对应的开关不得进枚举。
   现成的判据范例（照抄 `ConnectorCapability.java:153-163` 已写明的区分，不要改设计）：
   建表 DDL 子句门 `SUPPORTS_SORT_ORDER` 留在枚举里，因为引擎要在静态规划期判定
   `CREATE TABLE ... ORDER BY` 这个子句是否被接受，此时还拿不到写计划提供者；
   而运行期的行序 sink 特性 `requiresFullSchemaWriteOrder()` 在 provider 上。
   两者名字都沾「写」，但一个管「DDL 子句收不收」、一个管「写路径上行怎么排」，分层是对的。
4. **「值缺失」用 `Optional`，「整块子系统缺席」用 `null`** —— 这两者的区分写进规则，避免被当成两套竞争机制。

同时写清现状偏差：按表细化今天只对 5 个能力生效（`hasScanCapability` 的 5 个调用方），
接口承诺的是 13 个；第 17 号任务负责补齐，在那之前**不要假设自己新加的枚举常量能按表细化**。

**规则二：异常分四类，并给出 fail loud 与静默降级的判据。**

- 连接器一律抛 `DorisConnectorException` 或其子类（这是引擎唯一会翻译的一族，32 处 `catch`）。
- 四类：**用户错误**（SQL 或参数不合法 / 对象不存在）、**配置错误**（catalog 属性缺失或矛盾）、
  **远端不可用**（元数据服务或存储超时、拒绝、鉴权失败）、**内部错误**（连接器自身的不变量被破坏）。
- 判据：**凡是用户显式发起、结果会被用户直接看到的操作，一律 fail loud**（DDL、写入、
  `ANALYZE ... WITH SAMPLE` 这类显式统计采样）；**凡是引擎为优化而做的尽力而为探测，失败必须静默降级**
  （返回空 / 返回默认值，并记日志），因为它们不该让查询失败。
- 用这条判据顺带把两处矛盾定性（具体修文档是第 8 号任务的事，本任务只写规则）：
  `ConnectorStatisticsOps.java:96` 的 `listFileSizes` 文档写「出错必须返回空、不得抛异常」，
  唯一实现 `HiveConnectorMetadata.java:931-955` 故意抛出且有单测锁死——按判据**实现是对的、文档是错的**，
  显式采样吞异常会让采样因子静默塌成 1.0、产出错误统计。
  `ConnectorMetadata.java:141-143` 的 `resolveTimeTravel` 文档写「不支持的规格返回空」，
  iceberg 与 hudi 对不支持的规格抛 `DorisConnectorException`（`IcebergConnectorMetadata.java:2038` 附近、
  `HudiConnectorMetadata.java:488` 附近），paimon 一律返回空——按判据抛出是对的（用户显式写了
  `FOR VERSION AS OF`，静默当作没写会给出错的结果集），应把文档改成据实。

**规则三：thrift 只允许出现在面向 BE 的协议边界上。**

- 承认结构性事实：BE 是 C++、没有插件机制，面向 BE 的负载必须走 thrift。
- 把 §二（f）的完整清单原样写进文档（5 个位置），并明确：**不再新增以 thrift 类型为入参的方法**；
  新增的返回值若必须携带 BE 负载，只允许复用清单里已有的类型。
- 把 `fe-connector-spi` 侧那三处「为了 thrift-free 而做的字符串/整数化」的现状与理由记下来，
  并注明这条 thrift-free 承诺**并未真正兑现**（`spi` 依赖 `api`，`api` 依赖 `fe-thrift`），
  所以它是一处局部约定而不是模块级不变量。是否统一留待后续，不在本任务范围。

**规则四：`api` 与 `spi` 的划分以「谁实现」为准，并写明现状偏差。**

- 规则：**连接器实现、引擎消费** 的类型放 `fe-connector-api`；**引擎实现、连接器消费** 的类型
  以及服务发现入口放 `fe-connector-spi`。依赖方向 `spi → api` 单向（已核实：`api` 里
  `org.apache.doris.connector.spi` 零命中）。
- 写明偏差：`ConnectorSession`、`ConnectorHttpSecurityHook`、`ConnectorValidationContext` 三个「引擎实现」的类型
  今天在 `fe-connector-api` 里；这属于已知偏差，**不在本任务里搬**（搬动会波及全部连接器的 import）。
- 写明**两个模块名整体是反着的**：按业界惯例（也是 Trino 的用法）「连接器要实现的东西」叫 SPI、
  「连接器要消费的引擎服务」叫 API，而这里 `fe-connector-api`（95 个文件）装的是连接器要实现的，
  `fe-connector-spi`（5 个文件）装的是连接器要消费的。**明确不改模块名**（改名波及所有连接器的 pom
  与 fe-core 依赖，收益只是命名），只把这件事写清楚，让读代码的人第一眼就知道名字别当真。
- 顺手修两处 pom 描述：删掉 `fe-connector-spi/pom.xml:38` 里全仓不存在的 `ConnectorTypeMapper`；
  把 `fe-connector-api/pom.xml:35-38` 的「Consumer-facing API」改成据实（连接器实现、引擎消费）。

**规则五：生命周期与线程模型。**

- `Connector` 每个 catalog 一个实例（`Connector.java:36-41` 的类文档已写，保持）。
- `getMetadata` 返回的实例：**每条语句每个 catalog 恰好一个**，由引擎在语句结束时关闭；
  `close()` **必须幂等**；**单个实例不得跨线程共享**。文档里指向引擎侧的唯一入口
  `PluginDrivenMetadata`（它已经把这个契约写清了，`PluginDrivenMetadata.java:28-42`），
  并在 `ConnectorMetadata.close()`（`:233`）上交叉写一句。
- **统计接口（`ConnectorStatisticsOps` 全部方法）会在引擎的后台统计线程上被调用，
  且引擎不会钉线程上下文类加载器**：连接器若要按名反射加载捆绑库中的类，必须自己在方法内
  把线程上下文类加载器钉到插件类加载器上（现成范式：`HiveConnectorMetadata.java:939-954`）。
  这条是本任务里唯一「不写就会炸」的契约，必须写进 `ConnectorStatisticsOps` 的类文档，
  并在包级说明里点一句。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/package-info.java` | **新建**。ASF 头 + 五条规则的英文成文；每条规则附「现状偏差」一句 |
| `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/package-info.java` | **改写**（现文 `:18-28`）。补规则四（以「谁实现」为准 + 模块名倒置 + 不改名）、补一句「连接器同时用到 `fe-connector-api`，规则全文见那份包级说明」，删掉「本包定义连接器必须履行的契约」这种与事实相反的表述 |
| `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorStatisticsOps.java` | 类文档（`:27-30`）加一段：后台统计线程调用 + 引擎不钉线程上下文类加载器 + 连接器自钉的要求 |
| `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorMetadata.java` | `close()`（`:233`）上加一句幂等 + 每语句一实例 + 不跨线程；类文档（`:36-43`）指向 `PluginDrivenMetadata` 的契约 |
| `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/Connector.java` | `getMetadata`（`:45`）文档补每语句一实例；11 个空安全读取口（`:115` 起）各加一句「连接器不得覆写，真源在 provider」 |
| `fe/fe-connector/fe-connector-spi/pom.xml` | `:38` 删掉 `ConnectorTypeMapper` |
| `fe/fe-connector/fe-connector-api/pom.xml` | `:35-38` 把「Consumer-facing API」改成据实描述 |

写作约束（照抄 `fe-connector-cache/package-info.java` 的形式）：ASF 许可头必需（checkstyle 的 `Header` 模块
按 `checkstyle-apache-header.txt` 校验）；单行不超过 120 字符（`LineLength`）；文件末尾 LF 换行
（`NewlineAtEndOfFile`）；**不要在 `package-info.java` 里写 import**——本仓库 `UnusedImports` 模块没有开
`processJavadoc`（`fe/check/checkstyle/checkstyle.xml:167`），只在 javadoc 里用到的 import 会被判为未使用，
跨包引用一律写全限定名或用 `{@code ...}`。

### 5.3 明确不要顺手做的事

| 不要做 | 为什么 |
|---|---|
| 不要改模块名（`fe-connector-api` ↔ `fe-connector-spi`） | 波及所有连接器的 pom 与 fe-core 依赖，收益只是命名对齐惯例；本任务只把倒置这件事写清楚 |
| 不要把 `ConnectorSession` / `ConnectorHttpSecurityHook` / `ConnectorValidationContext` 搬到 `spi` | 会改动全部连接器的 import，属于独立的机械改动；本任务只记录偏差 |
| 不要删 `Connector` 上那 11 个空安全读取口 | 那是第 17 号任务的内容（要连带改引擎调用点）；本任务只写「连接器不得覆写」的契约 |
| 不要动按表能力的字符串通道 | 第 17 号任务负责类型化；本任务只写下目标规则与「今天只有 5 个能力生效」的偏差 |
| 不要顺手把 `resolveTimeTravel` / `listFileSizes` 的文档改了 | 那是第 8 号任务（成批修陈旧文档）。本任务只给判据，避免两个提交改同一段注释 |
| 不要给 `api` 的 8 个子包各补一份 `package-info.java` | 规则集中在一处才有人读；子包各一份会散掉，且首次就要维护 9 份 |
| 不要写 shell/正则静态门禁去校验「thrift 只出现在清单里」 | 本仓库已有结论：这类门禁只适合存在性与前缀类不变量，判断语言语义的门禁误报比漏报更毒（授权缓存门禁已因此被删）。thrift 清单靠评审 + 这份文档约束 |
| 不要往 `fe-core` 新增任何东西 | 当前阶段 `fe-core` 只出不进 |

---

## 六、怎么验证

**（1）编译门禁（最强单一信号）。** `package-info.java` 参与编译，注释里的 `{@link}` 写错不会挂编译，
所以编译只证明没有语法/头部问题：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T1C test-compile
```

必须是全反应堆、**含测试源**，禁用任何跳过测试编译的参数。判据是输出里的 `BUILD SUCCESS`。

**（2）checkstyle 必须过**（本任务真正会被卡的门）：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-api,fe-connector/fe-connector-spi checkstyle:check
```

关注四项：ASF 头、行长 120、末尾 LF、`package-info.java` 里没有 import。

**（3）javadoc 引用可解析（可选但建议）。** 对两个模块跑一次 javadoc 生成，确认没有
「reference not found」告警——写错的 `{@link}` 只会在这里暴露。

**（4）事实一致性自检（这是本任务最重要的验证，因为写错的规则比没规则更毒）。**
文档里出现的每个数字与清单，动手时用命令逐条复核一遍，不要照抄本文：

```
# thrift 清单是否仍是 4 个 import 文件 + 1 处内联全限定名
grep -rn 'org\.apache\.doris\.thrift\.' fe/fe-connector/fe-connector-api/src/main --include='*.java'
# Connector 上的空安全读取口是否仍是 11 个，且连接器仍零覆写
grep -n 'default boolean supports\|default boolean requires\|default Set<WriteOperation>' \
  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/Connector.java
grep -rn 'public boolean supportsWriteBranch\|public boolean requires' fe/fe-connector/*/src/main --include='*.java'
# 按表能力仍只服务 5 个调用方
grep -n 'hasScanCapability' fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalTable.java
# ConnectorTypeMapper 是否仍不存在（改完后应只剩生成物 .flattened-pom.xml 里的旧副本，那不算残留）
grep -rn 'ConnectorTypeMapper' fe/ --include='*.java' --include='*.xml'
```

**（5）不需要的验证。** 零行为改动：不需要新增单元测试、不需要变异验证、不需要端到端回归。
（唯一的边界情况是 pom `<description>` 改动，它不参与任何逻辑。）

---

## 七、风险与回退

- **主要风险不是构建挂，而是把规则写错。** 一份写错的规则会让后续每个连接器作者照错的规则实现，
  比没有规则更贵。缓解办法有三条：每条规则必须能在 HEAD 上指出对应代码（本文 §二 的每一条都可复核）；
  每条规则后面附一句「现状偏差」，不把目标状态写成既成事实；把「这条规则由第几号任务兑现」写清，
  读者不会误以为今天就已经生效。
- **第二个风险是与第 8 号任务撞注释。** 第 8 号要成批修陈旧的接口文档，两个任务都会碰
  `ConnectorStatisticsOps` / `ConnectorMetadata` 的 javadoc。缓解：本任务只加「线程与生命周期」段落，
  **不修 `listFileSizes` / `resolveTimeTravel` 的文案**，文案留给第 8 号；若两者并行，先合本任务。
- **回退**：单个提交 revert 即可，零运行时影响（改动全是注释与 pom 描述）。

---

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`
  - 第五节（主题二）：能力声明的多条并行通道，5.3 的第一步就是本任务
  - 第九节（主题六）：两套相反的 thrift 规则，含建议写进包级说明的清单
  - 第十节 10.3 / 10.4：异常契约缺失、生命周期与线程模型没写进契约
  - 第十三节（主题十）：两个公共模块的边界说不清 + 模块名倒置 + `ConnectorTypeMapper` 陈旧描述
  - 第十四节：被推翻或收窄的说法（动手前值得看一眼，避免把好设计写成缺陷）
- 同目录 `README.md` 第二节任务清单：第 10、17 号任务在依赖表里依赖本任务
- 格式模板：`fe/fe-connector/fe-connector-cache/src/main/java/org/apache/doris/connector/cache/package-info.java`
- 引擎侧已成文的生命周期契约（写规则时直接引用，不要重写）：
  `fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenMetadata.java:28-42`

---

## 九、施工后订正（2026-07-25 落地时实测，以本节为准）

本节记录动手前复核发现的、与 §二 不符的事实。**§二 里被订正的说法不要再引用。**

1. **异常族：引擎翻译的是两族，不是一族。** 元数据 / DDL / DML / 扫描规划路径认 `DorisConnectorException`（32 处 catch）；**属性校验路径只认 `IllegalArgumentException`**——`PluginDrivenExternalCatalog.checkProperties` 捕获它并把 message 重抛为 `DdlException`，那里它是唯一会被解包的类型，5 个连接器共 13 处抛点依赖它，`HiveConnectorProvider` 的注释已写明「这是唯一…」。规则文档已按「分路径」写。**若按 §二 那句「只翻译一族」去统一异常，会把建目录的报错退化成内部异常。**
2. **异常族计数的口径错了。** §二(e) 的五个数字是 main+test 混计。生产（`src/main`）真实数字：`DorisConnectorException` 395、`UnsupportedOperationException` **50**（不是 330——那 330 里 280 处是测试替身的未实现桩）、`IllegalArgumentException` 109、`RuntimeException` 90、`IllegalStateException` 23。也就是说生产代码已经约 74% 集中在 `DorisConnectorException` 上，「混用严重」这个论据要按生产口径收窄。
3. **异常族已经有一个子类，且是承重的。** `HiveDirectoryListingException extends DorisConnectorException`，作用是让扫描路径只 catch「可跳过的列目录失败」，而普通 `DorisConnectorException` 仍然失败整个查询。规则文档把它作为**已获批准的扩展范式**引用，而不是另发明分类。
4. **缺席取得器是 4 个不是 3 个**：漏了 `getEventSource`（§5.1 规则一里本来就列了 4 个，是 §二(c) 少写一个）。
5. **「能力开关一律默认 false」在今天不成立**：`ConnectorPushdownOps.supportsCastPredicatePushdown` 默认 `true`；且 `supportsXxx` 形态还出现在 `ConnectorScanPlanProvider`（3 个）与三个非 provider 接口上。规则文档写成「目标 + 唯一既有例外」。
6. **统计方法的线程模型比 §二(g) 写的更宽**：不是「后台统计线程」单数，而是三个不同的守护线程池（列统计缓存加载 `STATS_FETCH`、采样 ANALYZE 的分析作业执行器、外部行数刷新执行器），且**没有一条路径钉类加载器**；带快照的 `getTableStatistics` 重载只在查询线程上。文档按「多个后台池 + 把所有方法都当可后台调用」写。
7. **`fe-connector-api/pom.xml` 的 `<description>` 跨 :35-43**（不是 :35-38），后面还有一段关于 fe-thrift provided 作用域的说明，改写时要保留。
8. **`package-info.java` 里不能写 import 的理由是错的**：`UnusedImports` 的 `processJavadoc` 在 checkstyle 9.3 下默认**开启**，只在 javadoc 里用到的 import 不会被判未使用。实际约束是另一条：**全仓没有任何 javadoc 校验绑定**（8 个 pom 里的 javadoc 插件全是 `skip=true`），所以 `{@link}` 写错既不会挂构建也不会告警，只能靠人核。另外连接器路径的 javadoc 内容类检查（`JavadocMethod` / `MissingJavadocType` 等）被 `suppressions.xml` 整体豁免。

**实际落地与 §5.2 的两处偏差**：
- `Connector` 上 11 个空安全读取口没有逐个加「不得覆写」注释（11 行近乎相同的注释是噪音），改为在**类文档里写一次**、覆盖全部 `supportsXxx` / `requiresXxx` / `supportedWriteOperations`，并说明反面（返回 `null` 的取得器才是声明点）。
- `ConnectorStatisticsOps` / `ConnectorMetadata` 的改动只加生命周期与线程段落，未碰 08 号负责的文案，按 §七 的约定执行。
