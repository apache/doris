# 连接器公共接口（fe-connector-api / fe-connector-spi）设计评审

调研日期：2026-07-25
调研范围：`fe/fe-connector/fe-connector-api`、`fe/fe-connector/fe-connector-spi` 的全部接口与数据结构，
以及 8 个连接器实现（es / hive / hudi / iceberg / jdbc / maxcompute / paimon / trino）和 fe-core 插件驱动路径的对应调用点。
本文只做分析和建议，不改动任何代码。

---

## 〇、复核说明（2026-07-26 追加）

**这份文档是一次设计评审的快照，不是"当前接口现状"的描述。**

- **调研基线**：`7ff51a106f0`（分支 `catalog-spi-review-19`）。正文的行号、方法数、"谁实现了什么"全部以那一刻为准。
- **复核基线**：`53a753e0a1b`，距调研基线 **44 个提交**。这 44 个提交里有相当一部分正是本文建议的落地。
- 同一天另有一轮**独立重做**的审查，产出 `connector-public-interface-cleanup/audit-report.md` 及其 `tasks/` 目录。两份并存而不是互相覆盖：两轮独立结论**一致的部分**本身就是可信度最强的证据，**分歧的部分**才是需要人拍板的地方。逐条交叉核对见那份报告的附录 C。

复核对本文只做三类改动，其余一字未动：

1. **状态戳**（`> **状态（复核 …）**：…`）：标注该节描述的问题已经落地、或已经失去对象。**分析正文原样保留**——"当时看到了什么、为什么这么判断"才是这份文档的资产。
2. **交叉核对修正**（`> **交叉核对修正**：…`）：回到代码实测后**不成立**的事实。原句同样保留，修正紧随其后。
3. **就地重算**：第二节的两张表、以及正文里出现的具体方法数与构造函数数，直接换成复核日的数字；第五节的行动清单按复核日重编。这两块是全文唯一会被人照着动手的部分。

**方法数的计数口径**（原文未声明，复核统一为）：只数写在该接口体内的方法声明；`default` 方法计入；**重载分别计**；注解不计；继承来的不计（需要连同继承一起看的地方单独标注）。

---

## 一、调研目的与判断标准

这两个模块是"公共契约层"：fe-core 通过它们驱动所有外部数据源，连接器通过它们实现自己的能力。
目标是让**新增一个连接器时，开发者只需要在自己的插件模块里写代码，不需要改 fe-core、也不需要改 api/spi 这两个公共模块**。

因此本文用四条标准去检查每一个接口：

1. **中立性**：公共接口里不应该出现只有某一种数据源才成立的概念、名字或行为。
2. **必要性**：每个方法都应该有真实的调用方；没人调用的方法是负债——它会让新连接器的作者以为"必须实现"。
3. **单一职责**：一个接口应该只表达一件事。把互不相干的能力塞进同一个接口，会迫使实现者面对一大堆与自己无关的方法。
4. **契约自洽**：接口文档写的规则，实现必须真的遵守；反过来，实现普遍采用的行为，文档不能写反。

---

## 二、现状概览

| 模块 | 内容 | 调研日（`7ff51a106f0`） | 复核日（`53a753e0a1b`） |
|---|---|---|---|
| `fe-connector-api` | 连接器要实现的业务接口 + 中立数据结构 | 95 个源文件，10149 行 | **101 个源文件，10978 行** |
| `fe-connector-spi` | 插件发现与引擎回调（`ConnectorProvider`、`ConnectorContext` 等） | 5 个源文件，646 行 | 5 个源文件，819 行 |

> **就地重算**：原文这两格写的是"约 9800 行"与"约 600 行"，实测偏低（文件数 95 是对的）。
>
> **状态（复核 `53a753e0a1b`）**：值得注意的是，八批清理删掉了大量死接口面之后，`fe-connector-api` 反而**净增 6 个文件、829 行**——删掉的方法被写进去的契约文档抵消了。**任何"公共接口面在收缩"的论据都不成立**，收缩发生在"要实现的方法"上，不在代码行数上。

核心接口的方法规模：

| 接口 | 调研日方法数 | 复核日方法数 | 说明 |
|---|---|---|---|
| `ConnectorTableOps` | 43 个方法名（46 个声明，含 3 组重载） | 家族合计 **43 个声明**（41 个方法名）：聚合接口自身只剩 **2 个**，其余分布在 6 个域接口（13 / 4 / 4 / 11 / 7 / 2） | 表相关的一切（已按域拆分，见 E1 状态戳） |
| `Connector` | 32（实测应为 34） | **21**（18 个方法名） | 连接器总入口 |
| `ConnectorScanPlanProvider` | 24 | 23（20 个方法名） | 读路径规划 |
| `ConnectorContext` | 19 | 18（17 个方法名） | 引擎提供给连接器的服务 |
| `ConnectorSession` | 14（实测应为 15） | 15 | 会话上下文 |
| `ConnectorWritePlanProvider` | 12 | 12 | 写路径规划 |
| `ConnectorMetadata` | 11（自身）+ 继承 6 个子接口，合计约 70 | 自身 **10** + 继承 6 个子接口，家族合计 **75 个声明** | 元数据总入口 |

> **交叉核对修正**：`Connector` 在调研日实测是 34 个方法（不是 32），`ConnectorSession` 实测是 15 个（不是 14）。
>
> **状态（复核 `53a753e0a1b`）**：`Connector` 从 34 掉到 21，是三批删除叠加的结果——写特性镜像 11 个（`dfbefc790e8`）、属性描述两个 getter（`a7de939fba9`）、REST 直通换成判空探针（`1f2452165ec`），期间新增了 `ownsHandle` 与 `getRestPassthrough`。

需要先说明：这套接口有不少做得很好的地方——几乎所有方法都有默认实现（新连接器可以只实现自己支持的部分）、
数据结构基本是中立的值对象、`ConnectorContext` 刻意避开了 Thrift 类型（用字符串和中立的 `ConnectorBrokerAddress` 传递）、
每语句作用域（`ConnectorStatementScope`）把缓存生命周期交给引擎管理。本文聚焦的是仍然存在的问题。

---

## 三、问题总览

按"是否阻碍目标"排优先级：

| 编号 | 问题 | 影响 |
|---|---|---|
| A | 新增连接器**仍然必须改 fe-core**（类型白名单 + 两处按类型的分支） | 直接违背目标 |
| B | 公共接口里混入了源专有语义（10 处） | 中立性破坏，新连接器要面对无意义的方法 |
| C | 存在无人调用的"死接口"（5 处） | 误导实现者，增加维护成本 |
| D | 能力声明有 5 套并存机制 | 新连接器不知道该用哪套 |
| E | 四个核心接口职责严重耦合 | 实现者要在几十个不相干方法里找自己需要的 |
| F | 一批语义不清的接口（单位/空值/命名） | 容易实现错，且错得很安静 |
| G | 对称性缺口：异构网关连接器有潜在错误 | 潜在缺陷 |
| H | 4 处实现与接口文档相互矛盾 | 契约不可信 |

> **逐类状态（复核 `53a753e0a1b`）**
>
> | 编号 | 复核结论 |
> |---|---|
> | A | **大部分已解除**：类型白名单已删（A1），能力枚举未变且未构成障碍（A3），分片类型枚举已删（A4）；**只剩 A2 那两处展示名 switch**，属已知未决 |
> | B | **10 处中：2 处已落地、1 处符号消失、2 处经复核不算违规（其中 1 处的建议照做会打断 iceberg 与 paimon）、1 处已中立化改名、其余仍在** |
> | C | **5 处死接口已删 4 处半**；唯一完整幸存的是 C2（`estimateScanRangeCount`），C6 的布尔位也还在 |
> | D | **5 套并存机制已减为 4 套**（CSV 那套已类型化），且选择规则已成文写进包级说明 |
> | E | **E1 已按域拆分、E3 已从 34 个方法减到 21 个**；E2、E4、E5 原样 |
> | F | **8 条中：1 条（`getProperties`）符号已删、1 条（`getWritePartitioning` 三态）经复核不成立、1 条（同名反向失效接口）冲突已随删除消失**；其余仍在 |
> | G | **G1 已落地、G3 的规则已写下（但两半未统一）**；G2 的前提在调研日就是错的，缺口比原文窄得多 |
> | H | **H2 作废、H3/H4 失去对象、H5 仍在**；**H1 仍然成立且升级为三方矛盾，是全文最该先做的一条** |

---

## 四、问题详述

### A. 新增连接器仍然必须修改 fe-core

这是最直接违背目标的一类问题，共 3 处。

#### A1. fe-core 里写死了"哪些类型走插件路径"的白名单

> **状态（复核 `53a753e0a1b`）：已落地。** `SPI_READY_TYPES` 全仓零命中，白名单由 `23b971a4db9` 整体删除；路由改为先问插件注册表。本节建议的"反向名单"也已经存在：`CatalogFactory.BUILTIN_CATALOG_TYPES = {"lakesoul", "doris", "test"}` 配 `isBuiltinCatalogType`，且那三个内建类型名成为保留字——插件声明它们时在注册期就被拒绝。

`fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java:56`

```java
private static final Set<String> SPI_READY_TYPES =
        ImmutableSet.of("jdbc", "es", "trino-connector", "max_compute", "paimon", "iceberg", "hms");
```

只有出现在这个集合里的 catalog 类型才会去查找连接器插件（第 110 行）。
也就是说：即使一个新连接器完整实现了 `ConnectorProvider` 并正确注册到 `META-INF/services`、
插件包也放进了插件目录，**只要不在这行字符串里加上自己的类型名，`CREATE CATALOG` 就完全不会走插件路径**。

**背景**：这个白名单是迁移期的安全阀——迁移过程中同一个类型可能同时存在"老的内建实现"和"新的插件实现"，
白名单保证只有已经验证过的类型才切到新路径。

**原因**：它把"这个类型有没有插件"（可以运行时发现）和"这个类型允不允许用插件"（人工决策）混在了一起。

**解决方向**：把判断反过来——先问插件注册表"有没有 provider 支持这个类型"，有就走插件路径，没有再走内建分支。
人工决策改由"是否把插件包放进插件目录"来表达，这本来就是插件机制天然的开关。
迁移期若仍需保留强制内建的能力，可以改成一个**反向的**小名单（"这些类型即使有插件也强制走内建"），
迁移完成后直接删掉，而不是每加一个连接器就改一次。

#### A2. fe-core 里按 catalog 类型做的两处 switch

> **状态（复核 `53a753e0a1b`）：仍然成立，且属于已知未决而不是遗漏。** 两处 switch 原样存在（符号未变，行号已漂到 `getEngine()` / `getEngineTableTypeName()` 各自的定义处）。后续有一轮专门中立化了另外两处按类型名匹配的引擎分支（`73d90807eaf`），审到这两处时判定**暂不下沉**——它们只影响对外展示名，不影响功能可达性。

`fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalTable.java:1275` 和 `1313`

两个方法（`getEngine()` 与 `getEngineTableTypeName()`）都是对 catalog 类型字符串做 switch，
把 `"jdbc" / "es" / "iceberg" / "trino-connector" / "max_compute" / "paimon" / "hms"` 分别映射成对外展示的引擎名和表类型名。
这两个值会出现在 `SHOW TABLE STATUS`、`information_schema.tables`、REST 接口里。

**背景**：这是为了兼容——迁移前每种外部表都是一个独立的 Java 类，有各自的展示名；
迁移后它们都变成同一个 `PluginDrivenExternalTable`，如果不 switch 就会统一显示成 `Plugin`，属于用户可见的行为回退。

**影响程度**：比 A1 轻。新连接器不加分支也能工作，只是展示名会落到默认值。
但这仍然是"公共模块里按数据源名字分叉"的写法，与既定的架构原则冲突。

**解决方向**：把展示名变成连接器自己声明的东西。最小改动是在 `ConnectorProvider` 上加一个
`default String getEngineDisplayName() { return getType(); }`，让 fe-core 直接取值；
已有的 7 个连接器各自返回它们的历史名字，switch 整体删除。这样新连接器不写也有合理默认值。

#### A3. 能力项是一个封闭枚举

`fe-connector-api/.../ConnectorCapability.java`

`ConnectorCapability` 是一个 13 项的枚举。连接器通过 `Connector.getCapabilities()` 返回自己支持的集合。
问题是：**任何一个新连接器只要需要一项现有枚举没覆盖的能力，就必须修改 `fe-connector-api`**。

这一点比 A1/A2 更微妙，因为"引擎要理解这项能力才能据此改变行为"，所以能力项本身确实需要引擎认识。
但目前这 13 项中有相当一部分（见下表）本质上是"某个具体连接器的行为开关"，而不是通用能力：

| 能力项 | 引擎侧真实用途 | 通用性 |
|---|---|---|
| `SUPPORTS_MVCC_SNAPSHOT` | 是否走快照读路径 | 通用 |
| `SUPPORTS_VIEW` | 是否把对象当视图 | 通用 |
| `SUPPORTS_PARTITION_STATS` | `SHOW PARTITIONS` 渲染几列 | 通用 |
| `SUPPORTS_PASSTHROUGH_QUERY` | `query()` 表函数 | 偏 JDBC |
| `SUPPORTS_SHOW_CREATE_DDL` | 是否渲染属性（防止 JDBC 泄漏密码） | 通用，但动机是安全兜底 |
| `SUPPORTS_TOPN_LAZY_MATERIALIZE` / `SUPPORTS_NESTED_COLUMN_PRUNE` | 优化器开关 | 通用（能力协商） |
| `SUPPORTS_METADATA_PRELOAD` | 是否预热元数据 | 通用（纯性能） |
| `SUPPORTS_USER_SESSION` | 是否注入用户凭证、是否绕开共享缓存 | 通用（安全） |
| `SUPPORTS_SORT_ORDER` | `CREATE TABLE ... ORDER BY` 是否被接受 | 通用（语法门禁） |
| `SUPPORTS_COLUMN_AUTO_ANALYZE` / `SUPPORTS_SAMPLE_ANALYZE` | 统计信息采集方式 | 通用 |
| `SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE` | 嵌套列 DDL 是否被接受 | 通用 |

**结论**：枚举本身可以保留（引擎必须认识能力项），但需要**明确一条规则并写进枚举的类注释**：
只有"引擎必须据此改变自己行为"的开关才能进这个枚举；
只有连接器自己需要知道的东西一律不进。
另外，从这些枚举项的注释里可以看出，绝大多数都是在替换迁移前的"按类名判断"逻辑，
所以这个枚举天然带有"历史包袱清单"的性质，值得在迁移收尾时重新审一遍哪些可以合并或删除。

#### A4. 另外两个封闭枚举的实际情况

- `ConnectorScanRangeType`（4 项）：见 C1，引擎根本不读，属于死接口，删掉即可，不构成扩展障碍。
- `ConnectorColumnCategory`（3 项）：只有 3 个中立取值（默认 / 合成列 / 生成列），语义清晰，没有扩展压力，可以保留。

> **状态（复核 `53a753e0a1b`）**：`ConnectorScanRangeType` 已由 `da60bfadbac` 整族删除（见 C1 状态戳）。`ConnectorColumnCategory` 保持不变。A3 的 `ConnectorCapability` 枚举**仍是 13 项、一项没增没减**，本节结论未受这 44 个提交影响。

---

### B. 公共接口里混入了源专有语义

这类问题的共同表现是：接口的名字或语义只有在某一种数据源下才讲得通，
新连接器的作者读到它们时无法判断"我要不要实现"。

下表是逐条核对"某个方法在 8 个连接器里到底谁实现了"得到的结果（只列出真正有问题的）：

| 接口方法 | 实现者 | 问题性质 |
|---|---|---|
| `Connector.executeRestRequest(path, body)` (`Connector.java:303`) | 仅 es | HTTP REST 透传是 Elasticsearch 独有的访问方式，却挂在所有连接器的总入口上 |
| `ConnectorTableOps.executeStmt(session, stmt)` (`:432`) | 仅 jdbc | "直接执行一条 SQL 语句"假设远端是一个 SQL 数据库 |
| `ConnectorTableOps.getColumnsFromQuery(session, query)` (`:440`) | 仅 jdbc | 同上，依赖远端能对任意 SQL 做预编译取元数据 |
| `ConnectorTableOps.isPartitionValuesSysTable(...)` (`:88`) | 仅 hive | 方法名直接暴露了 fe-core 内部某个表函数的实现方式 |
| `ConnectorScanPlanProvider.getSerializedTable(nodeProps)` (`:520`) | 仅 paimon | 注释明写"目前用于 Paimon 把序列化的 Table 对象传给 BE" |
| `ConnectorScanPlanProvider.adjustFileCompressType(inferred)` (`:125`) | hive、hudi | 存在的唯一理由是 Hadoop 生态把 LZ4 块格式写成 `.lz4` 后缀 |
| `ConnectorScanRange.isNativeReadRange()` (`:166`) | 仅 paimon | 只为在 EXPLAIN 里打印一行 `paimonNativeReadSplits=x/y` |
| `ConnectorPartitionValues.HIVE_DEFAULT_PARTITION` (`:26`) | 公共常量 | Hive 的魔法字符串 `__HIVE_DEFAULT_PARTITION__` 写死在中立的 scan 包里 |
| `ConnectorContext.sanitizeJdbcUrl(url)` (`:79`) | 引擎侧服务 | 引擎给所有连接器提供的服务里出现"JDBC URL 清洗" |
| `ConnectorValidationContext.validateAndResolveDriverPath` / `computeDriverChecksum` (`:50` / `:59`) | 引擎侧服务 | "驱动包路径校验"和"驱动包 MD5"只对 JDBC 有意义 |
| `Connector.schemaCacheTtlSecondOverride()` (`:359`) | iceberg、paimon | 注释直接说明它的存在是为了让 Paimon 的缓存开关也能管 schema 缓存 |

> **逐行状态（复核 `53a753e0a1b`）**
>
> | 表中的方法 | 复核结论 |
> |---|---|
> | `executeRestRequest` | **已落地**（`1f2452165ec`）：搬到窄接口 `ConnectorRestPassthrough`，经 `Connector.getRestPassthrough()` 判空探针暴露——正是下面"解决方向 3"提的做法 |
> | `executeStmt` / `getColumnsFromQuery` | **仍然成立**。两者仍是 `ConnectorTableOps` 的 default 方法，唯一实现仍是 jdbc；按域拆分之后，这两个是聚合接口自身**仅剩的两个方法**，接口注释已自认它们"应当独立成可选接口" |
> | `isPartitionValuesSysTable` | **仍然成立**，符号搬到 `ConnectorTableMetadataOps`，引擎仍在直接调用 |
> | `getSerializedTable` | **符号已不存在**：`60e9fe6f8f4` 把扫描节点属性键契约集中之后，它变成属性 map 里的一个中立键——正是下面"解决方向 2"提的做法 |
> | `adjustFileCompressType` | 见下方**交叉核对修正**，本行应从这张表移除 |
> | `isNativeReadRange` | **仍然成立**，引擎仍在读它 |
> | `HIVE_DEFAULT_PARTITION` | **已中立化改名**为 `ConnectorPartitionValues.NULL_PARTITION_NAME`（`ec24a866d45`，值一个字节没动，它是持久化标识）；**但没有下沉到 hive**，见下方交叉核对修正 |
> | `sanitizeJdbcUrl` | **仍然成立** |
> | `validateAndResolveDriverPath` / `computeDriverChecksum` | 见下方**交叉核对修正**：前者不是 JDBC 专有 |
> | `schemaCacheTtlSecondOverride` | **仍然成立** |

还有两个偏"默认值不中立"的问题：

- `ConnectorScanRange.getFileFormat()` 默认返回 `"jni"`（`:67`）。"jni"不是一种文件格式，
  而是 BE 侧的一种读取机制。把格式和机制混在一个字段里，会让新连接器误以为自己必须返回某种文件格式。
- `ConnectorScanRange.getTableFormatType()` 默认返回 `"plugin_driven"`（`:121`），而注释举的例子是 `"jdbc"`、`"hive"`。
  这个字符串直接决定 BE 用哪个读取器，属于 FE/BE 之间的约定，却没有任何类型约束。

**为什么会变成这样**：这些方法几乎都是迁移过程中"某个连接器需要一个口子"时加上去的。
加一个 `default` 方法成本极低（不影响其他连接器），于是公共接口逐渐变成了所有连接器需求的并集。

**解决方向**（三条，按适用场景选）：

1. **能中立化的就中立化**。例如 `adjustFileCompressType` 的本质是"连接器有权决定最终压缩类型"，
   把方法名和文档改成中立表述（不再提 Hadoop 和 LZ4）即可；
   `HIVE_DEFAULT_PARTITION` 的本质是"连接器自己的空分区哨兵值"，应该由 hive 连接器持有，
   公共层只保留"连接器可以声明哪些值代表 NULL"的中立能力（`ConnectorPartitionInfo` 已经有 `partitionValueNullFlags` 这条更好的路子）。
2. **纯展示/诊断类的，改成中立的键值对**。`isNativeReadRange` 只服务于 EXPLAIN 的一行文字，
   而 `ConnectorScanPlanProvider.appendExplainInfo` 已经提供了"连接器自己往 EXPLAIN 里追加内容"的通道，
   前者应该被后者吸收掉。`getSerializedTable` 同理——它的内容最终就是进 `nodeProperties` 的一个值，没必要单独开一个方法。
3. **真正只属于一种数据源的，移到专门的可选接口上**。参考 api 里已有的好做法：
   `RewriteCapableTransaction`、`WriteBlockAllocatingConnectorTransaction` 是两个**窄的、可选实现的**接口，
   引擎用 `instanceof` 判断，连接器不实现就等于不支持。
   `executeRestRequest`（REST 透传）、`executeStmt` + `getColumnsFromQuery`（SQL 透传）
   完全可以照这个模式做成 `ConnectorRestPassthrough`、`ConnectorSqlPassthrough` 两个小接口。
   同理，`ConnectorValidationContext` 里的驱动包相关方法应该拆到一个 JDBC 专用的校验上下文里。

> **交叉核对修正 ①（针对解决方向 1 的前半句）：`adjustFileCompressType` 不算中立性违规，这一整条撤销。**
> 它的方法名与默认实现（恒等）本来就是中立的；javadoc 里提 Hadoop 与 LZ4 的那一段，**恰恰是在解释这个钩子为什么必须存在**，末句原文就是"把这个 hadoop 专有事实圈在连接器里，通用节点保持与数据源无关"。它是"通用节点不得出现数据源专有代码"这条规则的**正面示范**，不是违规。把这段说明删掉，只会让下一个维护者不知道这个钩子干什么，进而把 LZ4 重映射写回通用扫描节点。
> 注意：它出现在 G3 那份 thrift 类型清单里的那一行**依然成立**（入参是 `TFileCompressType`），两者不矛盾——一处讲语义中立性，一处讲类型依赖。
>
> **交叉核对修正 ②（针对解决方向 1 的后半句）：空分区哨兵不能下沉到 hive 连接器。**
> 三条硬约束：引擎自己的分区路径解析在用它（`FilePartitionUtils`）；引擎渲染 `partition_values()` 表函数那一格也在用它（`MetadataGenerator`）；**非 hive 的 paimon 连接器主动把空分区归一成这个串**。下沉会让 `fe-core` 反过来依赖 hive 插件，直接违反"`fe-core` 只出不进"的纪律。
> 实际走的是另一条路（`ec24a866d45`）：**原地中立化改名** `HIVE_DEFAULT_PARTITION` → `NULL_PARTITION_NAME`，值一个字节没动，并把引擎侧那份重复定义删掉——调研日 FE 树里有两份同串定义，现在只剩一份。真正下沉的是**只对目录名式分区成立**的那三个归一方法，它们被内联进唯一使用者 hudi（`2ed7da4cb68`），hudi 自己持有私有的 `"\N"`。
>
> **交叉核对修正 ③（针对上表第 10 行与解决方向 3 的末句）：`validateAndResolveDriverPath` 不是 JDBC 专有，这条建议在调研日就是错的。**
> iceberg 与 paimon 都在用它做 `CREATE CATALOG` 的驱动 URL 安全门（paimon 侧还有单测钉住），照"拆到 JDBC 专用校验上下文"去做会**直接打断这两个连接器**。真正只属于 JDBC 的只有 `computeDriverChecksum`（唯一调用方是 jdbc）。

---

### C. 无人调用的死接口

这些方法/类型仍在公共接口里，但**在整个仓库（排除测试）中找不到引擎侧的调用点**。
它们的危害不是运行时错误，而是误导：新连接器的作者读到接口时会认真去实现它们。

#### C1. 扫描分片类型（`ConnectorScanRangeType` + 两个 getter）

> **状态（复核 `53a753e0a1b`）：已整族删除**（`da60bfadbac`）。枚举、两个 getter、以及连带发给 BE 的那个死属性键全部消失，全仓零命中；新增的双向断言测试守住"不再有这个键发给 BE"。
>
> **交叉核对修正**：本节写的"全部 7 个连接器"是 **8 个**——漏了 trino（`TrinoScanRange`）。按"7 个"去删会漏改 trino 而编译失败。另外，两个方法的默认值情况**并不对称**：分片上那个是接口抽象方法、没有默认实现，所以 8 个分片类被迫逐个实现；扫描计划提供者上那个**有默认值** `FILE_SCAN`，只有 hive / es / jdbc 三家做了与默认值等价的多余覆写。

- `ConnectorScanPlanProvider.getScanRangeType()`（`:52`），文档写着"引擎据此决定生成哪种 Thrift 扫描分片结构"。
- `ConnectorScanRange.getRangeType()`（`:43`），同样的说法。

实际情况：`fe-core/src/main` 中**没有任何一处读取这两个方法**（只有测试类里的匿名实现）。
引擎统一走文件扫描分片结构，具体差异由 `populateRangeParams` 由连接器自己填。
但这两个方法**没有默认实现或者默认值形同虚设**，于是全部 7 个连接器都老老实实实现了 `getRangeType()`
（es / hive / hudi / iceberg / jdbc / maxcompute / paimon），hive、es、jdbc 还额外实现了 `getScanRangeType()`。

这是一次纯粹的无效劳动，而且文档是错的。

**建议**：删除 `ConnectorScanRangeType` 枚举和这两个方法；如果将来真的需要多种分片结构，
应该由 `ConnectorScanRange` 的具体子类型来表达，而不是一个平行的枚举标签。

#### C2. `ConnectorScanPlanProvider.estimateScanRangeCount`（`:432`）

> **状态（复核 `53a753e0a1b`）：仍然成立，且是 C 段唯一的幸存者。** 44 个提交没有一个碰过它——接口声明仍在 `ConnectorScanPlanProvider`，唯一实现仍是 `JdbcScanPlanProvider`，`fe-core` 仍然零调用。C 段其余六条（C1 / C3 / C4 / C5 / C7，以及 C6 的一半）都已处置，**只有这一条被漏下了**。

文档说"引擎可能用它预分配资源或决定扫描并行度"。全仓搜索结果：**只有接口声明和 JDBC 的一个实现，零调用点**。

**建议**：删除。真正在用的并行度提示是 `streamingSplitEstimate`（见后文），两者功能重叠。

#### C3. `ConnectorTableOps.listPartitionValues`（`:499`）

> **状态（复核 `53a753e0a1b`）：已删除**（`891709a08e7`）。该提交的正文同时记录了本节指出的文档错误——javadoc 指向 `partition_values()`，实际走的是分区列举那条路。

文档说"被 `partition_values()` 表函数和列去重优化使用"。
实际情况：`partition_values()` 表函数（`fe-core/.../tablefunction/PartitionValuesTableValuedFunction.java`）
走的是分区列 + 分区项那条路，从不调用这个方法。
但 paimon、maxcompute、hudi 三个连接器都实现了它（并且实现里还要处理列顺序对齐等细节）。

**建议**：确认历史上的调用点确实已经迁走后删除；若还有保留价值，必须把文档改成真实用途。

#### C4. 连接器属性描述（`ConnectorPropertyMetadata` + `Connector.getTableProperties()` / `getSessionProperties()`）

> **状态（复核 `53a753e0a1b`）：已删除**（`a7de939fba9`），选的是本节两个选项里的"删除"。值对象与 `Connector` 上那两个 getter 一并消失。**注意不要误伤同名方法**：`PluginDrivenExternalTable.getTableProperties()` 与 `ConnectorSessionImpl.getSessionProperties()` 是引擎侧另外两个活方法，与本节无关。

`ConnectorPropertyMetadata` 是一个 120 行、带泛型和 4 个工厂方法的完整值对象，
配套 `Connector` 上两个返回 `List<ConnectorPropertyMetadata<?>>` 的方法（`Connector.java:235` 和 `:240`）。

全仓搜索（含测试目录）：**除了它自己的定义文件之外，`ConnectorPropertyMetadata` 这个类型名在整个仓库里一次都没出现过**。
（注意不要与 `PluginDrivenExternalTable.getTableProperties()` 混淆，那是另一个同名方法，返回的是普通字符串 map。）

这是一个完整设计好、但从未接线的属性描述子系统。

**建议**：要么删除，要么补上真实用途（例如驱动 `CREATE CATALOG` 的属性校验和 `SHOW` 类语句的属性文档）。
保持现状是最差的选择——它给新连接器作者一个错误信号，以为需要声明属性元数据。

#### C5. `ConnectorPartitionHandle`（handle 包）

> **状态（复核 `53a753e0a1b`）：已删除**（`d258eb20f2a`）。

一个空的标记接口，全仓零引用（fe-core 和所有连接器都没用过）。

**建议**：删除。

#### C6. 与"是否重写了方法"重复的能力声明

> **状态（复核 `53a753e0a1b`）：仍然成立。** 布尔位与建库方法都还在，仍然靠实现者手工保持同步；本节提的两个补救（改抛"不支持"异常 / 加契约测试）都没做。

`ConnectorSchemaOps.supportsCreateDatabase()`（`:58`）与 `createDatabase(...)`（`:63`）是一对：
前者返回 true 才会走"建库前先检查远端是否存在"的逻辑，后者是真正的建库实现。
当前 4 个连接器（hive / iceberg / paimon / maxcompute）两者都实现了，暂时没有不一致。

问题在于这两者**在语义上必然同进同退**，却由实现者手工保持同步。
一个新连接器只实现 `createDatabase` 而忘了 `supportsCreateDatabase`，
`CREATE DATABASE IF NOT EXISTS` 的行为就会悄悄退化，且没有任何检查会发现。

**建议**：删除布尔方法，改为让默认的 `createDatabase` 抛出一个引擎可识别的"不支持"异常，
引擎据此判断；或者至少加一条契约测试强制两者一致。

#### C7. `Connector` 上 6 个写特性的空安全转发

> **状态（复核 `53a753e0a1b`）：已删除**（`dfbefc790e8`），而且删得比本节建议的更彻底——不是"把空安全逻辑挪到引擎侧工具方法"，而是把这一族**连同 per-handle 变体共 11 个**（本节估的是 9 个）整体删掉，引擎改为先按表句柄取写计划提供者、再直接问它。副作用是 G1 描述的对称性缺口**在结构上不再可能存在**。

`Connector.java:132`–`:186` 有 6 个方法（`supportsWriteBranch`、`requiresParallelWrite`、
`requiresFullSchemaWriteOrder`、`requiresPartitionLocalSort`、`requiresPartitionHashWrite`、
`requiresMaterializeStaticPartitionValues`），每一个的实现都是同一句话：
"取写计划提供者，为 null 则返回 false，否则转发同名方法"。

核对结果：**没有任何连接器重写过 `Connector` 上的这 6 个方法**，全部只在 `ConnectorWritePlanProvider` 上实现。
所以这 6 个方法纯粹是给引擎用的便利转发。

**建议**：这不算错误，但它让 `Connector` 接口膨胀了 6 个方法（加上 3 个 per-handle 变体共 9 个），
新连接器作者需要判断"这些我要不要实现"。
更干净的做法是把这段空安全逻辑放到引擎侧的一个工具方法里，`Connector` 只保留 `getWritePlanProvider(handle)`。

#### C8. 重载堆叠

> **状态（复核 `53a753e0a1b`）：部分处置。** 建表的两个重载已合成**一个**（javadoc 明写"刻意只保留一个 create"），删掉的正是本节点名的那个丢信息的 legacy 版；建库的 cascade 重载也已收成单个四参版本，javadoc 记下了旧重载导致 `DROP DATABASE ... FORCE` 静默不级联的原因。**`planScan` 的 4 个重载、以及"带快照 / 不带快照"那两组重载原样还在**，本节提的"一个方法 + 一个请求对象"没有做。

- `ConnectorScanPlanProvider.planScan` 有 **4 个重载**（4/5/6/7 参数），后一个默认委托给前一个。
  新连接器必须读完 4 段文档才知道该实现哪一个。
- `ConnectorTableOps.createTable` 有 2 个重载，旧的那个（`schema, properties`）会丢掉分区、分桶、`IF NOT EXISTS` 信息，
  文档自己称之为"legacy"。而且它的两个参数存在冗余——`ConnectorTableSchema` 里本来就有 `properties` 字段，
  默认实现（`:241`）把 `request.getProperties()` 同时塞进 schema 和第二个参数。
- `ConnectorSchemaOps.dropDatabase` 有 2 个重载（是否 cascade）。
- `ConnectorTableOps.getTableSchema` / `getColumnHandles`、`ConnectorStatisticsOps.getTableStatistics`
  各有"带快照"和"不带快照"两个版本。

**建议**：把参数逐步增长的重载合并成**一个方法 + 一个请求对象**。
`ConnectorCreateTableRequest` 已经是这个模式的正确示范，`planScan` 应该照做
（`ConnectorScanRequest` 承载 columns / filter / limit / requiredPartitions / countPushdown），
这样以后再加规划维度就不用再开一个重载。带快照的那组重载可以把快照并入请求对象或做成可选参数。

---

### D. 能力声明有 5 套并存机制

这是新连接器作者最容易困惑的地方。目前"我支持某个功能"可以用 5 种完全不同的方式表达：

| 机制 | 例子 | 引擎侧判断方式 |
|---|---|---|
| 1. 枚举集合 | `getCapabilities()` 返回 `SUPPORTS_VIEW` | 集合包含判断 |
| 2. 接口上的布尔方法 | `supportsCreateDatabase()`、`supportsTableSample()`、`supportsBatchScan()`、`supportsColumnHandleSnapshotPin()`、`supportsCastPredicatePushdown()`、`supportsSystemTableTimeTravel()` | 直接调用 |
| 3. getter 返回 null 表示不支持 | `getScanPlanProvider()`、`getWritePlanProvider()`、`getProcedureOps()`、`getEventSource()` | 判空 |
| 4. 窄的可选接口 + `instanceof` | `RewriteCapableTransaction`、`WriteBlockAllocatingConnectorTransaction` | 类型判断 |
| 5. 表级能力用字符串 CSV 传递 | `ConnectorTableSchema` 的 `__internal.connector.per-table-capabilities` 键，值是枚举名的逗号串 | 解析字符串 |

五套机制的取舍确实各有道理（枚举适合静态、getter 适合有实现体、`instanceof` 适合"不支持就没有这个方法"），
但目前**没有一处文档说明选择规则**，结果是同一类问题在不同地方用了不同解法。
最典型的矛盾是：写能力（`WriteOperation` 集合）曾经在枚举里、后来搬到了 provider 上，
`ConnectorCapability` 的类注释还专门解释了这件事；
而扫描能力（`SUPPORTS_TOPN_LAZY_MATERIALIZE`、`SUPPORTS_NESTED_COLUMN_PRUNE`）却留在枚举里。

第 5 种尤其值得注意：把一组枚举值序列化成 CSV 字符串，塞进本来用于承载表属性的 map，
再由 fe-core 解析回枚举。它绕开了类型系统，出错时不会在编译期暴露，只会在运行时表现为"能力莫名其妙没生效"。

**建议**：定一条明确的规则并写进 `ConnectorCapability` 的类注释，例如：

- 有实现体的能力（读、写、存储过程、事件源）→ 用 getter 返回 null 表达；
- 纯粹的布尔开关且引擎需要在**规划期静态判断**的 → 进 `ConnectorCapability` 枚举；
- 只有一种数据源需要的窄能力 → 独立的可选接口 + `instanceof`；
- 需要**按表**变化的能力 → 这是目前唯一确实缺失的机制，应该做成一个正经的接口方法
  （例如 `Set<ConnectorCapability> getTableCapabilities(session, handle)`），而不是 CSV 字符串。

> **状态（复核 `53a753e0a1b`）**
>
> - **第 5 种机制已经消失**（`5a6d76abadb`）：那个 `__internal.` 的 CSV 键全仓零命中，`ConnectorTableSchema` 上换成了类型化的 `Set<ConnectorCapability>` 字段与 getter，字段 javadoc 里还留了一句"它曾经是一个装枚举名 CSV 的保留键"。**本节建议的第 4 条已经落地**，形态与建议一致（载体是表 schema 而不是新的 `getTableCapabilities(session, handle)` 方法）——**现在再照建议加一个方法，会造出第二条并行通道**。
> - **"没有一处文档说明选择规则"已被推翻**（`d572751af49`）：`fe-connector-api` 的 `package-info.java` 里现在有"规则一 —— 一项能力只在三层里的**恰好一层**声明"，把本节这四条取舍写成了成文规则。
> - 前 4 种机制原样保留，本节对它们的描述仍然准确。

---

### E. 核心接口职责耦合

#### E1. `ConnectorTableOps` —— 43 个方法，7 类互不相干的职责

> **状态（复核 `53a753e0a1b`）：已按域拆分**（`a9567456c44`），并且每个域接口都写了"最少实现集"。实际拆出来的 6 个域接口与本节提议的分组基本一一对应，只有三处命名不同：句柄/schema 组叫 `ConnectorTableMetadataOps`（不是继续叫 `ConnectorTableOps`）、列 DDL 组叫 `ConnectorColumnEvolutionOps`（不是 `ConnectorColumnDdlOps`）、分区枚举组叫 `ConnectorPartitionListingOps`（不是 `ConnectorPartitionOps`）；系统表那三个方法没有单独成组，并进了 `ConnectorTableMetadataOps`。`ConnectorTableOps` 自身现在只剩 2 个方法（`executeStmt` / `getColumnsFromQuery`，见 B 段），其余全在域接口里。
>
> **连带后果**：本文 B 段与 C 段所有形如 `ConnectorTableOps.java:NNN` 的锚点，现在都指向一个已经不含那个成员的文件——核对时请按符号名去对应的域接口里找。

按语义分组：

| 职责 | 方法 |
|---|---|
| 表句柄与 schema | `getTableHandle`、`getTableSchema`×2、`getColumnHandles`×2、`supportsColumnHandleSnapshotPin`、`listTableNames` |
| 系统表 | `listSupportedSysTables`、`getSysTableHandle`、`isPartitionValuesSysTable` |
| 视图 | `viewExists`、`listViewNames`、`getViewDefinition`、`dropView` |
| 表级 DDL | `createTable`×2、`dropTable`、`renameTable`、`truncateTable`、`renderShowCreateTableDdl` |
| 列 DDL | `addColumn`、`addColumns`、`dropColumn`、`renameColumn`、`modifyColumn`、`reorderColumns` + 5 个嵌套列版本 |
| 快照引用与分区规格 DDL | `createOrReplaceBranch`、`createOrReplaceTag`、`dropBranch`、`dropTag`、`addPartitionField`、`dropPartitionField`、`replacePartitionField` |
| 分区枚举 | `listPartitionNames`、`listPartitions`、`listPartitionValues` |
| 杂项 | `getPrimaryKeys`、`getTableComment`、`executeStmt`、`getColumnsFromQuery`、`buildTableDescriptor` |

一个只读的连接器（比如一个新的对象存储格式）需要实现的只有第一组和"分区枚举"，
但它必须面对全部 43 个方法的文档才能确认这一点。

**建议**：按上表拆成 `ConnectorTableOps`（句柄/schema/列表）、`ConnectorViewOps`、`ConnectorTableDdlOps`、
`ConnectorColumnDdlOps`、`ConnectorSnapshotRefOps`、`ConnectorPartitionOps` 若干个接口，
`ConnectorMetadata` 继续继承它们（对现有实现零影响，因为 Java 的接口继承是扁平的），
但新连接器可以按需只看自己关心的那几个。这是纯粹的文档与认知成本优化，不改变任何运行时行为。

#### E2. `ConnectorScanPlanProvider` —— 24 个方法，混了 6 类东西

> **状态（复核 `53a753e0a1b`）：仍然完全成立。** 尤其是本节点出的那个矛盾——接口声称提供者"每次调用新建、无状态"，却要求它释放另一次调用中开启的读事务——原样存在，hive 仍然靠连接器级的读事务管理器绕过。这是全文仍然开放的问题里最实的一条。

规划（`planScan` 4 个重载 + `streamSplits` + `planScanForPartitionBatch`）、
能力开关（`supportsBatchScan` / `supportsTableSample` / `supportsSystemTableTimeTravel` / `ignorePartitionPruneShortCircuit`）、
Thrift 填充（`populateScanLevelParams` / `getDeleteFiles`）、
EXPLAIN 渲染（`appendExplainInfo`）、
诊断（`collectScanProfiles`）、
**事务生命周期**（`releaseReadTransaction`）。

最后一项特别值得指出：接口自己的文档在 `getScanPlanProvider(handle)` 处写明
"提供者是每次调用新建的、无状态的"（`Connector.java:76`），
但 `releaseReadTransaction(queryId)`（`:539`）要求这个"无状态"对象能够释放另一次调用中开启的事务。
hive 的实现只能靠把状态放在连接器级的 `HiveReadTransactionManager` 上来绕过这个矛盾
（`HiveScanPlanProvider.java:347`）。也就是说，接口声明的对象生命周期和它承担的职责是冲突的。

**建议**：把每查询的读事务生命周期移到 `Connector` 上（连接器才是长生命周期对象），
或者引入一个显式的"查询级资源"抽象；EXPLAIN 与诊断方法可以合并成一个"扫描诊断"接口。

#### E3. `Connector` —— 32 个方法

> **状态（复核 `53a753e0a1b`）：大部分已落地，方法数 34 → 21。** 本节列举的负担里，**REST 透传**、**属性描述**、**写特性转发（实为 11 个）** 三项已经删除或搬走；**缓存失效那 4 个 `invalidate*` 原样还在**，本节建议的"拆成 `ConnectorCacheInvalidation` 可选接口"没有做。异构网关路由（`ownsHandle` + per-handle getter）也还在，但 per-handle getter 从 4 组减到 3 组。

除了合理的入口方法外，还承担了：缓存失效（4 个 `invalidate*`）、连通性测试（3 个）、
存储属性推导、schema 缓存 TTL 覆盖、REST 透传、属性描述（死接口）、异构网关路由（`ownsHandle` + 4 组 per-handle getter）、
写特性转发（9 个）。

**建议**：至少把缓存失效（4 个方法）拆成一个 `ConnectorCacheInvalidation` 可选接口——
大多数连接器不缓存任何东西，这 4 个方法对它们完全是噪音。

#### E4. `ConnectorContext` —— 19 个方法，引擎服务的大杂烩

包含：身份（catalog 名/id）、认证（`executeAuthenticated`）、HTTP 安全钩子、JDBC URL 清洗、
缓存失效回调、兄弟连接器工厂、**存储相关的 8 个方法**（凭证归一化、URI 归一化 3 个重载、
BE 文件类型、broker 地址、BE 存储属性、类型化存储属性、文件系统、空目录清理）、BE 连通性探测。

存储那 8 个方法明显自成一体，应该是一个独立的 `ConnectorStorageContext`（由 `ConnectorContext` 提供）。

> **状态（复核 `53a753e0a1b`）：仍然成立，一步未动。** `ConnectorStorageContext` 这个类型名全仓零命中，从未被创建过。这一条已经在另一条工作线里单独立项，被标为**高危**——它跨引擎与全部插件的边界，落地必须配插件包重部署冒烟。

#### E5. `ConnectorMetadata` 是一个约 70 方法的聚合接口

> **状态（复核 `53a753e0a1b`）：结构未变，自身方法 11 → 10**（"属性"那个已随 F4 一并删除）。本节指出的"MVCC 与 handle 改写方法散在 `ConnectorMetadata` 自身而不是一个 Ops 子接口里"仍然成立。注意它继承的 6 个接口是 `ConnectorSchemaOps` / `ConnectorTableOps` / `ConnectorPushdownOps` / `ConnectorStatisticsOps` / `ConnectorWriteOps` / `ConnectorIdentifierOps`——而 `ConnectorTableOps` 自己又继承了 6 个域接口，所以家族合计已是 75 个声明。

它继承 6 个 Ops 接口，自己再加 11 个（属性、5 个 MVCC 相关、3 个 handle 改写、快照 schema）。
这些 MVCC 与 handle 改写方法（`applySnapshot`、`applyRewriteFileScope`、`applyTopnLazyMaterialization`）
在语义上属于同一族——"在规划前把某个信息织进表句柄"——但它们分散在 `ConnectorMetadata` 自身而不是一个 Ops 子接口里。

---

### F. 语义不清的接口

#### F1. `ConnectorScanRange.getLength()` 的单位不确定（`:56`）

> **状态（复核 `53a753e0a1b`）：仍然成立，一步未动。** `getLength()` 的文档仍然只写"字节数"，各连接器的语义分歧原样存在，`supportsTableSample()` 这个补丁式的布尔开关也还在，仍然只有 hive 声明。

文档写"要读取的字节数，-1 表示整个文件"。
实际语义按连接器而异：hive/iceberg 是字节数；MaxCompute 默认与 Paimon 的 JNI 分片返回 -1；
MaxCompute 的行偏移模式返回的是**行数**。

这个歧义已经产生过真实后果：任何"按大小做采样/切分"的通用逻辑都不能直接用这个值，
所以 `supportsTableSample()`（`ConnectorScanPlanProvider:268`）才不得不存在，
用一个额外的布尔开关来声明"我的 length 真的是字节数"。目前只有 hive 声明了它。

**建议**：把字段拆开——`getLengthInBytes()` 语义唯一，行数之类的连接器私有信息进 `properties`。
在此之前，至少要把这个歧义写进 `getLength()` 的文档（现在文档是明确说"字节"的，与事实不符）。

#### F2. `null` 与空集合的三态区分

三处接口用"null 和空集合表示不同含义"来编码信息：

- `ConnectorWritePlanProvider.getWriteSortColumns`（`:96`）：`null` = 无写排序；空 list = 有排序但列不可解析。
- `ConnectorWritePlanProvider.getWritePartitioning`（`:119`）：`null` = 未分区；空 spec = 另一回事。
- `ScanNodePropertiesResult`：靠一个 `hasConjunctTracking` 布尔区分"没有跟踪"和"跟踪了但全部下推"。

> **交叉核对修正：第二条不成立，应从本节删除。** `getWritePartitioning` 只有**两态**，不存在"空 spec = 另一回事"这个第三态。接口 javadoc 写得很明确，还给了理由：`null`（**而不是空 spec**）表示目标未分区，对齐旧实现的"是否已分区"判据，引擎据此回退到非分区的合并分布。唯一的生产者是 iceberg（未分区表返回 `null`，有测试钉住），唯一的消费者是引擎侧的行级合并写入节点，全仓没有任何一处让"空规格"表示第三种含义。
> 同一节的另外两条（写排序列的 `null` vs 空 list、`hasConjunctTracking` 布尔位）**是真的**，且复核日仍然成立——写排序列的接口默认值确实是 `null`。
>
> **状态（复核 `53a753e0a1b`）**：本节提的补救（改用 `Optional` 或显式的 `WriteOrdering.none()`）没有做。

这种编码方式很容易被实现者写反（返回 `Collections.emptyList()` 而不是 `null` 就会改变行为），
而且编译器不会提醒。

**建议**：改用 `Optional<List<...>>`，或者引入显式的 `WriteOrdering.none() / WriteOrdering.of(...)`。

#### F3. `ConnectorWriteHandle.getWriteContext()` 名不符实

> **状态（复核 `53a753e0a1b`）：仍然成立。** 连接器侧的方法名没改；而引擎侧现在已经在用本节提议的那个名字（`getStaticPartitionSpec()`），于是两个名字跨边界并存，比调研日更值得统一。

方法名是"写上下文"（暗示是一个自由的信息袋），但它的文档自己承认：
唯一的生产者只往里放静态分区规格，三个消费方（hive/iceberg/maxcompute）也都当静态分区规格用。

**建议**：直接改名为 `getStaticPartitionSpec()`。

#### F4. `ConnectorMetadata.getProperties()` 没有契约（`:54`）

> **状态（复核 `53a753e0a1b`）：本节已作废——符号已删除**（`891709a08e7`），选的是本节两个选项里的"删除"。

文档只有一句"返回连接器级属性"。是 catalog 属性？是连接器自己派生的属性？给谁看的？
在 fe-core 的插件驱动路径上找不到调用点。

**建议**：补充契约或删除。

#### F5. `ConnectorSession` 有三条读配置的路径且互相重叠

> **状态（复核 `53a753e0a1b`）：仍然成立，一步未动。** 两个命名空间仍在 `getProperty` 里被静默合并，同名时会话变量仍然静默覆盖 catalog 配置。

`getCatalogProperties()`（catalog 属性全集）、`getSessionProperties()`（会话变量全集）、
`getProperty(name, type)`（`ConnectorSession.java:75`）。
第三个的引擎实现（`ConnectorSessionImpl.java:131`）会**先查会话变量、再查 catalog 属性**——
两个命名空间被静默合并，调用方无法知道拿到的值来自哪一边，同名时会话变量会静默覆盖 catalog 配置。

**建议**：让 `getProperty` 明确它查哪个命名空间，或者干脆删掉它（只有 hive 的 2 处在用）。

#### F6. 参数风格不统一：句柄 vs 字符串名

`ConnectorTableOps` 内部同时存在两种寻址方式：

- 用表句柄：`getTableSchema(session, handle)`、`dropTable(session, handle)`、`listPartitions(session, handle, filter)`……
- 用库名/表名字符串：`getPrimaryKeys(session, dbName, tableName)`、`getTableComment(session, dbName, tableName)`、
  `viewExists(session, dbName, viewName)`、`getViewDefinition(...)`、`dropView(...)`

后者绕过了句柄，意味着连接器要么重新解析一次表、要么维护两条查找路径。
更实际的影响是：**异构网关连接器无法按句柄把请求路由给正确的兄弟连接器**，
因为字符串参数里没有任何信息说明这张表属于哪种格式。

> **交叉核对修正：结论的后半句不成立。** 前半句是对的——网关**确实无法用"按句柄类型路由"这个惯用手法**去处理名字寻址的方法，这一点现在已经明文写进 SPI 文档（`ConnectorTableMetadataOps` 的接口注释专门有一段："某方法按**名字**而不是句柄寻址；异构网关按具体句柄类型把外来表路由给兄弟连接器，因此无法这样路由名字寻址的方法；如果你在写网关，必须显式处理它"）。
> 但后半句"字符串参数里没有任何信息说明这张表属于哪种格式"是**错的**：网关完全可以拿库名/表名回查一次元数据再判定格式，hive 网关现在就是这么做的（取回 HMS 表对象 → 交给格式探测器 → 只对 iceberg 表返回注释，取不到就降级成空串）。这条路径正是靠它修好了"异构目录下 iceberg 表注释显示为空"这个用户可见缺陷。
>
> **状态（复核 `53a753e0a1b`）**：本节建议的"统一为句柄寻址"没有做；名字寻址的那一组里，`getPrimaryKeys` 已随死接口面一并删除（`891709a08e7`），其余仍在。

**建议**：统一为句柄寻址。视图相关的方法确实可能在拿到句柄前调用，那就应该有一个明确的
"库/表名寻址"分组并说明为什么，而不是与句柄方法混排。

#### F7. 两个方向相反但同名的失效接口

- `Connector.invalidateTable/invalidateDb/invalidateAll/invalidatePartition`（`Connector.java:312`–`:336`）：
  **引擎 → 连接器**，用于 `REFRESH TABLE` 等命令通知连接器丢弃自己的缓存。
- `ConnectorMetaInvalidator.invalidateTable/invalidateDatabase/invalidateAll/invalidatePartition/invalidateStatistics`
  （`fe-connector-spi/.../ConnectorMetaInvalidator.java`）：
  **连接器 → 引擎**，用于连接器收到元数据变更事件后通知引擎丢弃缓存。

两组方法名几乎完全一样、参数也几乎一样（只有 `invalidateDb` vs `invalidateDatabase`、
以及 partition 参数一个是"分区名列表"一个是"分区值列表"这两处细微差别），方向却相反。
这是一个非常容易写错的设计，而且写错了不会报错，只会表现为"缓存偶尔不刷新"。

> **状态（复核 `53a753e0a1b`）：命名冲突已经消失，本节的改名建议应当撤销。**
> "连接器 → 引擎"那一整套推模型（`ConnectorMetaInvalidator` 及其引擎侧实现）已由 `0a21334f95e` 删除——它是死的：连接器 `src/main` 里除了两个类加载器钉桩包装类的透明转发没有任何生产调用，而引擎侧实现自己在注释里承认按分区失效履约不了（降级成整表失效）、统计失效是空操作。反向的元数据变更通知现在走**拉模型**（引擎侧的事件同步驱动定期向连接器 `pollOnce`）。
> 活着的那一组（`Connector.invalidate*`，引擎 → 连接器）原样保留。**不要**为了消歧义去给它改名而动 8 个连接器——名字已经不撞了。

**建议**：至少把其中一组重命名以体现方向（例如引擎→连接器的那组叫 `onRefreshTable/onRefreshDatabase`），
并统一 partition 参数的语义（现在一个传规范化分区名、一个传分区值列表）。

#### F8. 用字符串 map 传递结构化信息

> **状态（复核 `53a753e0a1b`）：部分落地。** 七个保留键里，**表级能力那个已经提升为类型化字段**（`5a6d76abadb`，见 D 段状态戳），**主键列那个已随死接口面删除**（`891709a08e7`）。分区列、分桶列、以及三个"已渲染好的 SQL 子句"仍然靠字符串键传递，本节对它们的批评仍然成立。

`ConnectorTableSchema` 定义了 7 个 `__internal.` 前缀的保留键，用来在**表属性 map** 里夹带结构化信息：
分区列（CSV）、主键列（CSV）、分桶列（CSV）、表级能力（枚举名 CSV）、
以及三个已渲染好的 SQL 片段（LOCATION 子句、PARTITION BY 子句、ORDER BY 子句）。

这么做的原因可以理解——避免为每种信息都往 `ConnectorTableSchema` 加字段。
但代价是：这些本该是类型化字段的东西，现在靠字符串键约定传递，拼错一个键不会有任何提示。
尤其是"已渲染的 SQL 子句"这三个键，等于让连接器直接生成 Doris SQL 文本，
把语法渲染的责任推给了插件；一旦 Doris 的 DDL 语法变化，所有连接器都要跟着改。

**建议**：把分区列/主键列/分桶列/表级能力提升为 `ConnectorTableSchema` 的正式字段（有类型、有默认值）；
三个 SHOW CREATE 相关的渲染键，改为让连接器返回结构化描述（例如分区字段 + 变换名 + 参数），由 fe-core 负责渲染文本。

---

### G. 对称性缺口与潜在缺陷

#### G1. 6 个写特性只有 3 个提供了"按表"变体

> **状态（复核 `53a753e0a1b`）：已落地，走的是本节推荐的那条路。** `dfbefc790e8` 把 `Connector` 上这一族转发方法（连同 per-handle 变体共 11 个）整体删除，引擎改为先 `getWritePlanProvider(handle)` 拿到提供者、再直接读特性。**"按表语义天然成立"因此兑现了，这个对称性缺口在结构上不再可能出现。**

异构网关连接器（hive catalog 同时服务 plain-hive、iceberg-on-HMS、hudi-on-HMS 三种表）
需要按表决定用哪个写计划提供者。`Connector` 为此提供了带表句柄的重载，但**只覆盖了一半**：

| 写特性 | 有连接器级方法 | 有按表变体 |
|---|---|---|
| `supportedWriteOperations` | 是 | 是 |
| `supportsWriteBranch` | 是 | 是 |
| `requiresPartitionHashWrite` | 是 | 是 |
| `requiresMaterializeStaticPartitionValues` | 是 | 是 |
| `requiresParallelWrite` | 是 | **否** |
| `requiresFullSchemaWriteOrder` | 是 | **否** |
| `requiresPartitionLocalSort` | 是 | **否** |

核对当前实现：hive 与 iceberg 的 `requiresParallelWrite` 和 `requiresFullSchemaWriteOrder` **恰好都是 true**，
两者的 `requiresPartitionLocalSort` **恰好都是 false**（只有 MaxCompute 是 true），
所以今天不会出错。但这属于"碰巧对齐"，不是设计保证。

一旦出现某个通过网关委派、且这三项与网关本身不同的表格式（例如把 MaxCompute 作为兄弟连接器接入），
引擎就会拿网关自己的写特性去规划兄弟连接器的写入，表现为分布方式错误 → 输出文件数异常或写入结果不正确。

**建议**：要么补齐三个缺失的按表变体，要么（更好）取消这 9 个转发方法，
让引擎直接通过 `getWritePlanProvider(handle)` 拿到 provider 再读特性——那样按表语义天然成立，不存在对称性问题。

#### G2. 写能力自洽性校验从未在真实连接器上运行

> **交叉核对修正：本节的核心事实在调研日就是错的。** 用 `git grep` 回到 `7ff51a106f0` 复核，那一刻**已经有四个真实连接器的契约测试在调用** `validate`：iceberg、elasticsearch、jdbc、maxcompute。所以"8 个真实连接器没有任何一个调用过它""这 4 条规则今天完全没有被验证"两句都不成立，类注释描述的机制是**部分已经实现**的机制、不是虚构的机制。
> 真实缺口比原文窄得多、也具体得多：**唯一声明"分区哈希写"的 hive 没有调用点**，因此涉及它的两条不变量缺少真实连接器的正样本，只有引擎侧那个假连接器测试在压。照原文"给 8 个连接器各补一个契约测试"去做，其中 4 个是重复建设，而真正缺的那一个原文根本没点出来、最可能被漏掉。
>
> **状态（复核 `53a753e0a1b`）**：
> - 类注释已经修好（`29b46660ac4`），现在专门有一段"Actual coverage today"，写明四家在调、并点名 hive 是那两条不变量唯一的空白。**H2 那一小节因此整条作废。**
> - 调用点数量没变（4 个连接器 + 1 个引擎侧假连接器测试），但四家的**有效性**并不一样：es 是只读连接器，写计划提供者为 `null`，`validate` 一进来就早退，等于零条不变量被压到；jdbc 有写但四个前件全 `false`，属于空过；**真正压到真前件的只有 iceberg（声明分支写）与 maxcompute（声明分区本地排序）**。
> - 本节提的"把检查扩展到按表变体"没有做，而且现在有了更明确的说法：验证器读的是连接器级提供者，引擎写路径按表解析提供者，异构网关可以在这里自洽却按表答得不同——这一点已被写进类注释，标为**有意为之、不在验证器职责范围内**。

`ConnectorContractValidator`（`fe-connector-api/.../ConnectorContractValidator.java`）
定义了 4 条写能力自洽规则（例如"要求分区本地排序就必须同时要求并行写和全 schema 顺序"、
"两个分区分布模式互斥"）。它的类注释写着：

> 这些不变量由**各连接器的契约测试**（构建每个连接器并调用 `validate`）来强制执行。

实际情况：全仓唯一的调用点是 `fe-core/src/test/.../ConnectorContractValidatorTest.java`，
它用的是**手写的假连接器**，8 个真实连接器没有任何一个调用过它。

也就是说这 4 条规则今天完全没有被验证。
更关键的是：它只检查连接器级的方法，即使被调用，也检查不到 G1 描述的按表场景。

**建议**：在每个连接器模块加一个契约测试真正调用它（这正是注释里承诺的做法），
并把检查扩展到按表变体；同时修正注释，不要描述并不存在的机制。

#### G3. Thrift 中立性存在两套相反的规则

`fe-connector-spi` 严格避开 Thrift：`ConnectorContext.getBackendFileType` 返回的是**枚举名字符串**
（`"FILE_S3"`），broker 地址用中立的 `ConnectorBrokerAddress` 而不是 `TNetworkAddress`，
并且两处都写了注释解释"为了让这个 SPI 保持无 Thrift 依赖"。

但 `fe-connector-api` 完全相反——它直接依赖 `fe-thrift`（`pom.xml` 里是 `provided` 依赖），
并在 5 个地方把 Thrift 类型放进接口签名：

| 位置 | Thrift 类型 |
|---|---|
| `ConnectorScanRange.populateRangeParams` | `TTableFormatFileDesc`、`TFileRangeDesc` |
| `ConnectorScanPlanProvider.populateScanLevelParams` | `TFileScanRangeParams` |
| `ConnectorScanPlanProvider.adjustFileCompressType` | `TFileCompressType` |
| `ConnectorScanPlanProvider.getDeleteFiles` | `TTableFormatFileDesc` |
| `ConnectorWriteHandle.getSortInfo` / `ConnectorSinkPlan` | `TSortInfo` / `TDataSink` |
| `ConnectorTableOps.buildTableDescriptor` | `TTableDescriptor`（用全限定名内联写在签名里） |

同一套契约的两半采用了完全相反的原则，而且没有任何文档说明为什么。
这会让新连接器的作者无所适从：我到底能不能用 Thrift 类型？

> **状态（复核 `53a753e0a1b`）：规则已经写下来了，结论也正如本节预判，但两半并未统一。** `d572751af49` 在两个模块各写了一份包级说明，`fe-connector-api` 那份的"规则三"就是"thrift 类型只出现在 BE 协议边界上"，并给了本节猜的那个理由（BE 是 C++、没有插件机制，发往 BE 的载荷只能是 thrift 类型），还列出了**完整的出现位置清单**并加了一条硬约束："不要新增以 thrift 类型作**入参**的方法"。"没有任何文档说明为什么"这句已被推翻。
> 但它没有把两半统一成一条规则：`fe-connector-spi` 的 thrift 回避被明确记为**那个模块的局部约定、而不是模块级不变量**，并写明"是否统一两种风格不在本次范围内"。也就是说本节指出的分歧被**如实记录**了，没有被消除。
> 另外，`buildTableDescriptor` 用全限定名内联写在签名里这一点，已经被逐字写进那份权威清单（"写成内联全限定名，不是 import"）。它不再是一处"没人知道的风格不一致"；要不要改成 import 现在纯属风格取舍。

**建议**：明确一条规则并写进两个模块的 `package-info`。
现实地看，让连接器直接构造 `TDataSink` 是有道理的（否则引擎要理解每种 sink 的方言），
所以更可能的结论是"api 允许 Thrift，spi 不允许"，那就应该把这条规则写清楚，
并顺手修掉 `buildTableDescriptor` 里用全限定名内联的写法（与其他 5 处风格不一致）。

---

### H. 实现与接口文档相互矛盾的具体案例

以下 4 处是逐条核对得出的、文档与实现直接冲突的地方。

#### H1. `listFileSizes` 的异常契约被实现有意违背

> **状态（复核 `53a753e0a1b`）：仍然成立，而且证据比调研日更强——现在是三方矛盾。** 除了本节列出的接口文档与 hive 实现互相打脸之外，`d572751af49` 新写进包级说明的一条规则站在**实现**这一边：「响亮失败 vs 静默降级——用户显式要求的操作（DDL、写入、显式的采样式 `ANALYZE`）必须响亮失败，因为在那里吞掉错误会产生一个看起来很权威的错误结果；引擎为优化而机会性发起的操作才必须静默降级」，而且它**点名**了这条命令。
> 三方里有两方说"该抛"，所以本节的判断（该改的是接口文档）现在有了成文依据。**这一条至今没有人修**——负责"修正与实现矛盾的接口文档"的那一批工作把它明确排除在范围之外了（它被归到异常契约那一批），于是两批之间漏了下来。它是全文剩下的开放项里**最便宜也最该先做的一条**：改接口 javadoc 一句话，不动任何行为。

接口文档（`ConnectorStatisticsOps.java:87`–`:95`）：

> 尽力而为：重写方必须在任何列举错误时返回空集合而不是抛异常（统计信息不能让查询失败）。

唯一的实现（hive，`HiveConnectorMetadata.java:931`）没有任何 try/catch，
远端列举失败会直接向上抛。而且这不是疏忽——同一个方法的注释（`:922`）明确写道：

> 这里的列举错误会**向上传播**（不同于 `estimateDataSizeByListingFiles` 的尽力而为 -1）：
> 它支撑的是一条显式的 `ANALYZE ... WITH SAMPLE` 命令，历史实现也是让命令响亮地失败，
> 而不是让采样器把缩放因子静默塌缩成 1.0。

fe-core 的调用点（`PluginDrivenExternalTable.java:1077`）同样没有兜底 try/catch。

两边都写了详细理由，但结论相反。**实现方的理由更站得住**（用户显式发起的 `ANALYZE` 应该失败得明确，
而不是静默产生错误的统计值），所以应该修改的是接口文档。
但只要这个矛盾还在，任何新连接器的作者都会照着接口文档去吞异常，从而引入静默的统计错误。

#### H2. `ConnectorContractValidator` 的执行方式与注释不符

见 G2：注释声称由各连接器的契约测试调用，实际只有一个用假连接器的 fe-core 测试。

> **本小节整条作废**（复核 `53a753e0a1b`）。两个理由：一，前提在调研日就是错的（当时已有四个真实连接器的契约测试在调，见 G2 的交叉核对修正）；二，类注释已经在 `29b46660ac4` 里补上了"Actual coverage today"段落，如实写明谁在调、以及 hive 那个缺口。**这里不再有"实现与文档矛盾"。**

#### H3. `listPartitionValues` 的用途与注释不符

见 C3：注释声称被 `partition_values()` 表函数使用，实际该表函数不走这条路径，引擎侧零调用。

> **本小节已失去对象**（复核 `53a753e0a1b`）：方法连同它那段错误注释一起被 `891709a08e7` 删除了。

#### H4. `ConnectorScanRangeType` 的用途与注释不符

见 C1：两处注释都声称"引擎据此决定生成哪种 Thrift 扫描分片结构"，实际引擎从不读取，
但 7 个连接器都实现了。

> **本小节已失去对象**（复核 `53a753e0a1b`）：整族被 `da60bfadbac` 删除。另，是 **8 个**连接器不是 7 个（漏了 trino），见 C1 的交叉核对修正。

#### H5. `ConnectorProvider.create()` 违反父接口契约

> **状态（复核 `53a753e0a1b`）：仍然成立，一步未动。** 那个无参 `create()` 仍然是"实现了就为了抛异常"。

`ConnectorProvider extends PluginFactory`，而 `PluginFactory` 要求实现无参的 `create()`。
`ConnectorProvider` 的做法是（`ConnectorProvider.java:93`）：

```java
@Override
default Plugin create() {
    throw new UnsupportedOperationException(
            "ConnectorProvider does not support no-arg create(). "
            + "Use create(Map, ConnectorContext) instead.");
}
```

注释坦承"提供它只是为了满足 `PluginFactory` 契约"。
这是典型的"继承了一个不适用的接口"——任何按 `PluginFactory` 泛型处理插件的代码，
碰到连接器插件都会在运行时炸掉。而且 `PluginFactory` 还有一个 `create(PluginContext)`，
其默认实现就是委托给无参 `create()`，所以这条路径同样会抛异常，问题面比表面看到的更大。

**建议**：把 `PluginFactory` 中真正共用的部分（`name()` 等）抽成一个更小的父接口，
`ConnectorProvider` 只继承那个；或者让 `PluginFactory` 的 `create()` 变成可选的。

---

## 五、改进建议汇总（按优先级）

> ### ⚠ 先读这张总账（复核 `53a753e0a1b`，2026-07-26）
>
> 下面 17 条是**调研日**（`7ff51a106f0`）写下的原文，逐字保留。此后 44 个提交里有相当一部分正是它们的落地，**照原文动手会重复劳动、扑空、或改坏正在跑的功能**。请先按这张总账过一遍。
>
> | # | 复核状态 | 说明 |
> |---|---|---|
> | 1 | **已完成** | 白名单整体删除；且本条提的"反向名单"也已存在（三个内建类型名成为保留字） |
> | 2 | **仍开放** | 两处展示名 switch 原样；后续一轮审过并判定暂不下沉，属已知未决 |
> | 3 | **已完成** | 按表能力已类型化，载体是表 schema 上的正式字段。**⚠ 别再照原文加 `getTableCapabilities(session, handle)`，那会造出第二条并行通道** |
> | 4 | **部分完成** | 分片类型枚举族已删、`ConnectorPartitionHandle` 已删；**`estimateScanRangeCount` 没删，仍是零调用的死接口**——整条勾掉会漏掉它 |
> | 5 | **已完成** | 两项都选了"删除"：属性描述子系统、`listPartitionValues` |
> | 6 | **部分完成** | H2 作废（前提在调研日就是错的）、H3/H4 失去对象；**H1 一步未动，且已升级为三方矛盾——这是全文最便宜、最该先做的一条** |
> | 7 | **应收窄** | 调研日就已有四个连接器的契约测试在调。真正要补的只有 **hive** 一家（它是"分区哈希写"的唯一声明者）。hudi / paimon / trino 不声明任何被检查的能力位，补了也只是恒真断言 |
> | 8 | **部分完成** | REST 直通已抽成窄接口；**SQL 直通那两个仍挂在 `ConnectorTableOps` 上**，且它们现在是该聚合接口仅剩的两个方法 |
> | 9 | **部分完成** | `getSerializedTable` 已随扫描属性键契约集中而消失；`isPartitionValuesSysTable`、`isNativeReadRange` 仍在，引擎仍在直接读 |
> | 10 | **⚠ 两半都撤销** | 常量走的是"原地中立化改名 + 去重"而不是下沉，**按原文去 hive 连接器找会扑空**；`adjustFileCompressType` 的 Hadoop 段落是有意保留的正面示范。见 B 段交叉核对修正 ① 与 ② |
> | 11 | **⚠ 大部分撤销** | `validateAndResolveDriverPath` 不是 JDBC 专有，iceberg 与 paimon 都在用；照做会打断这两个连接器。只有 `computeDriverChecksum` 是 jdbc-only |
> | 12 | **部分完成** | `ConnectorTableOps` 已按域拆分；**`ConnectorStorageContext` 从未创建**（高危、已单独立项）；缓存失效那 4 个方法仍在 `Connector` 上 |
> | 13 | **仍开放** | `planScan` 的 4 个重载原样，"一个方法 + 一个请求对象"没有做 |
> | 14 | **已完成** | 走的正是本条推荐的方案：取消转发方法、统一走 `getWritePlanProvider(handle)`，对称性缺口在结构上不再可能存在 |
> | 15 | **部分完成 + 一半撤销** | 三态编码里 `getWritePartitioning` 那条本就不成立（只有两态），写排序列那条仍在；`getWriteContext()` 未改名（引擎侧已用新名，两名跨边界并存）；`getLength()` 单位未澄清；**给同名反向失效接口改名这半撤销**——推模型已整套删除，冲突不存在 |
> | 16 | **部分完成** | 表级能力已类型化、主键那个键已删；分区列 / 分桶列 / 三个已渲染 SQL 子句的键仍在 |
> | 17 | **部分完成 + 一半撤销** | 规则已写进两份包级说明，但两半**未统一**（spi 的 thrift 回避被记为局部约定，是否统一明确列为不在范围内）；**"修掉内联全限定名"这半撤销**，它已被逐字写进权威清单 |
>
> **合计**：完全完成 4 条（1 / 3 / 5 / 14）、部分完成 8 条（4 / 6 / 8 / 9 / 12 / 15 / 16 / 17）、仍开放 2 条（2 / 13）、应收窄 1 条（7）、应撤销 2 条（10 / 11）。
>
> 另外，本清单**没有覆盖**到、但复核确认仍然开放的还有：E2 的读事务生命周期矛盾、F5 的两个属性命名空间静默合并、C6 的建库布尔位与建库方法手工同步、H5 的无参 `create()` 只为抛异常。

### 第一优先级：解除"新增连接器必须改 fe-core"

1. 把 `CatalogFactory.SPI_READY_TYPES` 白名单改为"由插件注册表决定"，迁移期若需要保留强制内建能力则改成反向名单。
2. 把 `PluginDrivenExternalTable` 的两处按类型 switch 改为从 `ConnectorProvider` 取展示名，
   已有连接器各自返回历史名字，保证零行为变化。
3. 补齐唯一真正缺失的扩展机制：**按表能力**应有正式接口方法，替换目前的 CSV 字符串键。

### 第二优先级：删除死接口，修正错误文档

4. 删除：`ConnectorScanRangeType` + `getRangeType()` + `getScanRangeType()`（顺带减少 7 个连接器的无效实现）、
   `estimateScanRangeCount`、`ConnectorPartitionHandle`。
5. 处置：`ConnectorPropertyMetadata` + `Connector.getTableProperties()/getSessionProperties()`（接线或删除）、
   `listPartitionValues`（确认后删除或修正文档）。
6. 修正 4 处错误文档（H1–H4），其中 H1 应改接口文档而非改实现。
7. 让每个连接器的契约测试真正调用 `ConnectorContractValidator`。

### 第三优先级：中立化

8. 把 `executeRestRequest`、`executeStmt` + `getColumnsFromQuery` 移出通用接口，
   改为窄的可选接口 + `instanceof`（照抄 `RewriteCapableTransaction` 的现成模式）。
9. `isPartitionValuesSysTable`、`getSerializedTable`、`isNativeReadRange` 三个方法的信息
   分别可以由现有的系统表接口、`nodeProperties`、`appendExplainInfo` 承载，予以吸收。
10. `HIVE_DEFAULT_PARTITION` 常量下沉到 hive 连接器；`adjustFileCompressType` 的文档去 Hadoop 化。
11. `ConnectorValidationContext` 里的驱动包校验方法拆到 JDBC 专用的校验上下文。

### 第四优先级：结构与语义

12. 拆分 `ConnectorTableOps`（按 E1 的 7 组），拆出 `ConnectorStorageContext`，
    把缓存失效从 `Connector` 拆成可选接口。
13. `planScan` 的 4 个重载合并为"一个方法 + 一个请求对象"。
14. 补齐或取消写特性的按表变体（G1），推荐取消转发方法、统一走 `getWritePlanProvider(handle)`。
15. 消除 `null` vs 空集合的三态编码；`getWriteContext()` 改名为 `getStaticPartitionSpec()`；
    澄清 `getLength()` 的单位；给两组同名反向的失效接口改名。
16. 把 `__internal.` 保留键中的结构化信息提升为 `ConnectorTableSchema` 的正式字段，
    三个 SQL 渲染键改为结构化描述 + fe-core 渲染。
17. 明确并写下 Thrift 中立性规则（api 与 spi 分别适用什么），修掉 `buildTableDescriptor` 的内联全限定名。

### 建议同时补充的一份文档

> **状态（复核 `53a753e0a1b`）：已落地**（`d572751af49`）。两个模块各写了一份包级说明。本节点的四件事里，能力声明的机制选择规则（规则一）、中立性红线、thrift 使用边界（规则三）都写了；"一个最小连接器需要实现哪几个方法"改用另一种更强的形态兑现——按域拆分之后**每个域接口各自写了自己的最少实现集**，并配了一个标注必须实现的注解。

以上很多问题的根源是**没有一份"新增连接器指南"**。
建议在 `fe-connector-api` 的 `package-info.java` 里写清楚四件事：

- 一个最小连接器需要实现哪几个方法（应该是很短的一个列表）；
- 能力声明的机制选择规则（见 D）；
- 什么可以进公共 api、什么必须留在插件里（中立性红线）；
- Thrift 类型的使用边界。

---

## 六、附录：核对方式说明

- "谁实现了某个方法"：对 8 个连接器模块的 `src/main` 做符号级检索后逐一确认。
- "引擎是否调用"：在 `fe-core/src/main` 做调用点检索，排除测试目录与 `target` 目录；
  对判定为"零调用"的 5 处，另做了一次全仓（排除 `target`）复核。
- "文档与实现是否一致"：对接口文档中出现"必须 / MUST / 引擎会……"的断言，逐条回到实现与调用点核对。
- 本文所有行号基于调研当日的工作区状态（分支 `catalog-spi-review-19`）。

本文未覆盖的部分（如需要可后续补充）：
下推表达式体系（`pushdown` 包 17 个类）、DDL 变更描述对象（`ddl` 包 12 个类）、
MVCC 数据结构（`mvcc` 包 5 个类）的内部设计；这三组主要是值对象，中立性上没有发现明显问题，
但 `ConnectorType`（7 个构造函数）和 `ConnectorPartitionInfo`（6 个构造函数）存在明显的构造函数堆叠，
建议改用构造器模式，这一点可以并入第四优先级一起处理。

> **就地重算**：原文这里写的是 `ConnectorPartitionInfo` 有 8 个构造函数，实测调研日与复核日都是 **6 个**（`ConnectorType` 的 7 个是对的）。两个数字在复核日均未变化，这条建议本身仍然成立、也仍然没有做。
