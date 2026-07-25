# 25. 修正同日另一份评审文档里的事实错误与结论

> **优先级**：收尾（随时可做，不阻塞任何代码任务） ｜ **风险**：低 ｜ **前置依赖**：无
> **影响模块**：不涉及任何 maven 模块。只改 `plan-doc/` 下的一份 markdown 文档，不动一行 Java。
> **预计改动规模**：1 个文件，约 35～45 行的就地修改（其中新增 3 行头部说明，其余是句子级替换）。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

同一天早些时候另一轮审查留下了 `plan-doc/connector-api-spi-design-review-2026-07-25.md`（689 行）。这份文档整体质量很高，但里面有 5 处经回到代码实测**不成立**的事实、3 条方向应当反转的结论；这些错误恰好落在「删什么 / 改什么名 / 把哪个常量搬到哪里」这类会被人照着动手的地方。本任务是把这 8 处就地改掉，并在文档头部加一句说明它与本任务空间的关系——不是用本轮的调研报告覆盖它。

## 二、背景：现在的代码是怎么写的

被修正的对象是文档而不是代码，所以这一节讲的是**那份文档写了什么**，以及**代码在 HEAD 上实际是什么样**。以下每一条都已在 `7ff51a106f0` 上用 grep / Read 核实过。

### 2.1 契约校验器到底有没有真实连接器在调用

那份文档第 529–531 行写：

> 实际情况：全仓唯一的调用点是 `fe-core/src/test/.../ConnectorContractValidatorTest.java`，它用的是**手写的假连接器**，8 个真实连接器没有任何一个调用过它。

实测不是这样。除了 fe-core 那个假连接器测试，还有 4 个连接器的契约测试在真的构建自己的连接器并调用它：

- `fe/fe-connector/fe-connector-es/src/test/java/org/apache/doris/connector/es/EsScanPlanProviderTest.java:332`
- `fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/IcebergConnectorTest.java:368`
- `fe/fe-connector/fe-connector-jdbc/src/test/java/org/apache/doris/connector/jdbc/JdbcDorisConnectorTest.java:187`
- `fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/MaxComputeConnectorContractTest.java:66`

再看校验器自己的 4 条规则（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorContractValidator.java:44`、`:49`、`:57`、`:65`）与谁声明了被检查的能力：

| 校验器里的规则 | 涉及的能力位 | 唯一声明它的连接器 | 有没有真实连接器的正样本 |
|---|---|---|---|
| 分支写必须同时支持普通 INSERT | `supportsWriteBranch` | iceberg（`IcebergWritePlanProvider.java:327`） | 有（iceberg 契约测试） |
| 分区本地排序必须同时要求并行写 + 全 schema 顺序 | `requiresPartitionLocalSort` | maxcompute（`MaxComputeWritePlanProvider.java:160`） | 有（maxcompute 契约测试第 63 行还专门断言了这一条） |
| 分区哈希写必须同时要求并行写 + 全 schema 顺序 | `requiresPartitionHashWrite` | hive（`HiveWritePlanProvider.java:139`） | **没有**（hive 没有契约测试调用点） |
| 两个分区分布模式互斥 | 上面两个一起 | 只有 hive 会踩到 | **没有** |

所以真实缺口很窄：**唯一声明分区哈希写的 hive 没有调用点**，因此涉及它的两条不变量缺一个真实连接器正样本。而校验器类注释（`:29-34`）说的「由各连接器的契约测试执行」是**部分已经实现**的机制，不是虚构的机制。

### 2.2 分片类型两个方法的实现者数量与默认值

那份文档第 211–212 行写「这两个方法没有默认实现或者默认值形同虚设，于是全部 7 个连接器都老老实实实现了 `getRangeType()`（es / hive / hudi / iceberg / jdbc / maxcompute / paimon）」，第 636–637 行的行动清单也写「顺带减少 7 个连接器的无效实现」。

实测：`getRangeType()` 是 8 个连接器都实现了，漏掉的是 trino——`fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoScanRange.java:79`。

而两个方法的默认值情况并不对称：

- `ConnectorScanRange.getRangeType()`（`fe-connector-api/.../scan/ConnectorScanRange.java:43`）是**接口抽象方法，没有默认实现**，所以 8 个分片类被迫逐个实现。
- `ConnectorScanPlanProvider.getScanRangeType()`（`.../scan/ConnectorScanPlanProvider.java:52-53`）**有默认值** `FILE_SCAN`，只有 hive / es / jdbc 三家做了与默认值等价的多余覆写。

### 2.3 写侧分区规格并不存在「空规格＝另一回事」

那份文档第 417 行写：`getWritePartitioning`「`null` = 未分区；空 spec = 另一回事」。

实测接口文档写得很清楚，而且给了理由（`fe-connector-api/.../write/ConnectorWritePlanProvider.java:107-109`）：

```
 * <p>{@code null} (not an empty spec) means the target is unpartitioned, mirroring the legacy
 * {@code spec().isPartitioned()} gate — the engine then falls back to its non-partitioned merge
 * distribution.
```

唯一的生产者是 iceberg（`IcebergWritePlanProvider.java:285`），未分区表返回 `null`（有测试 `IcebergWritePlanProviderTest.java:664` 钉住）；唯一的消费者是 `fe-core/.../PhysicalExternalRowLevelMergeSink.java:302`。全仓没有任何一处让「空规格」表示第三种含义。所以这是**虚构的第三态**——同一小节里另外两条（写排序列的 `null` vs 空 list、`hasConjunctTracking` 布尔位）是真的。

### 2.4 规模数字

| 那份文档写的 | 位置 | 实测 |
|---|---|---|
| `fe-connector-api` 约 9800 行 | 第 28 行 | 10149 行（95 个源文件这个数字是对的） |
| `Connector` 32 个方法 | 第 36 行 | 34 个 |
| `ConnectorSession` 14 个方法 | 第 39 行 | 15 个 |
| `ConnectorPartitionInfo` 8 个构造函数 | 第 688 行 | 6 个（同句里 `ConnectorType` 7 个构造函数是对的） |

### 2.5 三条要反转的结论

1. **推模型的失效接口（第 475 行的建议：给两组同名反向接口之一改名）**——实测 `ConnectorMetaInvalidator` 这套「连接器 → 引擎」的推模型是死的：连接器 `src/main` 里除了 iceberg / paimon 两个类加载器钉桩包装类的透明转发，没有任何生产调用；引擎侧实现 `fe-core/.../ExternalMetaCacheInvalidator.java` 自己在注释里写着按分区失效履约不了（`:61-68` 降级成整表失效）、统计失效是空操作（`:71-77`）。既然一整套要删（另有一份《删除推模型的缓存失效接口》任务负责删代码），名字冲突自然消失，不需要给活着的那组（`Connector.invalidate*`）改名去动 8 个连接器。
2. **分区空值哨兵（第 186 行、第 650 行的建议：`HIVE_DEFAULT_PARTITION` 下沉到 hive 连接器）**——实测不能下沉，三条硬约束：引擎自己的分区路径解析在用它（`fe-core/.../datasource/scan/FilePartitionUtils.java:143`）；非 hive 连接器 paimon 主动把空分区归一成这个串（`PaimonConnectorMetadata.java:1263`）；引擎侧已经存在第二份同串定义（`fe-core/.../datasource/TablePartitionValues.java:47`）。下沉只会变成三份，还会让 fe-core 反过来依赖 hive 插件。
3. **压缩类型调整方法（第 164 行把它列进「源专有语义混入」，第 650 行要求「文档去 Hadoop 化」）**——实测 `adjustFileCompressType`（`ConnectorScanPlanProvider.java:125`）的方法名与默认值本来就是中立的（默认恒等），javadoc 里提 Hadoop / LZ4 的那段（`:117-121`）**恰恰是在解释为什么必须有这个钩子**，末句原文就是 `This keeps that hadoop-specific fact inside the connector; the generic node stays connector-agnostic.`——它是「通用节点不得出现数据源专有代码」这条规则的正面案例，不是违规。把这段说明删掉，只会让下一个维护者不知道这个钩子干什么、进而把 LZ4 重映射写回通用节点。

## 三、为什么这是个问题

这份文档不是随笔，它是会被人当施工依据用的：它的第五节就是一份编号 1–17 的行动清单。清单里的条目一旦照着做，后果是实打实的：

- **会做重复劳动**：按「8 个连接器没有一个调用契约校验器」去给 8 个模块补契约测试，其中 4 个是已经存在的；而真正缺的那一个（hive）在原文里根本没被点出来，最可能被漏掉。
- **会写出编译不过的删除补丁**：按「7 个连接器」去删分片类型，漏掉 trino，`TrinoScanRange` 立刻编译失败。这类错误会被编译挡住，代价只是返工，但它会让人怀疑整份清单的可信度。
- **会改坏正在跑的功能**：把 `HIVE_DEFAULT_PARTITION` 下沉到 hive 连接器，引擎自己的分区路径解析和 paimon 的空分区归一化都会断，而且方向上让 fe-core 依赖插件——违反本阶段「fe-core 只出不进」的纪律。这一条是清单里唯一有真实破坏力的。
- **会删掉有价值的解释**：把压缩类型调整方法的 javadoc「去 Hadoop 化」，等于删掉唯一说明这个钩子为何存在的段落，下一个维护者很可能把 LZ4 重映射写回通用扫描节点——那才是真正的中立性违规。
- **会白花一次跨 8 个连接器的改名**：按「给同名反向的失效接口改名」去动活着的那一组，触及 8 个连接器加 fe-core；而正确做法是把死的那一整套删掉，名字冲突自然消失。

至于规模数字，本身不影响正确性，但它们出现在「现状概览」表里，是别人引用最多的部分；错的数字会一路传下去。

修文档而不是留个勘误在别处，是因为读那份文档的人不会同时读勘误。

## 四、用一个最小例子说明

| 那份文档怎么说 | 一个人照着动手会发生什么 | 实测事实 |
|---|---|---|
| 8 个连接器没有一个调用契约校验器，这 4 条规则今天完全没被验证 | 在 8 个连接器模块各加一个契约测试——其中 4 个是重复建设 | 4 个连接器已经在调，只差 hive 一个（它是唯一声明分区哈希写的连接器） |
| 全部 7 个连接器实现了 `getRangeType()` | 删除时漏掉 trino，编译直接失败在 `TrinoScanRange.java:79` | 是 8 个 |
| 写侧分区规格有「空规格＝另一回事」的三态 | 去改一个不存在的三态，或误以为返回空规格是合法的第二种语义 | 只有 `null` / 非空规格两态，接口文档写得很明确 |
| `HIVE_DEFAULT_PARTITION` 下沉到 hive 连接器 | fe-core 的分区路径解析与 paimon 的归一化都会断，fe-core 反向依赖插件 | 引擎和 paimon 都在用，引擎侧还有第二份同串定义 |
| `adjustFileCompressType` 的文档「去 Hadoop 化」 | 删掉唯一解释这个钩子为何存在的段落 | 那段说明本身就是「把数据源专有事实圈在连接器里」的正面示范 |

## 五、解决方案

### 5.1 目标状态

改完之后，`plan-doc/connector-api-spi-design-review-2026-07-25.md`：

1. 头部（现在第 6 行「本文只做分析和建议，不改动任何代码。」之后）多出一小段，大意是：同一天另有一轮独立重做的审查，结论与本任务空间的报告 `plan-doc/connector-public-interface-cleanup/audit-report.md` 及其 `tasks/` 目录并存；两份都保留，因为两轮独立结论的一致部分本身就是可信度最强的证据，分歧部分才是需要人拍板的地方；本文中经交叉核对修正过的地方均以「（交叉核对修正）」标注。
2. 5 处事实错误就地改成实测事实，涉及数字的直接换数字，涉及结论推导的把作废的推导一起删掉。
3. 3 条结论按 2.5 节反转，行动清单（第 625 行起那一节）里对应的条目同步改掉，避免正文改了、清单还写着旧结论。

**不新增小节、不改写它的分析框架、不把本轮报告的内容搬进去。**

### 5.2 改动清单

| 那份文档的位置 | 现在写的 | 改成 |
|---|---|---|
| 第 6 行之后 | — | 新增头部说明段（见 5.1 第 1 点） |
| 第 28 行 | 约 9800 行 | 10149 行 |
| 第 36 行 | `Connector` 32 | 34 |
| 第 39 行 | `ConnectorSession` 14 | 15 |
| 第 164 行 | 把 `adjustFileCompressType` 列为源专有语义混入 | 整行从该表删除，并在表后补一句：它的钩子形态与 javadoc 说明是正面示范，不算中立性违规（它仍然出现在第 551 行的 thrift 类型清单里，那一条另算，见 5.3） |
| 第 183–184 行 | 「`adjustFileCompressType` 把方法名和文档改成中立表述即可」 | 删掉这半句（方法名本来就中立）；`HIVE_DEFAULT_PARTITION` 那半句改成：不能下沉，理由见 2.5 第 2 条的三处引用 |
| 第 186 行 | `HIVE_DEFAULT_PARTITION`「应该由 hive 连接器持有」 | 改成保持在公共层，并保留「连接器可声明哪些值代表 NULL」这条中立能力的说法 |
| 第 211–212 行 | 两个方法都没有默认实现 / 7 个连接器 | 分片上那个是抽象方法（8 个连接器被迫实现，含 trino）；扫描计划提供者上那个有默认值 `FILE_SCAN`，只有 hive / es / jdbc 做了等价覆写 |
| 第 417 行 | 空 spec = 另一回事 | 删掉这一条（同小节另两条保留），并注明接口文档已明确 `null`（不是空规格）表示未分区且给了理由 |
| 第 475 行 | 建议给其中一组失效接口改名 | 改成：删掉推模型那一整套（零连接器调用、引擎侧履约不了分区粒度契约），名字冲突随之消失；并指向本任务空间里《删除推模型的缓存失效接口》那份任务文档 |
| 第 529–531 行 | 8 个真实连接器没有一个调用过 | 4 个连接器在调（列出 4 个文件与行号）；真实缺口＝唯一声明分区哈希写的 hive 没有调用点 |
| 第 532 行与第 536 行 | 「这 4 条规则今天完全没有被验证」「注释描述了并不存在的机制」（两句不相邻：前一句在 532 行，后一句在 536 行的建议段里） | 两句都作废：改成 4 条规则里两条有真实连接器正样本（iceberg / maxcompute），涉及分区哈希写的两条没有；注释描述的机制是部分已实现 |
| 第 589–591 行（`ConnectorContractValidator` 的执行方式与注释不符） | 指向上面那条错误结论 | 整条降级为「注释与实现基本一致，缺口只在 hive」，或直接删除该条并在原处留一行说明它被交叉核对推翻 |
| 第 636–637 行 | 「顺带减少 7 个连接器的无效实现」 | 8 个 |
| 第 642 行 | 让每个连接器的契约测试真正调用校验器 | 收窄为：给 hive 补一个契约测试调用点（其余 4 家已有；hudi / paimon / trino 不声明任何被检查的能力位，补了也只是恒真断言，可选） |
| 第 650 行 | 常量下沉 + 文档去 Hadoop 化 | 两半都撤销，替换为 2.5 第 2、3 条的结论 |
| 第 659 行 | 消除 `null` vs 空集合的三态编码 | 保留，但把写侧分区规格从这条的适用范围里去掉 |
| 第 688 行 | `ConnectorPartitionInfo` 8 个构造函数 | 6 个 |

改动时在每处修改后缀一个统一标记（例如「（交叉核对修正）」），让后来的读者能一眼看出哪些句子被改过、哪些是原稿。

### 5.3 明确不要顺手做的事

- **不要用 `audit-report.md` 覆盖或重写那份文档。** 保留两份的理由已经写在 5.1 的头部说明里。
- **不要顺手校对它其余的数字与结论。** 本任务只动经实测的这 8 处；未经核实的地方保持原样，比改成一个没人验过的新数字更安全。
- **不要顺手改 Java。** 删除分片类型枚举族、删除推模型失效接口、给 hive 补契约测试，各自是本任务空间里独立的代码任务（见第八节的文件名）。本任务改完之后 `git diff --stat` 应该只有一个 markdown 文件。
- **不要把 `adjustFileCompressType` 从第 551 行的 thrift 类型清单里删掉。** 它的入参是 `TFileCompressType`，这一条（公共接口签名里出现 thrift 类型）依然成立，与「不算中立性违规」的那条结论并不矛盾——一处是语义中立性，一处是类型依赖。
- **不要把本轮报告附录 C.3 那六条补记搬进那份文档。** 附录 C.3 是「那份文档独有、经核实成立、应当并入的六条」——分片格式默认值 `"jni"`、建库布尔位与建库方法必须同进同退、带快照与不带快照的重载堆叠、thrift 返回类型写成内联全限定名、「提供者无状态」与「释放跨调用读事务」自相矛盾、按名字寻址导致异构网关拿不到表注释。那些是本轮的内容，各有归属任务。

## 六、怎么验证

本任务不改 Java，所以**全反应堆 test-compile 不是本任务的验收信号**（如果有 Java 变更出现，说明范围越界了，应当退回）。验收靠两件事：改动后逐条重跑证据命令，和对原始错误字符串做「已消失」断言。

改完后在仓库根依次执行，逐条核对输出与文档里写的一致：

```bash
# 契约校验器的真实调用点：应有 4 个连接器测试文件
grep -rn "ConnectorContractValidator.validate" fe/fe-connector/*/src/test | sort
# 唯一声明分区哈希写的连接器：应只有 hive 的 main
grep -rn "boolean requiresPartitionHashWrite" fe/fe-connector/*/src/main
# 分片类型实现者：应有 8 个连接器（含 trino）
grep -rln "ConnectorScanRangeType" fe/fe-connector/*/src/main | sort
# 两个方法的默认值差异
grep -n "getRangeType" fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanRange.java
grep -n -A2 "getScanRangeType" fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanPlanProvider.java
# 写侧分区规格的两态契约
grep -n -B12 "getWritePartitioning" fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/write/ConnectorWritePlanProvider.java
# 规模数字
find fe/fe-connector/fe-connector-api/src/main/java -name "*.java" | wc -l
find fe/fe-connector/fe-connector-api/src/main/java -name "*.java" -exec cat {} + | wc -l
grep -c "public ConnectorPartitionInfo(" fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorPartitionInfo.java
# 分区空值哨兵不能下沉的三处证据
grep -rn "HIVE_DEFAULT_PARTITION" fe/fe-core/src/main/java/org/apache/doris/datasource/scan/FilePartitionUtils.java \
  fe/fe-core/src/main/java/org/apache/doris/datasource/TablePartitionValues.java \
  fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java
```

原始错误字符串必须全部消失（下面每条都应无输出）：

```bash
cd plan-doc && grep -n "8 个真实连接器没有任何一个" connector-api-spi-design-review-2026-07-25.md
grep -n "全部 7 个连接器" connector-api-spi-design-review-2026-07-25.md
grep -n "约 9800 行" connector-api-spi-design-review-2026-07-25.md
grep -n "空 spec = 另一回事" connector-api-spi-design-review-2026-07-25.md
grep -n "8 个构造函数" connector-api-spi-design-review-2026-07-25.md
```

另外人工确认两点：头部说明段存在且指向的相对路径能打开；文末行动清单里没有残留与正文相反的旧结论（第 636–660 行整段读一遍）。

不需要单元测试、不需要变异验证、不需要端到端回归——本任务不产生任何运行时行为。

## 七、风险与回退

风险低：单文件文档改动，回退就是 `git checkout -- plan-doc/connector-api-spi-design-review-2026-07-25.md` 或 revert 那一个提交。

唯一真实风险是**改过头**：一边改一边顺手把整份文档拉平成本轮报告的口径，那样就丢掉了「两轮独立结论」这个可信度证据，也丢掉了那份文档独有的、经核实成立的若干条（接口规模的精确计数、写特性按表缺口、thrift 出现位置清单）。缓解办法就是 5.3 的第一、二条：只动清单里那 8 处 + 头部，改动处统一加标记，提交前用 `git diff` 逐行过一遍。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md` 附录 C：本任务的全部素材来源。C.2 是这 5 处事实错误与 3 处结论改动的原始记录，C.5 是「两份都保留」的处置建议。
- 同一报告附录 C.1：两轮互有胜负的地方，动手前值得读一遍，避免把那份文档比本轮更准的部分也一起改掉。
- 同一报告附录 C.3：那份文档独有、经核实成立的六条，它们属于本轮报告与其它任务的范围，**不要**写回那份文档。
- 本任务空间 `tasks/13-delete-scan-range-type-enum.md`：分片类型枚举族的实际删除任务，本任务只是把「7 个」改成「8 个」，真正删代码在那里。
- 本任务空间 `tasks/14-delete-push-model-cache-invalidation.md`：推模型失效接口的实际删除任务，对应 2.5 第 1 条反转后的结论。
- 本任务空间 `tasks/08-fix-stale-interface-docs.md`：那份文档里「接口文档与实现互相矛盾」那一批的落地任务；其中关于契约校验器注释的那一条需要按本任务修正后的口径来做，不要再按「注释描述了不存在的机制」处理。
