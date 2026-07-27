# 08. 修正与实现矛盾的接口文档（一批）

> **优先级**：第二优先级（零风险，可与「把公共接口的书写规则写下来」那个任务同批提交） ｜ **风险**：低 ｜ **前置依赖**：无。但与「删掉没有调用方的接口面」那个任务有三处重叠，处理办法见 5.2 表格与 5.3
> **影响模块**：`fe-connector-api`（全部是 javadoc 文字改动）；可选一处 `fe-connector-hive` 的测试补充
> **预计改动规模**：9 个文件，60～90 行注释；除「待拍板的一条默认值」外零行为改动
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

公共接口上有九处 javadoc 描述的引擎行为，和引擎里真实发生的事情不一致（有的说「引擎会读这个值」而引擎从不读，有的说「引擎会帮你兜底」而兜底代码已经被删掉），另外三处 javadoc 里还留着只有我们内部看得懂的设计代号；这个任务把这些文字逐条改成实测事实，并把其中一条真实的安全隐患单独提出来请你拍板。

## 二、背景：现在的代码是怎么写的

九条逐一列出（每条都在 `7ff51a106f0` 上核实过）。

**（1）`ConnectorTableOps.listPartitionValues`**（`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorTableOps.java:493-498`，签名紧随其后的 `:499-501`）文档写「Used by the `partition_values()` TVF and by column-distinct-value optimizations」。实测 `fe-core/src/main` 下这个方法名零命中；`partition_values()` 表函数的真实路径是 `MetadataGenerator.java:2035` → `PluginDrivenExternalTable.getNameToPartitionValues`（`:882`）→ `metadata.listPartitions`（`:899`）。

**（2）`ConnectorScanRangeType`**（`.../api/scan/ConnectorScanRangeType.java:20-33`）文档写「Each type maps to a specific Thrift scan range variant」，`ConnectorScanRange.java:33-35` 与 `ConnectorScanPlanProvider.java:44-51` 也重复了同样的话。实测 `fe-core/src/main` 从不调 `getRangeType()` 或 `getScanRangeType()`；分片一律被包成 `PluginDrivenSplit extends FileSplit`（`PluginDrivenSplit.java:35-47`），真正区分格式的是 `ConnectorScanRange.populateRangeParams`（`ConnectorScanRange.java:182`）的多态覆写。

**（3）`ConnectorEventSource.getCurrentEventId`**（`.../api/event/ConnectorEventSource.java:44-49`）文档写「Used by the master to cheaply decide whether there is anything new to pull before calling `pollOnce`」。实测唯一命中是 hms 的实现（`HmsEventSource.java:58`）；引擎侧的元存储事件驱动只调 `pollOnce`（`MetastoreEventSyncDriver.java:164`）。

**（4）`ConnectorContractValidator`**（`.../api/ConnectorContractValidator.java:29-34`）文档写这些不变量「are enforced by the per-connector contract tests (which build each connector and call validate)」。实测只有四个连接器的测试在调：iceberg（`IcebergConnectorTest.java:368`）、es（`EsScanPlanProviderTest.java:332`）、maxcompute（`MaxComputeConnectorContractTest.java:66`）、jdbc（`JdbcDorisConnectorTest.java:187`）。而 `:57-62` 与 `:65-69` 那两条关于「按分区哈希写」的不变量，唯一声明该能力的连接器是 hive（`HiveWritePlanProvider.java:139`），hive 的测试里没有任何 `validate` 调用——真实连接器上没有正样本，只有 `fe-core` 里用假连接器构造的 `ConnectorContractValidatorTest` 覆盖。另外该类校验的是连接器级取得器（`connector.requiresPartitionHashWrite()`），而引擎写路径读的是按表重载（`Connector.java:167-171`，经 `PluginDrivenExternalTable.requirePartitionHashOnWrite`，`:378-390`）。

**（5）`ConnectorProcedureOps.getSupportedProcedures`**（`.../api/procedure/ConnectorProcedureOps.java:48-52`）文档写「used by the engine for routing, validation, and SHOW-style discovery」。实测：路由按表类型（`ExecuteActionFactory.java:57-59`）加执行模式（`ConnectorExecuteAction.java:142` 的 `getExecutionMode`）；未知过程名由连接器在 `execute` 内部拒绝，引擎侧的 `isSupported` 恒返回 true 并在注释里写明了这一点（`ConnectorExecuteAction.java:227-231`）；唯一读取点 `ExecuteActionFactory.getSupportedActions`（`:78-89`）自身零生产调用方，注释自陈是「no live caller today」的预留。

**（6）`ConnectorPartitionInfo.orderedPartitionValues`**（`.../api/ConnectorPartitionInfo.java:52-61`）文档写「Empty means "not supplied": fe-core then falls back to parsing partitionName itself (unchanged behavior)」。实测 `fe-core` 侧的名字解析兜底已经删除，`PluginDrivenMvccExternalTable.java:328-332` 写着「There is no name-parsing fallback anymore」，并用 `Preconditions.checkState(partitionValues.size() == types.size(), ...)` 做元数硬校验；这个抛出被调用方的 try/catch 接住（`:271-302`），后果是该分区被跳过、整表退化成「无分区」。

**（7）`ConnectorMvccSnapshot`**（`.../api/mvcc/ConnectorMvccSnapshot.java:25-32` 与 `:77`）类文档写「serialized into BE scan ranges so the read path sees a consistent version」，`getProperties()` 文档写「Connector-specific metadata propagated to BE」。实测这个类连 `Serializable` 都没实现（`:34`）；引擎不把它放进任何分片，`properties` 的唯一消费者是连接器自己的 `applySnapshot`（paimon `PaimonConnectorMetadata.java:757-769`、iceberg `:2092`、hudi `:594/:645`），这条契约写在 `ConnectorMetadata.java:151-160`。

**（8）`ConnectorMetadata.getSyntheticScanPredicates`**（`.../api/ConnectorMetadata.java:169-185`）文档写「the engine NEVER discriminates by connector here; it applies whatever the connector returns」。前半句是真的，后半句不成立：反向转换器 `ConnectorExpressionToNereidsConverter` 只接受 `ConnectorAnd`、五种比较（EQ/LT/LE/GT/GE，`:100-114`）、能按名绑定到扫描输出列的列引用（`:124-135`）和 STRING 字面量（`:144-155`），其余一律抛 `AnalysisException`（类注释 `:53-59` 明确说这是有意的 fail loud）。

**（9）`ConnectorPushdownOps.supportsCastPredicatePushdown`**（`.../api/ConnectorPushdownOps.java:60-74`）文档写「When this returns false, the engine will strip any conjuncts containing CAST expressions from the filter before passing it to the connector」。实测这个剥离只发生在残余谓词那条路上（`PluginDrivenScanNode.buildRemainingFilter`，`:2053-2079`）；`applyFilter` 那条路上引擎直接把全部 conjuncts 转换后交给连接器（`convertPredicate`，`:874-878` 调 `buildFilterConstraint`），不查这个能力位。而正向转换器遇到 CAST 是**直接把外壳拆掉、只推里面的子表达式**（`ExprToConnectorExpressionConverter.java:108-109`：`return convert(expr.getChild(0));`），连接器看到的是一个「看起来没有类型转换」的谓词，没有任何标记可供自查。今天覆写这个方法的是 paimon（恒 false）、maxcompute（恒 false）、jdbc（按会话开关）；实现了 `applyFilter` 的三个连接器 hive、hudi、trino 全部继承默认值 `true`。

**（10）内部代号**：`Connector.java:217` 写着「Design S8: storage-property derivation is owned by the connector」；`handle/ConnectorTransaction.java:79` 与 `pushdown/ConnectorPredicate.java:24` 各写着一个「(O5-2)」。另有六处裸工单号 `#65329`（`ConnectorColumn.java:61/147/217/222`、`ConnectorType.java:81`、`ConnectorTableOps.java:326`）。

## 三、为什么这是个问题

公共接口的 javadoc 是新连接器作者唯一的行为契约来源——他不会去读 `fe-core`，读不了也不该读（连接器是独立打包、独立类加载器的插件）。文档说错引擎行为，代价有三种，且都已经在这九条里出现：

1. **照文档写就是写错。** 第 6 条最典型：文档承诺「不填分区值，fe-core 会自己解析分区名」，照此实现的新连接器会得到「所有分区被跳过 → 表被当成无分区表 → 分区裁剪全丢、`EXPLAIN` 显示 `partition=0/0`」。这是用户能观察到的性能塌陷，而且不报错、不告警，只在日志里留一条 warn。
2. **排查成本白烧。** 第 2、3、5 条都是「文档说引擎会读，引擎不读」。作者按文档设置了值、发现不生效，就会去翻引擎，翻不到东西，最后只能靠读全仓才敢下结论。
3. **文档把一个真实隐患描述成了已经解决的问题。** 第 9 条最危险：文档让人相信「返回 false 就安全了」，而实际 `applyFilter` 路径完全不看这个位。hive 的 `applyFilter` 会用等值谓词直接裁掉元存储分区（`HiveConnectorMetadata.java:1085-1116`），拿到的又是被拆掉类型转换外壳的谓词——一旦源侧的比较语义与 Doris 的强转语义不一致，就是**多裁分区、少返回行，而且 BE 复算补不回来**（分区已经不在扫描范围里）。这一条的行为后果是代码路径推断，未跑端到端验证，但机制是确证的。

内部代号那几处是另一类问题：这些文件最终会随 PR 进上游社区，`Design S8` / `O5-2` 对仓库外的读者是纯噪音，而且它替换掉了本该写在那里的技术理由。

## 四、用一个最小例子说明

拿第 9 条（隐式类型转换那条）举例。假设一张 hive 分区表，分区列 `dt` 是 `STRING` 类型：

```sql
CREATE TABLE hive_catalog.db.t (id INT, v STRING) PARTITIONED BY (dt STRING);
SELECT * FROM hive_catalog.db.t WHERE dt = 20240101;   -- 注意右边是数字，不是字符串
```

| 环节 | 文档让人以为会发生 | 实际发生 | 应该被文档写成 |
|---|---|---|---|
| Doris 分析谓词 | —— | 变成 `CAST(dt AS INT) = 20240101` | 同 |
| 引擎要不要剥掉这个含转换的谓词 | 「hive 没声明支持转换下推 → 引擎会剥」 | hive 继承了默认值 `true`（没声明过），而且 `applyFilter` 这条路根本不查这个位 | 只有残余谓词那条路才会剥；`applyFilter` 从不剥 |
| 连接器收到什么 | 收不到这个谓词 | 收到 `dt = 20240101`（转换外壳被正向转换器拆掉了） | 连接器收到的比较可能已被拆壳，且无从自查 |
| 后果 | 无 | hive 用它去裁元存储分区；若源侧的字符串/数字比较语义与 Doris 不同，就少扫分区、少返回行 | 明确写出这个风险由连接器承担 |

「新人照文档写」的另一面，用第 6 条一句话说明：新连接器实现 `listPartitions` 时，按文档「值可以不填，fe-core 会解析名字」只填 `partitionName` —— 今天的真实结果是这张表所有分区都被静默跳过，表变成「无分区」。

## 五、解决方案

### 5.1 目标状态

除下面这一条待拍板项外，**全部是 javadoc 文字改动，零签名改动、零行为改动**。

待拍板项：`ConnectorPushdownOps.supportsCastPredicatePushdown` 的默认值。

```java
// 现状（ConnectorPushdownOps.java:72-74）
default boolean supportsCastPredicatePushdown(ConnectorSession session) {
    return true;
}

// 选项 A（调研报告的建议，安全侧）：默认 false，能正确处理拆壳后比较语义的连接器显式声明 true
default boolean supportsCastPredicatePushdown(ConnectorSession session) {
    return false;
}
```

三个选项，请你选一个：

- **选项 A：默认翻成 false。** 与本仓库既有纪律（能力用 `supportsXxx()` 默认 false 的 opt-in 声明）一致。**新发现的代价**：今天 iceberg、hive、hudi、es、trino 五个连接器全都继承 `true`（hive 网关下挂的 iceberg-on-HMS 等异构表同样受影响），翻转会让它们在残余谓词那条路上失去含类型转换谓词的下推（正确性上更安全，但可能变慢）；要恢复就得逐个连接器判断「拆壳后的比较语义与远端是否一致」，而连接器目前无从自查，这个判断没有客观依据。所以选 A 意味着一次跨五个连接器的行为改动，不适合和文档批一起走。
- **选项 B（推荐）：本批只改文档，默认值不动。** 把两条路径的差异、正向转换器拆壳的事实、以及「风险由连接器承担」写进 javadoc，保持这个批次零行为改动；默认值翻转与逐连接器判断另开一项，配端到端回归。
- **选项 C：彻底修。** 引擎在拆壳处给该比较打标记，连接器能自查后再决定默认值。这需要在公共接口上加表达能力，超出本任务范围，只作为远期方向记录。

### 5.2 改动清单

| 文件（均在 `fe-connector-api` 下） | 位置 | 做什么 |
|---|---|---|
| `api/ConnectorContractValidator.java` | `:29-34` | 把「由每个连接器的契约测试强制」改成实测口径：今天调用它的是 iceberg / es / maxcompute / jdbc 四个连接器的测试；并补一句「按分区哈希写的两条不变量在真实连接器上没有正样本（唯一声明该能力的 hive 没调），只有 `fe-core` 的假连接器测试覆盖」；再补一句「本类校验连接器级取得器，引擎写路径读的是按表重载 `Connector.requiresPartitionHashWrite(handle)`」 |
| `api/procedure/ConnectorProcedureOps.java` | `:48-52` | 删掉「引擎用于 routing、validation」；改成：路由按表类型加执行模式，未知名由连接器在 `execute` 内拒绝；本方法唯一读取点是为 `SHOW` 类发现预留、今天无生产调用方 |
| `api/ConnectorPartitionInfo.java` | `:52-61` | 删掉「fe-core 回退解析分区名」；改成：走 MVCC 分区项路径的连接器**必须**提供该列表，留空会让 `fe-core` 的元数校验失败、该分区被跳过、整表退化成无分区（丢分区裁剪）。**同一文件 `:41-49` 关于 NULL 标记「留空=全部非 null」的描述是正确的，不要顺手改** |
| `api/mvcc/ConnectorMvccSnapshot.java` | `:25-32`、`:77` | 删掉「serialized into BE scan ranges」和「propagated to BE」；改成：引擎只在 FE 内把它当查询期 MVCC 钉子传递；`properties` 由连接器自己在 `applySnapshot` 里织进表句柄（契约见 `ConnectorMetadata.applySnapshot`），是否传到 BE 完全由连接器的扫描计划提供者决定 |
| `api/ConnectorMetadata.java` | `:177-179` | 保留「引擎不按连接器区分」（这句是对的）；把「applies whatever the connector returns」改成明确的语法边界：只支持「与」+ EQ/LT/LE/GT/GE + 可按名绑定到扫描输出列的列引用 + STRING 字面量，超出即抛异常（有意 fail loud），并指向反向转换器 |
| `api/ConnectorPushdownOps.java` | `:60-74` | 按 5.1 选定的选项改。选项 B 时：写清只有残余谓词路径会剥、`applyFilter` 路径不剥、正向转换器遇 CAST 直接拆壳因而连接器无从自查、默认 `true` 意味着风险由连接器承担 |
| `api/Connector.java` | `:217` | 删掉 `Design S8:` 这个代号，保留其后的技术论述（「存储属性派生归连接器所有，fe-core 不解析元存储属性」本身就是完整理由） |
| `api/handle/ConnectorTransaction.java` | `:79` | 删掉 `(O5-2)` |
| `api/pushdown/ConnectorPredicate.java` | `:24` | 删掉 `(O5-2)` |
| 六处 `#65329`（`api/ConnectorColumn.java:61/147/217/222`、`api/ConnectorType.java:81`、`api/ConnectorTableOps.java:326`） | 同左 | 裸工单号对仓库外读者不可解。改成行为描述（「省略 COMMENT 表示保留原注释、显式空串表示清空」），确实需要留追溯线索时写全 `apache/doris#65329` |

**与「删掉没有调用方的接口面」那个任务重叠的三行**，本任务默认**不动**，因为它们整体是删除对象，改了也会被删掉：

| 符号 | 处置 |
|---|---|
| `ConnectorTableOps.listPartitionValues` 的文档 | 该方法在删除任务里连三个连接器实现一起删。若你决定保留它，则改文档为：零生产调用方，`partition_values()` 表函数实际走 `listPartitions` |
| `ConnectorScanRangeType` / `ConnectorScanRange.getRangeType` / `ConnectorScanPlanProvider.getScanRangeType` 的文档 | 三者在删除任务里整体删。若决定保留，则改文档为：引擎从不读它，格式差异由 `populateRangeParams` 的多态覆写决定 |
| `ConnectorEventSource.getCurrentEventId` 的文档 | 该方法在删除任务里删。若决定保留，则改文档为：零调用方，游标完全由 `pollOnce` 的结果驱动 |

**一项可选的测试补充**（零行为改动，但不是文档）：试着在 hive 已有的、已经构造过连接器的测试里（例如 `fe-connector-hive/src/test/.../HiveConnectorCapabilitiesTest.java`）加一行 `ConnectorContractValidator.validate(connector, "hive")`，让「按分区哈希写」的两条不变量在真实连接器上有正样本。注意 hive 的 `getWritePlanProvider()` 会走 `getOrCreateClient()`（`HiveConnector.java:256-258`），如果在无元存储的单测环境里构造不出来，**不要硬凑**——改为在 `ConnectorContractValidator` 的 javadoc 里如实记下这个缺口即可。

### 5.3 明确不要顺手做的事

- **不要顺手删接口。** 这九条里有三条的符号是另一个任务的删除对象；本任务不承担删除，也不承担「先标过时」。
- **不要顺手改 `supportsCastPredicatePushdown` 的默认值**，除非你拿到的答复是选项 A；即使是 A，也要作为独立提交、配逐连接器判断与回归，不要混进文档批。
- **不要顺手修 `ConnectorPartitionInfo` 里那条 NULL 标记的文档**（`:41-49`）——它与 `PluginDrivenMvccExternalTable.java:334-336` 的校验一致，是对的。
- **不要顺手改 `ConnectorMetadata.getSyntheticScanPredicates` 的引擎侧行为**去支持更多表达式节点。反向转换器有意 fail loud，扩语法是另一件事。
- **不要为「文档与实现是否一致」写 shell 或正则门禁。** 本仓库已有结论：这类门禁只适合存在性与前缀类不变量，理解语言语义的活轮不到它，误报比漏报更毒。文档一致性靠评审。
- **不要在 `fe-core` 里补代码去迎合旧文档**（例如把删掉的分区名解析兜底加回来）。当前阶段 `fe-core` 只出不进，正确做法是改文档、让连接器提供数据。
- **不要顺手统一异常分层、命名或重载堆叠**，那些是调研报告里另外的条目。

## 六、怎么验证

1. **编译门禁（最强单一信号）**：全反应堆含测试源编译，禁用跳过测试编译的参数。
   `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile`（不要加 `-Dmaven.test.skip=true`）。纯 javadoc 改动本身不会破坏编译，这一步的作用是抓「顺手改了签名却没改调用方」以及 `{@link}` 指向不存在符号导致的 javadoc/checkstyle 失败。
2. **代码风格**：javadoc 行长受 checkstyle 限制，改完跑 FE 的 checkstyle（`fe-code-style` 那套），确认零新增告警。
3. **`{@link}` 有效性自查**：本次新增的交叉引用（`ConnectorMetadata.applySnapshot`、`Connector.requiresPartitionHashWrite(ConnectorTableHandle)`、反向转换器类名）逐个 grep 确认符号存在且可见性允许被链接（`ConnectorExpressionToNereidsConverter` 在 `fe-core` 里，连接器模块看不到它，**只能以文字提及，不能写成 `{@link}`**）。
4. **单元测试**：选项 B（只改文档）时无需新增测试，也不需要变异验证——没有可变异的行为。若采纳选项 A，则至少要改 `JdbcConnectorMetadataTest.testDefaultPushdownOps_alwaysTrue`（`:211-216` 现在断言默认为 `true`），并为 hive / hudi / trino 各补一条「显式声明」的断言；跑测试必须禁用 maven build cache，否则 surefire 会被静默跳过而 `BUILD SUCCESS` 是空的。
5. **端到端回归**：选项 B 不需要。选项 A 需要，且必须覆盖 hive 分区表上含隐式类型转换的等值谓词（分区裁剪结果与行数）。
6. **代号清理的收尾检查**：在两个公共模块的 `src/main` 下重跑一次代号 grep，确认零命中：
   `grep -rnE "Design [A-Z][0-9]+|\([A-Z][0-9]+-[0-9]+\)" fe/fe-connector/fe-connector-api/src/main fe/fe-connector/fe-connector-spi/src/main`

## 七、风险与回退

- 文档改动的风险只有一种：**把新的错话写进去**。对策是这份文档里每条断言都带了实测位置，动手时逐条重新 grep 一遍（以符号名为准，不要相信行号），改完请一个没参与的人对着代码复核一遍文字。
- 与删除任务的重叠已在 5.2 隔离。若删除任务先落地，本任务对应三行自然作废；若本任务先落地又不小心改了那三处，删除任务会把它们连注释一起删掉，不产生冲突，只是白做。
- 回退成本为零：全部是注释，`git revert` 即可，无持久化格式、无有线格式、无类型标签涉及。
- 唯一的真实风险来自选项 A（翻默认值）：它会让五个连接器在残余谓词路径上少推谓词，属于「更安全但可能更慢」，且一旦为了恢复性能给某个连接器显式声明 `true`，就等于把一个无从自查的正确性风险重新打开。因此建议单独走，不与本批混提。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md` 第 11.2 节：本任务的对照表来源（九行「文档说的 vs 实际」）。
- 同一报告第十一节开头四条「有用户可见后果的缺陷」：其中隐式类型转换那条与本任务的待拍板项同源，但归责在连接器侧，另开修复。
- 同一报告主题四（「没有调用方或没有实现方的接口面」）的 7.2、7.3 两张删除表：`listPartitionValues`、`ConnectorScanRangeType` 一族、`getCurrentEventId` 都在其中，对应本文 5.2 的重叠三行。
- 同一报告第十节（「接口契约写得不完整」）：`listFileSizes`、`resolveTimeTravel` 两条是「实现对、文档错」，处置方式与本任务同类，但属于异常契约那一批，不在本任务范围。
- 同一报告第十四节（「被推翻或收窄的说法」）：动手前值得看一眼，避免把有理由的设计当成缺陷改坏。
- 本任务应与「把公共接口的书写规则写下来」那个任务同批提交：那份规则里应当包含「javadoc 描述引擎行为时必须给出可核实的引擎侧位置」这一条，本任务是它的第一次应用。

---

## 九、施工后订正与落地口径（2026-07-25）

**拍板结果：选项 B**——本批只改文档，`supportsCastPredicatePushdown` 的默认值不动；翻转另开一项，需配逐连接器判断与端到端回归。

复核发现的订正（**以本节为准**）：

1. **第（2）条要收窄：`getRangeType()` 不是死的。** fe-core 确实从不读它，但 `fe-connector-api` 自己的 `ConnectorScanRange.populateRangeParams` **默认实现**读它，并把 `connector_scan_range_type=<NAME>` 写进 `TTableFormatFileDesc` 的 jdbc 参数——这是 BE 可见的字符串，而 jdbc 是唯一不覆写 `populateRangeParams` 的连接器，所以这条默认路径是活的。**「引擎从不读」这句话必须限定为「fe-core 从不读」，并且删除分片类型枚举族的那个任务要按「会改变 jdbc 发给 BE 的内容」处理**（已在那份任务文档里加了警示）。本批未改这三处文档（按原约定留给删除批次）。
2. **第（9）条的风险面比文档写的宽**：不只 hive，hudi 也用同样的方式从等值/`IN` 谓词裁分区（trino 则把约束转交给 trino 自己的元数据），所以「拆壳后比较语义不一致 → 少扫分区」的风险对这三个连接器都成立。已按此写进 javadoc。同时 hive 的裁剪不只吃等值，还吃未取反的 `IN` 列表。
3. **第（7）条的读取方多一处**：`properties` 除了三个连接器的 `applySnapshot`，还有 hudi 的 `getSyntheticScanPredicates` 会读；fe-core 全程不读。已按此写。
4. **第（10）条的代号清理已做完**：`Design S8` 1 处、`(O5-2)` 2 处、裸工单号 6 处，全部改成行为描述；两个公共模块 `src/main` 的代号正则复扫为零命中。
5. **可选的 hive 契约测试正样本没做**：按 §5.2 末尾的处置，未硬凑；`ConnectorContractValidator` 的类文档已如实记下这个缺口（两条按分区哈希写的不变量在真实连接器上无正样本）。
