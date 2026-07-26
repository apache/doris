# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🔥 构建命令（照抄，别用更早版本的写法）

旧交接文档里"一轮 4～6 分钟"的命令会让 maven 卡死一小时。根因已定位：checkstyle 的扫描目录取模块的**编译源根**，而 `fe-thrift` / `fe-sql-parser` / `fe-grpc` 把 `target/generated-sources` 加进了源根 → 生成代码被当成待审计源码，而抑制注释过滤器按违规条数重扫全文 → 退化成平方级。

**固定用这两条**（实测全反应堆 2 分 56 秒）：

```bash
# ① 全反应堆含测试源编译 + 跑指定测试（checkstyle 摘出去）
mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
  -pl '!fe-connector/fe-connector-hms-hive-shade,!fe-connector/fe-connector-paimon-hive-shade' \
  -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -T1C \
  test-compile test -Dtest='<类名清单>' -Dsurefire.failIfNoSpecifiedTests=false

# ② checkstyle 只对本次真正改动的模块单独跑（它们没有生成代码，秒过）
mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
  -pl <改动的模块清单> -Dmaven.build.cache.enabled=false checkstyle:check
```

`-pl` 缩到单模块对 `checkstyle:check` **安全**（纯源码审计）；对 `test-compile` 仍然不安全。

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：八批已合入（共 35 个提交）。正确性缺陷清零；死接口面删除全部完成；下推契约成文；扫描属性键契约集中；引擎里按数据源名判定的分支全部中立化（含本批最后两处）；能力声明形状问题已根治；公共模块里最后一个数据源品牌常量已中立化。**

已完成：07、08、10、11 / 12 / 13 / 14、15、09、01～06、两个真实用户可见缺口、21 与 16、17，**外加本批的 20 / 22 / 19（5 个提交）**。

**剩下的只有三项**：
- **25**（勘误清单，纯文档，随时可做，最省事的收尾）；
- **18**（建表能力下沉，**高风险**，需先拍板报错文案变化，且仓库里**根本不存在**分桶子句的正向端到端护栏——必须自己补一条）；
- **23**（引擎上下文里的存储服务拆分，**高危**，必须插件包重部署冒烟）。

建议顺序：**25 → 18 → 23**。

---

## 📌 本批落地后的事实变化

1. **公共模块里不再有任何数据源品牌的字符串常量。** `HIVE_DEFAULT_PARTITION` → `NULL_PARTITION_NAME`（**值一个字节没动**，`__HIVE_DEFAULT_PARTITION__` 是持久化标识），**没有保留过时别名**（它是编译期常量，树外连接器的字节码里早已内联，别名只对源码重编译有意义，却会永久留住那个品牌名）。
2. **三个"名字通用、语义只对目录名式分区成立"的归一方法已从公共模块删除**，内联进唯一使用者 hudi。hudi 现在有一个私有的 `HUDI_NULL_PARTITION_VALUE = "\N"`；hive / paimon / iceberg / fe-core 一律渲染空串。
3. **`fe-core` 里那份重复的哨兵定义已删**，FE 侧只剩一处定义。（**BE 侧另有两处 C++ 硬编码**，`vhive_utils.cpp` 与 `vhive_table_writer.cpp`，与 FE 无链接关系。）
4. **分布式过程的结果列不再在引擎里。** 引擎交出中立的 `ConnectorRewriteStatistics`（四个数字），连接器渲染 schema + 行。两处调用点（正常汇总 / 零分组早退）都走它。
5. **`Connector` 上的 `executeRestRequest` 已删**，换成 `getRestPassthrough()` 判空探针 + `ConnectorRestPassthrough` 可选能力接口。返回 null 的子系统取用方清单从 4 项变 5 项（**两处**枚举都改了：接口 javadoc + 包级说明规则一）。
6. **通用扫描节点里最后两段按格式名硬判的分支已删。** 引擎改发两个中立事实（`SYNTHETIC_PUSHDOWN_LIMIT` / `SYNTHETIC_ALL_CONJUNCTS_PUSHED`，都在 `ScanNodePropertyKeys`），ES 自己决定要不要请求提前停止。**没有新增第三个键**——BE 每批行数走已有的会话属性通道。
7. **剪枝已提前到 thrift 委派之前**，thrift 路径现在也传属性 map 的**副本**（与 EXPLAIN 路径一致）。
8. **`ScanNodePropertyKeys` 的"合成键只用于 EXPLAIN、永不发给 BE"那句不变量已改写**——现在两条委派路径都注入。
9. **上一批遗留在包级说明里的两处失效表述已修**：写特性镜像那段（描述的 11 个方法已被删除，与接口自身注释直接矛盾）、以及"只在扫描能力路径上读按表集合"那句。

---

## ⚠️ 做下一批之前必看

1. **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar。** 跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。（`checkstyle:check` 例外。）
2. **checkstyle 不再随 test-compile 跑，必须单独跑。** 方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`（第二个字符也必须小写）；**`CustomImportOrder` 会因为 import 顺序失败**（本批踩了两次：`ConnectorType` 必须排在 `DorisConnectorException` 前，`ConnectorRewriteGroup` 必须排在 `ConnectorRewriteStatistics` 前）；`UnusedImports` 是强制项且不对 connector 包豁免。
3. **【本批用户明确】变异验证只对高价值改动做。** 判据=改错了会**静默**产生错误结果、且新测试是唯一护栏。纯改名、删死接口面、javadoc、import 顺序**不做**（编译器已兜住，而一次变异要重跑全反应堆约 3 分钟）。
4. **一次跑多个变异时，maven 会在第一个失败的模块中止**，下游模块根本不会执行。要么加 `--fail-at-end`，要么分开跑；且仍要保证"一个变异对应一个测试类"以便归因。
5. **冻结基线只冻结 `ConnectorMetadata`**（`fe-connector-api/src/test/resources/connector-metadata-methods.txt`，75 行）。改 `Connector` 或 `ConnectorProcedureOps` **不需要**动它——本批再次实证。
6. **删除类改动必须配全仓符号 grep + 清空 `test-classes` 后重跑**（增量编译会跳过未改模块，陈旧 class 留到运行期才炸）。本批收尾清了 66 个 `test-classes` 目录，259 个测试全绿。
7. **编译器抓不到注释里的引用。** 本批删三个方法时，有 **5 处测试注释**指向即将消失的符号，全靠 grep 才发现。
8. **要证明"行为逐字不变"，就先把新测试写出来在旧实现上跑绿。** 本批 hudi 那条路径**原本零覆盖**，先绿后删让"等价"从声称变成测量。
9. **任务文档全部写于 21 号完成之前，行号漂移 40～70 行，且 19 号的核心机制已作废**（它教人在引擎里新增私有合成键常量 + 在连接器里抄一份同样的字面量，那正是 21 号刚删掉的模式）。**动手前必须按符号重侦察**，别信行号也别信机制描述。
10. **`git rm` 会立即入暂存区。** 拆提交时逐个 `git diff --cached --stat` 核对。
11. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉。** 一律 `> 日志文件 2>&1` 再 grep；`| tail` 之后的 `$?` 是 `tail` 的。
12. **纯 Mockito mock 上的新方法默认返回 null。** 本批给 `ConnectorProcedureOps` 加了一个方法，引擎侧**两个**测试立刻 NPE（不是断言失败），第三个测试"绿着但静默返回 null"。加 SPI 方法后必须查所有 mock 该接口的测试。

---

## 📍 这条线在解决什么（现状）

1. ~~「新增连接器不需要修改公共模块」在代码上是假的。~~ **建目录路径已兑现（15）；四处特性级软阻塞已中立化（16）；能力声明形状已根治（17）；扫描节点里最后两段源专有分支已归位（19）；分布式过程结果列已交还连接器（22）。** 残余只有**建表能力**（18 号）。
2. ~~新连接器作者无处可依。~~ **已解决**：两个模块各有包级说明，表操作接口按域拆分，谓词下推包有六节契约，扫描属性键契约有常量类且每个键写明方向与前置条件，能力枚举每条写明作用域与原因。
3. ~~接口文档与实现大面积脱节。~~ **已解决**（本批又修掉上一批遗留的两处）。

---

## 🧭 待用户拍板

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板二十二条**（本批新增四条）。

**本批拍板的四条**：
- 空分区哨兵**直接改中立名、不留过时别名**（理由见「事实变化」第 1 条）。
- ES 的 EXPLAIN 与实际下推判据不一致**逐字保留**，加 ATTN 注释 + 单独立项（修它需要 ES 集群验证，不该混进"行为不变的重构"）。
- REST 直通接口做成**通用路径透传**（原样搬迁），不做成两个 ES 具名方法（否则 ES 词汇进中立模块）。
- 那两个 HTTP 端点的既有安全面**记录并单独立项**，本批不动。

**仍待拍板（都在 18 号动手前）**：
- **含隐式类型转换的谓词下推默认值**（08 号）：`supportsCastPredicatePushdown` 默认 `true`，而它承诺的「引擎会先剥掉类型转换」只对残余谓词那条路径成立。翻成 `false` 是跨六个连接器的行为改动。
- **建表能力下沉的报错文案变化**（18 号）。

---

## 🧾 顺带发现、留给后续批次

**本批新增（按值得做的顺序）**：

- **那两个 ES 兼容 HTTP 端点存在既有安全面**（已拍板：单独立项）。`table` 请求参数**不做任何校验**就被拼进 ES 的 URL 路径；口令检查只在 `Config.enable_all_http_auth` 打开时才做；**全程没有目录级权限检查**。也就是说能访问 FE HTTP 端口的人可以借它向该目录的 ES 集群转发任意路径的请求。落点是端点侧的索引名合法性校验 + 权限检查，**不要**去改连接器。
- **EXPLAIN 与实际下推的判据不一致**（已拍板：逐字保留）。打印那半缺 `limit <= batch_size`，实际下推那半有；`batch_size` 小于 LIMIT 时用户看到一个并未生效的提示。两半现在都在 `EsScanPlanProvider` 里、相隔一个方法，已加 ATTN 注释。修它要同步改 `external_table_p2/es/test_es_query_predicate_correctness.groovy`。
- **hudi 的 `\N` 与其余连接器的空串渲染分歧**（已拍板：本批不统一）。现已证实 **BE 在空值位为 true 时根本不看那个字符串**（`partition_column_filler.h` 的 `fill_partition_column_from_path_value` 直接早返回，五个 reader 都喂同一个标志），所以统一是安全的、只是需要端到端确认。统一后 hudi 那个私有常量可整个删掉。
- **合成键机制有个既有的洞**：`nativeReadSplitNum` / `totalReadSplitNum` 只在 `getSplits` 里赋值，批模式的 `startSplit` / `startStreamingSplit` 从不赋值 → 批模式下 paimon 那行渲染成 `0/0`。**新加的两个键不受影响**（limit 与 conjuncts 在两条路径上都成立），但下次再加"引擎侧事实"要注意这一点。
- **`EsScanRange.getFileFormat()` 返回 `"es_http"` 是死代码**且与 SPI 契约矛盾（`ConnectorScanRange` 的 javadoc 写明 es 这类走 JNI 的应返回 `"jni"`）：`EsScanRange` 覆写了 `populateRangeParams` 且从不调默认实现，唯一消费者到不了。
- **`PluginDrivenScanNode.TABLE_FORMAT_TYPE = "plugin_driven"` 全仓零引用**，可删的死常量。
- **引擎仍在一处按字符串比较哨兵**（`MetadataGenerator` 渲染 `partition_values()` 表函数那一格），与 `PluginDrivenMvccExternalTable` javadoc 里写下的「fe-core 绝不按字符串比较哨兵」铁律直接抵触。本批只改了它引用哪个常量。真修法是从 `ConnectorPartitionInfo` 的空值标志取——**是行为变更，单独立项**。
- **`TablePartitionValues.toListPartitionItem` 里的哨兵比较实际不可达**（只有非 MVCC 基类路径会走到，而 jdbc/es/trino/maxcompute 都不渲染这个哨兵）。删掉它能让 fe-core 再净减一点，但对树外连接器是行为变更。

**沿用的**：

- **`ConnectorContractValidator` 在生产代码里零调用方**（只有 5 个测试类调 `validate`）。是留、是移进测试、还是接到真实入口，**单独立项**。
- **带时间旅行钉住的委派路径没有反射兄弟能力**（`HiveConnectorMetadata` 里带 MVCC 快照参数的 `getTableSchema` 重载直接转发给兄弟）→ 钉住快照读 iceberg-on-HMS 表时按表能力在丢。**单独立项**。
- **两个只写不读的属性键仍在**（paimon/hudi 的 `table_format_type`、es 的 `_table_name`、hive 的 `hive.text.json_serde_lib`）。判活需要单独核对 BE 与 JNI 侧。
- **hudi 的 `partition_values()` 可能落后一个缓存过期时间**（经 `listPartitions` 读缓存）。
- **es 有一条与 01/02/03 同根因、尚未修的缺陷**：`EsQueryDslBuilder` 把 Doris 的 `REGEXP` 模式串原样交给 ES 的 `regexp` 查询（Doris 部分匹配、Lucene 整串锚定）→ 少行。ES 侧有干净的 `notPushDownList` 拒绝机制可落点。
- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，可删的死类。
- **`ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个活方法**，且是安全相关的，做同名清理时极易误伤。
- **`CatalogFactory` 里 `lakesoul` 那条硬失败在重放期仍会让 FE 退出**（15 号刻意没动）。
- **`ConnectorScanRange.getLength()` 是「契约写了单位、实现各行其是」的第三处**（契约写字节数、maxcompute 在行偏移模式返回行数）。
- **`ConnectorSession.getStatementScope` 默认不记忆**，与 06 号根治的是同一机理的另一处。
- **早期计划文档里「HMS 事件管线通过连接器回调接口通知引擎」那条旧决策已被 14 号作废**；留给 25 号一并补作废说明。
- **两套残差协议仍未合并**（`remainingFilter` 与 not-pushed 下标）。前置是实现细粒度反查。
- **25 号的勘误清单还要补两条**（本批新发现的文档事实错误）：`ConnectorRewriteDriver` 类注释里引用的两个旧类名在树里**已不存在**；`ConnectorWritePlanProviderDefaultsTest` 说 `getWriteSortColumns` 默认返回空列表、实际返回 `null`。

---

## 🧪 欠下的端到端（本地无集群，一律标「待集群验证」，不得当作已通过）

**本批新欠 3 条**：

1. `external_table_p2/es/test_es_query_predicate_correctness.groovy:94-135` —— 六处 `ES terminate_after` 断言（ES 7 与 ES 8 各三处）。本批把这一行从 `pushdown agg=` **之后**移到了**之前**；现有断言都是 `contains`，仓库内也没有 ES 的整段 EXPLAIN golden 文件，所以应当仍过，但必须实跑确认。
2. `external_table_p2/es/test_es_catalog_http_open_api.groovy` —— 两个 REST 端点各 curl 一次（ES 5/6/7/8 四个目录）。这是 REST 能力接口那半的验收门。
3. iceberg `rewrite_data_files` 的**五个**套件（`test_iceberg_rewrite_data_files` / `_where_conditions` / `_parallelism` / `_expression_conversion` / `test_iceberg_v3_row_lineage_rewrite_data_files`）。注意：主套件其实**只取值不断言**，真正守住列顺序的只有 `_where_conditions` 那个；列名与列类型端到端**兜不住**，只有新增的连接器单测在守。

**沿用**：paimon 目录查询回归（`serialized_table` 缺字段是 BE 硬失败）、hive 文本/CSV/JSON 表读回归、文件缓存准入 + `SWITCH <es 目录>` + 事件同步预热、异构目录嵌套列 DDL 与 iceberg 表注释（26/27 号写好未跑）、异构 HMS 目录上的 `ANALYZE`/Top-N/嵌套列裁剪/`SHOW CREATE TABLE`、以及在一个连接器构建失败的插件目录上跑 `CREATE TABLE ... ORDER BY` 确认报「不支持排序」而不是 NPE。

---

## ⚙️ 其余构建与验证的坑（实测，直接复用）

1. **maven build cache 会静默跳过测试执行**：跑测试一律加 `-Dmaven.build.cache.enabled=false`。
2. **maven 一律用绝对路径 `-f`**；`cd` 会让后续相对路径失效（本 session 踩过）。
3. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
4. **严禁 `git add -A`**（工作树有大量历史遗留临时文件与含明文密钥的配置），一律 path-whitelist。
5. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**。
6. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**（建表路径），与本线改动无关。
7. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了——除非 `Mockito.mock(X.class, CALLS_REAL_METHODS)` + **`doReturn(...).when(x).getConnector()`**（必须用 `doReturn`）。
8. **hudi 单测里的 `stub(...)` 执行器会把整个 metaClient lambda 换掉**，别以为 `HudiConnectorPartitionListingTest` 绿了就等于分区列举全路径被覆盖。
9. **fe-core 测试里注私有字段用 `org.apache.doris.common.jmockit.Deencapsulation`（仓库自带）**，不是 `mockit.Deencapsulation`。
10. **`PluginDrivenScanNode` / `PluginDrivenExternalTable` 的单测范式**：`Mockito.mock(类.class, CALLS_REAL_METHODS)` + `Deencapsulation.setField(...)`，再 `Deencapsulation.invoke(...)`。纯静态辅助方法则直接断言、不用 mock（本批新增的注入辅助方法就是这个形态）。
11. **`fe-connector-es` 模块没有 mockito / jmockit / fe-core 依赖**，只有纯 JUnit 5；`TFileScanRangeParams` 来自 `provided` 作用域的 `fe-thrift`，在测试类路径上可直接构造（本批第一次这么用）。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批：07 + 08 + 10 | 全反应堆含测试源 `test-compile` 通过；79 个单测通过；冻结测试双变异验证均变红 |
| 2026-07-25 | 修掉调研期发现的两个用户可见缺口 | 单测通过；e2e 已写出但**未执行** |
| 2026-07-25 | 第二批：11 号五个提交 | `test-compile` 通过；83 个单测 + checkstyle 通过；两次变异验证 |
| 2026-07-25 | 第三批：15 号两个提交 | `test-compile` + checkstyle 通过；52 个单测通过；三次变异验证均如期变红 |
| 2026-07-25 | 第四批：01～06 六个正确性缺陷 | `test-compile` + checkstyle **BUILD SUCCESS**；27 个测试类全绿；8 个变异一轮跑完全部被捕获 |
| 2026-07-25 | 第五批：09 + 14 + 13 + 12 四个提交 | `test-compile` + checkstyle **BUILD SUCCESS**；八个模块全量单测 634 个全绿；4 个变异全部被捕获 |
| 2026-07-26 | 第六批：21 一个提交 + 16 三个提交 | 两轮 `test-compile` + checkstyle **BUILD SUCCESS**；33 个测试类全绿；5 个变异全部被捕获 |
| 2026-07-26 | 第七批：17 四个提交 | `test-compile` **BUILD SUCCESS**；四批合计 566 个测试全绿；1 个变异如期只红新增断言；**定位并绕过了让构建卡死 60+ 分钟的 checkstyle 退化** |
| 2026-07-26 | **第八批：20 两个提交 + 22 一个提交 + 19 两个提交**（哨兵下沉/中立改名 + 分布式过程结果列交还连接器 + REST 直通改能力接口 + 扫描节点 ES 分支归位） | 侦察阶段 9 个并行核查单元推翻了 19 号的**核心机制**（它写于 21 号完成之前）；收尾清空 66 个 `test-classes` 目录后，全反应堆含测试源 `test-compile` **BUILD SUCCESS**、27 个测试类 **259 个测试全绿**；checkstyle 全部改动模块 0 违规；被删符号全仓复扫为零；**4 个高价值变异全部如期变红**（其中两个实测推翻了任务文档写的变异口径）；新增 3 个测试类 + 15 个用例；新欠 3 条 e2e、新立 7 个待办项 |

### 本批订正的任务文档事实（任务文档已大面积过时，核对一律以符号名为准）

1. **19 号的核心机制作废**：合成键早已被 21 号集中进公共模块的键契约类、由两侧共享常量消费；文档教人「引擎新增三个私有常量 + 连接器抄一份字面量」，照做等于重造刚删掉的重复模式。
2. **19 号提议的第三个合成键（BE 每批行数）是多余的**：引擎早已把**所有可见会话变量**整体导出给连接器，`batch_size` 本来就在里面。
3. **19 号说那两个 REST 端点「既无单测也无回归用例」是错的**：有一个 p2 套件专门 curl 这两个端点并断言响应内容。
4. **20 号的变异口径有一条是错的**：删掉字面量 `\N` 那个判空分支后，**值断言仍然是绿的**，只有空值标志那一列能抓到它。文档按值断言写会得到一个抓不住该变异的假护栏（实测坐实）。
5. **20 号漏了会导致构建失败的 import 清理**：删方法后公共模块的三个 `java.util` import 悬空，且引擎两个测试文件的 import 也会悬空——`UnusedImports` 是强制项。
6. **20 号的测试表缺了唯一生产可达的边界**（分区目录名形如 `列名=` 时值是空串）；它列的 Java 空引用那一行反而生产不可达。
7. **22 号说端到端「兜住列顺序与取值」是不准确的**：主套件只取值**不断言**，真正断言四个下标的只有 `_where_conditions` 一个；列名与列类型端到端完全没有护栏。
8. **22 号漏了一个访问级别障碍**：连接器动作的取结果列方法是 `protected`，跨包的那个测试类断言不了它，只能放同包的动作测试里。
9. **三份文档的行号整体漂移 40～70 行。**

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
