# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🔥 先看这一条：构建命令变了，照抄旧的会卡死一小时

**交接文档一直写的「一轮 4～6 分钟」现在不成立。** 这一轮实测：按旧命令跑，maven 跑了 **61 分钟一个模块都没走完**，两个构建线程各烧掉 53 分钟 CPU。

**根因**（已定位到栈，与任何人的改动无关）：checkstyle 的扫描目录取的是模块的**编译源根**，而 `fe-thrift` / `fe-sql-parser` / `fe-grpc` 把 `target/generated-sources` 加进了源根 → **生成代码被当成待审计源码**。这些生成文件违规数以万计，而 checkstyle 的抑制注释过滤器是**按违规条数重扫全文**的，于是退化成平方级。手写代码违规为 0，永远不触发这条路径。
**为什么以前没事**：`fe-thrift/target` 里的生成代码是 2026-07-26 00:01 才被某次构建生成出来的；在那之前这个工作树里根本没有生成源码可扫。

**从现在起用这两条命令**（实测全反应堆 **2 分 56 秒**）：

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

`-pl` 缩到单模块对 `checkstyle:check` 是**安全**的（纯源码审计，不解析兄弟 jar）；对 `test-compile` 仍然不安全。

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：七批已合入（共 30 个提交）。正确性缺陷清零；死接口面删除全部完成；下推契约成文；扫描属性键契约集中；引擎里按数据源名判定的四处分支全部中立化；能力声明的两处形状问题（按表能力字符串化、写特性镜像）已根治。**

已完成：07、08、10、11 / 12 / 13 / 14、15、09、01～06、两个真实用户可见缺口、21 与 16，**外加本批的 17（4 个提交）**。

**建议的下一步**：按 README 第三节继续 **19 / 20 / 22**（中立化与结构整理）。它们互不耦合，可任选其一起步。之后是 18（高风险，需先拍板报错文案）与 23（高危，需插件包重部署冒烟）。

---

## 📌 本批落地后的事实变化（更新你对这条线的认知）

1. **按表能力不再是字符串。** `ConnectorTableSchema` 多了一个 `Set<ConnectorCapability> tableCapabilities` 字段（配 5 参构造器 + `getTableCapabilities()`），保留控制键 `PER_TABLE_CAPABILITIES_KEY` 已删除，`RESERVED_CONTROL_KEYS` 从 6 个变成 **5 个**。载体是 `PluginDrivenSchemaCacheValue`（同样加了一个字段，进程内缓存、无 Gson、不持久化）。
2. **`ConnectorCapability` 的 13 个常量现在每一个都写明了作用域**（按目录 / 按目录∪按表），且每个「只能按目录」的都写了**原因**。这是这条线里最有价值的一份文字资产——以前「哪 5 个能按表细化」只能靠反查引擎私有方法的调用方才能知道。
3. **引擎的按表能力解析入口改名 `hasScanCapability` → `hasCapability`**（"scan" 早就不成立：它服务 ALTER TABLE 嵌套列演进和两项统计信息采集）。
4. **hive 的兄弟反射收窄到显式子集** `SIBLING_INHERITABLE_CAPABILITIES`（5 项），方法名去掉 "Scan"。行为不变（多发的本来就被丢弃），但意图从「靠引擎读得窄兜着」变成写在代码里。
5. **`PluginDrivenExternalCatalog` 多了 `hasConnectorCapability(cap)`**，替掉 4 处目录外部的重复判断，顺带修掉建表分析期那处**缺 null 检查的 NPE**。**目录内部两处仍读裸字段**（读会话构建路径与列表路径，强制初始化是错的），且已加注释说明。
6. **`Connector` 上 11 个写特性镜像方法已全部删除，零新增**（用户拍板：不造工具类）。引擎/校验器改为取一次提供者再解包；按表的用 `Optional.map` 链（`map` 遇 null 自动变空，空安全白给）。
7. **契约校验器从取 8 次提供者变成取 1 次**，并对无写连接器早返回。iceberg 每次取提供者都会真连远端目录，所以这是真实收益（但只影响测试路径，见下）。

---

## ⚠️ 做下一批之前必看（前十条沿用，13、14 是本批新增的硬教训）

1. **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar。** 跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。（`checkstyle:check` 例外，见开头。）
2. **checkstyle 的方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`——第二个字符也必须小写。** 本批又踩了一次（`aConnectorWithout...` 被拒）。**且现在 checkstyle 不再随 test-compile 跑了，必须单独跑，否则这类问题要到 CI 才暴露。**
3. **冻结基线只冻结 `ConnectorMetadata`**（`fe-connector-api/src/test/resources/connector-metadata-methods.txt`，现 **75 行**）。删 `Connector` 上的方法**不需要**动它——本批已实证。
4. **「全反应堆 test-compile 能一次证明引用全清」是错的**，而且比想象的更错：
   - 它对 javadoc `{@link}` 引用结构性失明（旧结论）；
   - **它对增量编译跳过的模块也失明**（本批新证）。见第 13 条。
5. **`git rm` 会立即入暂存区。** 拆提交时逐个 `git diff --cached --stat` 核对。
6. **任务文档里的「不需要变异验证」不要照信。** 连续四批的变异全部如期变红。
7. **变异验证可以一次跑完**，但要保证「一个变异对应一个测试类」，靠「失败的测试类互不重叠」做归因。
8. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉。** 一律 `> 日志文件 2>&1` 再 grep。
9. **从冻结基线失败信息里拷方法清单时，最后一行会粘上断言后缀**，拷完检查最后一行以 `)` 结尾。
10. **删接口方法后要顺手查「现在没人用的 import 和字段」**（编译不报 unused private field，但 checkstyle 报 unused import）。本批清掉 4 个。
11. **一个提交里两半改动交织时，不要硬拆。** 本批 4 个提交都是天然可分的（收窄 / 类型化 / 目录访问器 / 删镜像），互不交织，所以拆得干净。
12. **`Env.java` 里 `catalogIf` 是裸类型 `CatalogIf`**，直接传给带泛型的方法会让整个调用变 unchecked。先赋给 `Map<String, String>` 局部变量再传。
13. **【新】删除类改动必须配全仓符号 grep，编译不算数。** 本批删 11 个方法后全反应堆 `test-compile` **BUILD SUCCESS**，但 iceberg 与 hudi 两个测试类仍在调已删方法——maven 因为**这两个模块自身源码没变**而跳过了重编译，陈旧 class 一直留到运行期才炸成 `NoSuchMethodError`。
    **对策**：删除批次收尾时 ①全仓 grep 被删符号（含方法引用 `::` 形式）②`find fe -type d -name test-classes -path '*/target/*' | xargs rm -rf` 后再跑一次全量验证。
14. **【新】纯 Mockito mock 上的新方法默认返回 false/null，会让测试「绿着但没测」或直接挂。** 本批给目录加了一个访问器，4 个用 `Mockito.mock(PluginDrivenExternalCatalog.class)` 的测试里有 2 个立刻变红。
    **两种正确处理**（按被测对象选，别一律 stub）：
    - 访问器本身属于被测范围 → `Mockito.mock(X.class, Mockito.CALLS_REAL_METHODS)` + **`doReturn(...).when(x).getConnector()`**（必须用 `doReturn`，`when(x.getConnector())` 会在打桩时真的调用它，触发初始化）；
    - 目录只是协作者 → 直接 stub 访问器，并在注释里指明真实实现由哪个测试守护。

---

## 📍 三句话交代这条线在解决什么

1. ~~「新增连接器不需要修改公共模块」在代码上仍是假的。~~ **`CREATE CATALOG` 路径已兑现（15 号），四处特性级软阻塞已中立化（16 号），能力声明的形状问题已根治（17 号）**。残余的只有建表能力（18 号）。
2. ~~新连接器作者无处可依。~~ **已解决**：两个模块各有包级说明，`ConnectorTableOps` 已按域拆分，谓词下推包有六节契约，扫描属性表的键契约有常量类，**能力枚举现在每条都写明作用域与原因**。
3. ~~接口文档与实现大面积脱节。~~ **已解决**。

---

## 🧭 待用户拍板（未决之前不要顺手做掉）

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板十八条**（本批新增两条，见下）。

**本批新拍板的两条**：
- **引擎按表读取能力的范围不扩大**（保持 5 项，不做任务文档原计划的 5→8）。理由：「是否视图」是在**表对象初始化过程中**问的，「元数据预载」的用途就是决定要不要加载元数据——两者若改读表级集合（存放在表结构缓存里），会造成初始化倒置，每张表都多一次远程往返；而扩大在今天是纯机制、零行为收益（hive 是全仓唯一的按表写入方，收窄后只写引擎会读的 5 项）。**已把这个理由写进枚举注释**，将来要扩大是有据可argue的评审项，不是一行改动。
- **删镜像方法后不引入工具类**，调用点直接取提供者。公共模块净删 11 个方法、零新增。

**仍待拍板**：
- **含隐式类型转换的谓词下推默认值**（08 号）：`supportsCastPredicatePushdown` 默认 `true`，而它承诺的「引擎会先剥掉类型转换」只对残余谓词那条路径成立。翻成 `false` 是跨六个连接器的行为改动。
- **建表能力下沉的报错文案变化**（18 号；同一文档还纠正了「仓库里根本不存在分桶子句的正向端到端护栏」）。

---

## 🧾 顺带发现、留给后续批次

**本批新增的三条**：

- **`ConnectorContractValidator` 在生产代码里零调用方**（只有 5 个测试类调 `validate`）。它是一个住在 main 源码里、只被测试用的契约检查器。是留、是移进测试、还是接到某个真实入口，**单独立项**。
- **带时间旅行钉住的委派路径没有反射兄弟能力**（`HiveConnectorMetadata` 里那个带 MVCC 快照参数的 `getTableSchema` 重载直接转发给兄弟，不走 `reflectSiblingCapabilities`）。也就是说**钉住快照读 iceberg-on-HMS 表时，按表能力已经在丢**。这是既有缺口，本批刻意没在「行为不变」的重构里顺手改。**单独立项**，需要判断是补反射还是那条路径本就不该有能力。
- **两处 e2e 欠账**（本批的单测只覆盖 FE 侧装配）：①异构 HMS 目录上跑 `ANALYZE`、带 `LIMIT ... ORDER BY` 的 Top-N、嵌套列裁剪、`SHOW CREATE TABLE`，确认按表能力三项仍生效且 `SHOW CREATE TABLE` 输出**没有**变化；②在一个连接器构建失败的插件目录上跑 `CREATE TABLE ... ORDER BY`，确认报的是「不支持排序」而不是 NPE（这一条是本批修掉的缺陷，没有单测能覆盖，需要真目录）。

**沿用的**：

- **本轮更早批次欠的三条 e2e**：paimon 目录查询回归（`serialized_table` 缺字段是 BE 硬失败）、hive 文本/CSV/JSON 表读回归（12 个后缀经过符号替换，打错一个字母表现为该属性静默失效）、文件缓存准入 + `SWITCH <es 目录>` + 事件同步预热。
- **两个只写不读的属性键仍在**（paimon/hudi 的 `table_format_type`、es 的 `_table_name`、hive 的 `hive.text.json_serde_lib`）。判活需要单独核对 BE 与 JNI 侧。**单独立项**。
- **hudi 的 `partition_values()` 可能落后一个缓存过期时间**（经 `listPartitions` 读缓存）。
- **es 有一条与 01/02/03 同根因、尚未修的缺陷**：`EsQueryDslBuilder` 把 Doris 的 `REGEXP` 模式串原样交给 ES 的 `regexp` 查询（Doris 部分匹配、Lucene 整串锚定）→ 少行。ES 侧有干净的 `notPushDownList` 拒绝机制可落点。
- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，可删的死类。
- **`ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个活方法**，且是安全相关的，做同名清理时极易误伤。
- **`CatalogFactory` 里 `lakesoul` 那条硬失败在重放期仍会让 FE 退出**（15 号刻意没动）。
- **`ConnectorScanRange.getLength()` 是「契约写了单位、实现各行其是」的第三处**（契约写字节数、maxcompute 在行偏移模式返回行数）。
- **`ConnectorSession.getStatementScope` 默认不记忆**，与 06 号根治的是同一机理的另一处。
- **早期计划文档里「HMS 事件管线通过连接器回调接口通知引擎」那条旧决策已被 14 号作废**；留给 25 号一并补作废说明。
- **两套残差协议仍未合并**（`remainingFilter` 与 not-pushed 下标）。前置是实现细粒度反查。
- **`ConnectorWritePlanProviderDefaultsTest` 的一段说明是错的**：它写 `getWriteSortColumns` 默认返回空列表，实际返回 `null`（断言本身是对的，只有散文错）。同类小勘误还有：契约校验器的不变量编号从 #2 起（没有 #1），而 `IcebergConnectorTest` / `MaxComputeConnectorContractTest` 的注释还在引用一个已被删掉的「#1 运行时探针」。留给 25 号。

---

## ⚙️ 其余构建与验证的坑（实测，直接复用）

1. **maven build cache 会静默跳过测试执行**：所有跑测试的命令都要加 `-Dmaven.build.cache.enabled=false`。
2. **`mvn ... | tail` 之后的 `$?` 是 `tail` 的**；读 `BUILD SUCCESS` / `BUILD FAILURE` 行。
3. **maven 一律用绝对路径 `-f`**。
4. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
5. **严禁 `git add -A`**（工作树有大量历史遗留临时文件与含明文密钥的配置），一律 path-whitelist。
6. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**。
7. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**（建表路径），与本线改动无关。
8. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了（要真 Env）——除非按第 14 条用 `CALLS_REAL_METHODS` + `doReturn` 桩住它。
9. **hudi 单测里的 `stub(...)` 执行器会把整个 metaClient lambda 换掉**，别以为 `HudiConnectorPartitionListingTest` 绿了就等于分区列举全路径被覆盖。
10. **fe-core 测试里注私有字段用 `org.apache.doris.common.jmockit.Deencapsulation`（仓库自带），不是 `mockit.Deencapsulation`**。
11. **测不了的东西就直说。** 本批建表 NPE 修复与表值函数那道门都没有单测（需要真目录），已在提交信息里点明并列为待补 e2e。
12. **`PluginDrivenScanNode` / `PluginDrivenExternalTable` 的单测范式**：`Mockito.mock(类.class, CALLS_REAL_METHODS)` + `Deencapsulation.setField(...)`，再 `Deencapsulation.invoke(...)`。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批：07 + 08 + 10 | 全反应堆含测试源 `test-compile` 通过；79 个单测通过；冻结测试双变异验证均变红 |
| 2026-07-25 | 修掉调研期发现的两个用户可见缺口（异构目录嵌套列 DDL、iceberg 表注释） | 单测通过；e2e 已写出但**未执行** |
| 2026-07-25 | 第二批：11 号五个提交 | `test-compile` 通过；83 个单测 + checkstyle 通过；两次变异验证 |
| 2026-07-25 | 第三批：15 号两个提交 | `test-compile` + checkstyle 通过；52 个单测通过；三次变异验证均如期变红 |
| 2026-07-25 | 第四批：01～06 六个正确性缺陷 | `test-compile` + checkstyle **BUILD SUCCESS**；27 个测试类全绿；8 个变异一轮跑完全部被捕获；2 个新 e2e **未执行** |
| 2026-07-25 | 第五批：09 + 14 + 13 + 12 四个提交 | `test-compile` + checkstyle **BUILD SUCCESS**；八个模块全量单测 634 个全绿；4 个变异全部被预期测试类捕获 |
| 2026-07-26 | 第六批：21 一个提交 + 16 三个提交 | 两轮全反应堆含测试源 `test-compile` + checkstyle **BUILD SUCCESS**；33 个测试类全绿；5 个变异全部被捕获；新增 4 个测试类；欠 3 条 e2e |
| 2026-07-26 | **第七批：17 四个提交（hive 收窄兄弟反射 / 按表能力类型化 / 目录作用域能力访问器 / 删 11 个写特性镜像）** | 全反应堆含测试源 `test-compile` **BUILD SUCCESS**（收尾一轮清空全部 `test-classes` 强制真实重编）；四批合计 **190+165+130+81 个测试**全绿；**1 个变异如期只红新增断言**（40 个用例中 39 个照常通过，坐实该变异此前无人看守）；checkstyle 全部改动模块 0 违规；**定位并绕过了一个让构建卡死 60+ 分钟的 checkstyle 退化**；新欠 2 条 e2e、新立 2 个待办项 |

### 本批订正的任务文档事实（任务文档已大面积过时，核对一律以符号名为准）

1. **保留控制键是 6 个不是 7 个**（主键那个上一批已删），删掉本批的之后剩 **5 个**；任务文档还按「另外 6 个」写。
2. **任务文档要删的行号区间 `Connector.java:115-186` 落在 HEAD 上会误删按表取写计划提供者的方法**。实际区间是 `:130-:201`。
3. **建表构造点是 22 处不是 23 处**，全是 4 参形态。
4. **写计划提供者的实现是 4 个不是 3 个**（漏了 jdbc 那个，它不覆写任何特性）。
5. **任务文档列的三个待改测试全都不需要动**（`PhysicalConnectorTableSinkTest` / `InsertIntoTableCommandTest` / `InsertOverwriteTableCommandTest` 只 mock 引擎侧门面方法）；**反而漏了 8 个真正会挂的测试**。
6. **11 个镜像方法里有 4 个连引擎都没在用**，其中一个全仓零调用方——那部分是删死代码而非重构。
7. **hudi 连接器一个能力都不声明**（整个模块零 `ConnectorCapability` 引用），这是 hudi-on-HMS 拿不到任何标记的机制，改动时不能破坏。

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
