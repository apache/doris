# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🔥 构建命令（照抄，别用更早版本的写法）

旧交接文档里"一轮 4～6 分钟"的命令会让 maven 卡死一小时。根因已定位：checkstyle 的扫描目录取模块的**编译源根**，而 `fe-thrift` / `fe-sql-parser` / `fe-grpc` 把 `target/generated-sources` 加进了源根 → 生成代码被当成待审计源码，而抑制注释过滤器按违规条数重扫全文 → 退化成平方级。

**固定用这两条**：

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

**当前状态：九批已合入（共 38 个提交）。正确性缺陷清零；死接口面删除基本完成；下推契约成文；扫描属性键契约集中；引擎里按数据源名判定的分支全部中立化；能力声明形状问题已根治；公共模块里最后一个数据源品牌常量已中立化；同日另一份评审文档已入库并按 HEAD 标注。**

已完成：07、08、10、11 / 12 / 13 / 14、15、09、01～06、两个真实用户可见缺口、21 与 16、17、20 / 22 / 19，**外加本批的 25（3 个提交）**。

**剩下的只有两项**：

- **18**（建表能力下沉，**高风险**，需先拍板报错文案变化，且仓库里**根本不存在**分桶子句的正向端到端护栏——必须自己补一条）；
- **23**（引擎上下文里的存储服务拆分，**高危**，必须插件包重部署冒烟）。

另有 **10 条"复核登记的开放项"**（README 里新加了一张表），只登记未排期。其中**第一条最便宜也最该先做**：统计接口"列举文件大小"的异常契约与它唯一的实现、以及包级说明里的"响亮失败"规则**三方矛盾**，两方说该抛，改接口 javadoc 一句话即可，不动任何行为。它是被两批工作**从缝里漏下来**的——修正过时接口文档那一批把它明确排除在范围外（归到异常契约那一批），而那一批从未立项。

---

## 📌 本批落地后的事实变化

1. **`plan-doc/connector-api-spi-design-review-2026-07-25.md` 从此进入版本控制。** 它此前**从未在任何分支任何提交里存在过**（不是加了又删，就是从来没人 `git add`），而三份已入库文档在引用它 —— 对只克隆了提交的人全是悬空引用。**分两个提交**：先原样入库保住快照，再提交修订，于是"可 diff、可回退"第一次成立。
2. **处置方案不是原计划的"就地改 8 处"。** 重侦察发现那份文档写于 **44 个提交**之前，去重后约 **26 个独立论断**失效（散落 40 余处），而那份勘误清单**自己也过期了**：原 8 条里 4 条仍成立、3 条被代码变更取代、1 条彻底失去对象。改为**保留分析正文**（它是评审快照）+ 三类标注（状态戳 36 处、交叉核对修正 15 处、就地重算 3 处）+ **重编两块可消费内容**（现状概览的两张表、文末 17 条行动清单）。
3. **"当前现状"的权威位置已经在代码注释里。** 这是不做"全文重新校准"的根本理由——八批改动干的恰恰是把规则写进 `package-info.java`（规则一/规则三）、各域接口的最少实现集、契约校验器的"实际覆盖"段落。再维护一份 markdown 副本就是第二真相源，而且下一批落地后又整体过期。
4. **三处代码注释错误已修**（第三个提交）：值对象把四个数字的来源说反了（求和的是第一、三、四个，来自事务的是**第二个**）；一个测试类的类注释说默认值"必须是空列表"、而接口默认返回 `null` 且**该测试自己断言的就是 `null`**；两处引用了树中已不存在的旧类名。
5. **公共接口模块并没有在收缩。** 八批删除之后 `fe-connector-api` **净增 6 个文件、829 行**（95/10149 → 101/10978）——删掉的死接口面被写进去的契约文档抵消了。收缩发生在"要实现的方法数"上：`Connector` 从 34 掉到 21，`ConnectorTableOps` 家族从 46 个声明降到 43 个且自身只剩 2 个。**别再用"代码行数在减少"当论据。**

---

## ⚠️ 做下一批之前必看

1. **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar。** 跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。（`checkstyle:check` 例外。）
2. **checkstyle 不再随 test-compile 跑，必须单独跑。** 方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`（第二个字符也必须小写）；**`CustomImportOrder` 会因为 import 顺序失败**；`UnusedImports` 是强制项且不对 connector 包豁免。
3. **变异验证只对高价值改动做**：判据=改错了会**静默**产生错误结果、且新测试是唯一护栏。纯改名、删死接口面、javadoc、import 顺序**不做**。本批全是文档与注释，**没有做变异，也不该做**。
4. **一次跑多个变异时，maven 会在第一个失败的模块中止**，要么加 `--fail-at-end`，要么分开跑。
5. **冻结基线只冻结 `ConnectorMetadata`**（`fe-connector-api/src/test/resources/connector-metadata-methods.txt`）。改 `Connector` 或 `ConnectorProcedureOps` **不需要**动它。
6. **删除类改动必须配全仓符号 grep + 清空 `test-classes` 后重跑**（增量编译会跳过未改模块，陈旧 class 留到运行期才炸）。
7. **编译器抓不到注释里的引用。** 本批第三个提交修的三处全是注释——其中两处引用的类名早已不存在，编译一路绿灯。**删类之后要对类名做一次全仓 grep，包括注释。**
8. **【本批最重要的一条】任务文档与勘误清单本身都会过期，动手前必须按符号重侦察。** 本批的施工文档写于 44 个提交之前，它列的 8 条勘误里有 1 条的对象已经不存在、3 条已被代码取代——照做会在文档里新写一条指向已删符号的待办。**这是第 4 次复发。别信行号，也别信"照着改就行"的清单，先回到 HEAD 用符号核对。**
9. **有些"事实错误"在基线时就是错的，不是后来变的。** 本批查出 3 条：契约校验器"零真实连接器调用"（调研日就有 4 家在调）、驱动包路径校验"只对 JDBC 有意义"（iceberg 和 paimon 都在用，照建议拆会打断它们）、网关"无法按名字判定表格式"（hive 网关就是回查元数据判定的）。**复核一条结论时要同时问"它现在还成立吗"和"它当初成立过吗"。**
10. **`git rm` 会立即入暂存区。** 拆提交时逐个 `git diff --cached --stat` 核对。
11. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉。** 一律 `> 日志文件 2>&1` 再 grep；`| tail` 之后的 `$?` 是 `tail` 的。
12. **纯 Mockito mock 上的新方法默认返回 null。** 加 SPI 方法后必须查所有 mock 该接口的测试。
13. **仓库有 63 个顶层未跟踪项**（含明文密钥的配置、临时日志、workflow 脚本）。**严禁 `git add -A`**，一律显式路径。

---

## 📍 这条线在解决什么（现状）

1. 「新增连接器不需要修改公共模块」：**建目录路径已兑现（15）；四处特性级软阻塞已中立化（16）；能力声明形状已根治（17）；扫描节点里最后两段源专有分支已归位（19）；分布式过程结果列已交还连接器（22）。** 残余只有**建表能力**（18 号）。
2. 新连接器作者无处可依：**已解决**。两个模块各有包级说明，表操作接口按域拆分且每域写了最少实现集，谓词下推包有六节契约，扫描属性键契约有常量类，能力枚举每条写明作用域与原因。
3. 接口文档与实现大面积脱节：**已解决**，但**留了一条**——统计接口的异常契约（见"下一个 session 起步"）。

---

## 🧭 待用户拍板

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板二十五条**（本批新增三条）。

**本批拍板的三条**：

- 那份评审文档的处置深度选**"加状态戳 + 重编两块"**，不是只改 8 处、也不是全文重新校准。
- **纳入版本控制，分两次提交**（原样 + 修订）。
- 顺带发现的四处代码级问题里，**只修三处注释错误**；异常契约那条（改接口 javadoc 一句话）**本轮不动，登记待排期**。

**仍待拍板（都在 18 号动手前）**：

- **含隐式类型转换的谓词下推默认值**（08 号）：`supportsCastPredicatePushdown` 默认 `true`，而它承诺的「引擎会先剥掉类型转换」只对残余谓词那条路径成立。翻成 `false` 是跨六个连接器的行为改动。
- **建表能力下沉的报错文案变化**（18 号）。

---

## 🧾 顺带发现、留给后续批次

**本批新增的开放项已经登记进 [README.md](./README.md) 的"复核登记的开放项"表（10 条）**，不在这里重复。最值得先做的是异常契约那条。

**沿用的**：

- **那两个 ES 兼容 HTTP 端点存在既有安全面**（已拍板：单独立项）。`table` 请求参数**不做任何校验**就被拼进 ES 的 URL 路径；口令检查只在 `Config.enable_all_http_auth` 打开时才做；**全程没有目录级权限检查**。落点是端点侧的索引名合法性校验 + 权限检查，**不要**去改连接器。
- **EXPLAIN 与实际下推的判据不一致**（已拍板：逐字保留）。两半都在 `EsScanPlanProvider` 里、相隔一个方法，已加 ATTN 注释。修它要同步改 `external_table_p2/es/test_es_query_predicate_correctness.groovy`。
- **hudi 的 `\N` 与其余连接器的空串渲染分歧**（已拍板：不统一）。已证实 BE 在空值位为 true 时根本不看那个字符串，所以统一是安全的、只是需要端到端确认。
- **合成键机制有个既有的洞**：`nativeReadSplitNum` / `totalReadSplitNum` 只在 `getSplits` 里赋值，批模式的 `startSplit` / `startStreamingSplit` 从不赋值 → 批模式下 paimon 那行渲染成 `0/0`。
- **`EsScanRange.getFileFormat()` 返回 `"es_http"` 是死代码**且与 SPI 契约矛盾。
- **`PluginDrivenScanNode.TABLE_FORMAT_TYPE = "plugin_driven"` 全仓零引用**，可删的死常量。
- **引擎仍在一处按字符串比较哨兵**（`MetadataGenerator` 渲染 `partition_values()` 那一格），与 `PluginDrivenMvccExternalTable` javadoc 里的铁律抵触。真修法是从 `ConnectorPartitionInfo` 的空值标志取——**是行为变更，单独立项**。
- **`TablePartitionValues.toListPartitionItem` 里的哨兵比较实际不可达**；删掉它对树外连接器是行为变更。
- **`ConnectorContractValidator` 在生产代码里零调用方**。是留、是移进测试、还是接到真实入口，**单独立项**。（本批复核补充：四家连接器测试在调，但 es 走早退、jdbc 四前件全 false，**真正压到真前件的只有 iceberg 与 maxcompute**。）
- **带时间旅行钉住的委派路径没有反射兄弟能力** → 钉住快照读 iceberg-on-HMS 表时按表能力在丢。**单独立项**。
- **两个只写不读的属性键仍在**（paimon/hudi 的 `table_format_type`、es 的 `_table_name`、hive 的 `hive.text.json_serde_lib`）。
- **hudi 的 `partition_values()` 可能落后一个缓存过期时间**。
- **es 有一条与 01/02/03 同根因、尚未修的缺陷**：`EsQueryDslBuilder` 把 Doris 的 `REGEXP` 模式串原样交给 ES 的 `regexp` 查询（Doris 部分匹配、Lucene 整串锚定）→ 少行。
- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，可删的死类。
- **`ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个活方法**，且是安全相关的，做同名清理时极易误伤。
- **`CatalogFactory` 里 `lakesoul` 那条硬失败在重放期仍会让 FE 退出**（15 号刻意没动）。
- **`ConnectorScanRange.getLength()` 是「契约写了单位、实现各行其是」的第三处**。
- **`ConnectorSession.getStatementScope` 默认不记忆**，与 06 号根治的是同一机理的另一处。
- **两套残差协议仍未合并**（`remainingFilter` 与 not-pushed 下标）。前置是实现细粒度反查。

---

## 🧪 欠下的端到端（本地无集群，一律标「待集群验证」，不得当作已通过）

**本批新欠 0 条**（纯文档与注释，不产生任何运行时行为）。

**沿用**：ES 的六处 `terminate_after` 断言与两个 REST 端点 curl；iceberg `rewrite_data_files` 的五个套件；paimon 目录查询回归；hive 文本/CSV/JSON 表读回归；文件缓存准入 + `SWITCH <es 目录>` + 事件同步预热；异构目录嵌套列 DDL 与 iceberg 表注释（26/27 号写好未跑）；异构 HMS 目录上的 `ANALYZE`/Top-N/嵌套列裁剪/`SHOW CREATE TABLE`；在一个连接器构建失败的插件目录上跑 `CREATE TABLE ... ORDER BY` 确认报「不支持排序」而不是 NPE。

---

## ⚙️ 其余构建与验证的坑（实测，直接复用）

1. **maven build cache 会静默跳过测试执行**：跑测试一律加 `-Dmaven.build.cache.enabled=false`。
2. **maven 一律用绝对路径 `-f`**；`cd` 会让后续相对路径失效。
3. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
4. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**。
5. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**（建表路径），与本线改动无关。
6. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了——除非 `Mockito.mock(X.class, CALLS_REAL_METHODS)` + **`doReturn(...).when(x).getConnector()`**。
7. **hudi 单测里的 `stub(...)` 执行器会把整个 metaClient lambda 换掉**。
8. **fe-core 测试里注私有字段用 `org.apache.doris.common.jmockit.Deencapsulation`（仓库自带）**。
9. **`PluginDrivenScanNode` / `PluginDrivenExternalTable` 的单测范式**：`Mockito.mock(类.class, CALLS_REAL_METHODS)` + `Deencapsulation.setField(...)`，再 `Deencapsulation.invoke(...)`。
10. **`fe-connector-es` 模块没有 mockito / jmockit / fe-core 依赖**，只有纯 JUnit 5。
11. **数方法数要有口径并写下来**。本批给那份文档重算接口规模时定的口径是：只数写在该接口体内的声明、`default` 计入、重载分别计、**注解不计**（`@ConnectorMustImplement` 会被朴素的"标识符 + 左括号"正则数进去，一度让列 DDL 那个域接口从 11 虚高到 22）、继承的不计。

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
| 2026-07-26 | 第八批：20 两个提交 + 22 一个提交 + 19 两个提交 | 侦察阶段 9 个并行核查单元推翻了 19 号的**核心机制**；清空 66 个 `test-classes` 目录后全反应堆 `test-compile` **BUILD SUCCESS**、27 个测试类 259 个测试全绿；checkstyle 0 违规；4 个高价值变异全部如期变红 |
| 2026-07-26 | **第九批：25 三个提交**（评审文档原样入库 + 按 HEAD 标注 + 三处代码注释修正） | 侦察阶段 16 个并行核查单元推翻了**任务文档的处置方案本身**（8 条勘误里 1 条对象已不存在、3 条被代码取代）；全文复核查出约 **26 个独立论断失效**（40 余处文字）、其中 **3 条在调研日就是错的**；产出 36 处状态戳 + 15 处交叉核对修正 + 3 处就地重算 + 重编 17 条行动清单；三个模块 checkstyle **0 违规**；新登记 10 条无人认领的开放项 |

### 本批订正的任务文档事实

1. **施工文档写的回退方式根本执行不了**：那份文档从未被 git 跟踪，`git checkout -- <它>` 只会报 `pathspec did not match`，也没有 commit 可 revert。
2. **勘误清单第 2.2 条已失去对象**：分片类型枚举族整族已删，"把 7 改成 8"照做等于新写一条指向已删符号的待办。
3. **勘误清单第 2.4 条的 6 个数字只剩 3 个准**：模块规模、`Connector` 方法数都已变。
4. **勘误清单第 2.5 第 2 条的三条硬约束第 3 条已假**（引擎侧重复定义已删），且漏列了引擎的第三个消费者。
5. **施工文档第六节那批"原始错误字符串必须消失"的断言，只有 2 条仍适用**——按新方案，另外 3 处刻意保留原句 + 加修正块。
6. **交接文档记的"`ConnectorRewriteDriver` 引用的两个旧类名都不存在"只对一半**：另一个类是活的，只是搬进了 iceberg 连接器；同时漏了一处同类引用。

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
