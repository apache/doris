# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：四批已合入（共 18 个提交）。正确性缺陷已经清零。**

已完成：07（写下规则）、08（文档据实）、10（按域拆接口）、11（删第一批死接口面）、15（删目录类型白名单）、**01～06 六个正确性缺陷（6 个提交）**，外加两个真实用户可见缺口（异构 HMS 目录下嵌套列 DDL、iceberg 表注释为空）。

**这一批落地后的事实变化（更新你对这条线的认知）**：

1. **`ConnectorType` 现在会在构造期硬校验复杂类型的形状**。ARRAY 恰好 1 个子类型、MAP 恰好 2 个、STRUCT 子类型非空且字段名等长同序无 null；四组可选按子元素列表**不得比 children 长**（**短是合法的**，见下面第 3 条）。任何新造复杂类型的代码（含测试）都会被它挡。
2. **`ConnectorOr` 现在拒绝少于两个分支**，并对传入列表做真拷贝。
3. **`fe-connector-spi` 多了一个公共类 `ForwardingConnectorContext`**，iceberg / paimon 的钉桩包装类改成继承它。**往 `ConnectorContext` 加方法时必须同时在基类补一个转发**，否则 `ForwardingConnectorContextTest` 会红；如果新方法会进插件代码，两个钉桩子类还必须各自覆写并加钉桩（基类只保证不丢转发，不保证钉桩）。
4. **hudi 的分区「最后修改时间」换成了真正的 epoch 毫秒**，同一个值也是 hudi 的数据版本令牌，量级从 ~2.0e16 降到 ~1.7e12。

**建议的下一步**：**09 + 12 + 13 + 14**（下推契约补全 + 两批需要连带改连接器的删除）。理由：09 号正是 01/02/03 三条缺陷的根因（逐算子语义与「不可精确翻译必须放弃下推」从来没写进公共契约），刚修完实现、趁热把契约补上最省重新理解的成本；12/13/14 是接口面删除的后续批次，前置的 11 号已完成。若想先做中立化，**16 + 17 + 21** 也已解锁。

**做下一批之前必看八条（前六条沿用，7、8 是本批新增）**：

1. **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar。** 跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。
2. **checkstyle 的方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`**——**第二个字符也必须小写**，且 `test-compile` 阶段才报。
3. **冻结基线是硬约束。** `ConnectorMetadataSurfaceTest` 把 `ConnectorMetadata` 的方法签名冻结在 `fe-connector-api/src/test/resources/connector-metadata-methods.txt`（**80 行**）。**任何删除/新增 `ConnectorMetadata` 方法的批次必须在同一提交里重新生成它**（跑那个测试、从失败信息的 "Full actual surface:" 拷贝，**不要加 ASF 头**）。本批没动它。
4. **「全反应堆 test-compile 能一次证明引用全清」是错的。** 它对 javadoc `{@link}` 引用结构性失明。删除批次的 grep 清单必须包含只在注释里出现的名字。
5. **`git rm` 会立即入暂存区。** 拆提交时逐个 `git diff --cached --stat` 核对。
6. **任务文档里的「不需要变异验证」不要照信。** 本批 8 个变异全部如期变红（见下面「验证口径」）。
7. **变异验证可以一次跑完，但要保证「一个变异对应一个测试类」。** 本批把 8 个变异同时打进代码跑一轮，靠「失败的测试类互不重叠」做归因，省了 7 轮全反应堆构建。**坑**：变异本身可能违反 checkstyle（删掉 `ConnectorOr` 的防御性拷贝会让 `import java.util.ArrayList` 变成 unused import → `BUILD FAILURE`，一个测试都没跑），做变异时要连带把 import 一起处理。
8. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉。** 只剩 `BUILD SUCCESS` 是无法判断测试有没有真跑的。**一律 `> 日志文件 2>&1` 再 grep**。

---

## 📍 三句话交代这条线在解决什么

1. ~~「新增连接器不需要修改公共模块」在代码上仍是假的。~~ **已兑现（15 号）**：`CREATE CATALOG` 无条件问插件。残余的是特性级门（16 / 17 / 18）。
2. ~~新连接器作者无处可依。~~ **已解决**：两个模块各有一份包级说明，`fe-connector-api` 现有**七条规则**；`ConnectorTableOps` 已按域拆成 6 个父接口。本批又把三条「靠自觉」的不变量变成了可执行约束（复杂类型形状、OR 分支数、上下文转发完整性）。
3. ~~接口文档与实现大面积脱节。~~ **已解决**。剩余的文档问题在 09 号（下推表达式契约）——**这是本批三条谓词缺陷的共同根因，建议下一个就做**。

---

## 🔑 动手前必须知道的五件事

1. **行号信内容不信文档。** 全部任务文档的行号以 `7ff51a106f0` 为准；10 / 11 / 15 / 01～06 落地后，`ConnectorTableOps`、`Connector`、`ConnectorProvider`、`ConnectorType`、`ConnectorOr`、`ConnectorPartitionInfo`、`ConnectorPartitionSpec`、`ConnectorCreateTableRequest`、`ConnectorMvccSnapshot`、`ConnectorPushdownOps`、`PluginDrivenScanNode`、`CatalogFactory`、`ConnectorPluginManager`、`ConnectorFactory`、`TrinoPredicateConverter`、`TrinoTypeMapping`、`PaimonPredicateConverter`、`HudiConnectorMetadata`、`HudiScanPlanProvider`、两个 `TcclPinningConnectorContext` 的行号都已作废。核对一律以符号名为准。
2. **先读调研报告第十四节（被推翻或收窄的说法）和第十六节（明确不建议动的部分）。** 误报比漏报更毒。11 号动手前推翻/订正了任务文档 14 条事实，15 号订正 6 条，**本批订正 2 条**（见下）。
3. **删除类任务必须全仓复扫**，且统计连接器实现分布时**按符号 grep，不要按类名模式**（trino 的元数据类叫 `TrinoConnectorDorisMetadata`）。
4. **`fe-core` 只出不进。** 本批**零 fe-core 改动**（六条缺陷全部在连接器与公共模块内可解）。
5. ~~**两个连接器的类加载器钉桩包装类是雷区**（06 号任务；23 号依赖它先做）。~~ **06 号已完成**，两个包装类现在各只剩 `executeAuthenticated`（iceberg 另有一个包私有的认证器访问器）。**23 号的前置已就位。**

---

## ⚙️ 构建与验证的坑（实测，直接复用，别再踩）

1. **全反应堆 `test-compile` 必须排除两个 shade 模块**，否则 hive 相关模块必然编译失败（`package org.apache.hadoop.hive.conf does not exist`）。**这与你的改动无关，不要去 debug 它**：
   ```
   mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
     -pl '!fe-connector/fe-connector-hms-hive-shade,!fe-connector/fe-connector-paimon-hive-shade' \
     -Dmaven.build.cache.enabled=false -T1C test-compile
   ```
2. **maven build cache 会静默跳过测试执行**：所有跑测试的命令都要加 `-Dmaven.build.cache.enabled=false`。`fe/.mvn/maven.config` 是**未跟踪文件**，不要依赖它。
3. **`mvn ... | tail` 之后的 `$?` 是 `tail` 的**；读 `BUILD SUCCESS` / `BUILD FAILURE` 行，且**不要用 `| tail` 截断**（见必看第 8 条）。
4. **maven 一律用绝对路径 `-f`**，且**不要用 `-pl` 缩到单模块**。跑测试的形状是：全反应堆 + `-Dtest=<具体类名清单>` + `-Dsurefire.failIfNoSpecifiedTests=false`。一轮约 4～5 分钟。
5. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
6. **严禁 `git add -A`**（工作树有大量历史遗留临时文件与含明文密钥的配置），一律 path-whitelist。
7. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**（跑不了就生成不出基线），用 `assertEquals` 之类自包含断言。
8. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**（建表路径），与本线改动无关。**别把它当成自己改坏了**，也别顺手修，更**不能拿它当变异验证的判据**。
9. **`checkstyle:check` 会随 `test-compile` 一起跑**（`validate` 阶段），不需要单独跑；但注意它扫测试源，方法名、行长、unused import 都算。
10. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了（要真 Env）。
11. **hudi 单测里的 `stub(...)` 执行器会把整个 metaClient lambda 换掉**，所以 `collectPartitions` 里 lambda 内部的逻辑（本批的单位转换就在那里）**单测打不到**。要验证那一段只能靠 e2e 或代码评审——**别以为 `HudiConnectorPartitionListingTest` 绿了就等于分区列举全路径被覆盖了。**

---

## 📦 提交规范

- **每个任务一个独立 commit**（大批次可按删除项再拆）；任务文档 / 交接文档与代码**分开提交**。
- 提交信息**全英文**，标题形如 `[fix](catalog) fe-connector-xxx: <what changed>` 或 `[refactor](catalog) …`。正文写清「为什么是错的 / 为什么删得掉」和「为什么值得改」，**有用户可见文案或行为变化时必须点明并说明无测试依赖旧行为**，**有测试覆盖不到的部分必须写出来**（本批 05 号就明写了「两个调用点单测打不到」）。
- 结尾附 `Co-Authored-By:` 与 `Claude-Session:` 两行。
- `gh pr edit` 在上游仓库上是坏的，改标题/正文用 REST API。

---

## 🧭 待用户拍板（未决之前不要顺手做掉）

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板十条**：谓词下推默认值只改文档、最少实现集用注解+冻结测试、两个真实缺口顺手修、连接器自声明属性删除、建表请求的 isExternal 删除、目录类型路由用「插件优先 + 内建名保留字」、建目录失败列出已安装连接器类型、悬空注释单开一个纯注释提交、**复杂类型形状加构造期完整校验（本批）**、**六条正确性缺陷一次做完（本批）**。

**仍待拍板**：
- **建表能力下沉的报错文案变化**（18 号；同一文档还纠正了「仓库里根本不存在分桶子句的正向端到端护栏」）。
- **读侧的主键方法**（`getPrimaryKeys` + `PRIMARY_KEYS_KEY`）：建议删；若保留必须同时补契约文档并让至少一个连接器真正消费。要改 paimon 生产代码，归 12 号那一批。

---

## 🧾 顺带发现、留给后续批次

- **09 号（下推契约）现在有了三条实证**：01/02/03 都是「逐算子语义没写进公共契约 → 五个消费者各写一遍 → 有人写错」。补契约时可以直接引用这三条作为例子，`ConnectorComparison` 与 `ConnectorLike` 各自的一行文档就是现状。
- **es 有一条同根因、本批**没**修的缺陷**：`EsQueryDslBuilder` 把 Doris 的 `REGEXP` 模式串原样交给 ES 的 `regexp` 查询，而 Doris 的 regexp 是部分匹配、Lucene 的是整串锚定。语义不同、测试环境也不同，单独立项（ES 侧本来就有干净的 `notPushDownList` 拒绝机制，修起来有落点）。
- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，可删的死类。建议并入 12 号。
- `ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个**活**方法，且是安全相关的。它是不带限定名的同名符号，**做同名清理时极易误伤**。
- 同模块还有第三个**活的** `ConnectorPartitionInfo.UNKNOWN = -1L`（hive 主源与 fe-core 都在用）。
- **`CatalogFactory` 里 `lakesoul` 那条硬失败在重放期仍会让 FE 退出**（15 号刻意没动）。若要一并治，单独立项。
- **`ConnectorProvider.supports(catalogType, properties)` 的按属性分派现在第一次真正可用**（全仓 0 个覆写）。
- **`ConnectorScanRange.getLength()` 是同类的第三处单位违约**（契约写字节数、maxcompute 在行偏移模式返回行数）。05 号立的规矩可以直接套：公共接口凡写了单位的字段，都该有一条量级单测钉住。
- **`ConnectorSession.getStatementScope` 默认不记忆**，与 06 号根治的是同一机理的另一处（有默认值就会静默关掉性能优化）。06 号刻意没扩到它。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批落地：07 + 08 + 10 三个提交 | 全反应堆含测试源 `test-compile` 通过；`fe-connector-api` 79 个单测通过；冻结测试双变异验证均变红 |
| 2026-07-25 | 修掉调研期发现的两个用户可见缺口（异构目录嵌套列 DDL、iceberg 表注释），补单测与 e2e 用例 | 单测通过；e2e 已写出但**未执行**（需真集群） |
| 2026-07-25 | 第二批落地：11 号五个提交（删 5 个类 + 6 组死字段/死方法）。动手前 22 个 agent 推翻/订正任务文档 14 条事实 | 全反应堆含测试源 `test-compile` 通过；83 个单测 + checkstyle 通过；两次变异验证 |
| 2026-07-25 | 第三批落地：15 号两个提交（删目录类型白名单 + 保留字检测 + 重放降级；纯注释清 20 处悬空引用） | 全反应堆 `test-compile` + checkstyle 通过；52 个单测通过；三次变异验证均如期变红；e2e **未执行** |
| 2026-07-25 | **第四批落地：01～06 六个正确性缺陷，6 个独立提交**。动手前按符号在 HEAD 上逐条复核，六条全部属实，订正任务文档 2 条事实 | 全反应堆含测试源 `test-compile` + checkstyle **BUILD SUCCESS**；27 个测试类全绿（含 8 个既有复杂类型回归类）；**8 个变异一轮跑完、全部由预期的测试类捕获**；2 个新 e2e 用例已写出但**未执行**（需真集群） |

### 本批订正的任务文档事实（两条）

1. **04 号提出的「四组可选按子元素列表要么为空、要么长度恰好等于 children」过严。** `ConnectorType` 的四个访问器（`isChildNullable` / `getChildComment` / `getChildFieldId` / `isChildCommentSpecified`）都是**越界即视为未携带并回退默认值**，字段注释也明写「Empty (or shorter than children) means unset」。按任务文档字面实现会**硬失败一个被文档支持的合法状态**。实际落地成「不得比 children **长**，短是合法的」。
2. **05 号说「API 已用 javap 在 hudi-common 1.0.2 里核实」——本地 m2 同时存在 0.14.0 / 0.14.1 / 0.15.0 / 1.0.2 等多个版本，`fixInstantTimeCompatibility` 只在 1.0.2 里有**（0.14.0 没有）。工程用的确实是 1.0.2，所以结论成立；但拿错版本核对会得出相反结论。另外实测确认 14 位秒级 instant 的毫秒补位是 `999`（`DEFAULT_MILLIS_EXT`），**没有靠猜**。

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
