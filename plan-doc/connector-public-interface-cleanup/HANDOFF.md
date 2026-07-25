# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：三批已合入（共 12 个提交）。这条线的核心承诺已经兑现。**

已完成：07（写下规则）、08（文档据实）、10（按域拆接口）、11（删第一批死接口面）、**15（删目录类型白名单，2 个提交）**，外加两个真实用户可见缺口（异构 HMS 目录下嵌套列 DDL、iceberg 表注释为空）。

**15 号落地后的事实变化（更新你对这条线的认知）**：装上插件就能 `CREATE CATALOG`，不用改任何公共模块——这句话第一次为真。引擎不再持有「可接受的目录类型」清单；`doris` / `test` / `lakesoul` 成为保留字（插件声明它们会在注册期被拒）；重放一个没人服务的目录类型不再让 FE 退出。仍有几处**特性级**的按源名判定没动：建表能力（18）、能力位类型化（17）、按源名软阻塞（16）。

**建议的下一步**：**01～06 六个正确性缺陷**。理由：它们有用户可见后果（01/04 当前生效，01 是三路以上 OR 谓词静默少行、04 是复杂类型字段名变成 `col0`/`col1`），README 明确说不该排在设计整治后面；而设计整治的两个前提（规则写下来、白名单删掉）现在都已就位。若想继续清接口面，**09 + 12 + 13 + 14** 也全部解锁。

**做下一批之前必看六条（15 号实测，比任务文档新）**：

1. **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar。** 给 `fe-connector-spi` 加了方法后，`-pl fe-connector/fe-connector-hudi` 跑测试报「method does not override or implement a method from a supertype」——那是旧 jar，**不是你的代码错**。跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。（这是老坑第 4 条的推广：不只 `fe-core` 如此。）
2. **checkstyle 的方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`**——**第二个字符也必须小写**。`aTypeThatIsCreatable` 这种读起来很自然的测试名会红，且 `test-compile` 阶段才报。
3. **冻结基线是硬约束。** `ConnectorMetadataSurfaceTest` 把 `ConnectorMetadata` 的方法签名冻结在 `fe-connector-api/src/test/resources/connector-metadata-methods.txt`（现为 **80 行**）。**任何删除/新增 `ConnectorMetadata` 方法的批次必须在同一提交里重新生成它**，做法是跑那个测试、从失败信息的 "Full actual surface:" 拷贝。**那个文件没有 ASF 头，重新生成时不要加。** 15 号在 `ConnectorProvider`（`fe-connector-spi`）上加方法**没有**牵动它——那份基线只覆盖 `ConnectorMetadata`。
4. **「全反应堆 test-compile 能一次证明引用全清」是错的。** 它对 javadoc `{@link}` 引用结构性失明（fe-core 的 javadoc 插件是 `<skip>true</skip>`）。**删除批次的 grep 清单必须包含只在注释里出现的名字**，而且要预期数量级：15 号删一个私有字段，牵出 15 个文件 20 处注释。
5. **`git rm` 会立即入暂存区。** 多提交拆分时，先 `git rm` 再 `git add <另一批文件>` 会把删除卷进错误的提交。**拆提交时逐个 `git diff --cached --stat` 核对。**
6. **任务文档里的「不需要变异验证」不要照信。** 15 号文档这么写，但「把开关放错入口」是它唯一的高危错误，实测三次变异全部如期变红（其中一次同时暴露两个方向的破坏）。**只要存在「放错位置就静默坏掉」的形态，就做一次变异。**

---

## 📍 三句话交代这条线在解决什么

1. ~~「新增连接器不需要修改公共模块」在代码上仍是假的。~~ **已兑现（15 号）**：`CREATE CATALOG` 无条件问插件，引擎不再有类型白名单。残余的是特性级门（16 / 17 / 18）。
2. ~~新连接器作者无处可依。~~ **已解决**：两个模块各有一份包级说明。`fe-connector-api` 现有**七条规则**；`ConnectorTableOps` 已按域拆成 6 个父接口，每域写清「最少实现集」，并有注解 + 冻结测试钉住。
3. ~~接口文档与实现大面积脱节。~~ **已解决**（08 号那九处 + 11 / 15 号顺带修正的若干处）。剩余的文档问题在 09 号（下推表达式契约）。

---

## 🔑 动手前必须知道的五件事

1. **行号信内容不信文档。** 全部任务文档的行号以 `7ff51a106f0` 为准；10 / 11 / 15 号落地后，`ConnectorTableOps`、`Connector`、`ConnectorProvider`、`ConnectorPartitionSpec`、`ConnectorCreateTableRequest`、`ConnectorMvccSnapshot`、`ConnectorPushdownOps`、`PluginDrivenScanNode`、`CatalogFactory`、`ConnectorPluginManager`、`ConnectorFactory` 的行号都已作废。核对一律以符号名为准。
2. **先读调研报告第十四节（被推翻或收窄的说法）和第十六节（明确不建议动的部分）。** 误报比漏报更毒。11 号动手前的复核推翻/订正了那份任务文档 14 条事实，15 号又订正 6 条，**这类文档不能直接照做**。
3. **删除类任务必须全仓复扫**，且统计连接器实现分布时**按符号 grep，不要按类名模式**（trino 的元数据类叫 `TrinoConnectorDorisMetadata`）。
4. **`fe-core` 只出不进。**（15 号往 fe-core 加了 `createStandaloneCatalogConnector` 与 `isBuiltinCatalogType` 两个方法——那是插件路由基建、不是数据源相关代码，且换来删掉一整张类型白名单，属净减。往测试源补守护断言同样不违反这条。）
5. **两个连接器的类加载器钉桩包装类是雷区**（06 号任务；23 号依赖它先做）。

---

## ⚙️ 构建与验证的坑（实测，直接复用，别再踩）

1. **全反应堆 `test-compile` 必须排除两个 shade 模块**，否则 hive 相关模块必然编译失败（`package org.apache.hadoop.hive.conf does not exist`）。**这与你的改动无关，不要去 debug 它**：
   ```
   mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
     -pl '!fe-connector/fe-connector-hms-hive-shade,!fe-connector/fe-connector-paimon-hive-shade' \
     -Dmaven.build.cache.enabled=false -T1C test-compile
   ```
2. **maven build cache 会静默跳过测试执行**：所有跑测试的命令都要加 `-Dmaven.build.cache.enabled=false`。`fe/.mvn/maven.config` 是**未跟踪文件**，不要依赖它。
3. **`mvn ... | tail` 之后的 `$?` 是 `tail` 的**；读 `BUILD SUCCESS` / `BUILD FAILURE` 行。
4. **maven 一律用绝对路径 `-f`**（跨工具调用工作目录会持久），且**不要用 `-pl` 缩到单模块**（见上面「必看六条」第 1 条）。跑测试的形状是：全反应堆 + `-Dtest=<具体类名清单>` + `-Dsurefire.failIfNoSpecifiedTests=false`。
5. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
6. **严禁 `git add -A`**（工作树有大量历史遗留临时文件与含明文密钥的配置），一律 path-whitelist。
7. **e2e（groovy）需要真集群，本地跑不了**。
8. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**：**19 个用例 / 5 failures + 7 errors**（建表路径），与本线改动无关。**别把它当成自己改坏了**，也别顺手修（属另一条线），更**不能拿它当变异验证的判据**。
9. **`checkstyle:check` 会随 `test-compile` 一起跑**（`validate` 阶段），不需要单独跑；但注意它扫测试源，方法名与行长都算。
10. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了（要真 Env）。要断言「目录是插件建出来的还是降级注册的」，用假 provider 记录自己被问了几次——而且这样断言到的正是「引擎真的问了插件」这个不变量。

---

## 📦 提交规范

- **每个任务一个独立 commit**（大批次可按删除项再拆，11 号拆了 5 个，15 号拆了 2 个：功能 + 纯注释）；任务文档 / 交接文档与代码**分开提交**。
- 提交信息**全英文**，标题形如 `[refactor](catalog) fe-connector-api: <what changed>`。正文写清「为什么删得掉」和「为什么值得删」，删除类还要写清连带改动，**有用户可见文案变化时必须点明并说明无测试依赖旧文案**。
- 结尾附 `Co-Authored-By:` 与 `Claude-Session:` 两行。
- `gh pr edit` 在上游仓库上是坏的，改标题/正文用 REST API。

---

## 🧭 待用户拍板（未决之前不要顺手做掉）

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板八条**（详见该文件开头）：谓词下推默认值只改文档、最少实现集用注解+冻结测试、两个真实缺口顺手修、连接器自声明属性删除、建表请求的 isExternal 删除、**目录类型路由用「插件优先 + 内建名保留字」**、**建目录失败列出已安装连接器类型**、**悬空注释单开一个纯注释提交**。

**仍待拍板**：
- **建表能力下沉的报错文案变化**（18 号；同一文档还纠正了「仓库里根本不存在分桶子句的正向端到端护栏」）。
- **读侧的主键方法**（`getPrimaryKeys` + `PRIMARY_KEYS_KEY`）：建议删；若保留必须同时补契约文档并让至少一个连接器真正消费。要改 paimon 生产代码，归 12 号那一批。

---

## 🧾 顺带发现、留给后续批次

- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，是一个可删的死类（fe-core 只出不进，删除方向正确）。建议并入 12 号。
- `ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个**活**方法，且是安全相关的（哪些连接器不能声明 SHOW CREATE TABLE，否则泄露连接密码）。它是不带限定名的同名符号，**做同名清理时极易误伤**。
- 同模块还有第三个**活的** `ConnectorPartitionInfo.UNKNOWN = -1L`（hive 主源与 fe-core 都在用），与 11 号删掉的两个统计哨兵**不是**一回事。
- **`CatalogFactory` 里 `lakesoul` 那条硬失败在重放期仍会让 FE 退出**（15 号只把「引擎不认识的类型」改成降级，刻意没动它——那是「有意下线某类型」的用户可见语义）。若要一并治，单独立项。
- **`ConnectorProvider.supports(catalogType, properties)` 的按属性分派现在第一次真正可用了**（15 号之前它被外层白名单挡死，全仓 0 个覆写）。15 号只负责接通，不负责写第一个覆写。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批落地：07 + 08 + 10 三个提交 | 全反应堆含测试源 `test-compile` 通过；`fe-connector-api` 79 个单测通过；冻结测试双变异验证均变红 |
| 2026-07-25 | 修掉调研期发现的两个用户可见缺口（异构目录嵌套列 DDL、iceberg 表注释），补单测与 e2e 用例 | 单测通过；e2e 已写出但**未执行**（需真集群） |
| 2026-07-25 | 第二批落地：11 号五个提交（删 5 个类 + 6 组死字段/死方法）。动手前 22 个 agent 推翻/订正任务文档 14 条事实 | 全反应堆含测试源 `test-compile` 通过；`fe-connector-api` 83 个单测 + checkstyle 通过；冻结基线与新增断言各做一次变异验证 |
| 2026-07-25 | 第三批落地：15 号两个提交（删目录类型白名单 + 保留字检测 + 重放降级；纯注释清 20 处悬空引用）。动手前按符号复核，订正任务文档 6 条事实 | 全反应堆含测试源 `test-compile` + checkstyle 通过；52 个单测通过；**三次变异验证均如期变红**；`SPI_READY_TYPES` 全仓零命中；e2e（7 类型建目录 + hudi 读取）**未执行**，需真集群 |

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
