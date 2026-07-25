# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：两批已合入（共 10 个提交）。公共接口第一次净变小。**

已完成：07（写下规则）、08（文档据实）、10（按域拆接口）、**11（删第一批死接口面，5 个提交）**，外加调研期发现的两个真实用户可见缺口已修（异构 HMS 目录下嵌套列 DDL、iceberg 表注释为空）。

**建议的下一步**：**15**（删目录类型白名单）。理由：它是这条工作线的核心——在它合入之前，「新增连接器不需要修改公共模块」这句话在代码上是假的；而 11 号已经把删除批次的判据和验证套路跑通了。若想先清小项，**12/13/14**（需连带改连接器的删除）也已解锁。

**做下一批之前必看四条（11 号实测，比任务文档新）**：

1. **冻结基线是硬约束，任务文档里没有它。** `ConnectorMetadataSurfaceTest` 把 `ConnectorMetadata` 的方法签名冻结在 `fe-connector-api/src/test/resources/connector-metadata-methods.txt`（11 号删掉 `applyLimit` 后现为 **80 行**）。**任何删除/新增 SPI 方法的批次必须在同一提交里重新生成它**，做法是跑那个测试、从失败信息的 "Full actual surface:" 拷贝。**那个文件没有 ASF 头，重新生成时不要加**（`readBaseline` 只跳空行，16 行头会变成 16 条幽灵签名）。已双向变异验证过。
2. **「全反应堆 test-compile 能一次证明引用全清」是错的。** 它对 javadoc `{@link}` 引用结构性失明（fe-core 的 javadoc 插件是 `<skip>true</skip>`）。11 号就踩到：`PluginDrivenScanNode.pinMvccSnapshot()` 的注释里链着被删的 `tryPushDownLimit`。**删除批次的 grep 清单必须包含只在注释里出现的名字。**
3. **`git rm` 会立即入暂存区。** 多提交拆分时，先 `git rm` 再 `git add <另一批文件>` 会把删除卷进错误的提交，且那个提交自己编译不过。11 号因此重做过一次。**拆提交时逐个 `git diff --cached --stat` 核对。**
4. **`-pl fe-core` 单模块构建解析不了 `${revision}`**（`fe-authentication` 的 pom 解析失败）。要跑 fe-core 的测试只能走全反应堆 + `-Dtest=` 过滤 + `-Dsurefire.failIfNoSpecifiedTests=false`。

---

## 📍 三句话交代这条线在解决什么

1. **「新增连接器不需要修改公共模块」这句话今天在代码上仍是假的。** `CatalogFactory` 里有一个写死的七元素类型白名单，第三方连接器即使正确注册并成功装配，`CREATE CATALOG` 也不会走到它。这是 15 号任务，**仍未做**，是这条线的核心。
2. ~~新连接器作者无处可依。~~ **已解决**：两个模块各有一份包级说明。`fe-connector-api` 现有**七条规则**（能力声明分层、异常族、thrift 边界、模块划分、生命周期与线程模型、引擎行为须引证、**连接器旋钮该放哪**）；`ConnectorTableOps` 已按域拆成 6 个父接口，每域写清「最少实现集」，并有注解 + 冻结测试钉住。
3. ~~接口文档与实现大面积脱节。~~ **已解决**（08 号那九处 + 11 号顺带修正的若干处）。剩余的文档问题在 09 号（下推表达式契约）。

---

## 🔑 动手前必须知道的五件事

1. **行号信内容不信文档。** 全部任务文档的行号以 `7ff51a106f0` 为准；10 号和 11 号落地后，`ConnectorTableOps`、`Connector`、`ConnectorPartitionSpec`、`ConnectorCreateTableRequest`、`ConnectorMvccSnapshot`、`ConnectorPushdownOps`、`PluginDrivenScanNode` 的行号都已作废。核对一律以符号名为准。
2. **先读调研报告第十四节（被推翻或收窄的说法）和第十六节（明确不建议动的部分）。** 误报比漏报更毒。11 号动手前的复核推翻/订正了那份任务文档 14 条事实，**这类文档不能直接照做**，动手前必须按符号重扫一遍。
3. **删除类任务必须全仓复扫**，且统计连接器实现分布时**按符号 grep，不要按类名模式**（trino 的元数据类叫 `TrinoConnectorDorisMetadata`）。
4. **`fe-core` 只出不进。**（往 fe-core 测试源补一条守护断言不违反这条——11 号补过一条，因为删掉的三条断言只护着死的那一半。）
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
4. **maven 一律用绝对路径 `-f`**（跨工具调用工作目录会持久）。
5. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
6. **严禁 `git add -A`**（工作树有大量历史遗留临时文件与含明文密钥的配置），一律 path-whitelist。
7. **e2e（groovy）需要真集群，本地跑不了**。
8. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**：**19 个用例 / 5 failures + 7 errors**（建表路径），与本线改动无关。11 号改过这个文件（去实参）后**逐数字不变**，可作为下次的对照基线。**别把它当成自己改坏了**，也别顺手修（属另一条线），更**不能拿它当变异验证的判据**。
9. **`checkstyle:check` 值得单独跑一次**（`UnusedImports` 会因删字段而孤立 import，`test-compile` 不报）。

---

## 📦 提交规范

- **每个任务一个独立 commit**（大批次可按删除项再拆，11 号拆了 5 个）；任务文档 / 交接文档与代码**分开提交**。
- 提交信息**全英文**，标题形如 `[refactor](catalog) fe-connector-api: <what changed>`。正文写清「为什么删得掉」和「为什么值得删」，删除类还要写清连带改动。
- 结尾附 `Co-Authored-By:` 与 `Claude-Session:` 两行。
- `gh pr edit` 在上游仓库上是坏的，改标题/正文用 REST API。

---

## 🧭 待用户拍板（未决之前不要顺手做掉）

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板五条**（详见该文件开头）：谓词下推默认值只改文档、最少实现集用注解+冻结测试、两个真实缺口顺手修、**连接器自声明属性删除**、**建表请求的 isExternal 删除**。

**仍待拍板**：
- **建表能力下沉的报错文案变化**（18 号；同一文档还纠正了「仓库里根本不存在分桶子句的正向端到端护栏」）。
- **插件与内建目录类型的路由优先级**（15 号，文档采纳「插件优先」）。
- **读侧的主键方法**（`getPrimaryKeys` + `PRIMARY_KEYS_KEY`）：建议删；若保留必须同时补契约文档并让至少一个连接器真正消费。要改 paimon 生产代码，归 12 号那一批。

---

## 🧾 顺带发现、留给后续批次

- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，是一个可删的死类（fe-core 只出不进，删除方向正确）。建议并入 12 号。
- `ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个**活**方法，且是安全相关的（哪些连接器不能声明 SHOW CREATE TABLE，否则泄露连接密码）。它是不带限定名的同名符号，**做同名清理时极易误伤**。
- 同模块还有第三个**活的** `ConnectorPartitionInfo.UNKNOWN = -1L`（hive 主源与 fe-core 都在用），与 11 号删掉的两个统计哨兵**不是**一回事。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批落地：07 + 08 + 10 三个提交；动手前用 11 个 agent 复核三份文档的全部事实断言，推翻/收窄 9 条 | 全反应堆含测试源 `test-compile` 通过；`fe-connector-api` 79 个单测通过；冻结测试双变异验证均变红 |
| 2026-07-25 | 修掉调研期发现的两个用户可见缺口（异构目录嵌套列 DDL、iceberg 表注释），补单测与 e2e 用例 | 单测通过；e2e 已写出但**未执行**（需真集群） |
| 2026-07-25 | 第二批落地：11 号五个提交（删 5 个类 + 6 组死字段/死方法）。动手前 22 个 agent（11 项事实核查 + 11 项对抗反驳）推翻/订正任务文档 14 条事实 | 全反应堆含测试源 `test-compile` 通过；`fe-connector-api` 83 个单测 + checkstyle 通过；受影响连接器与 fe-core 单测全绿；冻结基线与新增的分区值断言各做一次变异验证、均如期变红；`HiveConnectorMetadataDdlTest` 的既有红判逐数字不变 |

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
