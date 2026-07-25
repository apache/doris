# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：五批已合入（共 22 个提交）。正确性缺陷清零；死接口面删除全部完成；下推契约已成文。**

已完成：07、08、10（写规则 / 文档据实 / 按域拆接口）、11 / 12 / 13 / 14（四批死接口面删除，**这一族到此结束**）、15（删目录类型白名单）、**09（下推契约）**、01～06 六个正确性缺陷，外加两个真实用户可见缺口（异构 HMS 目录下嵌套列 DDL、iceberg 表注释为空）。

**这一批落地后的事实变化（更新你对这条线的认知）**：

1. **公共谓词语言现在有契约了**。`fe-connector-api` 的 `pushdown` 包多了一份 `package-info.java`（六节：谁生产、总则「不能精确表达就整条放弃」、安全方向按用途反转、字面量的 8 种 Java 取值、两套残差协议的真实效力、`ConnectorFunctionCall` 兼作兜底载体）。**再写连接器谓词转换代码前先读它**；引用它比重述省事。
2. **`ConnectorScanRange` 只剩一个必须实现方法**（`getProperties()`）。分片类型枚举族整族删除，默认参数填充方法不再发 `connector_scan_range_type` 键——**这是本批唯一的有线变化**，只影响 jdbc（唯一吃默认实现的连接器），BE 与 JNI 两侧都不读那个键。
3. **`ConnectorContext` 从 19 个方法降到 18 个**：连接器通知引擎丢缓存的那套接口整套删除。失效现在只有一个方向（引擎调连接器，参数是分区**名**）。往 `ConnectorContext` 加方法仍须在 `ForwardingConnectorContext` 补转发（那条纪律不变）。
4. **建表与删库各只剩一个入口，且默认抛错而不是降级**。`createTable(session, request)` 与 `dropDatabase(session, db, ifExists, force)` 的默认实现现在直接抛「不支持」；原先的降级默认会静默丢掉分区/分桶/`IF NOT EXISTS` 与 `force`。
5. **冻结基线现在是 75 行**（原 80）：`getProperties` / 窄 `createTable` / 三参 `dropDatabase` / `getPrimaryKeys` / `listPartitionValues` 五条已从 `ConnectorMetadata` 表面消失。
6. **`ConnectorTableSchema.PRIMARY_KEYS_KEY` 不存在了**，`RESERVED_CONTROL_KEYS` 少一个成员。需要「保留键会被剥掉」的测试样本时用 `DISTRIBUTION_COLUMNS_KEY`。

**建议的下一步**：**16 + 17 + 21**（引擎里按数据源名判定的软阻塞分支中立化 + 按表能力从字符串升级为类型化集合 + 扫描节点属性键契约集中）。理由：删除族已收尾，接下来是「兑现新增连接器不改公共模块」的剩余特性级门；21 号是 19 号的前置，先做能解锁中立化那批。**从这一步起端到端是必需项而非兜底**（README 第三节写明）。

**做下一批之前必看十条（前八条沿用，9、10 是本批新增）**：

1. **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar。** 跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。
2. **checkstyle 的方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`**——**第二个字符也必须小写**，且 `test-compile` 阶段才报。
3. **冻结基线是硬约束。** `ConnectorMetadataSurfaceTest` 把 `ConnectorMetadata` 的方法签名冻结在 `fe-connector-api/src/test/resources/connector-metadata-methods.txt`（现 **75 行**）。**任何删除/新增 `ConnectorMetadata` 方法的批次必须在同一提交里重新生成它**（跑那个测试、从失败信息的 "Full actual surface:" 拷贝，**不要加 ASF 头**）。
4. **「全反应堆 test-compile 能一次证明引用全清」是错的。** 它对 javadoc `{@link}` 引用结构性失明。删除批次的 grep 清单必须包含只在注释里出现的名字。
5. **`git rm` 会立即入暂存区。** 拆提交时逐个 `git diff --cached --stat` 核对。
6. **任务文档里的「不需要变异验证」不要照信。** 连续两批的变异全部如期变红。
7. **变异验证可以一次跑完**，但要保证「一个变异对应一个测试类」，靠「失败的测试类互不重叠」做归因。做变异时连带处理 import（删掉某段可能让 import 变成 unused → `BUILD FAILURE`，一个测试都没跑）。
8. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉。** 一律 `> 日志文件 2>&1` 再 grep。
9. **从冻结基线失败信息里拷方法清单时，最后一行会粘上断言后缀。** JUnit 把 `" ==> expected: <true> but was: <false>"` 直接接在 "Full actual surface:" 列表的最后一行后面。照抄进基线文件 → 下一轮那一条永远对不上（本批踩过一次，现象是「明明只删了 5 个，却报第 6 个方法既 gone 又 added」）。拷完检查最后一行以 `)` 结尾。
10. **删接口方法后要顺手查「现在没人用的 import 和字段」。** 编译不会报 unused private field，但 checkstyle 会报 unused import；而删掉唯一读取方的私有字段（本批 hive 的 `properties`）要连带删赋值，并接受最宽构造器留一个未用形参（**不要**为此改 31 处构造点）。

---

## 📍 三句话交代这条线在解决什么

1. ~~「新增连接器不需要修改公共模块」在代码上仍是假的。~~ **`CREATE CATALOG` 路径已兑现（15 号）**。残余的是特性级门（16 / 17 / 18）。
2. ~~新连接器作者无处可依。~~ **已解决**：两个模块各有包级说明（`fe-connector-api` 七条规则），`ConnectorTableOps` 已按域拆成 6 个父接口，**谓词下推包现在也有自己的六节契约**。
3. ~~接口文档与实现大面积脱节。~~ **已解决**。本批把最后一处「文档语义与代码正好相反」（残差协议默认值）连同它在 fe-core 的镜像注释一起修掉。

---

## 🔑 动手前必须知道的五件事

1. **行号信内容不信文档。** 全部任务文档的行号以 `7ff51a106f0` 为准，且经过五批落地后**大面积作废**（10/11/12/13/14/15 与 01～06 都动过）。核对一律以符号名为准。特别注意：三个「表操作」方法在分域拆分后已不在 `ConnectorTableOps` 上，而在各自的域接口里。
2. **先读调研报告第十四节（被推翻或收窄的说法）和第十六节（明确不建议动的部分）。** 误报比漏报更毒。11 号动手前推翻/订正 14 条事实，15 号订正 6 条，第四批订正 2 条，**本批订正 4 条**（见下）。
3. **删除类任务必须全仓复扫**，且统计连接器实现分布时**按符号 grep，不要按类名模式**（trino 的元数据类叫 `TrinoConnectorDorisMetadata`）。
4. **`fe-core` 只出不进。** 本批在 fe-core 只**删**（一个类 + 一个测试类 + 若干测试断言）与**改注释**，零新增。
5. **两个连接器的类加载器钉桩包装类现在只剩 `executeAuthenticated`**（iceberg 另有一个包私有的认证器访问器）；`ConnectorContext` 的转发已收到 `ForwardingConnectorContext`。**23 号的前置就位。**

---

## ⚙️ 构建与验证的坑（实测，直接复用，别再踩）

1. **全反应堆 `test-compile` 必须排除两个 shade 模块**，否则 hive 相关模块必然编译失败（`package org.apache.hadoop.hive.conf does not exist`）。**这与你的改动无关，不要去 debug 它**：
   ```
   mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
     -pl '!fe-connector/fe-connector-hms-hive-shade,!fe-connector/fe-connector-paimon-hive-shade' \
     -Dmaven.build.cache.enabled=false -T1C test-compile
   ```
   跑测试就在同一条命令后面加 `test -Dtest='<类名清单>' -Dsurefire.failIfNoSpecifiedTests=false`（一轮约 4～6 分钟）。
2. **maven build cache 会静默跳过测试执行**：所有跑测试的命令都要加 `-Dmaven.build.cache.enabled=false`。`fe/.mvn/maven.config` 是**未跟踪文件**，不要依赖它。
3. **`mvn ... | tail` 之后的 `$?` 是 `tail` 的**；读 `BUILD SUCCESS` / `BUILD FAILURE` 行，且**不要用 `| tail` 截断**。
4. **maven 一律用绝对路径 `-f`**，且**不要用 `-pl` 缩到单模块**。例外：跑**整个**模块的全部测试时可以用 `-pl <模块清单>`（本批这样跑过 api/spi/jdbc/es/paimon/hudi/maxcompute/trino 八个模块共 634 个测试，全绿）。
5. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
6. **严禁 `git add -A`**（工作树有大量历史遗留临时文件与含明文密钥的配置），一律 path-whitelist。
7. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**，用 `assertEquals` 之类自包含断言。
8. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**（建表路径），与本线改动无关。**别把它当成自己改坏了**，也别顺手修，更**不能拿它当变异验证的判据**。
9. **`checkstyle:check` 会随 `test-compile` 一起跑**（`validate` 阶段），不需要单独跑；它扫测试源，方法名、行长（120）、unused import 都算。仓库的 checkstyle **没有开 JavadocStyle**，所以 javadoc 里的 HTML 表格不会被校验——但 `{@code <ch>}` 这种尖括号仍建议避开（写成 `{@code ESCAPE '!'}`）。
10. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了（要真 Env）。
11. **hudi 单测里的 `stub(...)` 执行器会把整个 metaClient lambda 换掉**，所以 `collectPartitions` 里 lambda 内部的逻辑单测打不到。**别以为 `HudiConnectorPartitionListingTest` 绿了就等于分区列举全路径被覆盖了。**

---

## 📦 提交规范

- **每个任务一个独立 commit**（大批次可按删除项再拆）；任务文档 / 交接文档与代码**分开提交**。
- 提交信息**全英文**，标题形如 `[fix](catalog) fe-connector-xxx: <what changed>` 或 `[refactor](catalog) …` / `[doc](catalog) …`。正文写清「为什么是错的 / 为什么删得掉」和「为什么值得改」，**有用户可见文案或行为变化时必须点明并说明无测试依赖旧行为**，**有测试覆盖不到的部分必须写出来**。
- 结尾附 `Co-Authored-By:` 与 `Claude-Session:` 两行。
- `gh pr edit` 在上游仓库上是坏的，改标题/正文用 REST API。

---

## 🧭 待用户拍板（未决之前不要顺手做掉）

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板十五条**（本批新增七条：主键接口删除、下推契约不加工具方法、hudi 表函数新鲜度只改注释、引擎侧镜像注释一并改、hive 死字段留未用形参、分片格式键与「零必须实现方法」都不做、iceberg 七处断言删掉）。

**仍待拍板**：
- **含隐式类型转换的谓词下推默认值**（08 号）：`supportsCastPredicatePushdown` 默认 `true`，而它承诺的「引擎会先剥掉类型转换」只对残余谓词那条路径成立。翻成 `false` 是跨六个连接器的行为改动。**本批已把这条差异写进 `pushdown` 包级说明的 Rule 4**，所以现在至少是「文档正确、默认值待议」。
- **建表能力下沉的报错文案变化**（18 号；同一文档还纠正了「仓库里根本不存在分桶子句的正向端到端护栏」）。

---

## 🧾 顺带发现、留给后续批次

- **hudi 的 `partition_values()` 可能落后一个缓存过期时间**（经 `listPartitions` 读缓存）。本批只把注释与测试改成据实描述，**是否改成绕缓存取最新是独立一项**。判据已写在 `HudiConnectorHmsCacheTest` 类注释里，别再从注释反推出错的映射。
- **es 有一条与 01/02/03 同根因、尚未修的缺陷**：`EsQueryDslBuilder` 把 Doris 的 `REGEXP` 模式串原样交给 ES 的 `regexp` 查询，而 Doris 的 regexp 是部分匹配、Lucene 的是整串锚定 → 少行。**本批已把「`REGEXP` 未锚定、整串锚定的远端不是合法直传目标」写进 `ConnectorLike` 的契约**，所以这条现在有明确的判据可引用。ES 侧本来就有干净的 `notPushDownList` 拒绝机制，修起来有落点。
- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，可删的死类。**12 号没有把它带上**（那批的清单是四组 SPI 方法），单独立项即可。
- **`ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个活方法**，且是安全相关的。它是不带限定名的同名符号，**做同名清理时极易误伤**。
- 同模块还有第三个**活的** `ConnectorPartitionInfo.UNKNOWN = -1L`（hive 主源与 fe-core 都在用）。
- **`CatalogFactory` 里 `lakesoul` 那条硬失败在重放期仍会让 FE 退出**（15 号刻意没动）。若要一并治，单独立项。
- **`ConnectorScanRange.getLength()` 是「契约写了单位、实现各行其是」的第三处**（契约写字节数、maxcompute 在行偏移模式返回行数）。05 号立的规矩可以直接套：公共接口凡写了单位的字段，都该有一条量级单测钉住。
- **`ConnectorSession.getStatementScope` 默认不记忆**，与 06 号根治的是同一机理的另一处（有默认值就会静默关掉性能优化）。
- **早期计划文档里「HMS 事件管线通过连接器回调接口通知引擎」那条旧决策已被本批作废**（`plan-doc/decisions-log.md` 的相关条目、`plan-doc/01-spi-extensions-rfc.md` 第 6 节）。按 14 号的约定：**补一句作废说明即可，不要改写历史进度记录**；这件事留给 25 号（历史文档勘误）一并做。
- **两套残差协议仍未合并**（`remainingFilter` 与 not-pushed 下标）。本批只把「非 null 残差 → 引擎一个都不摘」写清；真正合并的前置是实现细粒度反查（把残差子表达式对回原始 conjunct），那是行为改动。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批落地：07 + 08 + 10 三个提交 | 全反应堆含测试源 `test-compile` 通过；79 个单测通过；冻结测试双变异验证均变红 |
| 2026-07-25 | 修掉调研期发现的两个用户可见缺口（异构目录嵌套列 DDL、iceberg 表注释），补单测与 e2e 用例 | 单测通过；e2e 已写出但**未执行**（需真集群） |
| 2026-07-25 | 第二批落地：11 号五个提交（删 5 个类 + 6 组死字段/死方法）。动手前 22 个 agent 推翻/订正任务文档 14 条事实 | `test-compile` 通过；83 个单测 + checkstyle 通过；两次变异验证 |
| 2026-07-25 | 第三批落地：15 号两个提交（删目录类型白名单 + 保留字检测 + 重放降级；纯注释清 20 处悬空引用） | `test-compile` + checkstyle 通过；52 个单测通过；三次变异验证均如期变红 |
| 2026-07-25 | 第四批落地：01～06 六个正确性缺陷，6 个独立提交 | `test-compile` + checkstyle **BUILD SUCCESS**；27 个测试类全绿；**8 个变异一轮跑完全部被预期测试类捕获**；2 个新 e2e 用例**未执行** |
| 2026-07-25 | **第五批落地：09 + 14 + 13 + 12 四个提交**（下推契约成文；删反方向缓存失效 SPI；删分片类型枚举族；删四组零消费 SPI 并重新生成冻结基线 80→75）。动手前按符号在 HEAD 上逐条复核，订正任务文档 4 条事实 | 全反应堆含测试源 `test-compile` + checkstyle **BUILD SUCCESS**；八个受影响模块**全量**单测 634 个全绿（api 102 / spi 6 / 另六模块 526）；**4 个变异全部由预期的测试类捕获**（含新增的默认参数填充测试）；无需新增 e2e（唯一有线变化是 jdbc 少发一个 BE 不读的键，已被单测钉住） |

### 本批订正的任务文档事实（四条）

1. **14 号说要删「iceberg / paimon 两个包装类里各一行转发」——已过时。** 上一批引入 `ForwardingConnectorContext` 之后，那两个类只剩认证方法，转发收在公共基类里。真实删除点是**公共基类一处** + `fe-connector-spi` 的包级说明（它把这个接口列为引擎提供的服务之一，不改会留下悬空 `{@link}`）。
2. **09 号说 `pushdown` 包有 18 个类、含一个 limit 应答载体——现在是 17 个**，那个载体已在 11 号删掉。契约文档不能再引用它。
3. **09 号的「可选工具方法」性质已变。** 03 号修复时已在 paimon 内部落了私有守卫，所以这一项现在等价于「把私有守卫上提成公共 API」——据此拍板不做（见 open-decisions 第 10 条）。
4. **12 号的行号与归属全部作废**：三个待删方法在分域拆分后分别落在 `ConnectorPartitionListingOps` / `ConnectorTableDdlOps` / `ConnectorTableMetadataOps` 上；而且**冻结基线包含继承来的方法**，任务文档没提到要重新生成它（漏了这一步测试就会红）。另外 `ConnectorPartitionListingOps` 的类注释里已经写着「不要实现分区值枚举」（分域拆分时加的），删方法时要一并清掉那段。

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
