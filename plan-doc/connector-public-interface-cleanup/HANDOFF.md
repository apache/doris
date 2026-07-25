# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：六批已合入（共 26 个提交）。正确性缺陷清零；死接口面删除全部完成；下推契约成文；扫描属性键契约集中；引擎里按数据源名判定的四处分支全部中立化。**

已完成：07、08、10、11 / 12 / 13 / 14、15、09、01～06、两个真实用户可见缺口，**外加本批的 21（属性键契约集中）与 16（四处按源名判定的分支中立化）**。

**这一批落地后的事实变化（更新你对这条线的认知）**：

1. **扫描节点属性表的键现在有一个公共常量类**：`fe-connector-api` 的 `scan/ScanNodePropertyKeys.java`。它收录**引擎会读的键**（格式、目录分区列、`location.` 前缀、EXPLAIN 的远端 query、文本族 12 个后缀）与**引擎注入给 EXPLAIN 的 3 个合成键**。**字面量一个字节都没改**（`hive.text.` 前缀保留原值、只把符号名中立化）。以后连接器写属性一律引用它；连接器私有键（`paimon.*`、es 那批、hive 的 `transactional_hive`）仍留在各自连接器里。
2. **`ScanNodePropertiesResult` 的构造器已私有化**，改 `of(props)` / `withPushdownTracking(props, notPushed)` 两个具名工厂。「有没有做逐 conjunct 追踪」不再靠「调了哪个构造器」隐式编码。
3. **`ConnectorScanPlanProvider.getSerializedTable(Map)` 已删除**。paimon 改成在自己的 `populateScanLevelParams` 里直接 `params.setSerializedTable(...)`；`fe-core` 的 `FileQueryScanNode.getSerializedTable()` 钩子与 `PluginDrivenScanNode` 的覆写一并删除（纯减）。
4. **两个返回面的规矩写进 javadoc 了**：引擎只调 `getScanNodePropertiesResult`，其默认实现委派 `Map` 面；**连接器只应覆写其中一个**。es 那个从引擎侧不可达的 `Map` 面覆写已删。
5. **`ConnectorProvider` 多了两个默认方法**：`providesEventSource()`（默认 false）与 `defaultDatabaseOnUse()`（默认空）。它们挂在 **provider** 而不是 `Connector` 上是刻意的——这两个判定发生在目录**可能尚未初始化**的时刻，碰连接器实例会强制初始化。
6. **`fe-core` 多了一个中立查表入口**：`ConnectorFactory.findProvider(type, props)` / `ConnectorPluginManager.findProvider(...)`，按类型找 provider 但**不建连接器**。以后凡是「未初始化时刻要问连接器一句话」的需求都走它。
7. **`ConnectorScanPlanProvider.supportsFileCache()`（默认 false）取代了 `CACHEABLE_CATALOGS` 白名单**。判据从「目录类型名」变成「服务这张表的连接器声明」，因此在异构目录里是**按表句柄**解析的。hive / iceberg / paimon / **hudi** 声明 true，maxcompute / trino / jdbc / es 保持 false。`FileQueryScanNode` 留了一个默认 false 的 `isFileCacheAdmissionApplicable()` 钩子（TVF / 远程 Doris 扫描节点照旧在治理之外）。
8. **`SummaryProfile` 的 `ICEBERG_SCAN_METRICS` / `PAIMON_SCAN_METRICS` 已删**（用户拍板）。每条查询的执行摘要少两行恒为 `N/A` 的条目；真正的扫描指标子树不受影响（它不依赖这两个常量）。连接器侧「必须与 fe-core 常量一致」的注释是错的，已改。
9. **`MetastoreEventSyncDriver` 多了一个包私有方法 `seedCursorOfUninitializedCatalog`**（把预热判定+动作从 `realRun` 里提出来，行为不变），这是为了让「空闲目录不得被强制初始化」这条守卫**可被单测直接验证**。

**建议的下一步**：**17（按表能力类型化 + 删写特性镜像方法）**。它是 16/21 之后剩下的那条「能力声明形状不对」的线，改动面最大（约 20 个文件、建议三个提交），且与已完成部分无耦合。做完 17 之后按 README 第三节继续 19 / 20 / 22。

**做下一批之前必看十二条（前十条沿用，11、12 是本批新增）**：

1. **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar。** 跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。
2. **checkstyle 的方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`**——**第二个字符也必须小写**，且 `test-compile` 阶段才报。
3. **冻结基线是硬约束。** `ConnectorMetadataSurfaceTest` 把 `ConnectorMetadata` 的方法签名冻结在 `fe-connector-api/src/test/resources/connector-metadata-methods.txt`（现 **75 行**）。**任何删除/新增 `ConnectorMetadata` 方法的批次必须在同一提交里重新生成它**。注意：**它只冻结 `ConnectorMetadata`**——17 号要删的是 `Connector` 上的 11 个方法，**不需要动这个基线**（任务文档没写，容易误以为要动）。
4. **「全反应堆 test-compile 能一次证明引用全清」是错的。** 它对 javadoc `{@link}` 引用结构性失明。删除批次的 grep 清单必须包含只在注释里出现的名字。
5. **`git rm` 会立即入暂存区。** 拆提交时逐个 `git diff --cached --stat` 核对。
6. **任务文档里的「不需要变异验证」不要照信。** 连续三批的变异全部如期变红。
7. **变异验证可以一次跑完**，但要保证「一个变异对应一个测试类」，靠「失败的测试类互不重叠」做归因。
8. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉。** 一律 `> 日志文件 2>&1` 再 grep。
9. **从冻结基线失败信息里拷方法清单时，最后一行会粘上断言后缀**，拷完检查最后一行以 `)` 结尾。
10. **删接口方法后要顺手查「现在没人用的 import 和字段」**（编译不报 unused private field，但 checkstyle 报 unused import）。
11. **一个提交里两半改动交织时，不要硬拆。** 21 号任务文档建议拆两个提交，实际三份文件（接口、通用扫描节点、paimon）同时承载两半，硬拆会产生**没被测过的中间态**。本批的判断是：合成一个提交、在提交信息里讲清两半，**优于**为拆而拆。真要拆，就在动手前按提交顺序分两轮改+两轮验证。
12. **`Env.java` 里 `catalogIf` 是裸类型 `CatalogIf`**，`catalogIf.getProperties()` 返回**裸 `Map`**；直接把它传给带泛型的方法会让整个调用变成 unchecked、返回值退化成裸 `Optional`，于是 `flatMap(X::method)` 报 "invalid method reference"。先赋给一个 `Map<String, String>` 局部变量再传。

---

## 📍 三句话交代这条线在解决什么

1. ~~「新增连接器不需要修改公共模块」在代码上仍是假的。~~ **`CREATE CATALOG` 路径已兑现（15 号），四处特性级软阻塞已中立化（16 号）**。残余的是建表能力（18 号）与能力声明形状（17 号）。
2. ~~新连接器作者无处可依。~~ **已解决**：两个模块各有包级说明，`ConnectorTableOps` 已按域拆分，谓词下推包有六节契约，**扫描属性表的键契约现在也有一个常量类**。
3. ~~接口文档与实现大面积脱节。~~ **已解决**。本批又清掉两处（两个返回面缺的「只覆写一个」说明、连接器侧「必须与 fe-core 剖析常量一致」的假耦合）。

---

## 🔑 动手前必须知道的五件事

1. **行号信内容不信文档。** 全部任务文档的行号以 `7ff51a106f0` 为准，经过六批落地后**大面积作废**。核对一律以符号名为准。
2. **先读调研报告第十四节（被推翻或收窄的说法）和第十六节（明确不建议动的部分）。** 误报比漏报更毒。本批订正 3 条（见下）。
3. **删除类任务必须全仓复扫**，且统计连接器实现分布时**按符号 grep，不要按类名模式**。
4. **`fe-core` 只出不进。** 本批在 fe-core 净减（删白名单、删两个剖析常量、删一个钩子），新增的只有一个**中立**的 provider 查表入口，它的存在是为了删掉两个数据源名。
5. **17 号的三批顺序不能颠倒**：必须**先让 hive 收窄反射子集**（行为不变），**再**扩大引擎按表读取范围。反过来会在没人评审的情况下改变 iceberg-on-HMS 表的 `SHOW CREATE TABLE` / 视图判定 / 元数据预载行为。

---

## ⚙️ 构建与验证的坑（实测，直接复用，别再踩）

1. **全反应堆 `test-compile` 必须排除两个 shade 模块**，否则 hive 相关模块必然编译失败（`package org.apache.hadoop.hive.conf does not exist`）。**这与你的改动无关，不要去 debug 它**：
   ```
   mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
     -pl '!fe-connector/fe-connector-hms-hive-shade,!fe-connector/fe-connector-paimon-hive-shade' \
     -Dmaven.build.cache.enabled=false -T1C test-compile
   ```
   跑测试就在同一条命令后面加 `test -Dtest='<类名清单>' -Dsurefire.failIfNoSpecifiedTests=false`（一轮约 4～6 分钟）。
2. **maven build cache 会静默跳过测试执行**：所有跑测试的命令都要加 `-Dmaven.build.cache.enabled=false`。
3. **`mvn ... | tail` 之后的 `$?` 是 `tail` 的**；读 `BUILD SUCCESS` / `BUILD FAILURE` 行。
4. **maven 一律用绝对路径 `-f`**，且**不要用 `-pl` 缩到单模块**。
5. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
6. **严禁 `git add -A`**（工作树有大量历史遗留临时文件与含明文密钥的配置），一律 path-whitelist。
7. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**。
8. **`HiveConnectorMetadataDdlTest` 在本分支上本来就是红的**（建表路径），与本线改动无关。
9. **`checkstyle:check` 会随 `test-compile` 一起跑**（`validate` 阶段），不需要单独跑；它扫测试源。
10. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了（要真 Env）。
11. **hudi 单测里的 `stub(...)` 执行器会把整个 metaClient lambda 换掉**，别以为 `HudiConnectorPartitionListingTest` 绿了就等于分区列举全路径被覆盖。
12. **fe-core 测试里注私有字段用 `org.apache.doris.common.jmockit.Deencapsulation`（仓库自带），不是 `mockit.Deencapsulation`**（后者不在 fe-core 的测试依赖里，会报 `package mockit does not exist`）。
13. **测不了的东西就直说。** 本批 `Env.changeCatalog` 那四行没有单测（需要真会话），已在提交信息里点明并列为待补 e2e——不要为了「有测试」硬造一个只断言 mock 的空壳。
14. **`PluginDrivenScanNode` 的单测范式**：`Mockito.mock(节点.class, CALLS_REAL_METHODS)` + `Deencapsulation.setField(node, "connector"/"currentHandle", …)`，然后 `Deencapsulation.invoke(node, "方法名")`。`PluginDrivenScanNodeScanProviderSelectionTest` 与本批新增的 `PluginDrivenScanNodeFileCacheAdmissionTest` 都是这个形状。

---

## 📦 提交规范

- **每个任务一个独立 commit**（大批次可按删除项再拆）；任务文档 / 交接文档与代码**分开提交**。
- 提交信息**全英文**，标题形如 `[fix](catalog) fe-connector-xxx: <what changed>` 或 `[refactor](catalog) …` / `[doc](catalog) …`。正文写清「为什么是错的 / 为什么删得掉」和「为什么值得改」，**有用户可见文案或行为变化时必须点明并说明无测试依赖旧行为**，**有测试覆盖不到的部分必须写出来**。
- 结尾附 `Co-Authored-By:` 与 `Claude-Session:` 两行。
- `gh pr edit` 在上游仓库上是坏的，改标题/正文用 REST API。

---

## 🧭 待用户拍板（未决之前不要顺手做掉）

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板十六条**（本批新增一条：**剖析里两行恒为 `N/A` 的条目直接删除**，用户 2026-07-26 选「删掉」）。

**仍待拍板**：
- **含隐式类型转换的谓词下推默认值**（08 号）：`supportsCastPredicatePushdown` 默认 `true`，而它承诺的「引擎会先剥掉类型转换」只对残余谓词那条路径成立。翻成 `false` 是跨六个连接器的行为改动。文档已据实（`pushdown` 包级说明 Rule 4）。
- **建表能力下沉的报错文案变化**（18 号；同一文档还纠正了「仓库里根本不存在分桶子句的正向端到端护栏」）。

---

## 🧾 顺带发现、留给后续批次

- **本批新欠三条 e2e**（都需要真集群，本地跑不了；本批的单测只覆盖 FE 侧装配）：
  1. **paimon 目录查询回归**——`serialized_table` 改由 paimon 自己设进 thrift，BE 侧缺这个字段是**硬失败**（`missing serialized_table`）；
  2. **hive 文本 / CSV / JSON 表读回归**——`hive.text.*` 的 12 个后缀经过一次符号替换，打错一个字母的表现是「该属性静默失效」（例如列分隔符回退默认值 → 整行挤在第一列）；
  3. **文件缓存准入 + `SWITCH <es 目录>` + 事件同步预热**——异构 HMS 目录（hive+iceberg+hudi）配库级准入规则后规则仍生效；`SWITCH es 目录` 后 `SELECT DATABASE()` 仍是 `default_db`；一个 hms 目录在 FE 重启后不查询、直接等事件同步。
- **两个只写不读的属性键仍在**（paimon/hudi 的 `table_format_type`、es 的 `_table_name`、hive 的 `hive.text.json_serde_lib`）。21 号刻意没删——判活需要单独核对 BE 与 JNI 侧，混进纯结构调整会变成有行为风险的删除。**单独立项**。
- **hudi 的 `partition_values()` 可能落后一个缓存过期时间**（经 `listPartitions` 读缓存）。是否改成绕缓存取最新是独立一项。
- **es 有一条与 01/02/03 同根因、尚未修的缺陷**：`EsQueryDslBuilder` 把 Doris 的 `REGEXP` 模式串原样交给 ES 的 `regexp` 查询（Doris 是部分匹配、Lucene 是整串锚定）→ 少行。判据已写进 `ConnectorLike` 契约，ES 侧有干净的 `notPushDownList` 拒绝机制可落点。
- `fe-core` 的 `org.apache.doris.connector.ConnectorMvccSnapshotAdapter` **全仓库零引用**，可删的死类，单独立项。
- **`ConnectorCapability` 里那句 `{@code getTableProperties()}` 指的是 fe-core 那个活方法**，且是安全相关的，做同名清理时极易误伤。
- **`CatalogFactory` 里 `lakesoul` 那条硬失败在重放期仍会让 FE 退出**（15 号刻意没动）。
- **`ConnectorScanRange.getLength()` 是「契约写了单位、实现各行其是」的第三处**（契约写字节数、maxcompute 在行偏移模式返回行数）。05 号立的规矩可以直接套。
- **`ConnectorSession.getStatementScope` 默认不记忆**，与 06 号根治的是同一机理的另一处。
- **早期计划文档里「HMS 事件管线通过连接器回调接口通知引擎」那条旧决策已被 14 号作废**；留给 25 号（历史文档勘误）一并补作废说明。
- **两套残差协议仍未合并**（`remainingFilter` 与 not-pushed 下标）。真正合并的前置是实现细粒度反查。

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
| 2026-07-26 | **第六批：21 一个提交（属性键契约集中 + 具名工厂 + 删 `getSerializedTable`）+ 16 三个提交（provider 声明事件源与默认库 / 文件缓存能力位 / 删两个剖析常量）** | 两轮全反应堆含测试源 `test-compile` + checkstyle **BUILD SUCCESS**；21 号 19 个测试类全绿、16 号 14 个测试类全绿（其后重跑 9 个）；**5 个变异全部由预期的测试类捕获**（paimon 序列化表 1 个 + 本批 4 个）；新增 4 个测试类；欠 3 条 e2e（见上） |

### 本批订正的任务文档事实（三条）

1. **21 号说 `getSerializedTable()` 在 fe-core 侧返回 `String`——实际是 `Optional<String>`**，基类的调用点写法是 `getSerializedTable().ifPresent(params::setSerializedTable)`。
2. **21 号第六节的「零残留 grep」只覆盖 `src/main`**：三个合成键的裸字面量在 paimon 的 EXPLAIN 测试里还有 20 余处（那是测试自己构造 props，保留是对的），验证时别把测试源的命中当成漏改。
3. **17 号的前置检查里少一条**：删 `Connector` 上 11 个镜像方法**不需要**重新生成冻结基线（基线只冻结 `ConnectorMetadata`）。

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
