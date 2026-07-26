# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🔥 构建命令（照抄，别用更早版本的写法）

```bash
# ① 全反应堆含测试源编译 + 跑指定测试（checkstyle 摘出去）
mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
  -pl '!fe-connector/fe-connector-hms-hive-shade,!fe-connector/fe-connector-paimon-hive-shade' \
  -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -T1C \
  test-compile test -Dtest='<类名清单>' -Dsurefire.failIfNoSpecifiedTests=false

# ② checkstyle 只对本次真正改动的模块单独跑
mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml \
  -pl <改动的模块清单> -Dmaven.build.cache.enabled=false checkstyle:check
```

`-pl` 缩到单模块对 `checkstyle:check` **安全**；对 `compile` / `test-compile` **不安全**（本轮又踩了一次：`-pl fe-core compile` 直接报 `Could not resolve dependencies ... fe-authentication:pom:${revision}`）。

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单 → 挑中的那个任务自己的文档。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：十二批已合入（共 48 个提交）。25 个编号任务全部完成——本轮做掉最后一个（23 号，引擎上下文里的存储服务拆分）。**

### 本轮（23 号）落地

`ConnectorContext` 的 18 个方法里有 11 个是存储与 BE 侧的事，javadoc 占了整个文件的三分之二，而 jdbc / es / maxcompute / trino 四个连接器一个都用不到。这 11 个连 javadoc 原样搬进新的 `ConnectorStorageContext`，经 `ConnectorContext.getStorageContext()` 取得（默认 `NOOP`，永不返回 null）。

- **零行为变化**：默认值随方法一起搬，连接器拿到的答案逐字不变。
- **引擎侧不搬代码**：`DefaultConnectorContext` 同时实现两个接口、返回 `this`（拆成两个类会把带锁的文件系统缓存和 `close()` 生命周期一起搬走，SPI 看不到任何收益）。
- **转发基类从 11 个转发塌成 1 个**，以后再加存储服务在结构上不可能漏掉；两个钉桩包装类**零改动**。
- **顺带改名** `sanitizeJdbcUrl` → `sanitizeOutboundUrl`，并把 MUST 契约收窄到「连接器自行建连时」——iceberg 的 JDBC 元存储、paimon 的 JDBC 目录都把地址交给第三方 SDK 内部建连，连接器没有钩子点（与上游一致，不是迁移引入的）。

### 23 号任务文档三条前置结论中的两条不成立（本轮实证）

1. ~~「高危：接口一动两个钉桩包装类都要跟着改」~~ **已过期**。那是 06 号（转发基类）合入前写的；今天两个包装类只覆写 `executeAuthenticated`，存储一处不碰，本次**零改动**。风险实为「中」。
2. ~~「`sanitizeJdbcUrl` 全仓 7 处」~~ 实为 5 处（同上，包装类不再各占 2 行）。
3. **35 个调用点、8 个测试替身文件的数字准确**，与文档一致。

### 下一步

- README 里「复核登记的开放项」表还剩 9 条（死接口幸存者 `estimateScanRangeCount`、SQL 直通仍在共享接口上、建库布尔位手工同步、两个属性命名空间静默合并、`planScan` 4 重载、`getWriteContext()` 名不符实、`getLength()` 单位未澄清、读事务生命周期矛盾、契约校验器缺 hive 正样本）。
- 逐个连接器接入 `renderShowCreateTableDdl`（今天只有 hive 在用）——各自独立排期，不是 fe-core 重构。
- **积压的端到端**（见下）——需要集群。

---

## 📌 第十二批落地后的事实变化

1. **`ConnectorContext` 只剩 7 个方法 + 一个 `getStorageContext()`**（404 行 → 155 行）。新增存储服务加在 `ConnectorStorageContext`，**不要加回 `ConnectorContext`**（两个接口的注释都写了）。
2. **钉桩失效的残留风险写在两处注释里**：今天没有任何存储方法跑插件代码（全在引擎侧执行），这正是钉桩包装类从不包存储的原因；将来若有一个会跑插件代码，钉桩子类必须覆写 `getStorageContext()` 返回自己的包装。
3. **测试替身的静默退化是本次唯一隐性风险**：替身实现了 `ConnectorStorageContext` 却忘写 `getStorageContext()` 时能编译过，但覆写全变死代码、连接器拿到 `NOOP`。本轮做了变异验证：三个 `RecordingConnectorContext` 各删掉那个 getter → **32 个用例变红**（另有 13 个是本来就红的）。
4. **`ForwardingConnectorContextTest` 是反射驱动的**，`ConnectorContext` 上加方法不加转发会直接构建失败并点名方法。
5. **插件包必须与 FE 同版本部署**。混部（新 FE + 旧插件 zip）表现为运行期 `AbstractMethodError`，不是启动期拒绝。

---

## 📌 第十一批落地后的事实变化

1. **外部表的引擎名由连接器说了算**。`ConnectorProvider.displayEngineName()` 默认返回 `getType()`；fe-core 只在目录上解析一次（`PluginDrivenExternalCatalog.getDisplayEngineName()`）并记住，`PluginDrivenExternalTable` 的 `getEngine()` 与 `getEngineTableTypeName()` 都从它取，**两者恒等**。
2. **两处用户可见变化**：`SHOW CREATE TABLE` 的 `ENGINE=` 由 `JDBC_EXTERNAL_TABLE`/`PAIMON_EXTERNAL_TABLE`/… 变成 `jdbc`/`paimon`/…；trino-connector 与 max_compute 的 ENGINE 列由 NULL 变成 `trino-connector`/`maxcompute`。**没装插件的降级目录回落到目录类型名**，显示与从前一致。
3. **未来新连接器零成本**：不声明就用自己的目录类型名，不再是 `Plugin`/`PLUGIN_EXTERNAL_TABLE`。
4. **`displayEngineName()` 与 `acceptedCreateTableEngineNames()` 是两件事**，且刻意可以不同：hms 目录**显示** `hms`、**接受** `ENGINE=hive`。引擎从不拿展示名做判定。
5. **`realdata/` 目录是 gitignore 的本地副本**（`.gitignore:71`），所以那两个 maxcompute/paimon 的 `.out` 改了也不会入库；入库的只有 `regression-test/data/` 下的 3 个文件 19 行。

---

## 📌 第十批落地后的事实变化

1. **`CreateTableInfo` 里已经没有任何引擎名判定。** 四道门（补引擎名 / 九名或链 / 与目录一致性 / 子句允许列表）全部删除，换成 `resolveTargetCatalog()` 一处：解析目标目录 → 显式写了 `ENGINE=` 就交给目录判 → 只给内部目录补 `olap` → `isExternal` 改为 `!catalog.isInternalCatalog()`。
2. **外部目录的建表语句现在不带引擎名（null）。** 这是刻意的：分析之后没有任何代码读它（连接器请求由列、分区、分桶、属性组成）。`CreateTableCommand.needAuditEncryption()` 里那句「`getEngineName()` 可能是 null」的 ATTN 注释本来就预期了这个状态。
3. **子句允许列表是整个删掉，不是改写成能力位。** 四个能建表的连接器早就在自己的 `createTable` 里校验分区与分桶；fe-core 那份副本只是挡在前面，让 iceberg/paimon 那两条「用 `bucket(num, column)` 写在 `PARTITIONED BY` 里」的可操作文案在生产中永远走不到。现在它们生效了。
4. **`InternalCatalog.createTable` 的八分支引擎 if-链缩成 olap 一条。** `createMysqlTable` / `createBrokerTable` 一并删除——它们的唯一调用方就是刚删掉的那些分支，而且自 `checkEngineName` 开始对这三个名字无条件抛异常起就已经不可达。
5. **`MODIFY ENGINE` 子系统删除**（7 文件 36 处）。**刻意保留** `ModifyTableEngineOperationLog` / `OperationType.OP_MODIFY_TABLE_ENGINE` / `JournalEntity` 与 `EditLog` 的读分支 / `Alter.replayProcessModifyEngine`——老日志里可能还有这个操作，删掉 OperationType 会让老镜像读不出来。
6. **引擎名从来没有被持久化过**（本轮查实）：全 fe-core 没有任何 `@SerializedName` 的 engine 字段；内部表持久化 `TableType`，外部插件表持久化 `PLUGIN_EXTERNAL_TABLE`，引擎串都是运行时算的。所以这条线**不需要镜像版本号、不需要 gson 迁移、不需要 editlog 兼容垫片**。

---

## ⚠️ 做下一批之前必看

1. **`engineName == olap` 蕴含「内部目录」，但反过来不成立**（改造前）。内部目录里写 `ENGINE=hive` 过去合法地走非 olap 分支、活到执行期才被拒。本轮把它改成分析期拒绝，因此 `CreateTableCommandTest` 里那批「在内部目录写 `ENGINE=iceberg` 来够到外部分析分支」的捷径全部失效——已改为直接调 `PartitionTableInfo.convertToPartitionDesc(true)`。**以后写外部建表的 fe-core 单测，不要再用引擎名当捷径。**
2. **`isExternal` 不是显示用的。** 它喂给 `convertToPartitionDesc(isExternal)`，后者会把 `isAutoPartition` 置 true，是 transform 分区的命脉。任何动它的改动都要配用例。
3. **两个 `AnalysisException` 是不同的类**：`org.apache.doris.common.AnalysisException`（受检，`getMessage()` 带 `errCode = N, detailMessage = ` 前缀）与 `org.apache.doris.nereids.exceptions.AnalysisException`（非受检，无前缀）。跨这两族转换时用 `getDetailMessage()`，否则用户会看到多出来的前缀（本轮单测抓到过一次）。
4. **变异验证只对高价值改动做**。本轮做了 3 个（删保留字检查 / 不问目录 / `isExternal` 不再由目录推导），全部如期变红。
5. **删除类改动必须配全仓符号 grep + 清空 `test-classes` 后重跑**。
6. **纯 Mockito mock 上的新方法默认返回 null / 什么都不做**。加 SPI 方法后必须查所有 mock 该接口的测试。
7. **仓库有 60 余个顶层未跟踪项**（含明文密钥的配置、临时日志、workflow 脚本）。**严禁 `git add -A`**，一律显式路径。
8. **任务文档会过期，动手前必须按符号重侦察。** 本轮又实证一次：18 号文档的**核心机制不成立**——它主张把声明放连接器实例上、「字段为空才初始化」，而那个字段是 `transient`，FE 重启后对每个插件目录都是 null，照做等于把远端元数据往返塞进分析期。它还断言「仓库里不存在分桶子句的正向端到端护栏」，实际 `external_table_p2/maxcompute/test_max_compute_create_table.groovy` 有两条。**第 5 次复发。**

---

## 🧭 待用户拍板

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板三十条**（本轮新增五条，见上「本轮拍板的四条」）。

**仍待拍板**：

- **含隐式类型转换的谓词下推默认值**（08 号）：`supportsCastPredicatePushdown` 默认 `true`，而它承诺的「引擎会先剥掉类型转换」只对残余谓词那条路径成立。翻成 `false` 是跨六个连接器的行为改动。

---

## 🧾 顺带发现、留给后续批次

**本轮新增**：

- **外部目录的 rollup 拒绝失去了 fe-core 单测护栏**。`Catalog 'X' doesn't support rollup tables.` 这条只在外部分析分支里，而外部分支现在只有真插件目录能到达；原来靠「内部目录 + `ENGINE=iceberg`」够到它的两个断言已作废。要重建就得用真插件目录（e2e）或带假 provider 的单测。
- **`ConnectorFactory.initPluginManager` 会替换全局单例**，测试里用它注册假 provider 会影响同 JVM 的其它用例。要用它先想好隔离。

**沿用的**（未变）：ES 两个兼容 HTTP 端点的既有安全面（已拍板单独立项）；EXPLAIN 与实际下推判据不一致（已拍板逐字保留）；hudi 的 `\N` 渲染分歧（已拍板不统一）；合成键 `nativeReadSplitNum` 在批模式恒 `0/0`；`EsScanRange.getFileFormat()` 死代码；`PluginDrivenScanNode.TABLE_FORMAT_TYPE` 零引用；`MetadataGenerator` 按字符串比较哨兵；`TablePartitionValues.toListPartitionItem` 哨兵不可达；`ConnectorContractValidator` 生产零调用方；时间旅行委派路径没有反射兄弟能力；两个只写不读的属性键；hudi `partition_values()` 可能落后一个缓存过期；es `REGEXP` 模式串直传 Lucene 少行；`ConnectorMvccSnapshotAdapter` 零引用死类；`ConnectorCapability` 里 `getTableProperties()` 指的是 fe-core 活方法；`CatalogFactory` 的 `lakesoul` 硬失败；`ConnectorScanRange.getLength()` 单位分歧；`ConnectorSession.getStatementScope` 默认不记忆；两套残差协议未合并。

---

## 🧪 欠下的端到端（本地无集群，一律标「待集群验证」，不得当作已通过）

**第十二批新欠 1 类（最重的一条）**：

**插件包重部署冒烟**——23 号任务文档把它列为该任务的核心把关。本地无集群，**完全没跑**。步骤：`mvn package` 取各连接器 `target/doris-fe-connector-<type>.zip` → 清空并重新解包到 `connector_plugin_root` → 重启 FE 确认日志列出全部类型 → 跑下面七项 → 观察日志无 `ClassCastException` / `NoClassDefFoundError` / `AbstractMethodError`：

| 冒烟项 | 覆盖什么 |
|---|---|
| iceberg 目录 `INSERT`（对象存储 warehouse） | 写路径 BE 文件类型 + 地址归一 + 静态凭证；也是 iceberg-aws 反射建 S3 客户端那条路 |
| iceberg `DROP TABLE`（HMS 托管位置） | 空目录清理 |
| iceberg Kerberos 目录一读一写 | 钉桩与「连接器单一认证方」语义 |
| paimon REST 目录一次带临时凭证的扫描 | 临时凭证归一 + 批量地址归一器 |
| hive 分区表扫描 + `INSERT` | 引擎文件系统（14 处调用点里 6 处是它） |
| hudi 目录一次扫描 | BE 存储属性 + 地址归一 |
| `CREATE CATALOG … "test_connection"="true"`（iceberg + S3） | BE 连通性探测 |

另需跑 jdbc 目录用例一遍（改名影响它的建连路径）。

**第十一批新欠 2 类**：

1. **3 个 `.out` 基线共 19 行已改写、必须实跑**：`external_table_p0/nereids_commands/test_nereids_refresh_catalog.out`（7 行 → `ENGINE=jdbc`）、`external_table_p0/paimon/test_paimon_table_properties.out`（1 行 → `ENGINE=paimon`）、`external_table_p2/maxcompute/test_max_compute_create_table.out`（11 行 → `ENGINE=maxcompute`，需真实阿里云账号）。
2. **两处新行为全仓零断言**：trino-connector / max_compute 的 ENGINE 列不再是 NULL（`external_table_p0/trino_connector/test_trinoconnector_information_schema.groovy` 只 select 不校验，值得补一条）；`hms` 目录的 information_schema ENGINE 列仍是 `hms`（已有 `hive/test_information_schema_external.out` 14 行护着，**未变**，属回归护栏而非新欠）。

**第十批欠下的（仍未跑）**：

1. **7 处改写后的断言必须实跑**：`external_table_p0/iceberg/write/test_iceberg_create_table.groovy:61,66,71` 与 `external_table_p0/hive/ddl/test_hive_ddl.groovy:442,478,727,732`，文案已改为 `Engine 'X' does not match catalog 'Y'.`。
2. **iceberg / paimon 带 `DISTRIBUTED BY` 建表的新文案**（连接器自己的「用 `bucket(num, column)`」那条）目前**全仓零断言**，值得补一条。

**还欠一条本轮未写**：hive 目录上打开 `enable_create_hive_bucket_table` 后带 `DISTRIBUTED BY HASH(...) BUCKETS N` 成功建表的正向用例（maxcompute 的 p2 用例是唯一现存的正向分桶护栏，但要真实阿里云账号，日常 CI 不跑）。

**沿用**：ES 的六处 `terminate_after` 断言与两个 REST 端点 curl；iceberg `rewrite_data_files` 的五个套件；paimon 目录查询回归；hive 文本/CSV/JSON 表读回归；文件缓存准入 + `SWITCH <es 目录>` + 事件同步预热；异构目录嵌套列 DDL 与 iceberg 表注释；异构 HMS 目录上的 `ANALYZE`/Top-N/嵌套列裁剪/`SHOW CREATE TABLE`。

---

## ⚙️ 其余构建与验证的坑（实测，直接复用）

1. **maven build cache 会静默跳过测试执行**：跑测试一律加 `-Dmaven.build.cache.enabled=false`。
2. **maven 一律用绝对路径 `-f`**；`cd` 会让后续相对路径失效。
3. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
4. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**。
5. **`HiveConnectorMetadataDdlTest`（19 个里 12 红）与 `HiveCreateTableValidationTest`（10 个里 1 红）在本分支上本来就是红的**（建表路径），与本线改动无关——第十二批在干净 HEAD 上复现确认过计数一致。
10. **`-pl <子集>` 跑测试会撞上 `~/.m2` 里的陈旧 jar**：不在 `-pl` 列表里的上游模块从本地仓库解析，删过符号的模块会在运行期抛 `AbstractMethodError`（本轮 `JdbcScanRangeAndPropertiesTest` 踩到 `getRangeType()`）。一律用交接文档开头那条**全反应堆排除式**命令，或加 `-am`。
6. **checkstyle**：方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`（**第二个字符也必须小写**，本轮被 `aTemporaryTable...` 挡过一次）；`CustomImportOrder` 会因 import 顺序失败；`UnusedImports` 是强制项；**注释块前不得有连续两个空行**（`'/*' has more than 1 empty lines before`）。
7. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉**。一律 `> 日志文件 2>&1` 再 grep。
8. **fe-core 测试里注私有字段用 `org.apache.doris.common.jmockit.Deencapsulation`（仓库自带）**。
9. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**；`hasConnectorCapability` 同理。要在分析期读声明必须走 `ConnectorFactory.findProvider(type, props)`（provider 级，零远端）。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批：07 + 08 + 10 | `test-compile` 通过；79 个单测通过；冻结测试双变异验证均变红 |
| 2026-07-25 | 修掉调研期发现的两个用户可见缺口 | 单测通过；e2e 已写出但**未执行** |
| 2026-07-25 | 第二批：11 号五个提交 | `test-compile` 通过；83 个单测 + checkstyle 通过 |
| 2026-07-25 | 第三批：15 号两个提交 | `test-compile` + checkstyle 通过；52 个单测通过 |
| 2026-07-25 | 第四批：01～06 六个正确性缺陷 | `test-compile` + checkstyle **BUILD SUCCESS**；27 个测试类全绿；8 个变异全部被捕获 |
| 2026-07-25 | 第五批：09 + 14 + 13 + 12 四个提交 | 八个模块全量单测 634 个全绿；4 个变异全部被捕获 |
| 2026-07-26 | 第六批：21 + 16 四个提交 | 33 个测试类全绿；5 个变异全部被捕获 |
| 2026-07-26 | 第七批：17 四个提交 | 四批合计 566 个测试全绿；定位并绕过了让构建卡死 60+ 分钟的 checkstyle 退化 |
| 2026-07-26 | 第八批：20 + 22 + 19 五个提交 | 27 个测试类 259 个测试全绿；4 个变异如期变红 |
| 2026-07-26 | 第九批：25 三个提交（评审文档入库 + 按 HEAD 标注 + 三处注释修正） | 16 个并行核查单元推翻了任务文档的处置方案本身；新登记 10 条开放项 |
| 2026-07-26 | **第十二批：引擎上下文存储服务拆分两个提交**（`ConnectorStorageContext` 新接口 + 11 个方法搬迁 + 35 个调用点 + 8 个测试替身 / 出站地址检查改名与契约收窄） | 侦察推翻任务文档「高危」评级（钉桩包装类本次零改动）；全反应堆 `test-compile` **BUILD SUCCESS**；1359 个连接器单测通过（仅 13 个**本来就红**的 hive DDL 用例失败，已在干净 HEAD 上复现确认）；九个模块 checkstyle **0 违规**；2 个变异如期变红（32 + 4 个用例）；**插件包重部署冒烟完全未跑** |
| 2026-07-26 | **第十一批：展示引擎名交还连接器三个提交**（SPI 声明 + MaxCompute 覆写 / fe-core 删最后两个源名 switch / 3 个 `.out` 基线） | 侦察推翻了交接文档三条前置结论中的两条；全反应堆 `test-compile` **BUILD SUCCESS**；85 个单测全绿；三个模块 checkstyle **0 违规**；3 个变异全部如期变红（共 12 处断言）；19 行基线**待集群验证** |
| 2026-07-26 | **第十批：引擎概念下沉五个提交**（MODIFY ENGINE 删除 / SPI 目录判定入口 / CreateTableInfo 改按目录路由 / InternalCatalog 分派收缩 / 统计异常契约） | 两轮侦察共 68 个 agent（含 50 条对抗复核，其中 15 条推翻或改判）；全反应堆 `test-compile` **BUILD SUCCESS**；105 个单测全绿；六个模块 checkstyle **0 违规**；3 个变异全部如期变红；7 处 e2e 断言已改写**待集群验证** |

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
