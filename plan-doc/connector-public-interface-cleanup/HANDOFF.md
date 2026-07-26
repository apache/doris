# 🤝 交接文档 · 连接器公共接口整治

> **滚动文档**：每轮结束后**覆盖式更新**，只保留下一个 session 必须的上下文。已完成工作的明细不落这里（在 `git log` 与各任务文档里）。
> **范围** = 把 `fe-connector-api` / `fe-connector-spi` 两个公共模块的接口设计规范化。
> **⚠ 与主线互不覆盖**：catalog SPI 迁移主线的交接文档是 `plan-doc/HANDOFF.md`（当前跟的是另一条线）。**不要用本文覆盖它，也不要用它覆盖本文。**

---

## 🆕🆕 最新一轮（2026-07-27）：rebase 到上游 `1aa5ae9597e` —— 「零冲突」但树是坏的

`git pull --rebase upstream-apache branch-catalog-spi`，**72 个提交全部重放，0 个文本冲突**，
`range-diff` 71 个 `=` / 1 个 `!`（第 28 个 `centralize the scan-node property key contract`，
差异只在 paimon 那段 javadoc 上下文——上游已先把 cpp 臂删了）。

上游这轮做了两件事：rebase 到 master `962bd5b28c7`（带进 #66008 / #66036 / #66021），外加两个自有提交
（`port #66008's paimon-cpp removal to the paimon connector scan path`、
`dedupe partition columns so a multi-transform spec cannot crash partition pruning`）。

**零文本冲突 ≠ 树是好的。** 本轮实测两处坏点，都不会以冲突形式出现：

1. **（rebase 造成）** 上游新增的 `PaimonScanPlanProviderTest.cppReaderSessionFlagNoLongerChangesThePlan`
   调的是**七参** `planScan`，而本分支第十四批已把它换成 `planScan(session, ConnectorScanRequest)`。
   两边**没碰同一行** → 三方合并各取一半 → 只有 `test-compile` 报
   "method planScan cannot be applied to given types"。已按请求对象逐字等价移植（builder 默认值
   = 上游传的那七个参数），提交 `b8935724788`。
2. **（不是 rebase 造成，早就红着）** `ConnectorMetadataSurfaceTest` 因为第十三批
   （`executeStmt` / `getColumnsFromQuery` 搬去 `ConnectorPassthroughSqlOps`）和
   `drop the create-database mirror switch`（删 `supportsCreateDatabase()`）**没同步基线资源**而失败。
   `ConnectorMetadata.java` 与 `connector-metadata-methods.txt` 在 rebase 前后**逐字节相同** → 证明与 rebase 无关。
   已刷新基线，提交 `c556938541b`。

**⚠ 下一个 session 的教训（这条最值钱）**：之所以 (2) 一直没被发现，是因为历次验证只跑
「全反应堆 test-compile + 本批改到的连接器模块」，**从没跑过 `fe-connector-api` 自己的测试套件**。
改了 `fe-connector-api` 的公共接口，就必须跑 `fe-connector-api` 的测试——`ConnectorMetadataSurfaceTest`
是全仓**唯一**的「录制基线」测试（`find fe/fe-connector -path '*/src/test/resources/*' -name '*.txt'` 只有一个命中），
删/加/改 SPI 方法签名必须在**同一个提交**里刷新 `src/test/resources/connector-metadata-methods.txt`。

**本轮验证**：全反应堆 `test-compile` BUILD SUCCESS（75 模块 / 0 error）；
`fe-connector-api` 110/110、`fe-connector-spi` 9/9、`fe-connector-paimon` 390（0 失败，1 个既有
live-connectivity skip）、`fe-connector-iceberg` 1151（0 失败，5 个既有 live-endpoint skip）、
`fe-filesystem-api` 75/75、`fe-connector-hms-shared` 104/104；fe-core 分区裁剪相关 6 个类 110/110
（上游这轮改了 `PruneFileScanPartition`）；两个改动模块 checkstyle 各 0 violation。
**e2e 未跑（本地无集群）**，仍是欠账。

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

`-pl` 缩到单模块对 `checkstyle:check` **安全**；对 `compile` / `test-compile` **不安全**。

**对 `test` 同样不安全，且失败方式很误导**（2026-07-27 实测）：`-pl fe-connector/fe-connector-paimon` 不带
`-am` 时，兄弟模块从 `~/.m2` 取**陈旧 jar**，surefire 在**测试发现阶段**就炸，报的是
`TestEngine with ID 'junit-vintage' failed to discover tests` + `Tests run: 0`，真因要翻
`target/surefire-reports/*.dump` 才看得到（`NoClassDefFoundError: .../ConnectorScanRequest`）。
**要么全反应堆，要么 `-pl <模块> -am`。**

---

## 🆕 下一个 session 起步

**必读顺序**：本文 → [README.md](./README.md) 的任务清单。
**不要通读** `audit-report.md`（1600 余行），按 README 里的章节导航 grep 定位。

**当前状态：十四批已合入（共 57 个提交）。25 个编号任务 + 「复核登记的开放项」10 条全部完成。**

### 下一步：只剩需要集群的事

代码侧这条工作线已经做完。剩下的全部是**积压的端到端与插件包重部署冒烟**（见下方欠账清单），本地无集群一律跑不了。

如果要继续在代码上找事做，下面这些是本轮顺带记下的、**没有人认领**的候选（都不是这条线的欠账）：

- `streamSplits` 与 `getScanNodeProperties` / `getScanNodePropertiesResult` 仍是位置参数（5 参 / 4 参）。它们**没有重载链**，所以没有 `planScan` 那种「实现了却静默失效」的陷阱，本轮刻意没动。真要统一风格可以让它们也接 `ConnectorScanRequest`。
- 引擎构造扫描请求时若漏掉 `.countPushdown(...)`，COUNT(*) 下推会静默退化成全量扫描，而 fe-core 侧**没有任何单测**压这个调用点（连接器侧有 paimon/iceberg 的 countPushdown 用例，但它们直接调 `planScan`）。这不是本轮引入的，改造前那个位置参数同样没被压住。
- 逐个连接器接入 `renderShowCreateTableDdl`（今天只有 hive 在用），各自独立排期。

---

## 📌 第十四批落地后的事实变化（1 个提交）

1. **`ConnectorScanPlanProvider.planScan` 只有一个方法了**：`planScan(ConnectorSession, ConnectorScanRequest)`。四个重载（4/5/6/7 参）全部删除。
2. **新增下推信号不再新增重载**：往 `ConnectorScanRequest` 加一个带默认值的字段即可，所有连接器自动拿到，**不可能再出现「实现了最短的那个抽象方法、静默失去 limit / 分区裁剪 / COUNT 下推」**。
3. **`planScanForPartitionBatch(session, request, partitionBatch)`**，默认实现走 `request.withRequiredPartitions(batch)`。hive 仍然覆写它（它的 `planScan` 不是按分区集作用域的，继承默认会每批重放整个裁剪集 → 重复行）。
4. **请求对象的默认值就是「引擎没有额外要求」**：无过滤、limit = -1、分区集为空（= 扫全部）、无 COUNT 下推。`requiredPartitions` 传 `null` 与传空等价（引擎在裁剪到零时早就短路了，走不到这里）。
5. **零行为变化**：每个连接器收到的值与改造前逐字相同；批模式那条路仍然是「无 limit、无 COUNT 下推」。

---

## 📌 第十三批落地后的事实变化（8 个提交）

1. **`estimateScanRangeCount` 已删除**（SPI 默认 + jdbc 覆写），全仓复扫零命中。
2. **`ConnectorTableOps` 现在真的什么都不声明了**，只是六个域接口的聚合。SQL 直通搬进新的 **`ConnectorPassthroughSqlOps`**（可选接口，jdbc 实现）。
3. **`ConnectorCapability.SUPPORTS_PASSTHROUGH_QUERY` 已删除**。判据改为「metadata 是否 `instanceof ConnectorPassthroughSqlOps`」——**实现接口就是声明本身**，不再有能力位与实现两个答案。两个入口（`query()` TVF、`CALL EXECUTE_STMT`）都改成类型判定。
4. **`ConnectorSchemaOps.supportsCreateDatabase()` 已删除**，`CREATE DATABASE IF NOT EXISTS` 的远端存在性预检改为**无条件**（对齐 Trino 的 `CreateSchemaTask`）。
   **⚠ 唯一的用户可见行为变化**：jdbc / es / trino / hudi 目录上 `CREATE DATABASE IF NOT EXISTS <远端已存在的库>` 由「报 CREATE DATABASE not supported」变成**静默成功**。两个问题都不回答的连接器（`databaseExists` 保持默认 false）行为不变。
5. **`ConnectorWriteHandle.getWriteContext()` → `getStaticPartitionSpec()`**，与引擎侧产出方同名。
6. **`JdbcQueryTableValueFunction` → `PluginDrivenQueryTableValueFunction`**（fe-core 里最后一个按数据源命名、实际服务任意连接器的类）。
7. **`supportsCastPredicatePushdown` 现在没有任何连接器是「继承来的」**：iceberg / es / trino 就地声明 `true` 并写明各自拿这个谓词做什么、`true` 是接受风险而非安全声明；hive / hudi **对残余谓词零消费**（`planScan` 与 `getScanNodeProperties` 都不看），这个开关对它们是死的，因此不加空覆写，改在 SPI 文档里记下整张地图。零行为变化。
8. **契约校验器的每条不变量都有真连接器正样本了**：maxcompute 压 local-sort 臂，新增的 `HiveConnectorContractTest` 压 hash 臂与两臂互斥。

---

## 📌 第十二批落地后的事实变化

1. **`ConnectorContext` 只剩 7 个方法 + 一个 `getStorageContext()`**（404 行 → 155 行）。新增存储服务加在 `ConnectorStorageContext`，**不要加回 `ConnectorContext`**。
2. **钉桩失效的残留风险写在两处注释里**：今天没有任何存储方法跑插件代码；将来若有，钉桩子类必须覆写 `getStorageContext()` 返回自己的包装。
3. **测试替身的静默退化是隐性风险**：替身实现了 `ConnectorStorageContext` 却忘写 `getStorageContext()` 时能编译过，覆写全变死代码。
4. **`ForwardingConnectorContextTest` 是反射驱动的**，`ConnectorContext` 上加方法不加转发会直接构建失败并点名方法。
5. **插件包必须与 FE 同版本部署**。混部表现为运行期 `AbstractMethodError`，不是启动期拒绝。

---

## 📌 第十一批 / 第十批落地后的事实变化（仍然成立）

1. **外部表的引擎名由连接器说了算**（`ConnectorProvider.displayEngineName()`，默认取目录类型名，只有 MaxCompute 覆写）。`SHOW CREATE TABLE` 的 `ENGINE=` 与 information_schema 的 ENGINE 列取同一个值。
2. **`displayEngineName()` 与 `acceptedCreateTableEngineNames()` 是两件事**：hms 目录**显示** `hms`、**接受** `ENGINE=hive`。
3. **`CreateTableInfo` 里没有任何引擎名判定**，改按目标目录路由；外部目录的建表语句不带引擎名（null），这是刻意的。
4. **`MODIFY ENGINE` 子系统已删除**，但**刻意保留** `ModifyTableEngineOperationLog` / `OperationType.OP_MODIFY_TABLE_ENGINE` / 重放分支——老镜像要能读。
5. **引擎名从来没有被持久化过**，所以这条线不需要镜像版本号 / gson 迁移 / editlog 垫片。

---

## ⚠️ 做下一批之前必看

1. **`UnusedLocalVariable` 是开启的**（第十四批踩到）：把方法参数改成「从请求对象解包成同名局部变量」时，只解包**真正用到**的那些——多解一个就是 checkstyle 失败。判断「用到了吗」别用裸 grep（hudi 的 `columns` 只出现在**别的方法**里，多解了一个）。
2. **任务文档与登记表会过期，动手前必须按符号重侦察。** 第十三批又实证一次：登记表把「hive 契约正样本」「读事务矛盾」估成中等成本，实际都是低；而 `supportsCastPredicatePushdown` 登记的「五个连接器继承默认值」是错的——其中两个（hive/hudi）根本不消费残余谓词，那个开关对它们是死的。**别按登记表的成本估算排期，先看代码。**
2. **`git commit` 提交的是整个索引，不是你刚 `git add` 的那几个文件。** 第十三批踩到：更早的 `git mv` 把重命名留在索引里，被第一个提交顺手带走，留下一个**编译不过**的中间提交（文件名已改、类名未改）。修法是 `git reset --mixed HEAD~N` 后逐个重做。**每次提交前先 `git status --porcelain` 看索引里到底有什么。**
3. **改名类改动会撞行长上限**：`getWriteContext` → `getStaticPartitionSpec` 让 iceberg 一行超 120 字符被 checkstyle 挡。改名后一定要跑改动模块的 `checkstyle:check`。
4. **`CustomImportOrder` 对新增 import 很敏感**：`sed` 插 import 时按字典序插，`ConnectorMetadata` 在 `ConnectorPassthroughSqlOps` 之前。
5. **hive 连接器的单测不能碰 `getOrCreateClient()`**（会建真的 `ThriftHmsClient`，测试环境没有 Hadoop 栈）。要真实的写提供者就直接 `new HiveWritePlanProvider(null, props, ctx)`（构造是纯赋值），或匿名子类覆写 `getWritePlanProvider()`。
6. **纯 Mockito mock 上的新方法默认返回 null / 什么都不做**。加 SPI 方法后必须查所有 mock 该接口的测试。
7. **仓库有 60 余个顶层未跟踪项**（含明文密钥的配置、临时日志、workflow 脚本）。**严禁 `git add -A`**，一律显式路径。
8. **删除类改动必须配全仓符号 grep + 清空 `test-classes` 后重跑**。

---

## 🧭 待用户拍板

完整清单在 **[open-decisions.md](./open-decisions.md)**。**已拍板三十四条**（第十三批新增四条：建库布尔位删除 / SQL 直通独立接口并删能力位 / `planScan` 收成请求对象 / CAST 下推保持 true 但逐连接器显式声明）。

**目前没有待拍板项。**

---

## 🧾 顺带发现、留给后续批次

**第十三批新增**：

- **`PluginDrivenQueryTableValueFunction.getScanNode()` 不做类型判定**：入口 `createQueryTableValueFunction` 已经拒过不实现直通接口的目录，所以这里直接建扫描节点。若将来有第二个入口能绕过工厂，这里要补判定。
- **`CALL EXECUTE_STMT` 没有任何 e2e**（只有 jdbc 一家实现，仓库里查不到断言）。

**沿用的**（未变）：ES 两个兼容 HTTP 端点的既有安全面（已拍板单独立项）；EXPLAIN 与实际下推判据不一致（已拍板逐字保留）；hudi 的 `\N` 渲染分歧（已拍板不统一）；合成键 `nativeReadSplitNum` 在批模式恒 `0/0`；`EsScanRange.getFileFormat()` 死代码；`PluginDrivenScanNode.TABLE_FORMAT_TYPE` 零引用；`MetadataGenerator` 按字符串比较哨兵；`TablePartitionValues.toListPartitionItem` 哨兵不可达；`ConnectorContractValidator` 生产零调用方；时间旅行委派路径没有反射兄弟能力；两个只写不读的属性键；hudi `partition_values()` 可能落后一个缓存过期；es `REGEXP` 模式串直传 Lucene 少行；`ConnectorMvccSnapshotAdapter` 零引用死类；`CatalogFactory` 的 `lakesoul` 硬失败；`ConnectorSession.getStatementScope` 默认不记忆；两套残差协议未合并；逐个连接器接入 `renderShowCreateTableDdl`（今天只有 hive 在用，各自独立排期）。

---

## 🧪 欠下的端到端（本地无集群，一律标「待集群验证」，不得当作已通过）

**第十三批新欠 1 条（唯一一条会改变用户可见行为的）**：

**`CREATE DATABASE IF NOT EXISTS` 在不能建库的目录上**：拿一个 jdbc（或 es / trino / hudi）目录，对一个**远端已存在**的库执行 `CREATE DATABASE IF NOT EXISTS <db>`，断言**成功且无输出**（改动前会报 `CREATE DATABASE not supported`）；再对一个**不存在**的库执行同一条语句，断言仍然报 `CREATE DATABASE not supported`。两条都需要真集群。另需回归 jdbc 的 `query()` TVF 与 `CALL EXECUTE_STMT`（入口判定换成了类型判定）。

**第十二批新欠 1 类（最重的一条，仍未跑）**：

**插件包重部署冒烟**——`mvn package` 取各连接器 `target/doris-fe-connector-<type>.zip` → 清空并重新解包到 `connector_plugin_root` → 重启 FE 确认日志列出全部类型 → 跑下表七项 → 观察日志无 `ClassCastException` / `NoClassDefFoundError` / `AbstractMethodError`：

| 冒烟项 | 覆盖什么 |
|---|---|
| iceberg 目录 `INSERT`（对象存储 warehouse） | 写路径 BE 文件类型 + 地址归一 + 静态凭证 |
| iceberg `DROP TABLE`（HMS 托管位置） | 空目录清理 |
| iceberg Kerberos 目录一读一写 | 钉桩与「连接器单一认证方」语义 |
| paimon REST 目录一次带临时凭证的扫描 | 临时凭证归一 + 批量地址归一器 |
| hive 分区表扫描 + `INSERT` | 引擎文件系统 |
| hudi 目录一次扫描 | BE 存储属性 + 地址归一 |
| `CREATE CATALOG … "test_connection"="true"`（iceberg + S3） | BE 连通性探测 |

另需跑 jdbc 目录用例一遍。

**第十一批欠的 2 类**：3 个 `.out` 基线共 19 行已改写必须实跑（`test_nereids_refresh_catalog.out` → `ENGINE=jdbc`、`test_paimon_table_properties.out` → `ENGINE=paimon`、`test_max_compute_create_table.out` → `ENGINE=maxcompute` 需真实阿里云账号）；trino-connector / max_compute 的 ENGINE 列不再是 NULL 但全仓零断言，值得补一条。

**第十批欠的**：7 处改写后的断言必须实跑（`test_iceberg_create_table.groovy:61,66,71` 与 `test_hive_ddl.groovy:442,478,727,732`，文案已改为 `Engine 'X' does not match catalog 'Y'.`）；iceberg / paimon 带 `DISTRIBUTED BY` 建表的新文案全仓零断言；hive 打开 `enable_create_hive_bucket_table` 后的正向分桶建表用例仍缺。

**沿用**：ES 的六处 `terminate_after` 断言与两个 REST 端点 curl；iceberg `rewrite_data_files` 的五个套件；paimon 目录查询回归；hive 文本/CSV/JSON 表读回归；文件缓存准入 + `SWITCH <es 目录>` + 事件同步预热；异构目录嵌套列 DDL 与 iceberg 表注释；异构 HMS 目录上的 `ANALYZE`/Top-N/嵌套列裁剪/`SHOW CREATE TABLE`。

---

## ⚙️ 其余构建与验证的坑（实测，直接复用）

1. **maven build cache 会静默跳过测试执行**：跑测试一律加 `-Dmaven.build.cache.enabled=false`。
2. **maven 一律用绝对路径 `-f`**；`cd` 会让后续相对路径失效。
3. **`-Dtest='org.apache.doris.datasource.**'` 这种全包扫描会超时被砍**，用具体类名清单。
4. **e2e（groovy）需要真集群，本地跑不了**。**没有 `.out` 基线的新用例不要用 `qt_`**。
5. **`HiveConnectorMetadataDdlTest`（19 个里 12 红）与 `HiveCreateTableValidationTest`（10 个里 1 红）在本分支上本来就是红的**（建表路径），与本线改动无关。
6. **全反应堆必须 `-Dcheckstyle.skip=true`**（checkstyle 扫 generated-sources 会退化成平方级，构建卡死 60+ 分钟），checkstyle 单独对改动模块跑。
7. **`-pl <子集>` 跑测试会撞上 `~/.m2` 里的陈旧 jar**，一律用开头那条全反应堆排除式命令，或加 `-am`。
8. **checkstyle**：方法名正则 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`（第二个字符也必须小写）；`CustomImportOrder` 按字典序；`UnusedImports` 强制；注释块前不得有连续两个空行；行长 120。
9. **`mvn ... | tail -60` 会把 `Tests run:` 行冲掉**。一律 `> 日志文件 2>&1` 再 grep。
10. **fe-core 测试里注私有字段用 `org.apache.doris.common.jmockit.Deencapsulation`（仓库自带）**。
11. **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**；`hasConnectorCapability` 同理。要在分析期读声明必须走 `ConnectorFactory.findProvider(type, props)`（provider 级，零远端）。

---

## 📈 进度记录

| 日期 | 做了什么 | 结果 |
|---|---|---|
| 2026-07-25 | 独立 clean-room 调研（14 个并行审查单元 + 30 批对抗复核） | 172 条结论成立/部分成立，4 条被推翻；产出 `audit-report.md` |
| 2026-07-25 | 建立本任务空间，按优先级拆出 25 个任务并各写一份施工文档 | 代码零改动 |
| 2026-07-25 | 第一批：07 + 08 + 10 ｜ 第二批：11 号 ｜ 第三批：15 号 ｜ 第四批：01～06 | 逐批 `test-compile` + 单测 + 变异验证通过 |
| 2026-07-25 | 第五批：09 + 14 + 13 + 12 | 八个模块全量单测 634 个全绿；4 个变异全部被捕获 |
| 2026-07-26 | 第六批：21 + 16 ｜ 第七批：17 ｜ 第八批：20 + 22 + 19 ｜ 第九批：25 | 逐批全绿；第七批定位并绕过了让构建卡死 60+ 分钟的 checkstyle 退化 |
| 2026-07-26 | 第十批：引擎概念下沉（5 提交） / 第十一批：展示引擎名交还连接器（3 提交） / 第十二批：引擎上下文存储服务拆分（2 提交） | 全反应堆 `test-compile` **BUILD SUCCESS**；变异全部如期变红；e2e 基线**待集群验证** |
| 2026-07-27 | **第十四批：扫描计划请求对象 1 个提交**（`ConnectorScanRequest` 新增 + `planScan` 四重载合一 + `planScanForPartitionBatch` 改签名 + 14 个连接器覆写塌成 8 个 + 引擎两个调用点 + 约 20 个测试文件） | 全反应堆 `test-compile` **BUILD SUCCESS**；26 个扫描相关测试类 **402 个测试全绿**；十个模块 checkstyle **0 违规**；2 个变异如期变红（`withRequiredPartitions` 丢过滤条件、批默认忘记按批重定作用域——正是这次改动唯二能静默出错的地方） |
| 2026-07-26 | **第十三批：开放项清理 8 个提交**（删死接口 `estimateScanRangeCount` / 三条契约文档澄清 / hive 契约校验正样本 / 写句柄改名 `getStaticPartitionSpec` / 直通 TVF 中立命名 / 删建库镜像开关 / SQL 直通独立成可选接口并删重影能力位 / CAST 下推逐连接器显式声明） | 全反应堆 `test-compile` **BUILD SUCCESS**；4 组共 22 个测试类全绿（其中一轮 258 个用例）；九个模块 checkstyle **0 违规**；`CREATE DATABASE IF NOT EXISTS` 的行为变化**待集群验证**；`planScan` 重载合并**已拍板未动手** |

**上下文用量超过 30% 就找一个干净节点覆写本文并通知用户开新 session 续做**，不要等窗口满。
