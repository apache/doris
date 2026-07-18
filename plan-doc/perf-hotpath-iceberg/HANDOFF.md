# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（PERF-01 ~ 04）

- **PERF-01**（`484f0e0c125`）：胖 handle（查询内）+ 跨查询 `IcebergTableCache`（**凭证 gate**）。一次规划远端 loadTable 3~7→1。
- **PERF-02**（`518d0599cbf`）：跨查询 `IcebergPartitionCache`（键 `(TableIdentifier, snapshotId)`、**无 gate**）。分区表分析期 PARTITIONS 扫描每查询→每快照一遍。
- **PERF-03**（`0b96f2e6c78`）：跨查询 `IcebergFormatCache`（键 `(TableIdentifier, currentSnapshotId)`、值格式名、**无 gate**）。无 write-format 表整表格式推断每查询→每快照一遍。
- **PERF-04**（`2e5f393779c`）：抽惰性 `cacheBackedFileScanTasks`（delete 索引 eager + data manifest 惰性扁平映射）三处复用，把 manifest cache 接回大表流式 + COUNT(*)（**缓存与不 OOM 兼得**，否决审计"退回物化"因重引 legacy OOM）。小结 `designs/FIX-PERF-04-*-summary.md`。
- 四轮全 iceberg 模块单测绿（932→943→957→962 / 0 fail / 1 skip），checkstyle 绿，0 回归。
- **可复用产物**：三套 `(TableIdentifier[,snapshotId])` + `MetaCacheEntry` 缓存基建（table/partition/format）；**凭证 gate 判据**（值含 FileIO/凭证→gate，纯元数据→无 gate，已三次印证）；「缓存只存 loader 原始输出、映射/抛点放 getOrLoad 之外、失败透传 unchecked 不入缓存」范式；`loadCountForTest`/`cache.size()` 度量守门法；「delete 索引 eager + 惰性扁平映射」范式。

---

# 🆕 下一个 session = **PERF-05（C9 information_schema.tables 每表 loadTable 只为取 comment）**

## 病灶（触发面广、教科书级"伪装成轻访问器"）

`FrontendServiceImpl.listTableStatus` 的 **fe-core** `for (TableIf table : tables)` 循环里**无条件**调 `status.setComment(table.getComment())`（不看请求是否要 comment 列，且在 `table.readLock()` 下**串行**）→ `PluginDrivenExternalTable.getComment`（fe-core）→ `IcebergConnectorMetadata.getTableComment`（连接器）→ **每表一次 `loadTable`**（HMS getTable RPC + metadata.json GET，或 REST loadTable）。一个库 N 张 iceberg 表 = 一条 `SELECT * FROM information_schema.tables` / `SHOW TABLE STATUS` 付 **N 次串行远端 load**（每次 50~300ms），几百表数十秒~分钟，BI 工具高频触发。

## 第一件事（立项流程见 README §单项立项流程）

1. **复核（动码前，行号信 grep 不信文档；PERF-01~04 后行号已漂移）**：grep `FrontendServiceImpl.listTableStatus`(fe-core) 的循环 + `setComment` 是否仍无条件、是否在 readLock 下；`PluginDrivenExternalTable.getComment` 是否有缓存字段；**`IcebergConnectorMetadata.getTableComment` 当前是裸 `loadTable` 还是已走 PERF-01 的 `resolveTableForRead`（tableCache）**？——这决定修法起点。
2. **⚠ 铁律约束（关键）**：循环在 **fe-core**（`FrontendServiceImpl` / `PluginDrivenExternalTable.getComment`），**「fe-core 源只出不进」→ 不得改 fe-core 循环、不得给 fe-core 加缓存 comment 字段**。修法必须**连接器侧**。
3. **设计**（写 `designs/FIX-PERF-05-...-design.md`），候选方向：
   - **(a) 连接器侧 comment 缓存**（键 `TableIdentifier`、值 comment `String`、**无凭证 gate**——纯元数据）：`getTableComment` 先查缓存。**镜像 PERF-03 format cache**。**注意**：这消不掉**首次** N 次 load（无批量 SPI），但让**重复** information_schema 查询坍缩（BI 高频→除以 QPS-over-TTL）；REFRESH TABLE/DB/CATALOG 失效。
   - **(b) 若 getTableComment 尚未走 tableCache**：至少改走 PERF-01 的缓存 resolve，让"已被扫描加载过的表" + "重复查询"免费（与 (a) 可叠加）。
   - **批量 load（HMS `get_table_objects_by_name`）不可行**：需新 SPI 批量方法 + 改 fe-core 循环（fe-core 加法，越界）——排除，记为已知局限（首次 N load 无法在本约束下消除）。
4. **红队 + TDD**：度量守门 = 重复取同表 comment 时远端 `loadTable`/`getTableComment` 次数从 N→命中（仿 `loadCountForTest`）；gate = 三目录（plain/vended/session）comment 缓存均建（无 gate）+ REFRESH 失效。
5. 守铁律：缓存放连接器侧，值纯元数据无 gate；不碰 fe-core。

## ⚠️ 关键认知

- **缓存都挂 `IcebergConnector`**（长生命周期）；comment 缓存键用 `TableIdentifier`（comment 是表级属性，与快照无关，故**不需要 snapshotId**——与 format/partition 缓存不同，那两个随快照变，comment 随 DDL 变靠 REFRESH 失效即可）。
- **凭证 gate 判据**：comment 是纯元数据字符串（无 FileIO/凭证）→ **无 gate**（同 partition/format 缓存）。
- **首次 N load 是本约束下的硬下限**：诚实写进设计 Risk（不假装消除），只优化重复。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **可靠跑单测 = `mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<iceberg 测试类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`**（绝对 `-f <abs>/fe/pom.xml`）。原因：本 worktree `${revision}` + 未 flatten 已装 pom → `-pl iceberg` 解析不到 `fe-connector:pom:${revision}`，必须 `-am`；`-am test` 只到 test 相不产 hms-hive-shade shade jar（缺 `HiveConf`），故用 **`install`**。`-Dtest=<iceberg 类列表>`（=`ls src/test/.../*Test.java` 拼逗号，本轮 55 类，存 `scratchpad/iceberg-tests.txt`）让上游 0 匹配测试快跳。**checkstyle 单独** `mvn checkstyle:check -pl fe-connector/fe-connector-iceberg`。
2. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
3. **`mvn -q` 成功时不打 `BUILD SUCCESS`/`Tests run`（INFO 被抑制）**——本轮踩坑。验证成功用 **surefire XML 聚合**（`target/surefire-reports/TEST-*.xml` 的 tests/failures/errors 属性求和），别只看 exit code / grep BUILD 行。或去掉 `-q`。
4. **别 `nohup ... &` 套在 Bash `run_in_background` 里**——外层立即随 echo 退出、maven 变孤儿，"completed exit 0" 是假信号。直接 `mvn ... > log 2>&1`（run_in_background）。
5. **checkstyle**：LineLength(120) 对 test 源 suppress；主源 ≤120；import 序 `SAME_PACKAGE(org.apache.doris.*)→THIRD_PARTY→STANDARD_JAVA` 组内字母序组间空行。**新用类记得 import**（本轮 `java.util.Iterator` 漏 import 编译挂）。
6. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。
7. **并发探测**：本目录 `find fe -newermt '-120 sec'` + `pgrep -a -f maven`。`/mnt/disk1/yy/git/doris` 是另一 worktree，不冲突。
8. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 开放问题

- **PERF-05 首次 N load 硬下限**：本约束（fe-core 不可改）下，comment 缓存只能优化重复查询，首次 `SELECT * FROM information_schema.tables` 仍付 N 次串行 load。批量 load 需改 fe-core → 越界。设计须诚实记为已知局限（对齐 PERF-02 "范围决定" 的诚实口径）。
