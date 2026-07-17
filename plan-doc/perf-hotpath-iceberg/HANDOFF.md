# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（session 2，2026-07-18）

- **PERF-01**（commit `484f0e0c125`）：胖 handle（`IcebergTableHandle.transient resolvedTable`，查询内）+ 跨查询 `IcebergTableCache`（挂 `IcebergConnector`，凭证 gate）。同表规划期远端 `loadTable` 3~7→1。
- **PERF-02**（commit `518d0599cbf`）：跨查询 `IcebergPartitionCache`（键 `(TableIdentifier, snapshotId)`、值 raw 分区列表、**无凭证 gate**——纯元数据）。分区表分析期 PARTITIONS 扫描每查询→每快照一遍；MTMV 一次 refresh 的 4~6 遍→1。小结见 `designs/FIX-PERF-02-*-summary.md`。
- 两轮均全 iceberg 模块单测绿（943/0/1），checkstyle 绿，0 回归。
- **可复用产物**：`IcebergTableCache`(键 TableIdentifier) / `IcebergPartitionCache`(键 `(TableIdentifier,snapshotId)`+`loadCountForTest` 度量守门) 两套 `MetaCacheEntry` 基建；凭证 gate 判据（值含 FileIO→gate，纯元数据→无 gate）；胖 handle 携带模式。PERF-03/10 复用。

---

# 🆕 下一个 session = **PERF-03（#64134 planFiles 兜底复活，C2 C11）**

## 第一件事（立项流程见 README §单项立项流程）

1. **复核（动码前，行号信 grep 不信文档）**：按 findings.json C2/C11 重新 grep：`getScanNodeProperties → IcebergWriterHelper.getFileFormat → resolveFileFormatName → inferFileFormatFromDataFiles → table.newScan().planFiles()`（无过滤**整表 manifest 扫描**，只为拿"第一个数据文件的格式"）。门槛 = 表属性无 `write-format` 且无 `write.format.default`（迁移表 + 任何写引擎从不显式设该属性的表）。乘数 = 每次 properties 计算 1 次（无谓词 1/查询、带谓词 2/查询）。确认成本/乘数/纯冗余（planScan 自己的 planFiles 枚举里每个 `dataFile.format()` 都有）。
2. **⚠ 依赖已过时**：tasklist 写"依赖 PERF-01 的 convertPredicate 收窄消第二次"——但 **PERF-01 已删 Part B（convertPredicate 收窄，红队证伪为 no-op）**，故不存在"第二次"可消；PERF-03 就是把这一次整表扫描本身干掉。
3. **设计**（写 `designs/FIX-PERF-03-...-design.md`）：两条路线择一或结合——(a) 按 `(table UUID/TableIdentifier, snapshotId)` memoize 解析出的 format（快照不变⇒格式不变，复用 PERF-02 的 `(table,snapshotId)` 键+cache 套路，值=一个 `FileFormat`/字符串，纯元数据无 gate）；或 **(b) 从 planScan 自己的 split 枚举结果反推 scan 级 format 传下去**（更彻底：连那"一次"整表扫描都不做，因为数据规划本就要枚举 dataFile）。**倾向 (b)**（消除而非缓存重操作，更符合问题类），但要核 `getScanNodeProperties`(属性路径) 与 `planScan`(数据路径) 的先后/是否同 handle 血缘能共享枚举结果——若两路径独立触发则 (b) 需跨路径传递，退回 (a) memoize。
4. **红队 + TDD**：度量守门 = 无 write-format 的表规划期 `planFiles`（或 format 解析）远端次数从 1→0（(b)）或跨查询 1→命中（(a)）。可仿 PERF-02 的 `loadCountForTest` 计数法。
5. 守铁律：memo/派生放连接器侧；fe-core 不解析属性（format 组装在插件侧→thrift）。

## ⚠️ 关键认知（承 PERF-01/02）

- **缓存都挂 `IcebergConnector`**（长生命周期）；键统一用 `(TableIdentifier[, snapshotId])`，snapshotId 由 `IcebergLatestSnapshotCache` 稳定。别靠 handle 跨查询（handle 每查询新建）。
- **凭证 gate 判据**：缓存值**含 FileIO/凭证**（如 raw Table）→ gate 掉 `session=user`/`vended`；**纯元数据**（快照 id、分区列表、format）→ 无 gate。PERF-03 的 format 是纯元数据 → 无 gate。
- **测试禁共享可变 handle/静态态**：胖 handle 后 handle 带可变状态，跨用例复用的 `static` handle 会串味（PERF-01 踩过 `T1`）。
- **scan 级 format_type 决定 V1/V2 scanner**（见全局 memory）：PERF-03 若走 (b) 反推 format 传下去，务必别把整个连接器错钉在 `file_format_type=jni`（只 isSystemTable 该发 jni），对齐上游同名 ScanNode 的 `getFileFormatType()`。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **可靠跑单测 = `mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<iceberg 测试类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`**（绝对 `-f <abs>/fe/pom.xml`）。原因：本 worktree `${revision}` CI 版本 + 未 flatten 的已装 pom → `-pl iceberg` 单模块解析不到 `fe-connector:pom:${revision}`（`-Drevision=` 不透传传递依赖），必须 `-am`；且 `-am test` 只到 test 相不产 hms-hive-shade 的 shade jar（缺 `HiveConf`），故用 **`install`**（到 package 相跑 shade）。`-Dtest=<iceberg 类列表>`（=`ls src/test/.../*Test.java`）让上游 0 匹配测试快跳。**checkstyle 单独跑** `mvn checkstyle:check -pl ... -am`（测试跑时 `-Dcheckstyle.skip=true`）。
2. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
3. **后台 task 通知的 "exit code" 是尾部命令的**，非 maven —— 读日志 `BUILD SUCCESS`/`Tests run:` 行（`Monitor` 用 `until grep -qE 'BUILD SUCCESS|BUILD FAILURE'` 守 5s 轮询）。
4. **checkstyle**：LineLength(120) 对 **test 源已 suppress**；主源 ≤120；import 序 `SAME_PACKAGE(3)→THIRD_PARTY→STANDARD_JAVA` 组内字母序组间空行（`org.apache.doris.*`=SAME_PACKAGE，含嵌套类 import 不算 redundant）。
5. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。
6. **并发探测**：`pgrep -a -f maven`。注意 `/mnt/disk1/yy/git/doris`（另一 worktree）常有构建在跑——那不动本 `wt-catalog-spi` 的文件，非冲突；只看**本目录** `find fe -newermt '-120 sec'`。
7. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 开放问题

- 无。PERF-01/02 已合，进入 PERF-03。
