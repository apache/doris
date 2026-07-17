# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 上一个 session（session 2，2026-07-18）：**PERF-01 完成并全绿**

- **commit `484f0e0c125`**（`[perf]` 一个自包含 commit）。小结见 [`designs/FIX-PERF-01-table-memo-summary.md`](./designs/FIX-PERF-01-table-memo-summary.md)。
- 落地 = **胖 handle（`IcebergTableHandle.transient resolvedTable`，查询内单实例）+ 跨查询 `IcebergTableCache`（挂 `IcebergConnector`，gate=`isUserSessionEnabled()||restVendedCredentialsEnabled()` 关）**。统一读 helper `IcebergConnectorMetadata.resolveTableForRead`（胖 handle→cache→裸 loadTable）；provider `resolveTable` 胖 handle 优先 + per-call `wrapTableForScan`。Part B 未做（红队证伪）。
- **验证**：全 iceberg 模块 **932 pass / 0 fail / 1 skip**，checkstyle 绿。度量守门 `planningPassLoadsSameTableOnceViaFatHandle`（规划期同表远端 loadTable = 1，修前 2）。
- **可复用产物**：`(TableIdentifier)` 键的 `IcebergTableCache` 基建 + 胖 handle 携带模式 + `RecordingIcebergCatalogOps` 计数守门法 —— PERF-02/03/10 直接复用，别再造。

---

# 🆕 下一个 session = **PERF-02（分区视图跨查询缓存 + MTMV refresh pin，C7 C22 C23）**

## 第一件事（立项流程见 README §单项立项流程）

1. **复核（动码前，行号信 grep 不信文档）**：按 findings.json C7/C22/C23 重新 grep：分析期 `loadSnapshot → materializeLatest` 对分区表走 `IcebergPartitionUtils.loadRawPartitions`（PARTITIONS 元数据表 `planFiles()+rows()`，读该快照全部 data+delete manifest）；`StatementContext` 只单语句 memoize、跨查询零缓存；MTMV 一次 refresh 里 `isValidRelatedTable/alignMvPartition/generateRelatedPartitionDescs/getAndCopyPartitionItems` 各 materialize 同视图 4~6 次（无 refresh 级 pin + 枚举点间快照偏移风险）。确认成本/乘数仍成立。
2. **设计**（写 `designs/FIX-PERF-02-...-design.md`）：按 `(TableIdentifier, snapshotId)` 缓存**分区视图**（pin 已在 `beginQuerySnapshot` 后落在 handle 上），挂 `IcebergConnector`（复用 PERF-01 的 `MetaCacheEntry` 基建套路，值=分区视图对象）；MTMV 侧在 `MTMVRefreshContext` 加 refresh 级 `MvccSnapshot` pin。**注意 gate**：分区视图不携带凭证（是元数据），但仍要想清跨查询新鲜度语义是否 = legacy。
3. **设计红队**（对抗 review，clean-room 偏好）后再实现。
4. **TDD**：先 `RecordingIcebergCatalogOps`/分区扫描计数守门（分析期同表分区视图 build 从 N→1）。
5. 参考 PERF-01 的 gate/失效/handle-pin 三板斧，别各造一套 key。

## ⚠️ 关键认知（承 PERF-01，别重踩）

- **胖 handle memo 严格查询内**：`PluginDrivenScanNode.currentHandle` 每查询新建、`resolveConnectorTableHandle`→每次 fresh `getTableHandle`、不挂长生命周期 `ExternalTable`。任何"跨查询"缓存必须挂 `IcebergConnector`（长生命周期），不能靠 handle。
- **凭证红线**：任何缓存**携带 FileIO 凭证的对象**（raw Table）都要 gate 掉 `session=user`/`vended`；纯元数据（快照 id、分区视图值）不需要，但要确认值里不夹带 FileIO。
- **测试共享 static handle 会串味**：胖 handle 让 handle 带可变状态，任何跨用例复用的 `static final IcebergTableHandle` 都要改成每用例 fresh（PERF-01 踩过 `T1`）。

---

# 🧰 构建/验证坑（**本轮实证更新，务必照做**）

1. **可靠跑单测 = `mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<iceberg 测试类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`**（绝对 `-f <abs>/fe/pom.xml`）。原因：
   - 本 worktree 用 `${revision}` CI 版本，已装的子模块 pom **未 flatten**（parent 仍写 `${revision}`）→ `-pl iceberg` **单模块**永远解析不到 `fe-connector:pom:${revision}`（且 `-Drevision=` 不透传到传递依赖 pom）→ 必须 `-am`（reactor 解析 revision）。
   - `-am` + `test` 相**只到 test**，不产 `fe-connector-hms-hive-shade` 的 shade jar → iceberg 编译报 `HiveConf` 找不到；故用 **`install`**（到 package 相跑 shade）。
   - `-Dtest=<iceberg 类列表>` 让**上游模块 0 匹配测试**快速跳过，只 iceberg 跑全套（列表 = `ls src/test/.../*Test.java`）。
2. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 被静默跳过，BUILD SUCCESS 是陈旧文件）。
3. **后台 task 通知的 "exit code" 是尾部 `tail`/`echo` 的**，非 maven —— 读日志的 `BUILD SUCCESS`/`Tests run:` 行。
4. **checkstyle**：LineLength(120) 对 **test 源已 suppress**；主源须 ≤120；import 序 = `SAME_PACKAGE(3)→THIRD_PARTY→STANDARD_JAVA` 组内字母序、组间空行。单独跑 `mvn checkstyle:check -pl ... -am`。
5. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。
6. **并发 session 共享同一 worktree** —— 动码前探测（`git log/status` + `pgrep maven` 看 `etime`）。
7. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 开放问题

- 无。PERF-01 已合，进入 PERF-02。
