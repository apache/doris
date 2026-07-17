# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（session 1~3）

- **PERF-01**（commit `484f0e0c125`）：胖 handle（`IcebergTableHandle.transient resolvedTable`，查询内）+ 跨查询 `IcebergTableCache`（挂 `IcebergConnector`，**凭证 gate**）。同表规划期远端 `loadTable` 3~7→1。
- **PERF-02**（commit `518d0599cbf`）：跨查询 `IcebergPartitionCache`（键 `(TableIdentifier, snapshotId)`、值 raw 分区列表、**无凭证 gate**）。分区表分析期 PARTITIONS 扫描每查询→每快照一遍；MTMV 4~6 遍→1。
- **PERF-03**（commit `0b96f2e6c78`）：跨查询 `IcebergFormatCache`（键 `(TableIdentifier, currentSnapshotId)`、值格式名 `String`、**无凭证 gate**）。无 `write-format`/`write.format.default` 的表每查询整表格式推断 `planFiles()`→每快照一遍。只缓存推断兜底、失败不缓存、映射/抛点在 `getOrLoad` 之外；写路径 7 调用点未动（留 PERF-07）。小结 `designs/FIX-PERF-03-*-summary.md`。
- 三轮均全 iceberg 模块单测绿（932→943→957 / 0 fail / 1 skip），checkstyle 绿，0 回归。
- **可复用产物**：三套 `(TableIdentifier[,snapshotId])` + `MetaCacheEntry` 缓存基建（table/partition/format）；**凭证 gate 判据**（值含 FileIO/凭证→gate；纯元数据→无 gate，已三次印证）；「缓存只存 loader 原始输出、映射与抛点放 getOrLoad 之外、失败透传 unchecked 不入缓存」范式；`loadCountForTest` 度量守门法。

---

# 🆕 下一个 session = **PERF-04（IcebergManifestCache 两条旁路接回，C17 C18）**

## ⚠️ 触发面很窄（P1）：仅当用户显式开 `meta.cache.iceberg.manifest.enable=true`（**默认 OFF**）。修的是"缓存开了却在两条路径上被绕过"。

## 第一件事（立项流程见 README §单项立项流程）

1. **复核（动码前，行号信 grep 不信文档；PERF-01/02/03 后行号已漂移）**。两条独立旁路，同一病根（manifest cache 只接在同步 `planFileScanTask` 分支，`gate isManifestCacheEnabled`）：
   - **C17（streaming 大表踢出 cache）**：`FileQueryScanNode.createScanRangeLocations isBatchMode → PluginDrivenScanNode.computeBatchMode（streamingSplitEstimate ≥ num_files_in_batch_mode → streamingBatch，无 cache gate）→ startStreamingSplit → IcebergScanPlanProvider.streamSplits → scan.planFiles()`（裸 planFiles，**不走 cache**）。**讽刺点**：文件数 ≥ `num_files_in_batch_mode`（默认 1024）才走 streaming，而这**恰好是 manifest cache 想加速的大表**；legacy 在 batch 模式仍走 cache。
   - **C18（COUNT(*) 下推绕过 cache）**：`getSplits planScan(countPushdown=true) → planScanInternal（getCountFromSnapshot≥0）→ planCountPushdown → scan.planFiles()`，在到达 cache 分支（`planFileScanTask`）**之前 return**；且 `ParallelIterable` 激进提交所有 manifest 读任务（虽只要计数聚合）。
2. **设计**（写 `designs/FIX-PERF-04-...-design.md`）。审计建议方向：
   - C17 = **cache 开启时 `streamingSplitEstimate` 返回 -1**（= 本模块"count 不可下推/未知"同款 sentinel），令 `computeBatchMode` 退回同步物化 `planFileScanTask` 路径（走 cache）。**⚠ 关键风险须论证**：streaming 是**大表 OOM 保护**（不持全量 task 列表）。强制大表走同步物化会否重新引入 OOM？—— 必须核 legacy「batch 模式如何同时用 cache 又不 OOM」（读 `git show 83585fd5097^:.../IcebergScanNode` 附近），确认"cache 开→退同步"是否真 legacy-parity、还是要更细（如仅当 cache 命中才退）。这条别当"一行修复"轻下。
   - C18 = count 分支改走 `planFileScanTask`（honors cache），而非裸 `scan.planFiles()`。较独立、风险小，可先做。
3. **红队 + TDD**：度量守门 = manifest cache 开启时，streaming 大表 / COUNT(*) 下推的 manifest 读**命中 cache**（非每次裸 planFiles）。可仿现有 `IcebergManifestCache` 测试 + `getManifestCacheValue` 计数。
4. 守铁律：改动放**连接器侧**（`IcebergScanPlanProvider` 的 `streamingSplitEstimate`/`planCountPushdown`）；C17 的 -1 sentinel 是连接器决策，别在 fe-core 加 source-specific 分支。

## ⚠️ 关键认知（承 PERF-01/02/03）

- **缓存都挂 `IcebergConnector`**（长生命周期）；键统一 `(TableIdentifier[, snapshotId])`。`IcebergManifestCache` 是**已存在**的（path-keyed、no-TTL、cap 100k），PERF-04 不新建缓存，只是把两条绕过它的路径接回去。
- **凭证 gate 判据**：值含 FileIO/凭证→gate；纯元数据→无 gate。（PERF-04 不涉新缓存值，无关。）
- **PERF-05（C9 information_schema comment 每表 loadTable）改动小、收益明确**，若 PERF-04 的 OOM 风险论证卡住，可临时插队 PERF-05 热身（README 允许按收益/风险微调顺序，但默认按 §5 优先级）。
- **scan 级 format_type 决定 V1/V2 scanner**（全局 memory）：PERF-04 改 streaming/count 决策**不碰** format_type，但注意 streaming↔batch 模式切换别误动 `getFileFormatType` 路径。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **可靠跑单测 = `mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<iceberg 测试类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`**（绝对 `-f <abs>/fe/pom.xml`）。原因：本 worktree `${revision}` CI 版本 + 未 flatten 的已装 pom → `-pl iceberg` 单模块解析不到 `fe-connector:pom:${revision}`，必须 `-am`；且 `-am test` 只到 test 相不产 hms-hive-shade 的 shade jar（缺 `HiveConf`），故用 **`install`**。`-Dtest=<iceberg 类列表>`（=`ls src/test/.../*Test.java` 拼逗号，本轮 55 类）让上游 0 匹配测试快跳。**checkstyle 单独跑** `mvn checkstyle:check -pl fe-connector/fe-connector-iceberg`（测试跑时 `-Dcheckstyle.skip=true`）。
2. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
3. **别用 `nohup ... &` 套在 Bash `run_in_background` 里**（本轮踩坑）：外层会立即随 `echo` 退出、maven 变孤儿仍在跑 → "task completed exit 0" 是假信号。直接 `mvn ... > log 2>&1`（run_in_background）或用 `Monitor`/`until grep -qE 'BUILD SUCCESS|BUILD FAILURE' log` 守。读日志 `BUILD SUCCESS`/`Tests run:` 行，别信 exit code。
4. **checkstyle**：LineLength(120) 对 **test 源已 suppress**；主源 ≤120；import 序 `SAME_PACKAGE(org.apache.doris.*)→THIRD_PARTY→STANDARD_JAVA`，组内字母序、组间空行（`org.apache.iceberg.catalog.X` 排在 `org.apache.iceberg.T*` 之后、`org.apache.iceberg.io.*` 之前）。
5. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add 改动文件。
6. **并发探测**：本目录 `find fe -newermt '-120 sec'` + `pgrep -a -f maven`。`/mnt/disk1/yy/git/doris` 是另一 worktree 的构建，不动本 `wt-catalog-spi` 文件，非冲突。
7. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 开放问题

- **PERF-04 C17 的 OOM 风险**（见上第一件事 step 2）：cache 开启时把大表从 streaming 退回同步物化，是否重新引入 OOM？须核 legacy batch 模式如何兼顾 cache 与不 OOM，再定"退同步"的精确条件。这是 PERF-04 设计的核心待答项。
