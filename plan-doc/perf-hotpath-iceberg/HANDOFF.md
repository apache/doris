# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（PERF-01 ~ 07）

- **PERF-01**（`484f0e0c125`）：胖 handle + 跨查询 `IcebergTableCache`（**凭证 gate**）。规划期 loadTable 3~7→1。
- **PERF-02**（`518d0599cbf`）：`IcebergPartitionCache`（键 `(TableIdentifier, snapshotId)`、无 gate）。
- **PERF-03**（`0b96f2e6c78`）：`IcebergFormatCache`（键 `(TableIdentifier, currentSnapshotId)`、无 gate）。
- **PERF-04**（`2e5f393779c`）：抽惰性 `cacheBackedFileScanTasks` 三处复用，manifest cache 接回大表流式 + COUNT(*)。
- **PERF-05**（`aea3ebdd40e`）：`IcebergCommentCache`（键 `TableIdentifier`、无 gate），**仅 vended 非 session=user**。
- **PERF-06**（`6294edf2833`）：vended-credentials URI 归一化 scan 级「准备一次、逐文件套用」normalizer。
- **PERF-07**（`97bdcd6bdbe` fe-core + `ea7fd1f6e7a` iceberg）：每语句 `ConnectorStatementScope`（挂 `StatementContext`、构造期捕获）作读写共享的表加载归属者，一条语句一张表只加载一次；拆胖句柄；delete stash 下沉到作用域（删单例 + `IcebergRewritableDeleteStash` 类 + 测试）；v3 行级 DML + NONE fail-loud。写侧收益≈0，交付=架构连贯。**净增 fe-core SPI（碰铁律 A，用户签字）**。设计+小结见 `designs/FIX-PERF-07-unified-*`。
- iceberg 全模块单测绿（**968 pass / 1 skip**），fe-core 两段验 `ConnectorStatementScopeTest`+`ConnectorSessionImplTest` 绿，0 回归。

---

# 🆕 下一个 session = **PERF-08（维护路径逐单位重扫 / 无去重，C19+C21）**

> 改动小、收益明确、**不碰读写热路径**（相较 PERF-07 是轻活）；覆盖两条独立维护命令，同一「逐单位重扫、零去重」模式；实现上可各自独立 commit。**行号信 grep**（PERF-07 后可能又漂）。

## C19 — `rewrite_data_files`（分布式 compaction 规划）
- **病灶**：`ConnectorRewriteDriver.run` STEP3（约 :143）**每个** rewrite group 各调一次 `registerRewriteSourceFiles` → 每次一遍 `useSnapshot(...).planFiles()`（整表 manifest 扫描）。G 个 group = **G+1 次整表扫描**，G~50-200 时分钟级。
- **修复方向**：**union 所有 group 一次注册**（SPI `registerRewriteSourceFiles` 本就收 `Set<String>`）——把 G 次调用坍缩为 1 次，扫一遍 planFiles、按 group 分派。
- **⚠ 落地前 grep 定位**：`ConnectorRewriteDriver`（fe-core 通用驱动？还是连接器侧？）的 STEP3 循环真实结构 + `registerRewriteSourceFiles` 的连接器实现（iceberg 侧 `IcebergConnectorTransaction.registerRewriteSourceFiles`，PERF-07 recon 记其在 `:362`、re-derive 在 `:380-381`、当前 `startingSnapshotId`）；确认「union 一次注册」不破分布式 rewrite 的 per-group 文件归属/OCC。

## C21 — `expire_snapshots`
- **病灶**：`IcebergExpireSnapshotsAction.buildDeleteFileContentMap`（约 :271-293）对**每个** snapshot 读其全部 delete manifest、**无 visited-path 去重** —— 相邻 snapshot 的 manifest 大量重叠，S×M 串行远程读。
- **修复方向**：按 `ManifestFile.path()` **去重坍缩为 O(distinct)**（visited set）。
- **⚠ 落地前 grep 定位**：`IcebergExpireSnapshotsAction` 真实结构 + 该 map 的消费方（确认按 path 去重不丢任何该删的 content 条目）。

## 铁律提醒（PERF-08）
- 维护路径修复**不碰** fe-core 读写热缝、不碰 PERF-07 的作用域；若发现 C19 的驱动在 fe-core 通用层，遵「共享框架须 byte+cost 双不变」纪律 + 保持 connector-agnostic（对齐 `catalog-spi-plugindriven-no-source-specific-code`）。
- 立项先读审计报告 §5 对 C19/C21 的原始证据，再对照 HEAD 真实代码 review（HANDOFF 行号/方案可能过时）。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **iceberg 侧测试**：`mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`（绝对 `-f <abs>/fe/pom.xml`）。`${revision}` 未 flatten→必 `-am`；`-am test` 不产 shade jar 故用 **`install`**；类列表存 `scratchpad/iceberg-tests.txt`（`ls src/test/.../*Test.java | xargs -n1 basename | sed 's/.java//' | paste -sd,`）。
2. **动 fe-core 者必两段验**：iceberg 连接器不依赖 fe-core，`-pl iceberg -am` 反应堆无 fe-core——fe-core 改动 + 其单测须另跑 `mvn test -pl fe-core -am -Dtest=<fe-core 测试类> -DfailIfNoTests=false -Dmaven.build.cache.enabled=false -f <abs>/fe/pom.xml`。（PERF-08 若纯 iceberg 则免；若碰 fe-core 驱动则要。）
3. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
4. **别用 `mvn -q` 跑测试**：抑制 `BUILD SUCCESS`/`Tests run`。用 surefire XML 或 grep `Tests run:`。
5. **别 `nohup ... &` 套 `run_in_background`**——maven 变孤儿、假 exit 0。直接 `mvn ... >> log 2>&1`（run_in_background）；**通知里 exit code 是 echo 的**，grep 日志 `BUILD SUCCESS`。
6. **checkstyle 主源 ≤120**，且**扫 test 源**（PERF-07 踩坑：删测试后遗留 unused import 挂 checkstyle）——删测试后 grep 残留 import。import 序 `SAME_PACKAGE(org.apache.doris.*)→THIRD_PARTY→STANDARD_JAVA` 组内字母序组间空行。
7. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add（`git rm` 会自动 stage 删除，拆 commit 时用 `git restore --staged` 剔出）。
8. **并发探测**：本目录 `find fe -newermt '-120 sec'` + `pgrep -a -f maven`。`/mnt/disk1/yy/git/doris` 是另一 worktree，不冲突。
9. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 PERF-07 遗留（已闭环，仅记录）

- e2e 回归（独立 + HMS 网关目录 INSERT/DELETE/MERGE/OVERWRITE 无复活 + 分布式 rewrite + 跨 catalog MERGE + session=user/vended 归一）**留 P6.6 切换阶段统一补**（对齐 `hms-iceberg-delegation-needs-e2e`；当前 iceberg 未在 `SPI_READY_TYPES`，provider 休眠）。
- 架构记忆 `iceberg-table-resolution-cache-scoping` 待按本次落地更新（原记「stash 下单例待用户定高度」已由完整统一版闭环）。
