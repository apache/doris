# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（PERF-01 ~ 08）

- **PERF-01~07**：见 `progress.md` + `tasklist.md`（胖 handle + 跨查询 `IcebergTableCache`；分区视图缓存；`file_format_type` 兜底 memoize；manifest cache 两旁路接回；information_schema comment 缓存；vended-cred 每文件 normalizer；每语句 `ConnectorStatementScope` 读写共享一次加载）。
- **PERF-08（`89cc39c8d88` C19 fe-core + `be0035eff62` C21 iceberg）**：两条维护命令的「逐单位重扫/无去重」。
  - C19 `rewrite_data_files`：`ConnectorRewriteDriver` STEP3 逐组 `registerRewriteSourceFiles`（每次一遍全表 `planFiles()`）→ `unionSourceFilePaths(groups)` 并集**一次**登记；整表扫描 G+1→1。行为不变靠同一固定快照 + 组互斥 + 连接器按 path 去重。
  - C21 `expire_snapshots`：`buildDeleteFileContentMap` 加方法作用域 visited set 按 `manifest.path()` 去重；远程读 O(S×M)→O(distinct M)、结果 map 逐字节不变。
  - 2 独立对抗 agent 均判 parity 保持、0 破坏性发现。`ConnectorRewriteDriverTest` 5/5 + `IcebergExpireSnapshotsActionTest` 9/9，两处 BUILD SUCCESS + 0 checkstyle。权威设计/小结/红队见 `designs/FIX-PERF-08-maintenance-path-rescan-*`。

---

# 🆕 下一个 session = **P2 剩余（PERF-09 / PERF-10 / PERF-11 三选一）**

> 剩三项，均 P2（CPU/payload/框架层，触发门槛较高但模式典型）。**开场先按 README 流程 step 4 向用户复述并让其定下一个做哪个**（下面给推荐 + 各自要点；**行号信 grep**，PERF-07/08 后可能又漂）。

## 推荐先做：PERF-10（C8）——WHERE conjunct 每查询转换 5~6 次
- **轻活、iceberg-local、与已完成的 PERF-01 同一失效点**（node 字段 memo，宜顺手）。
- **病灶**：同组 conjunct 在 `buildRemainingFilter`(×3) + `buildScan` + `getScanNodeProperties`（EXPLAIN 专用序列化，非 EXPLAIN 也跑）被 `convertPredicate` 转换 5~6 次/查询。单次微秒~毫秒，与簇1 同源叠加。
- **修复方向**：node 字段 memo，与 `cachedPropertiesResult` **同点失效**（复用 PERF-01 立住的失效收窄模式）。
- **⚠ 落地前 grep**：`IcebergScanPlanProvider` 里 `convertPredicate` 的 5~6 个调用点真实位置 + 各自输入是否恒等（同一 conjunct 列表）→ 确认可安全 memo（同输入同输出、谓词不变量）。

## PERF-09（C5）——streaming pump 逐 split 重建 backend 候选集（**fe-core 框架层**）
- **⚠ 改的是 fe-core 通用框架**（惠及所有连接器、非 source-specific，**允许改**）；但属共享热路径，须证 **byte + cost 对所有连接器双不变**（对齐「共享框架须双不变」纪律 + 保持 connector-agnostic）。
- **病灶**：`startStreamingSplit:~1638` 逐 split `addToQueue` → `SplitAssignment` 每 split 重建 backend 候选集（`FederationBackendPolicy.computeScanRangeAssignment` 全量 backend 拷贝 + shuffle + multimap）+ `synchronized` 往返。10⁵~10⁶ split × ~100 BE ≈ 10⁷~10⁸ 冗余。
- **修复方向**：pump 侧微批（64~256/批）。**先 grep** 该泵 + `SplitAssignment` 现结构，确认微批不改分配语义（同一 split 集合的 backend 分配结果不变）。

## PERF-11（簇5，C12 C13 C14 C15）——per-split 不变量 + payload 放大（**批量、可拆多 commit**）
- 同一 `buildRange`/`populateRangeParams` 区域：C12 每 byte-slice 重算 partition JSON/identity map/delete 转换 →(specId,PartitionData) memo；C13 v3 delete 双重 thrift 转换 → 转一次 per-file 复用；C14 通用节点 per-split `LocationPath.of`/造完即弃的 columns-from-path/`getFileCompressType` → hoist（**⚠ 涉 fe-core 通用节点，保持 connector-agnostic**）；C15 delete 列表+partition JSON 逐 slice 复制进 `TFileRangeDesc` → per-file 共享引用。
- **先 grep** `buildRange` / `populateRangeParams` / `IcebergScanRange` 真实结构再逐条立项。

## 铁律提醒
- PERF-10/11(iceberg 部分) 修复放**连接器侧** node 字段/memo，不碰 fe-core（对齐 PERF-01 模式）。
- PERF-09 与 PERF-11 的 C14 碰 **fe-core 通用节点/框架**：允许改（跨连接器普惠）但**禁按源名分支** + 须证 byte+cost 双不变。
- 立项先读审计报告 §2/§5 对应簇原始证据（findings.json 有完整调用链），再对照 HEAD 真实代码 review（HANDOFF 行号/方案可能过时）。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **iceberg 侧测试**：`mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`（绝对 `-f <abs>/fe/pom.xml`）。`${revision}` 未 flatten→必 `-am`；`-am test` 不产 shade jar 故用 **`install`**。
2. **动 fe-core 者必两段验**：iceberg 连接器不依赖 fe-core，`-pl iceberg -am` 反应堆无 fe-core——fe-core 改动 + 其单测须另跑 `mvn test -pl fe-core -am -Dtest=<fe-core 测试类> -DfailIfNoTests=false -Dmaven.build.cache.enabled=false -f <abs>/fe/pom.xml`。（**PERF-09 与 PERF-11 的 C14 碰 fe-core → 要两段验；纯 iceberg 项免**。）PERF-08 已实证：C19 走 fe-core 段、C21 走 iceberg 段。
3. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
4. **别用 `mvn -q` 跑测试**：抑制 `BUILD SUCCESS`/`Tests run`。grep 日志 `BUILD SUCCESS` + `Tests run:`。
5. **别 `nohup ... &` 套 `run_in_background`**——maven 变孤儿、假 exit 0。直接 `mvn ... >> log 2>&1`（run_in_background）；**通知里 exit code 是 echo 的**，grep 日志 `BUILD SUCCESS`。
6. **checkstyle 主源 ≤120**，且**扫 test 源**（删测试/改 import 后 grep 残留 unused import）。import 序 `SAME_PACKAGE(org.apache.doris.*)→THIRD_PARTY→STANDARD_JAVA` 组内字母序组间空行。PERF-08 踩过：test helper 内联 builder 不具名类型 → 该 import 变 unused。
7. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。工作树另有大量**与本任务无关的 untracked 杂物**（`.claude/`、`plan-doc/` 其它、`docker/`…），提交只 add 本任务具体文件。
8. **并发探测**：本目录 `find fe -newermt '-120 sec'` + `pgrep -a -f maven`。`/mnt/disk1/yy/git/doris` 是另一 worktree，不冲突。
9. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 遗留（已闭环，仅记录）

- **e2e 统一补**：PERF-07 的读写归一 + PERF-08 的 `rewrite_data_files`/`expire_snapshots` 计数断言 e2e，均**留 P6.6 切换阶段统一补**（当前 iceberg 未在 `SPI_READY_TYPES`，provider 休眠；对齐 `hms-iceberg-delegation-needs-e2e`）。PERF-08 的 C19 分布式实路径单测非集群不可达，亦归此。
- 架构记忆 `iceberg-table-resolution-cache-scoping` 已随 PERF-07 落地闭环。
