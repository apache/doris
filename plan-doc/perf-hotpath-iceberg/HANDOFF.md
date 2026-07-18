# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（PERF-01 ~ 08 全，PERF-11 部分）

- **PERF-01~07**：见 `progress.md` + `tasklist.md`（胖 handle + 跨查询 `IcebergTableCache`；分区视图缓存；`file_format_type` 兜底 memoize；manifest cache 两旁路接回；information_schema comment 缓存；vended-cred 每文件 normalizer；每语句 `ConnectorStatementScope` 读写共享一次加载）。
- **PERF-08（`89cc39c8d88` C19 fe-core + `be0035eff62` C21 iceberg）**：维护命令「逐单位重扫/无去重」——`rewrite_data_files` union 一次登记（全表扫 G+1→1）；`expire_snapshots` 按 `manifest.path()` 去重（O(S×M)→O(distinct M)）。
- **PERF-10（C8，WHERE conjunct 转换）= 🔬 暂缓**：复核判**低价值**（纯 CPU 微秒、贵的 loadTable 已被 01/07 修掉）且干净不了（大头在 fe-core 通用节点 + 带副作用；连接器侧 EXPLAIN-only 切片因 `ConnectorSession` 无 explain 信号做不干净）。**用户拍板跳过**。详见 tasklist 该条。
- **PERF-11 部分（`10b7d29423f`，iceberg）= C12 + C15(a) + C13(plan)**：`buildRange` 穿 1 条目 `PerFileScratch`（键 `task.file()`），partition JSON/identity/delete 载体 per-slice→per-file + 同文件 k range 共享不可变实例 + v3 supply put per-file。3 视角对抗 byte-parity 0 破坏；全模块 1064 pass/1 skip。设计/小结/红队见 `designs/FIX-PERF-11-*`。

---

# 🆕 下一个 session = **P2 剩余（PERF-09 框架层 / PERF-11 remainder 二选一）**

> 剩两块。**开场先按 README 流程 step 4 向用户复述并让其定做哪个**（下面给要点；**行号信 grep**，PERF-08/11 后必漂）。PERF-10 已暂缓、不再是候选。

## PERF-09（C5）——streaming pump 逐 split 重建 backend 候选集（**fe-core 框架层**）
- **⚠ 改的是 fe-core 通用框架**（惠及所有连接器、非 source-specific，**允许改**）；但属共享热路径，须证 **byte + cost 对所有连接器双不变**（对齐「共享框架须双不变」纪律 + 保持 connector-agnostic）。**需两段验**（碰 fe-core）。
- **病灶**：`startStreamingSplit:~1638` 逐 split `addToQueue` → `SplitAssignment` 每 split 重建 backend 候选集（`FederationBackendPolicy.computeScanRangeAssignment` 全量 backend 拷贝 + shuffle + multimap）+ `synchronized` 往返。10⁵~10⁶ split × ~100 BE ≈ 10⁷~10⁸ 冗余。
- **修复方向**：pump 侧微批（64~256/批）。**先 grep** 该泵 + `SplitAssignment` 现结构，确认微批不改分配语义（同一 split 集合的 backend 分配结果不变）。

## PERF-11 remainder（C15b 线路字节 / C13-wire / C14）——**已部分完成，剩下三块各有前置**
- **C15(b) 线路字节 + C13(wire)**：`IcebergScanRange.populateRangeParams`（现 `:330`，行号信 grep）逐 range 把完整 delete 列表 `delete.toThrift()` + partition JSON 塞进每个 `TFileRangeDesc`。真正减线路字节须 **thrift params 级 delete-file 字典 + per-range 索引 + BE reader 改** = **协议演进（FE+BE 双向 + 兼容性），非回归修复**（审计 C15 自述）——**立项前须与用户确认是否要做这个大改**。
- **C14 fe-core 通用节点 per-split hoist**：`FileQueryScanNode`/`PluginDrivenScanNode` 逐 split `LocationPath.of`（URLEncoder+URI.create）、每 split 新建 provider（`getFileCompressType`）、造完即弃的 columns-from-path（被 `IcebergScanRange.populateRangeParams` unset）。**⚠ 框架层**、量级较小（~几百 ms@10 万 split）、须证 byte+cost 双不变。与 PERF-09 同属框架层，可一并评估。

## 铁律提醒
- PERF-11 remainder 的 C15b 碰 **BE + thrift 协议**：立项前必与用户确认（大改、非回归修复）。
- PERF-09 与 PERF-11 的 C14 碰 **fe-core 通用节点/框架**：允许改（跨连接器普惠）但**禁按源名分支** + 须证 byte+cost 双不变 + **两段验**。
- 立项先读审计报告 §2/§5 对应簇原始证据（findings.json 有完整调用链），再对照 HEAD 真实代码 review（HANDOFF 行号/方案可能过时）。PERF-11 已实证：C13 原证据（`IcebergRewritableDeleteStash`）PERF-07 已删——**动前必 grep 核实证据未 stale**。

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
