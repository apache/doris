# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（PERF-01 ~ 08 全，PERF-11 部分含 C14-provider，PERF-10 暂缓）

- **PERF-01~07**：胖 handle + 跨查询 `IcebergTableCache`；分区视图缓存；`file_format_type` 兜底 memoize；manifest cache 两旁路接回；information_schema comment 缓存；vended-cred 每文件 normalizer；每语句 `ConnectorStatementScope` 读写共享一次加载。
- **PERF-08（`89cc39c8d88` C19 + `be0035eff62` C21）**：`rewrite_data_files` union 一次登记；`expire_snapshots` 按 `manifest.path()` 去重。
- **PERF-10（C8）= 🔬 暂缓**：复核判低价值（纯 CPU 微秒、贵的 loadTable 已被 01/07 修掉），用户拍板跳过。
- **PERF-11 部分**：
  - `10b7d29423f`（iceberg）= **C12 + C15(a) + C13(plan)**：`buildRange` 穿 `PerFileScratch`（键 `task.file()`），partition JSON/identity/delete 载体 per-slice→per-file + 同文件 k range 共享不可变实例。
  - `87ff73b1a95`（**fe-core**）= **C14 子项 #3 provider memo**：`PluginDrivenScanNode.resolveScanProvider()` 按 `currentHandle` **身份键** memo（不可变 holder + volatile 发布）——per-split provider 重分配 O(splits)→ 每 handle 1 次。byte-identical、3 视角红队 0 缺陷。设计/小结见 `designs/FIX-PERF-11-scan-provider-memo-*`。

---

# 🆕 下一个 session = **P2 剩余（仅 PERF-09 与 PERF-11 的 C15b/C13-wire 两块，均需用户签字）**

> 本 session 已把 3 个候选**全部复核清楚**（recon+对抗 verify，证据均未失效）。剩下两块**都不是干净回归修复、都要用户先拍板**——**开场先按 README step 4 向用户复述这两块 + 各自的签字理由，让其定做哪个或收尾**。C14 只剩低价值/高风险的 #1/#2/#4（暂缓）。行号信 grep。

## PERF-09（C5）——streaming pump 逐 split 重建 backend 候选集（**fe-core 框架层，需签"接受分配语义变"**）
- **病灶（HEAD 已核）**：`PluginDrivenScanNode.startStreamingSplit:1638-1642` 逐 split `addToQueue` → `SplitAssignment.addToQueue:153`（`synchronized(assignLock)` + `computeScanRangeAssignment`）→ `FederationBackendPolicy.computeScanRangeAssignment:225-311` 每 split 全量 backend 拷贝 + fixed-seed shuffle + multimap + `ResettableRandomizedIterator`，**且 audit 漏算** `equateDistribution:320` O(B log B) 排序 + 两 IndexedPriorityQueue（`enableSplitsRedistribution` 默认 true、无生产 disable 路径→恒跑）。触 iceberg+trino 两流式。
- **成本**：纯 CPU/GC、无远程 IO，10⁵ split≈0.1-0.5s、10⁶ 几秒。有限、比远程 IO 类小一档。
- **⚠ 关键**：微批**非字节等价**——攒批令 shuffle+`equateDistribution` 从"单 split 空操作"变"真做均衡"（`nextBe`/`assignedWeightPerBackend` 是持久字段），**同一批 split 落到哪些后端会变**（功能安全：每 split 仍恰分一次、全数据读到，仅"哪个 split 去哪台机器"变，是负载均衡启发式）。且"一次一个"是照搬**上游** legacy `doStartSplit`——改它=在上游基线上演进，非修回归。→ **立项前须用户签"接受这个负载均衡语义变化"**（不能声称行为不变）。
- **修法**：pump 侧微批（64~256/批），re-check `needMoreSplit()` per 批 + 末批 flush；碰 fe-core → **两段验** + 分配正确性单测（每 split 恰一次/不丢不重/背压）；分配分布变化 + 时延收益须真 BE 分布式验。

## PERF-11 remainder — C15b 线路字节 / C13-wire（**协议演进，碰 BE+thrift，铁律 D 必签字**）
- **病灶（HEAD 已核）**：`IcebergScanRange.populateRangeParams`（现约 `:330`，wire loop `:413-417`）逐 range `delete.toThrift()` + partition JSON 塞进**每个** `TFileRangeDesc`；一个数据文件 k 个切片各带完整 delete 列表、SDK 共享 delete 重复 M×k。BE `be/src/format/table/iceberg_reader_mixin.h:435` 逐 range inline 读 `delete_files`、无字典。
- **成本**：线路字节 ∝ split 数 × 每分片 delete 数，删除密集/高 split MOR 表可达几 MB；仅 v2+ MOR 生效。低-中严重度。
- **⚠ 协议演进**：真减字节须 thrift **扫描节点级 delete-file 字典 + per-range 索引**（`TFileScanRangeParams`，可仿 paimon 字段 27/30 的 scan-node-level hoist 先例）+ **BE reader 改** + **跨版本兼容矩阵**（老BE↔新FE、新BE↔老FE）。**verify 纠正**：FE 发端**不需动 fe-core**——通用 SPI 挂钩 `ConnectorScanPlanProvider.populateScanLevelParams` 已存在、paimon 已用同法挂源专属 scan-node 字段，iceberg 照抄即可、零 fe-core 加面；但 **BE reader + 兼容矩阵是大头**。FE-only「memoize/share toThrift」半赢=**零线路字节收益**（thrift 每引用仍整块重写），多半不值单做。
- → **立项前必与用户确认是否要做这个大改**（非回归修复）。

## C14 剩余子项（暂缓，记录）
- #1/#2 路径重复解析（`PluginDrivenSplit.buildPath` `LocationPath.of` / `FileQueryScanNode.splitToScanRange` `toStorageLocation`）：假设"所有连接器路径已规整 + `new Path(s).toString()==s`"——Hadoop Path 规整尾斜杠/合并 `//`，**非天然字节安全**，须逐连接器证，性价比差。
- #4 build-then-discard 分区列（`IcebergScanRange.populateRangeParams` unset 掉通用节点造的 columns-from-path，仅身份分区 iceberg 表）：干净修须新增 `ConnectorScanRange` 能力位（默认 false）→ 给 fe-core/SPI **加面**（碰铁律 A）。
- 均低量级；用户 2026-07-19 拍板只做 #3。

## 铁律提醒
- PERF-09 碰 **fe-core 通用节点/框架**：允许改（跨连接器普惠）但**禁按源名分支** + 须证 byte+cost 双不变（本项恰恰**不能**证字节等价 → 走"接受语义变"签字路线）+ **两段验**。
- PERF-11 的 C15b 碰 **BE + thrift 协议**：立项前必签字（铁律 D）。
- 立项先读审计报告 §2/§5 对应簇 + findings.json 完整调用链，再对照 HEAD 真实代码 review（HANDOFF 行号/方案可能过时；本 session 已实证多处证据仍有效但行号漂移）。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **iceberg 侧测试**：`mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`（绝对 `-f <abs>/fe/pom.xml`）。`${revision}` 未 flatten→必 `-am`；`-am test` 不产 shade jar 故用 **`install`**。
2. **动 fe-core 者两段验**：iceberg 连接器**不依赖 fe-core**，`-pl iceberg -am` 反应堆无 fe-core——fe-core 改动 + 其单测须 `mvn test -pl fe-core -am -Dtest='<fe-core 测试类/通配>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false -f <abs>/fe/pom.xml`。**反之：纯 fe-core 改动（如本次 C14-provider）iceberg 模块不受影响、无需另跑**（无编译/测试耦合）。`mvn test -pl fe-core -am` 会跑各模块 checkstyle validate（grep `@ fe-core ... 0 Checkstyle violations`）。
3. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
4. **别用 `mvn -q` 跑测试**：抑制 `BUILD SUCCESS`/`Tests run`。grep 日志 `BUILD SUCCESS` + `Tests run:`。
5. **别 `nohup ... &` 套 `run_in_background`**——maven 变孤儿、假 exit 0。直接 `mvn ... >> log 2>&1`（run_in_background）；**通知里 exit code 是 echo 的**，grep 日志 `BUILD SUCCESS`。
6. **checkstyle 主源 ≤120**，且**扫 test 源**（删测试/改 import 后 grep 残留 unused import）。import 序 `SAME_PACKAGE(org.apache.doris.*)→THIRD_PARTY→STANDARD_JAVA` 组内字母序组间空行。
7. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。工作树另有大量与本任务无关的 untracked 杂物（`.claude/`、`plan-doc/` 其它、`docker/`…），提交只 add 本任务具体文件。
8. **并发探测**：本目录 `find fe -newermt '-120 sec'`（滤 `/target/`）+ `pgrep -a -f maven`。`/mnt/disk1/yy/git/doris` 是另一 worktree，不冲突。
9. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 遗留（已闭环，仅记录）

- **e2e 统一补**：PERF-07 读写归一 + PERF-08 计数断言 e2e，留 P6.6 切换阶段统一补（当前 iceberg 未在 `SPI_READY_TYPES`，provider 休眠）。
- 架构记忆 `iceberg-table-resolution-cache-scoping` 已随 PERF-07 闭环。
