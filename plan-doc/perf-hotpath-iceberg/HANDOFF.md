# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（PERF-01 ~ 06）

- **PERF-01**（`484f0e0c125`）：胖 handle + 跨查询 `IcebergTableCache`（**凭证 gate**）。规划期 loadTable 3~7→1。
- **PERF-02**（`518d0599cbf`）：`IcebergPartitionCache`（键 `(TableIdentifier, snapshotId)`、无 gate）。分区扫描每查询→每快照。
- **PERF-03**（`0b96f2e6c78`）：`IcebergFormatCache`（键 `(TableIdentifier, currentSnapshotId)`、无 gate）。格式推断每查询→每快照。
- **PERF-04**（`2e5f393779c`）：抽惰性 `cacheBackedFileScanTasks` 三处复用，manifest cache 接回大表流式 + COUNT(*)（缓存与不 OOM 兼得）。
- **PERF-05**（`aea3ebdd40e`）：`IcebergCommentCache`（键 `TableIdentifier`、无 gate），**仅 vended 非 session=user**。information_schema comment 每表 load 跨查询坍缩。
- **PERF-06**（`6294edf2833`）：vended-credentials URI 归一化 **scan 级「准备一次、逐文件套用」**。新增 SPI `newStorageUriNormalizer`（默认逐文件折回、零影响其它连接器），`DefaultConnectorContext` override 惰性 memo 一次派生；iceberg 三 scan-start 各构造一次 normalizer 穿六 seam；`TcclPinning` 透传。**否决 fe-core 框架 memo**（per-catalog 跨查询共享→并发冲刷+凭证保留）。一次 vended scan `StorageProperties.createAll` O(N_files+N_deletes)→1。小结 `designs/FIX-PERF-06-*-summary.md`。
- iceberg 全模块单测绿（→974 / 0 fail / 1 skip），fe-core `DefaultConnectorContextNormalizeUriTest` 13 绿，checkstyle 绿，0 回归。
- **可复用产物**：四套缓存基建（table/partition/format/comment）+ 一个 SPI「准备一次、套用多次」normalizer 范式；**gate 判据三条**（①值含凭证→gate ②纯元数据→无 gate ③授权在 load 里/session=user→不可共享）；**PERF-06 新判据「作用域即安全边界」**：同一份含凭证派生，跨查询做撞 gate、**scan 级做天然安全**（token 恒定、无跨用户、结束即回收）。

---

# 🆕 下一个 session = **PERF-07（C20 写路径一条 DML 3~5 次 load 同表）**

## 病灶（审计基线行号，动码前必重 grep）

一条 iceberg DML（INSERT/DELETE/MERGE）在规划+绑定期对同一张表串行 load 3~5 次：
`PhysicalIcebergMergeSink.getRequirePhysicalProperties:161→198`、`PhysicalPlanTranslator.visitPhysicalConnectorTableSink:675,703`、`PluginDrivenTableSink.bindDataSink:175` → `IcebergWritePlanProvider.resolveTable:689-702` / `beginWrite`（内含 `tableExists` ×2 + **无条件 refresh**）。每条 DML +3~5 次串行远程往返。
**修复方向（审计）**：语句级 resolve 一次传递；`exists` 从 load 结果推导（load 成功即存在，不必单独 tableExists）。

## 第一件事（立项流程见 README §单项立项流程）

1. **复核（动码前，行号信 grep；PERF-01~06 后已漂移）**：grep 上述五处 load/resolve/beginWrite/tableExists 调用点，重新确认：①这些 load 是否已被 PERF-01 的 `IcebergTableCache` 部分兜住（写路径是否走 resolveTableForRead？还是另一条 write-only 的 resolve/refresh？**PERF-05 复核先例：审计早于 PERF-01，残余可能已缩小**）②`beginWrite` 的**无条件 refresh** 是真必需还是可省（refresh 破坏 PERF-01 缓存命中？）③`tableExists ×2` 能否从 load 结果推导。
2. **⚠ 路由/归属**：写路径 resolve 分散在 **Nereids 物理算子层（fe-core `PhysicalPlanTranslator`/`PluginDrivenTableSink`）** 与 **连接器 `IcebergWritePlanProvider`** 两侧。语句级"resolve 一次传递"若要跨这两层，可能须动 fe-core 的 sink 绑定流程——**先核能否纯连接器侧收敛**（`IcebergWritePlanProvider` 内 statement 级 memo，键=queryId/statementId + TableIdentifier），若须动 fe-core 通用 sink 流程则**动码前上交用户**（对齐 PERF-06/铁律）。
3. **红队 + TDD**：度量守门 = 一条 DML 内对同表的 loadTable 远端次数从 3~5→1（可加计数器/spy，镜像 PERF-01 的 `loadCountForTest`）。**行为 parity 是硬闸门**：写语义（exists 判定、refresh 拿到最新快照）不得变——若省 refresh，须证写入仍绑定到正确的最新 metadata。
4. 守铁律：优先连接器侧；exists 从 load 推导别新增 RPC；别把 Table 缓存塞进 fe-core。

## ⚠️ 关键认知

- **写路径 refresh 与 PERF-01 读缓存的张力**：PERF-01 的 `IcebergTableCache` 按 `(TableIdentifier, snapshotId)` 缓存；写路径若无条件 `refresh()` 会绕过/失效它。核清"写必须看最新"与"缓存命中"能否兼得（可能写路径本就该 pin 一次最新快照后全语句复用，类似 PERF-01/02 的 pin 模式）。
- **statement 级作用域**：与 PERF-06 的 scan 级同理——写路径的 load 复用作用域是**一条语句**（queryId/statementId 稳定），非跨查询，故不涉凭证/授权 gate；memo 挂 statement context 或连接器侧按 queryId 键。
- **exists 从 load 推导**：`tableExists` 单独 RPC 多半可删——load 成功即存在、load 抛 NoSuchTable 即不存在；但要核**建表/CTAS 分支**（表尚不存在时 exists=false 是正常路径，不能把"不存在"当错误抛）。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **iceberg 侧测试**：`mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<iceberg 测试类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`（绝对 `-f <abs>/fe/pom.xml`）。原因见旧注：`${revision}` 未 flatten → 必须 `-am`；`-am test` 不产 shade jar 故用 **`install`**；`-Dtest=<类列表>`（`ls src/test/.../*Test.java` 拼逗号，本轮 56 类，存 `scratchpad/iceberg-tests.txt`）让上游快跳。
2. **⚠ 改到 fe-core 的项须单独验 fe-core**（PERF-06 实证）：**iceberg 连接器不依赖 fe-core**（SPI 解耦），`-pl iceberg -am` 反应堆里**没有 fe-core**——fe-core 的改动 + 其单测须**另跑** `mvn test -pl fe-core -am -Dtest=<fe-core 测试类> -DfailIfNoTests=false -Dmaven.build.cache.enabled=false -f <abs>/fe/pom.xml`。PERF-07 若动 fe-core sink 流程，两段都要验。
3. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
4. **别用 `mvn -q` 跑测试**：成功时抑制 `BUILD SUCCESS`/`Tests run` INFO 行。用 surefire XML 或 grep `Tests run:` 聚合验证。
5. **别 `nohup ... &` 套 `run_in_background`**——maven 变孤儿、"exit 0" 假信号。直接 `mvn ... >> log 2>&1`（run_in_background）；**通知里的 exit code 是 echo 的**，要 grep 日志 `BUILD SUCCESS`。
6. **checkstyle 主源 ≤120**（test 源 suppress）；import 序 `SAME_PACKAGE(org.apache.doris.*)→THIRD_PARTY→STANDARD_JAVA` 组内字母序组间空行；新用类记得 import（本轮四文件各加 `java.util.function.UnaryOperator`）。
7. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。
8. **并发探测**：本目录 `find fe -newermt '-120 sec'` + `pgrep -a -f maven`。`/mnt/disk1/yy/git/doris` 是另一 worktree，不冲突。
9. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 开放问题

- **PERF-07 路由（纯连接器 statement-memo vs 动 fe-core sink 绑定流程）**：写路径 resolve 横跨 Nereids 物理算子（fe-core）与 `IcebergWritePlanProvider`（连接器）。先核能否在 `IcebergWritePlanProvider` 内按 queryId/statementId 收敛（连接器侧）；若"resolve 一次传递"须跨 fe-core sink 层，则**动码前把方案上交用户定**（对齐 PERF-06 先例）。
- **refresh 必要性**：`beginWrite` 无条件 refresh 是否可省/可收敛为语句级 pin 一次，须与 PERF-01 读缓存的命中兼得 + 证写语义 parity。
