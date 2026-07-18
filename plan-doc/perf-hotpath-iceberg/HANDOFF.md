# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# ✅ 已完成（PERF-01 ~ 05）

- **PERF-01**（`484f0e0c125`）：胖 handle + 跨查询 `IcebergTableCache`（**凭证 gate**）。规划期 loadTable 3~7→1。
- **PERF-02**（`518d0599cbf`）：`IcebergPartitionCache`（键 `(TableIdentifier, snapshotId)`、无 gate）。分区扫描每查询→每快照。
- **PERF-03**（`0b96f2e6c78`）：`IcebergFormatCache`（键 `(TableIdentifier, currentSnapshotId)`、无 gate）。格式推断每查询→每快照。
- **PERF-04**（`2e5f393779c`）：抽惰性 `cacheBackedFileScanTasks` 三处复用，manifest cache 接回大表流式 + COUNT(*)（缓存与不 OOM 兼得）。
- **PERF-05**（`aea3ebdd40e`）：`IcebergCommentCache`（键 `TableIdentifier`、无 gate），**仅 vended 非 session=user**（红队：session=user 缓存绕过 per-user 授权=泄漏）。information_schema comment 每表 load 跨查询坍缩（vended）。小结 `designs/FIX-PERF-05-*-summary.md`。
- 五轮全 iceberg 模块单测绿（→973 / 0 fail / 1 skip），checkstyle 绿，0 回归。
- **可复用产物**：四套缓存基建（table/partition/format/comment）；**gate 三判据**：①值含 FileIO/凭证→gate；②纯元数据→无 gate；③**授权发生在 load 调用里（session=user）→即便值无凭证也不可共享缓存**（PERF-05 新增，与①②正交叠加）。「缓存只存 loader 原始输出、失败透传不缓存」；「delete 索引 eager + 惰性扁平映射」（PERF-04）。

---

# 🆕 下一个 session = **PERF-06（C3 REST vended-credentials 每文件重建 StorageProperties+Configuration）**

## 病灶

REST vended-credentials 目录，扫描规划期 `buildRange → normalizeUri → context.normalizeStorageUri(rawUri, vendedToken)`（`TcclPinningConnectorContext` → **fe-core** `DefaultConnectorContext.normalizeStorageUri:392-409`）→ `buildVendedStorageMap:225-242`（`CredentialUtils.filterCloudStorageProperties` + `StorageProperties.createAll`：遍历所有 provider + **建 hadoop `Configuration` + 逐 key set**）。**每 data-file range 一次 + 每 MOR delete file per task 一次 + 每 position-delete split 一次**（同步/流式/$position_deletes 三路径同）。vendedToken **整个 scan 内不变**（`planScanInternal:586-587` sync / `:458-459` streaming 只提取一次、同一 Map 实例穿所有调用），但每文件都重建整份 map+Configuration。50k 文件 ≈ **数十秒纯 FE CPU**（MOR 加倍）。门槛：REST + `iceberg.rest.vended-credentials-enabled=true`（空 token 在 `:227-229` 短路）。

## 第一件事（立项流程见 README §单项立项流程）

1. **复核（动码前，行号信 grep；PERF-01~05 后已漂移）**：grep `buildRange`/`convertDelete`/`buildPositionDeleteRange` 的 `normalizeUri` 调用点、`DefaultConnectorContext.normalizeStorageUri` + `buildVendedStorageMap`。**核心确认**：`buildVendedStorageMap(token)` 的重活（`StorageProperties.createAll` + hadoop Configuration）是**纯 token 派生、与 rawUri 无关**吗？（审计说是——URI 只用于路径归一化那半，cheap。）
2. **⚠ 路由决策（关键，可能需上交用户，像 PERF-04/05）**：`DefaultConnectorContext` 在 **fe-core**。两条路线：
   - **(a) 连接器侧 hoist**：iceberg 扫描期把 token→map 派生**提升到 scan 级**（token 已只提取一次），per-file 只做 cheap 路径归一化。**难点**：`normalizeStorageUri(uri, token)` 是 fe-core `ConnectorContext` API，重活封装在 fe-core 里；连接器要 hoist 需 fe-core 暴露"派生一次 map"的入口，或连接器自己做路径归一化——**核 `ConnectorContext` API 表面**能否连接器侧 hoist 而不改 fe-core。
   - **(b) fe-core 框架 memo**：`DefaultConnectorContext` 内按 **token 恒等键单条目 memo** `buildVendedStorageMap`。这是 fe-core **框架层**改动——按 README 铁律 2（PERF-09 先例：**改通用框架、惠及所有连接器、非 source-specific = 允许**）**大概率合规**，但属**共享热路径**，须证 **byte + cost 对所有连接器双不变**（对齐 PERF-09/共享 MVCC 纪律），且 token 作 key 要用**恒等/内容**键谨慎（同 Map 实例→identity 键最稳）。
   - **倾向**：先核 (a) 可行性；若 (a) 须改 fe-core API 才能 hoist，则 (b)（框架 memo，惠及所有 vended 连接器）反而更干净——但**两条都动到 fe-core 边界**，故**动码前把选择+理由用中文上交用户定**（不引用任务代号）。
3. **红队 + TDD**：度量守门 = 一次 vended scan 内 `buildVendedStorageMap`/`StorageProperties.createAll` 远端/CPU 次数从 O(N_files+N_deletes)→1（可加计数器或 spy）。
4. 守铁律：优先连接器侧；若走 fe-core 框架 memo，须 connector-agnostic（禁按源名分支）+ 双不变证明。

## ⚠️ 关键认知

- **token loop-invariant 是本修的根**：token 每 scan 提取一次、同一 Map 实例穿所有 `normalizeUri`；重活是 token 派生、与 per-file URI 无关 → 可 memo/hoist 一次。
- **三路径都要覆盖**：data-file range（buildRange）+ MOR delete（convertDelete）+ position-delete split（buildPositionDeleteRange），同步与流式两条。别只修一路。
- **gate 判据延伸**（PERF-05 新增）：本任务缓存的是 storage map（**含凭证/token 派生**）→ 若做跨 scan 缓存会撞凭证 gate；但本修是 **scan 内**复用（token 恒定），非跨查询，故**不涉凭证泄漏**——scan 内 hoist/memo 安全。

---

# 🧰 构建/验证坑（**实证，务必照做**）

1. **可靠跑单测 = `mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest='<iceberg 测试类逗号列表>' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false [-Dcheckstyle.skip=true]`**（绝对 `-f <abs>/fe/pom.xml`）。原因：`${revision}` + 未 flatten 已装 pom → `-pl iceberg` 解析不到 `fe-connector:pom:${revision}`，必须 `-am`；`-am test` 只到 test 相不产 hms-hive-shade shade jar（缺 `HiveConf`），故 **`install`**。`-Dtest=<类列表>`（`ls src/test/.../*Test.java` 拼逗号，本轮 56 类，存 `scratchpad/iceberg-tests.txt`）让上游快跳。
2. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）。
3. **别用 `mvn -q` 跑测试**（本轮踩坑）：成功时抑制 `BUILD SUCCESS`/`Tests run` INFO 行 → 无法从日志确认。**去掉 `-q`**，或用 surefire XML 聚合（`target/surefire-reports/TEST-*.xml` 求和 tests/failures/errors）验证。
4. **别 `nohup ... &` 套 `run_in_background`**——外层立即随 echo 退出、maven 变孤儿，"exit 0" 假信号。直接 `mvn ... > log 2>&1`（run_in_background）。
5. **checkstyle 主源 ≤120（test 源 suppress）**：本轮 2 处 javadoc 超行踩坑。import 序 `SAME_PACKAGE(org.apache.doris.*)→THIRD_PARTY→STANDARD_JAVA` 组内字母序组间空行；**新用类记得 import**（PERF-04 漏 `java.util.Iterator`）。checkstyle 单独 `mvn checkstyle:check -pl fe-connector/fe-connector-iceberg`。
6. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。
7. **并发探测**：本目录 `find fe -newermt '-120 sec'` + `pgrep -a -f maven`。`/mnt/disk1/yy/git/doris` 是另一 worktree，不冲突。
8. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 开放问题

- **PERF-06 路由（连接器 hoist vs fe-core 框架 memo）**：`DefaultConnectorContext` 在 fe-core。须先核 `ConnectorContext` API 能否连接器侧 hoist token→map 派生；若须改 fe-core 才能 hoist，则 fe-core 框架层 token-memo（惠及所有 vended 连接器、非 source-specific，PERF-09 先例大概率合规，但须双不变证明）可能更干净。**动码前把方案上交用户定**。
