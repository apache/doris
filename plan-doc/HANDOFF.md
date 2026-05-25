# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-25（白天 ③）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：**P1 批 A 编码完成**（T3 + T4 + T5 — scan-node SPI 收口；保留迁移期 fallback）
- **预估 context 使用**：~45%（健康；本次 session 直奔 batch A，无需 recon）

---

## ✅ 本 session 完成项

### 1. P1 批 A 三处源码改动

承接 Q3 决议（"保留 fallback"），在三处 hot path 加 `PluginDrivenExternalTable` 优先分支，老 instanceof 链原地保留：

| 任务 | 文件 | 改动要点 |
|---|---|---|
| **T3** | `PhysicalPlanTranslator.java:733-744` | `visitPhysicalFileScan` 中把现有 `PluginDrivenExternalTable` 分支从位置 8 提到位置 1；7 个连接器特定分支（HMS/Iceberg/Paimon/Trino/MaxCompute/LakeSoul/RemoteDoris）原地保留 |
| **T4** | `PhysicalPlanTranslator.java:824-842` | `visitPhysicalHudiScan` 顶部新增 `PluginDrivenExternalTable` 分支，路由到 `PluginDrivenScanNode.create(...)`；通过 `FileQueryScanNode` setters 透传 `tableSnapshot` + `scanParams`；`incrementalRelation` 待 P3 SPI 扩展（inline TODO 已落） |
| **T5** | `LogicalFileScan.java:207, 235, 256` | `computeOutput()` 加 `PluginDrivenExternalTable` 优先分支，调新增 helper `computePluginDrivenOutput()`（用 `getFullSchema() + virtualColumns`，与 `computeIcebergOutput` 同 shape）；`supportPruneNestedColumn()` 加 `PluginDrivenExternalTable → false` 显式分支；新增 import `PluginDrivenExternalTable` |

**关键不变量**：对当前已迁 SPI 连接器（JDBC、ES）行为完全等价——`getFullSchema == getBaseSchema` 且 `virtualColumns` 为空，`supportPruneNestedColumn` 原本就是 false。T4 分支今日不可达（`PhysicalHudiScan` 目前仅为 `HMSExternalTable + DLAType.HUDI` 创建），P3 Hudi 迁移时激活。

### 2. 验证

- `mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false` → **BUILD SUCCESS**
- `mvn -pl fe-core checkstyle:check` → **0 violations**
- 用户决议：不在本批补单测（理由：三处对当前 SPI 连接器行为等价；T4 分支本就不可达；PR CI 上 JDBC + ES regression-test 提供集成层信号）

### 3. 跟踪文档同步

- `tasks/P1-scan-node-cleanup.md`：T3/T4/T5 状态翻 ✅；新增 "白天 ③" 阶段日志条目
- `PROGRESS.md`：P1 进度条 60%；§三 P1 task 表三行状态翻 ✅；§四加 "白天 ③" 批 A 完成条目
- 本文件（HANDOFF.md）：覆盖更新到 P1 批 A 结尾状态

---

## 🚧 本 session 进行中 / 未完成

无。批 A 三处源码 + 跟踪文档全部就绪，等待 commit + push + 起 PR。

---

## 📝 关键认知 / 临时发现

继承上版认知不变。**本场新增**：

1. **T4 加的分支今日不可达** — `PhysicalHudiScan` 节点目前只为 `HMSExternalTable + DLAType.HUDI` 创建。我们提前在 `visitPhysicalHudiScan` 顶部加 `PluginDrivenExternalTable` 分支是"为 P3 准备地基"——P3 Hudi 迁移时 logical→physical 转换会开始产出 `PhysicalHudiScan(PluginDrivenExternalTable)`，那时该分支激活。Inline TODO 也记了 `incrementalRelation` 还需 SPI 扩展。
2. **`computeIcebergOutput(IcebergExternalTable iceTable)` 参数其实没用** — body 内只用 `table` 字段（class 成员），没碰 `iceTable`。说明该方法本质不是"iceberg-specific"，只是历史包装。新加的 `computePluginDrivenOutput()` 是 body 副本（避免重命名 iceberg 方法触发 Rule 3）；未来 Iceberg 真正迁到 SPI 时（P6），两个方法可合并。
3. **`ExternalTable.getFullSchema` vs `getBaseSchema`**：fullSchema 是所有列；baseSchema = fullSchema 过滤 `Column::isVisible`。当前 JDBC/ES 表无 hidden cols，所以 T5 切到 fullSchema 是 no-op；未来 SPI 连接器要 expose metadata col 时直接用 fullSchema 路径。
4. **`PluginDrivenScanNode.create()` 不接受 hudi-specific 参数** — `scanParams` 通过 `FileQueryScanNode.setScanParams()` 后置传入（FileQueryScanNode 是父类）；`incrementalRelation` 没有对应 setter，是真正的 SPI 缺口。
5. **`fe/` cwd 习惯**：bash tool 内部 cwd 在 session 内是持久的。本次 `cd fe` 一次后所有 mvn 命令都从 `/fe` 跑，不需要每次 `cd`。

---

## 🎯 下一个 session 第一件事

> 用户已确认 P1 顺序 A → B → C；批 A 完成，下一步是**批 B = T1**（删除 13 个 legacy JDBC client + JdbcFieldSchema）。**不要** re-ask 这个授权。

```
1. git branch --show-current  →  确认在 catalog-spi-02
2. 读 PROGRESS.md + 本 HANDOFF + tasks/P1-scan-node-cleanup.md
3. 开始批 B 编码（T1）：
   a. 第一步：解耦 3 个 fe-core 外部 caller（**不是** 直接删 client）：
      - PostgresResourceValidator.java     — 调 JdbcPostgreSQLClient 做什么？能否替成 SPI 调用或直接删？
      - StreamingJobUtils.java             — 同上
      - CdcStreamTableValuedFunction.java  — 同上
      如果 caller 自身已是 dead code（被 P0/P1 间接淘汰），直接删 caller；否则寻找替代 API（可能需要 SPI 扩展，那就升 DV-NNN）
   b. 处理 3 个测试文件（recon 时未细查，应一并跟踪）
   c. 批量删除：13 个 fe/fe-core/src/main/java/org/apache/doris/datasource/jdbc/client/Jdbc*Client.java + JdbcFieldSchema.java
   d. fe-core compile + checkstyle → 跟踪文档同步 → 用户 review → commit → push → PR
4. 当前工作树应为 clean（批 A 已 commit/push 完成）。如果还有 dirty plan-doc 文件，说明上次 session commit 未完成，先确认状态
5. 守门：批 B 主要改 fe-core/datasource/jdbc/，不动 fe-connector 模块，所以 `tools/check-connector-imports.sh` 不会触发；但 commit 前仍跑 `mvn -pl fe-core checkstyle:check`
```

---

## ⚠️ 开放问题 / 风险提示

继承上一版；批 A 关闭 3 项，新增 1 项：

### 批 A 关闭

- ~~T3/T4/T5 实施未定~~ — 已完成
- ~~Q1/Q2/Q3 决策点~~ — 已落地

### 新增（P1 批 A）

1. **T4 PluginDrivenScanNode 不支持 hudi 增量场景** — `incrementalRelation` 字段在 `PluginDrivenScanNode.create()` 没有传递通道。P3 Hudi 迁移时需要 SPI 扩展（候选：`ConnectorIncrementalScan` 接口或在 `ConnectorTableHandle` 上挂 incremental 上下文）。**当前**：T4 分支不可达，所以没有 runtime 风险，但 P3 启动前要明确扩展方案，否则 Hudi 真迁过来无法 incremental scan

### 沿用（保留）

2. **T1 删除有外部 caller**（P1-T01 本场未启动）：`PostgresResourceValidator` / `StreamingJobUtils` / `CdcStreamTableValuedFunction` 需要先解耦。如果 dead code 可直接删；否则要找替代 API（可能涉及 SPI 扩展，那就升 DV）
3. **T2 已推迟到 P4/P5**（用户决议 Q2，2026-05-25）：P1 阶段不动 fe-core 重复 converter；P4/P5 真正迁连接器时一起删 fe-core 副本 + caller
4. **T3 fallback 保留期跨度长**：从 P1 到 P7 整整 20 周，期间老分支必须仍能通过编译；任何对 `HMSExternalTable` / `IcebergExternalTable` 等 schema 的改动都要同步老分支。**缓解**：每个连接器在 P3-P7 迁移完成后立刻删对应 fallback，缩短窗口
5. （沿用 P0）`ColumnDefinition.defaultValue` SPI 缺位 — P5/P6 真用到时评估
6. （沿用 P0）LIST/RANGE `initialValues` flatten 缺位 — P5/P6 评估
7. （沿用 P0）`PluginDrivenExternalCatalog.createTable` 返回值丢失"已存在"信息 — P5/P6/P7 评估
8. （沿用 P0）bucket 算法名 `"doris_default"` / `"doris_random"` 占位 — Hive/Iceberg 自己推导
9. （沿用 P0）Maven build cache 误导；`mvn -pl fe-core` 必须 cwd=`fe/` + `-am`；`-Dtest=` 务必带 `-DfailIfNoTests=false`
10. （沿用 P0）`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 暂无 caller — P5/P6/P7 接通
11. （沿用 P0）`ConnectorMetaInvalidator.invalidatePartition` fallback 到 invalidateTable；`invalidateStatistics` no-op
12. （沿用 P0）`mvn -pl fe-core test` 不带 `-am` 失败

---

## 📂 当前关键文件清单

### 本场（2026-05-25 白天 ③）修改

```
MOD  fe/fe-core/.../nereids/glue/translator/PhysicalPlanTranslator.java  (+22 / -11)
MOD  fe/fe-core/.../nereids/trees/plans/logical/LogicalFileScan.java    (+25 / -0)
MOD  plan-doc/tasks/P1-scan-node-cleanup.md   (T3/T4/T5 → ✅；新增白天 ③ 阶段日志)
MOD  plan-doc/PROGRESS.md                     (P1 进度 60%；§三/§四 同步)
MOD  plan-doc/HANDOFF.md                      (本文件)
```

工作树状态（commit 前）：
```
 M fe/fe-core/.../PhysicalPlanTranslator.java
 M fe/fe-core/.../LogicalFileScan.java
 M plan-doc/HANDOFF.md
 M plan-doc/PROGRESS.md
?? plan-doc/tasks/P1-scan-node-cleanup.md
```

### P1 批 B（T1）涉及的源文件（recon 时确认存在）

```
fe/fe-core/src/main/java/org/apache/doris/datasource/jdbc/client/Jdbc{Client,ClientConfig,ClientException,ClickHouseClient,DB2Client,GbaseClient,MySQLClient,OceanBaseClient,OracleClient,PostgreSQLClient,SQLServerClient,SapHanaClient,TrinoClient}.java  (13 × ~210 LOC avg)
fe/fe-core/src/main/java/org/apache/doris/datasource/jdbc/util/JdbcFieldSchema.java                                                                  (129 LOC)
fe/fe-core/src/main/java/org/apache/doris/datasource/jdbc/PostgresResourceValidator.java                                                              (caller 1)
fe/fe-core/.../StreamingJobUtils.java                                                                                                                  (caller 2 — 路径待 batch B 启动时再 grep 定位)
fe/fe-core/.../CdcStreamTableValuedFunction.java                                                                                                       (caller 3 — 路径待 batch B 启动时再 grep 定位)
```

### 跟踪体系（沿用不变）

```
plan-doc/  (~225K, 18 文件)
├── 00-connector-migration-master-plan.md / 01-spi-extensions-rfc.md
├── README.md / PROGRESS.md / AGENT-PLAYBOOK.md / HANDOFF.md
├── decisions-log.md (18) / deviations-log.md (0) / risks.md (14)
├── tasks/{_template.md, P0-spi-foundation.md, P1-scan-node-cleanup.md}
└── connectors/{_template.md, jdbc, es, trino-connector, hudi, maxcompute, paimon, iceberg, hive}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **当前分支 `catalog-spi-02`**——基于最新 `upstream-apache/branch-catalog-spi`。批 A commit 已 push 到 fork（如果本场完成 push）；批 B 继续在同分支上累积
- **PR 目标分支永远是 `apache/doris:branch-catalog-spi`**——不是 master；批 A push 后用 `gh pr create --repo apache/doris --base branch-catalog-spi --head morningman:catalog-spi-02 --title "[P1-T03-T05] route plugin-driven scans first in nereids translator"`
- **commit message** 沿用 `[refactor](connector) [P1-Tnn] ...` 前缀风格（AGENT-PLAYBOOK §5.4）
- **Maven 命令**：cwd=`fe/`；`mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false`；测试用 `-Dtest=... -DfailIfNoTests=false`
- **`tools/check-connector-imports.sh` 守门**：批 B 改 fe-core/datasource/jdbc/，不动 fe-connector，所以守门不触发；但每批 commit 前仍建议跑 `mvn -pl fe-connector validate` 确认基础设施未被破坏
- **必读 AGENT-PLAYBOOK §六 anti-patterns** 再开始动手
- **批 B 启动前先 recon**：3 个外部 caller 的 grep + 阅读，确认是否是 dead code 或需要替代 API。建议用 Explore subagent 节省主线 context
- **T4 分支不可达**：批 A 加了，但今日没有产出 `PhysicalHudiScan(PluginDrivenExternalTable)` 的代码路径。这是"为 P3 准备"，**不是** 本阶段的工作产出 — 不要为它写测试，不要担心它的覆盖率
