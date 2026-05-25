# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-25（白天 ④）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：**P1 阶段关闭**（批 B = T1 推迟到 P8；in-scope 100% 完成）
- **预估 context 使用**：~25%（健康；本场无编码，主要是 recon + 用户决议 + 跟踪文档同步）

---

## ✅ 本 session 完成项

### 1. 批 B (T1) recon — 揭示 callers 非 dead code

启动批 B 前对 `Jdbc*Client.java` + `JdbcFieldSchema.java` 的 fe-core 引用做了 Explore subagent 调研。结论：

| Caller（路径） | Live? | 用途 |
|---|---|---|
| `job/extensions/insert/streaming/PostgresResourceValidator.java` | ✅ 活 | CREATE JOB 时校验 PG 复制槽 / 发布；被 StreamingJobUtils → StreamingInsertJob → CreateJobCommand 链调用 |
| `job/util/StreamingJobUtils.java` | ✅ 活 | `getJdbcClient()` + `getPrimaryKeys`/`getColumnsFromJdbc`/`getTablesNameList`，CDC 表枚举 + DDL 生成 |
| `tablefunction/CdcStreamTableValuedFunction.java` | ✅ 活 | `cdc_stream` TVF，被 `CdcStream.java:46` 调，streaming 作业执行链路 |

测试侧：`StreamingJobUtilsTest`（需重写）；`JdbcFieldSchemaTest` / `JdbcClickHouseClientTest` / `JdbcClientExceptionTest`（测 legacy 本身，随源删除）。

fe-connector 侧 SPI 替换 `Jdbc*ConnectorClient`（ClickHouse/DB2/MySQL/Oracle/PostgreSQL/SQLServer/SapHana/Gbase）已就位，但 **fe-core 不能直接 import** —— 会破坏 `tools/check-connector-imports.sh` 守门。

### 2. 用户决议（Q4）：推迟 T1 到 P8 收尾

- 删 T1 需要在 `ConnectorPlugin`/`ConnectorMetadata` 上为 CDC use case 暴露 `getPrimaryKeys` / `getColumnsFromJdbc` / `listTables` 新 capability — 是 SPI 扩展工作，超出 Master Plan §3.2 P1 scope
- 现状无 runtime 风险——legacy JDBC client 仍在原位，CDC 功能正常
- 决策：T1 推迟到 P8 收尾，与 streaming CDC 重构一起做（避免 P1 阶段引入 1-2 天计划外 SPI 设计）

P1 状态因此提前关闭：**in-scope (T3+T4+T5) 100% 完成；T1 推迟 P8；T2 推迟 P4/P5**。

### 3. 跟踪文档同步

- `tasks/P1-scan-node-cleanup.md`：元信息状态翻 ✅；验收标准重新对齐（标 🚫/[x]/🟡）；任务表 T1 翻 🚫 + 备注引用 Q4；新增 白天 ④ 阶段日志条目；当前阻塞项更新
- `PROGRESS.md`：header 项目总进度 16% → 20%；§一 P1 → 100% ✅；§一 P2 → 🚧 准备启动；全局进度 8% → 12%；§三 P1 表 header 改 "✅ 已完成"，T1 行翻 🚫；§四加 白天 ④ 条目；§七 session 状态更新
- `HANDOFF.md`（本文件）：覆盖更新到 P1 阶段关闭状态

---

## 🚧 本 session 进行中 / 未完成

无编码工作。剩余动作：

1. **commit 本场 plan-doc 改动** — 3 个文件（P1 task / PROGRESS / HANDOFF）
2. **push `catalog-spi-02` 到 morningman fork**（**待用户授权**）— 含批 A commit `43a12a05ffe` + 本场 doc commit
3. **`gh pr create --repo apache/doris --base branch-catalog-spi --head morningman:catalog-spi-02`**（**待用户授权**）

---

## 📝 关键认知 / 临时发现

继承前一版认知。**本场新增**：

1. **`tools/check-connector-imports.sh` 是一个隐含的设计约束** — fe-core 不能 import fe-connector 内部类（`org.apache.doris.connector.*`），所以"复用"SPI 实现唯一通道是 `ConnectorPlugin` 接口。批 B 直接 import `JdbcConnectorClient` 替换 `JdbcClient` 本能解法**走不通**——一定要经过 SPI capability 扩展。这条约束以前 P0 文档讲过，但批 B recon 时是第一次真正触发它
2. **CDC streaming 是 SPI 未覆盖的 use case** — 现有 SPI（ConnectorMetadata.getTable / listTables / getTableHandle）是面向"标准 SELECT"的，没暴露 PK 探测、columns-from-jdbc-driver、replication-slot 校验。P8 启动前需要先在 RFC 中起 §17 章节描述这套扩展，否则 P8 实施会 stall
3. **fe-connector 侧的 `Jdbc*ConnectorClient` 是 P0 阶段 JDBC 迁移的产物** — 它们没有暴露 PK / column-from-driver 接口（按 ConnectorMetadata 标准抽象设计），所以即便允许 fe-core 直接 import 也不能直接替换 legacy client。换言之 SPI 设计本身需要扩展（不只是 "改 import 路径"）

---

## 🎯 下一个 session 第一件事

> P1 已关闭。下一阶段 P2 (trino-connector，2 周)。**预备动作**：先把批 A push + PR，再做 P2 recon。

```
1. git branch --show-current → 确认在 catalog-spi-02
   git status → 应 clean（本场 doc commit 已 push 前提下）
   git log --oneline -3 → 应见 2 个本地未推 commit：
     a) 批 A scan-node 收口（43a12a05ffe）
     b) P1 关闭 + T1 推迟 P8 doc commit
2. 读 PROGRESS.md + 本 HANDOFF + tasks/P1-scan-node-cleanup.md（确认 P1 已 ✅）
3. push + PR（如本场尚未完成）：
   git push -u origin catalog-spi-02
   gh pr create --repo apache/doris --base branch-catalog-spi \
     --head morningman:catalog-spi-02 \
     --title "[P1-T03-T05] route plugin-driven scans first in nereids translator"
4. 启动 P2 (trino-connector) recon — 用 Explore subagent：
   a. fe-core 侧 `datasource/trinoconnector/` 现状（多少类、多少 LOC）
   b. fe-connector 侧 trino-connector 模块完成度（连接器看板里目前标 70%）
   c. SPI_READY 加进 `CatalogFactory.SPI_READY_TYPES` 的预条件
   d. 反向 instanceof：grep "instanceof.*Trino" in nereids/planner（看板里目前标 0/2）
5. 创建 plan-doc/tasks/P2-trino-connector-migration.md（_template.md 复制）
6. 守门：P2 改动跨 fe-core + fe-connector 双侧，每次 commit 前
   - `mvn -pl fe-connector validate` 触发 check-connector-imports.sh
   - `mvn -pl fe-core checkstyle:check`
```

---

## ⚠️ 开放问题 / 风险提示

继承前一版；批 B 关闭 1 项、转入 P8 待办 1 项；其余沿用。

### 本场关闭

- ~~T1 何时实施~~ — 已决：推迟 P8 收尾

### 本场新增（P8 待办）

1. **P8 SPI 扩展：CDC capability 群**：为 streaming CDC 在 SPI 上暴露 `getPrimaryKeys` / `getColumnsFromJdbc` / `listTables`（候选：`ConnectorMetadata` 新 default 方法 + 或 `ConnectorPlugin` 上的 `Optional<ConnectorCdcSupport>`）；改写 PostgresResourceValidator / StreamingJobUtils / CdcStreamTableValuedFunction 走 SPI；重写 StreamingJobUtilsTest；批量删 13 个 Jdbc*Client + JdbcFieldSchema + 3 个 legacy test。**预估**：~1-2 天 SPI 设计 + ~1 天实施
2. **P8 启动前 RFC 扩展**：在 `01-spi-extensions-rfc.md` 新增 §17 章节描述 CDC capability 设计；否则 P8 实施会 stall

### 沿用（保留）

3. **T4 PluginDrivenScanNode 不支持 hudi 增量场景** — `incrementalRelation` 待 P3 Hudi 迁移时 SPI 扩展
4. **T2 已推迟到 P4/P5**（用户决议 Q2，2026-05-25）
5. **T3 fallback 保留期跨度长**（P1 → P7 20 周）—— 每连接器在 P3-P7 迁移完成后立即删对应 fallback
6. （沿用 P0）`ColumnDefinition.defaultValue` SPI 缺位 — P5/P6 评估
7. （沿用 P0）LIST/RANGE `initialValues` flatten 缺位 — P5/P6 评估
8. （沿用 P0）`PluginDrivenExternalCatalog.createTable` 返回值丢失"已存在"信息 — P5/P6/P7 评估
9. （沿用 P0）bucket 算法名 `"doris_default"` / `"doris_random"` 占位 — Hive/Iceberg 自己推导
10. （沿用 P0）Maven build cache 误导；`mvn -pl fe-core` 必须 cwd=`fe/` + `-am`；`-Dtest=` 务必带 `-DfailIfNoTests=false`
11. （沿用 P0）`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 暂无 caller — P5/P6/P7 接通
12. （沿用 P0）`ConnectorMetaInvalidator.invalidatePartition` fallback 到 invalidateTable；`invalidateStatistics` no-op
13. （沿用 P0）`mvn -pl fe-core test` 不带 `-am` 失败

---

## 📂 当前关键文件清单

### 本场（2026-05-25 白天 ④）修改

```
MOD  plan-doc/tasks/P1-scan-node-cleanup.md   (元信息 ✅；验收标准重对齐；T1 → 🚫；新增白天 ④ 日志)
MOD  plan-doc/PROGRESS.md                     (P1 → 100% ✅；P2 → 准备启动；§三 T1 翻 🚫；§四加白天 ④)
MOD  plan-doc/HANDOFF.md                      (本文件覆盖更新)
```

工作树状态（本场 commit 前）：
```
 M plan-doc/tasks/P1-scan-node-cleanup.md
 M plan-doc/PROGRESS.md
 M plan-doc/HANDOFF.md
```

### 待 push 的本地 commit（catalog-spi-02 → upstream-apache/branch-catalog-spi）

```
43a12a05ffe  [refactor](connector) [P1-T03-T05] route plugin-driven scans first in nereids translator
???????????  [doc](connector) [P1] close P1 — defer T1 to P8, batch A only      ← 本场即将创建
```

### P2 (trino-connector) 涉及的目标（recon 时确认）

```
fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/   (待 recon — 看现状)
fe/fe-connector/fe-connector-trino-connector/                          (已存在；看板里标 70%)
nereids/glue/translator/PhysicalPlanTranslator.java                    (T3 fallback 待 P2 完成时清理 trino 分支)
CatalogFactory.SPI_READY_TYPES                                          (P2 末加 "trino-connector" 进白名单)
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

- **分支 `catalog-spi-02`**：本场结束时含 2 个本地未推 commit（批 A scan-node + P1 关闭 doc）。push 与 PR 创建是**风险动作**，必须先与用户确认（已在本场末尾问过；如本场已 push，下场看 `git log --oneline -3` 验证 `origin/catalog-spi-02` 同步）
- **PR 目标分支永远是 `apache/doris:branch-catalog-spi`**（不是 master）
- **commit message** 沿用 `[refactor|feat|doc](connector) [Pn-Tnn] ...` 前缀风格（AGENT-PLAYBOOK §5.4）
- **Maven 命令**：cwd=`fe/`；`mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false`；测试用 `-Dtest=... -DfailIfNoTests=false`
- **P2 启动前必读**：`connectors/trino-connector.md`（连接器看板里目前 70% 完成度）+ Master Plan §3.3 P2 章节
- **P2 主要工作量预估**：补齐 fe-connector trino-connector 模块剩余 30%（核心是 catalog 注册 + SPI_READY_TYPES）；删 fe-core 侧 trino-connector legacy；清掉 T3 fallback 中的 trino 分支（PhysicalPlanTranslator）
- **不要试图删 13 个 Jdbc*Client** — P1 阶段已决议推迟到 P8。看到 legacy jdbc client 不要技痒
