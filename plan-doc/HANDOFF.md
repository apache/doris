# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-24（深夜）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：P0 批 0 fe-core 桥接（T09-T12）—— 把新 SPI 接通到现有 fe-core 实现
- **预估 context 使用**：~40%（健康范围）

---

## ✅ 本 session 完成项

### 1. P0 批 0：fe-core 桥接（T09-T12）

| ID | 任务 | 文件 | 备注 |
|---|---|---|---|
| T09 ✅ | `DefaultConnectorContext.getMetaInvalidator()` override | edit `fe-core/.../connector/DefaultConnectorContext.java` | 返回 `new ExternalMetaCacheInvalidator(catalogId)` |
| T10 ✅ | `ExternalMetaCacheInvalidator`（fe-core 新类）| **新** `fe-core/.../connector/ExternalMetaCacheInvalidator.java` | 3 个方法直接代理 `ExternalMetaCacheMgr.invalidateCatalog/Db/Table`；`invalidatePartition` 暂回退到 `invalidateTable`；`invalidateStatistics` 暂 no-op |
| T11 ✅ | `PluginDrivenTransactionManager` 通用化 | edit `fe-core/.../transaction/PluginDrivenTransactionManager.java` | 新增 `begin(ConnectorTransaction)` 重载 + inner class 加 nullable `connectorTx` 字段，承接 SPI tx commit/rollback/close；legacy `long begin()` 路径行为完全不变 → JDBC/ES auto-commit 零回归 |
| T12 ✅ | `ConnectorMvccSnapshotAdapter`（fe-core 新类）| **新** `fe-core/.../connector/ConnectorMvccSnapshotAdapter.java` | 包装 `ConnectorMvccSnapshot` + implements `MvccSnapshot`（marker 接口） |

### 2. 验证

- `mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false` → **BUILD SUCCESS**
- `mvn -pl fe-core checkstyle:check` → **0 violations**
- `mvn -pl fe-connector/fe-connector-jdbc,fe-connector-es -am compile` → **BUILD SUCCESS**（下游 connector 零影响）

### 3. 文档同步（§5.1 五步纪律）

- ✅ `tasks/P0-spi-foundation.md`：T09-T12 状态翻 ✅，新增 2026-05-24（深夜）日志条目
- ✅ `PROGRESS.md`：§一 P0 进度条 30% → 44%（12/27 任务）；§三 P0 表更新；§四加 2026-05-24（深夜）条目；§七 session 状态滚动
- ✅ 本 HANDOFF.md 覆写
- N/A `connectors/<name>.md`（本次工作不属任何具体连接器）
- N/A `decisions-log.md` / `deviations-log.md`（本次完全遵循 RFC §6.4 / §7.4 / §8.4，未产生新决策或偏差）

---

## 🚧 本 session 进行中 / 未完成

**无**。批 0 全部 10 项任务（T03-T12）+ 编译验证 + 文档同步全部收尾。

---

## 📝 关键认知 / 临时发现

继承上版 HANDOFF 认知不变。**本场新增**：

1. **`ConnectorMetaInvalidator.invalidatePartition(values)` 与 fe-core `ExternalMetaCacheMgr.invalidatePartitions(names)` 语义不对齐**——SPI 给的是 partition column VALUES（如 `["2024", "01"]`），engine 缓存按 partition NAMES（如 `"year=2024/month=01"`）键控。重构 partition name 需要 partition column names，SPI 目前没带。**本场决策**：fallback 到 `invalidateTable`（正确但过粗），等 SPI 后续补 partition column metadata 时再做精确路由。**待评估是否走 DV 流程**——目前是 RFC §6.4 的实现 trade-off 而非偏差，先列为开放问题。
2. **`ConnectorMetaInvalidator.invalidateStatistics(db, t)` 当前 no-op**——SPI 契约要求"不能 drop schema cache"，但 fe-core `ExternalMetaCacheMgr` 没有 per-table stats-only invalidation 入口（`ExternalRowCountCache` 按 id 键控，与 SPI 的 name-based 调用不匹配）。先 no-op + 注释解释；待 P7 hive ACID 统计场景出现时再补。
3. **`PluginDrivenTransactionManager` 通用化策略**——RFC §7.4 的设计示例（`ConnectorTransaction begin(Connector, ConnectorSession)`）与现有 `TransactionManager.begin() -> long` 接口签名冲突。**本场决策**：surgical 路径——保留 legacy `long begin()`（JDBC/ES auto-commit 必需），新增 `long begin(ConnectorTransaction)` 重载。inner class 加 nullable `connectorTx` 字段。无 caller 现在调用新方法（是 forward-compat plumbing，等 P5/P6/P7 连接器迁移时接通）。
4. **`MvccSnapshot` 是 marker 接口**（fe-core 侧），所以 `ConnectorMvccSnapshotAdapter` 极简——只一个 `getSnapshot()` 暴露 SPI 类型。Iceberg / Paimon / Hudi 各自的 `*MvccSnapshot` 实现也都只是 wrapper。
5. **跨模块依赖方向重新确认**：`fe-core` → `fe-connector-api`（imports `org.apache.doris.connector.api.*`）；`fe-core` → `fe-connector-spi`（imports `org.apache.doris.connector.spi.*`）。**无循环**。本场 `ExternalMetaCacheInvalidator` 放 fe-core 是对的（因为它要 import `Env` + `ExternalMetaCacheMgr`，反向是禁止的）。

---

## 🎯 下一个 session 第一件事

### 强烈建议先 review 本场 4 文件改动再开始 P0-T13+

理由：T09-T12 是"SPI ↔ fe-core 互通"的实际胶水层。错了影响范围是 P0 之后所有连接器（每个都会读这层桥接）。

→ 新 session 第一步：用户人工/agent 评审本场 4 文件：
- `fe-core/.../connector/ExternalMetaCacheInvalidator.java` (新)
- `fe-core/.../connector/ConnectorMvccSnapshotAdapter.java` (新)
- `fe-core/.../connector/DefaultConnectorContext.java` (edit)
- `fe-core/.../transaction/PluginDrivenTransactionManager.java` (edit)

如有调整建议，走 DV 流程登记（特别是上面"关键认知 #1"的 `invalidatePartition` trade-off 是否要做精确路由）。

### Track A：本场 4 文件 commit

```
1. cd /Users/morningman/workspace/git/wt-fs-spi
2. git status
3. git add fe/fe-core/src/main/java/org/apache/doris/connector/ExternalMetaCacheInvalidator.java \
           fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorMvccSnapshotAdapter.java \
           fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java \
           fe/fe-core/src/main/java/org/apache/doris/transaction/PluginDrivenTransactionManager.java \
           plan-doc/PROGRESS.md plan-doc/HANDOFF.md plan-doc/tasks/P0-spi-foundation.md
4. git commit -m "[P0-T09..T12][fe-core] wire MetaInvalidator / Transaction / MvccSnapshot into fe-core"
```

### Track B：P0 批 1（DDL + Partition SPI，T13-T20）

仅在 Track A commit + review 后启动：

```
1. Read plan-doc/PROGRESS.md + plan-doc/HANDOFF.md
2. Read plan-doc/01-spi-extensions-rfc.md §4 (E1 CreateTableRequest) + §13 (E10 listPartitions)
3. 新增 8 个任务（T13-T20）：
   - T13: `ConnectorCreateTableRequest` + Partition/Bucket Spec POJO（5 个类）放 ddl 包
   - T14: `ConnectorTableOps.createTable(request)` default → 退化到旧 createTable
   - T15: `CreateTableInfoToConnectorRequestConverter`（fe-core 新类）
   - T16: `PluginDrivenExternalCatalog.createTable(stmt)` 接通 SPI
   - T17-T19: `ConnectorTableOps.listPartitionNames / listPartitions / listPartitionValues` default
   - T20: `ConnectorPartitionInfo` 追加字段（rowCount/sizeBytes/lastModifiedMillis）+ 向后兼容构造器
4. mvn -pl fe-connector -am compile + JDBC/ES 回归
5. 更新 tasks / PROGRESS / HANDOFF
```

预计 context 用量 ~50-60%（批 1 主要在 fe-connector-api 侧，文件相对独立）。

---

## ⚠️ 开放问题 / 风险提示

继承上版 5 项不变。**本场新增 / 更新**：

1. **`invalidatePartition` 的 SPI 设计 trade-off**（新）：当前 fallback 到 `invalidateTable`。是否要给 SPI `ConnectorMetaInvalidator.invalidatePartition` 加 `List<String> partitionColumnNames` 参数？若加，则 fe-core adapter 能精确路由到 `mgr.invalidatePartitions(...)`。**先收集 Hive/Iceberg 实际使用频率再决定**（P7 评估），目前的过粗 invalidation 在正确性上无问题，只是缓存命中率会差。
2. **`invalidateStatistics` no-op**（新）：fe-core `ExternalMetaCacheMgr` 缺 stats-only invalidation 入口。**可能需要**在 P7 hive ACID 写路径完工时补一个 `mgr.invalidateTableStats(catalogId, db, t)` 方法。**记到 R-014 之后**作为新风险？暂不加，先 no-op + 注释，等具体场景出现。
3. **Maven build cache 误导问题**（沿用上版）：所有 SPI 改动验证步骤强制加 `-Dmaven.build.cache.enabled=false` 或 `clean`。
4. **本次 7 文件改动尚未 commit**——见 Track A。
5. **`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 暂无 caller**（forward-compat plumbing）。当 P5/P6/P7 连接器开始实现 `ConnectorWriteOps.beginTransaction` 时，`BaseExternalTableInsertExecutor`（或同等位置）需要改为 if-else：连接器实现了 → 调新方法；否则 → 走 legacy `begin()`。**这一步留在 P5/P6/P7**，不在 P0 范围。
6. （沿用）`PluginDrivenTransactionManager` 通用化对 JDBC auto-commit 的回归——本场已通过 fe-connector-jdbc clean compile 验证语法层无回归；**行为层回归留 P0-T24 跑全套 JDBC regression test**。

---

## 📂 当前关键文件清单

### 本场新增 / 修改

```
NEW  fe/fe-core/src/main/java/org/apache/doris/connector/ExternalMetaCacheInvalidator.java
NEW  fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorMvccSnapshotAdapter.java
MOD  fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java
MOD  fe/fe-core/src/main/java/org/apache/doris/transaction/PluginDrivenTransactionManager.java
MOD  plan-doc/PROGRESS.md
MOD  plan-doc/tasks/P0-spi-foundation.md
MOD  plan-doc/HANDOFF.md（本文件）
```

### 跟踪体系（沿用不变）

```
plan-doc/  (~220K, 17 文件)
├── 00-connector-migration-master-plan.md / 01-spi-extensions-rfc.md
├── README.md / PROGRESS.md / AGENT-PLAYBOOK.md / HANDOFF.md
├── decisions-log.md (18) / deviations-log.md (0) / risks.md (14)
├── tasks/{_template.md, P0-spi-foundation.md}
└── connectors/{_template.md, jdbc, es, trino-connector, hudi, maxcompute, paimon, iceberg, hive}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **当前分支是 `catalog-spi-00`**。新 session 开场 `git branch --show-current` 确认。
- 接 Track B（批 1 DDL+Partition SPI）前**强烈建议人工 review 本场 4 文件改动**——它们是 SPI ↔ fe-core 胶水层，错了影响所有后续连接器。
- 上版 meta 建议#3 仍然成立："scope 判定以 tasks/Pn-*.md 为准，HANDOFF 只是建议起点"。批 0 在 tasks 表中明确划分为"SPI 接口三件套（T03-T08）"+"fe-core 桥接（T09-T12）"两个子批，本场补完后者，整批 0 收尾。下一 session 进入批 1（T13-T20）。
- 本场没动 RFC 一个字，无新 decision / deviation。沿用 meta 建议 "不要重新打开 D-001..D-018"。
- **本场对 `PluginDrivenTransactionManager` 的改造方式是个值得学习的 surgical change 模板**：保持接口签名 + 加重载 + nullable 字段 + 文档化两条路径——避免了对 `TransactionManager` 接口、`BaseExternalTableInsertExecutor` 调用方、JDBC/ES auto-commit 的任何破坏。批 1+ 的迁移如果遇到类似"老路径不能动 / 新功能要加"的情况，可参照此 pattern。
- **必读 AGENT-PLAYBOOK §六 anti-patterns** 再开始动手。
