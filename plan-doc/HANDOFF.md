# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-25（晚 ②）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：**P2 (trino-connector) recon + task 划分敲定**
- **预估 context 使用**：~25%（健康；本场仅 recon + 设计 + 文档）

---

## ✅ 本 session 完成项

### 1. P2 recon（3 路 Explore subagent 并行）

输出代码侧 facts（详尽版见 [tasks/P2-trino-connector-migration.md §阶段日志 2026-05-25 晚 ②](./tasks/P2-trino-connector-migration.md#stage-log)）：

- **fe-core 旧目录** `datasource/trinoconnector/`：10 个 .java / ~1760 LOC（最大头 `TrinoConnectorExternalCatalog` 329 / `TrinoConnectorScanNode` 342 / `TrinoConnectorPredicateConverter` 334）
- **5 个 live external caller**，全部机械路由（**无 P1-T01 那种藏起来的"活业务逻辑"**）：
  - `CatalogFactory.java:148` — factory dispatch
  - `ExternalCatalog.java:948` — enum switch → DB 实例化
  - `PhysicalPlanTranslator.java:779` — `instanceof TrinoConnectorExternalTable` → `new TrinoConnectorScanNode(...)`
  - `GsonUtils.java:402 / 457 / 476` — 3 个 class-token subtype 注册
- **反向 instanceof**：实测 1 处（dashboard "2" 为过时数字）
- **fe-connector-trino**：13 类 / 2162 LOC / **0 测试**；SPI 表面 ~95% 已 IMPL/DEFAULT；真缺 `validateProperties`、`preCreateValidation`、pushdown ops
- **SPI_READY 翻闸点**：`CatalogFactory.java:53` `ImmutableSet.of("jdbc", "es")`
- **Gson 兼容路径**：与 ES/JDBC 的 string-name redirect 同 pattern 可复用
- **import gate**：fe-connector-trino → fe-core 反向 import **0 次**，干净

### 2. 用户决议（2 个开放问题）

- **Q1**：Pushdown ops（applyFilter / applyProjection）→ **纳入 P2 批 A**（不推迟）
- **Q2**：fe-core 旧目录删除时 → **`GsonUtils:402/457/476` 三个 class-token 注册同步删除**（不留 stub 类）；image compat 全部由新增的 string-name redirect 承接

### 3. P2 task 文件 + 跟踪文档同步

- **新建** [`tasks/P2-trino-connector-migration.md`](./tasks/P2-trino-connector-migration.md)：13 tasks / 5 批次（A SPI 补齐 / B fe-core 桥接 / C 翻闸 / D 清旧 / E 测试+文档），含验收清单 + 阶段日志
- **更新** `PROGRESS.md`：§一 P2 行（10% / 🚧 / 链接到 P2 task 文件）；§三 加完整 P2 任务表；§四 加 2026-05-25 晚 ② 条目；§七 session 状态
- **更新** `connectors/trino-connector.md`：当前状态、Playbook 进度（10 文件 / 1 反向 instanceof）、SPI 矩阵（pushdown 新增）、进度日志

---

## 🚧 本 session 进行中 / 未完成

无。Recon 完整 + task 划分确认 + 文档同步完成。下场直接进编码。

---

## 📝 关键认知 / 临时发现

### 本场新增

1. **Dashboard 数字会随项目演进过时** — `connectors/trino-connector.md` 之前标"反向 instanceof 2 处"和"实现完成度 70%"。Recon 实测分别是 1 处（PhysicalPlanTranslator:779）和 ~95%（SPI 表面层面）。70% 的差距其实主要在测试缺位（0 → 4+ test class）和 fe-core 桥接（gson redirect / engine 名分支 / SPI_READY），而非 SPI 实现本身缺。**下次启动每个连接器迁移前都要 recon 一次校准，不要盲信看板**
2. **P2 比 P1-T01 安全** — fe-core 5 个 live caller 全部是机械路由（factory / enum switch / instanceof / gson subtype），**没有藏起来的业务逻辑**（不像 CDC streaming 那样会卡死删除）。P2 不需要 SPI 扩展，可以直接按 master plan §3.3 走
3. **GsonUtils 有两套 subtype 注册 pattern**（同文件不同位置）：
   - 旧：`registerSubtype(TrinoConnectorExternalCatalog.class, "TrinoConnectorExternalCatalog")` —— 类对象绑名（lines 402, 457, 476）
   - 新（ES/JDBC 迁移引入）：`registerSubtype(PluginDrivenExternalCatalog.class, "TrinoConnectorExternalCatalog")` —— 用 string redirect 把旧名指向新类（lines 411, 414 是 ES/JDBC 的同位置）
   - **P2-T03 加 3 行新 pattern；P2-T10 删 3 行旧 pattern**。这两步要在同一个 PR 里，否则镜像反序列化会撞 ambiguity

### 沿用前一版（仍然有效）

1. **`tools/check-connector-imports.sh` 是 fe-core import 的隐含设计约束** — fe-core 不能 import `org.apache.doris.connector.*`；"复用 SPI 实现"唯一通道是 ConnectorPlugin 接口
2. **P1 批 A 设计了"迁移期 fallback"模式** — `PluginDrivenExternalTable` 分支前置在 `PhysicalPlanTranslator.visitPhysicalFileScan`；老 instanceof 链原地保留。**P2-T08 删 trino 那一分支**，剩余 6 个连接器分支等 P3-P7 各自迁完时删
3. **T4 (P1) `incrementalRelation` 是 P3 Hudi SPI 缺口**（不影响 P2）

---

## 🎯 下一个 session 第一件事

> **目标**：启动 P2 批 A —— P2-T01（validateProperties + preCreateValidation）+ P2-T02（pushdown ops）。

```
1. 自检：
   git branch --show-current → 应为 catalog-spi-03
   git log --oneline -3 → 顶层应是 778c5dd610f (P1 merge)
   git status → 应该有 plan-doc/* 几个文件 dirty（PROGRESS / HANDOFF / connectors/trino-connector.md / tasks/P2-trino-connector-migration.md）
   → 决定先 commit 这些跟踪文件再开编码（推荐），还是混在批 A commit 里（不推荐）

2. 读：
   - PROGRESS.md（§三 P2 任务表）
   - 本 HANDOFF.md（本文件）
   - tasks/P2-trino-connector-migration.md（全部 13 task 说明 + 验收标准）
   - connectors/trino-connector.md（连接器 SPI 矩阵）

3. P2-T01 编码（约 30 LOC）：
   a. 读 fe-core `TrinoConnectorExternalCatalog.java` 的 `preCreateValidation` 实现（如果有；如果是分散校验，读各 property 检查点）
   b. 在 fe-connector-trino 的 `TrinoConnectorProvider.java` 实现 `validateProperties(Map<String, String>)`
   c. 在 `TrinoDorisConnector.java` 实现 `preCreateValidation()`
   d. 守门：`mvn -pl fe/fe-connector validate` （触发 check-connector-imports.sh）→ `mvn -pl fe-connector-trino -am compile` → `mvn -pl fe-connector-trino checkstyle:check`
   e. 单元测试可推到 P2-T11 批 E 一起做（也可以 T01 自带 test，看时间）
   f. commit prefix：`[feat](connector) [P2-T01] add validateProperties + preCreateValidation to trino-connector SPI`

4. P2-T02 编码（约 150-200 LOC，更大）：
   a. 设计 ConnectorExpression → Trino TupleDomain 转换（可复用现有 `TrinoPredicateConverter` 的 Doris→Trino 转换骨架）
   b. 在 `TrinoScanPlanProvider` 或 `TrinoConnectorDorisMetadata` 之一实现 `applyFilter` / `applyProjection`（看哪个更合适，先看 ES/JDBC 把 push-down 放哪）
   c. 单元测试**建议 inline**（pushdown 改动大，无测试很难 review）
   d. 守门同 T01
   e. commit prefix：`[feat](connector) [P2-T02] bridge Trino native pushdown to ConnectorPushdownOps`

5. 批 A 完成判据：T01+T02 两个 commit；fe-connector-trino 单元测试本地全绿；import gate 通过；本地 smoke（手动 CREATE CATALOG ... TYPE 'trino-connector' 走老路径，因 SPI_READY_TYPES 还没加，应 fallthrough 到老 factory 不出错）

6. 守门 / 验证清单：
   - `mvn -pl fe-connector validate`（cwd=`fe/`）
   - `mvn -pl fe-connector-trino -am compile`（cwd=`fe/`）
   - `mvn -pl fe-connector-trino test -DfailIfNoTests=false`（cwd=`fe/`）
   - `mvn -pl fe-core checkstyle:check`（cwd=`fe/`）—— 仅在批 A 改动了 fe-core 时

7. 不要在 P2-T01/T02 commit 里碰 fe-core —— 批 A 严格 fe-connector 侧；fe-core 改动等批 B 启动
```

---

## ⚠️ 开放问题 / 风险提示

### P2 in-flight 待解（执行 task 时再决议，不阻塞启动）

1. **批 A pushdown 的 applyFilter 实现位置** — `ConnectorPushdownOps` SPI 在 `Connector` 还是 `ConnectorMetadata`？需要在 T02 编码前查 ES/JDBC 怎么实现的。**ETA**：T02 启动时第一件事
2. **批 A 是 1 个 PR 还是 2 个 PR** — T01（30 LOC）+ T02（200 LOC）一起 PR 还是分开？建议合一个，因为都属于"fe-connector-trino SPI 补齐"。**待批 A 编码完时定**
3. **批 B 的 4 个 task 拆分粒度** — T03+T04+T05（Gson + registerCompatibleSubtype）改 fe-core 同一类文件，建议合并 commit；T06（engine 名分支）独立。**批 A 完成时再定**
4. **批 D T10 删 GsonUtils 三个旧注册必须和 T03 加新 redirect 同一个 PR** — 否则旧 FE image 反序列化会撞 ambiguity（旧名同时绑两个类）。**强约束**

### 沿用（前一版保留）

5. **P8 待办：CDC capability 群** — 为 streaming CDC 在 SPI 上暴露 `getPrimaryKeys` / `getColumnsFromJdbc` / `listTables`；改写 PostgresResourceValidator / StreamingJobUtils / CdcStreamTableValuedFunction 走 SPI；批量删 13 个 Jdbc*Client + JdbcFieldSchema + 3 个 legacy test。**预估**：~1-2 天 SPI 设计 + ~1 天实施
6. **P8 启动前 RFC 扩展**：在 `01-spi-extensions-rfc.md` 新增 §17 章节描述 CDC capability 设计
7. **T4 (P1) `incrementalRelation`** — 待 P3 Hudi 迁移时 SPI 扩展
8. **T2 (P1) 已推迟到 P4/P5**（用户决议 Q2，2026-05-25）
9. **T3 (P1) fallback 保留期跨度长**（P1 → P7 20 周）—— 每连接器在 P2-P7 迁移完成后立即删对应 fallback
10. （沿用 P0）`ColumnDefinition.defaultValue` SPI 缺位 — P5/P6 评估
11. （沿用 P0）LIST/RANGE `initialValues` flatten 缺位 — P5/P6 评估
12. （沿用 P0）`PluginDrivenExternalCatalog.createTable` 返回值丢失"已存在"信息 — P5/P6/P7 评估
13. （沿用 P0）bucket 算法名 `"doris_default"` / `"doris_random"` 占位 — Hive/Iceberg 自己推导
14. （沿用 P0）Maven build cache 误导；`mvn -pl fe-core` 必须 cwd=`fe/` + `-am`；`-Dtest=` 务必带 `-DfailIfNoTests=false`
15. （沿用 P0）`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 暂无 caller — P5/P6/P7 接通
16. （沿用 P0）`ConnectorMetaInvalidator.invalidatePartition` fallback 到 invalidateTable；`invalidateStatistics` no-op
17. （沿用 P0）`mvn -pl fe-core test` 不带 `-am` 失败

---

## 📂 当前关键文件清单

### 本场（2026-05-25 晚 ②）修改 / 新增

```
NEW  plan-doc/tasks/P2-trino-connector-migration.md  (13 tasks / 5 批次 / 验收清单 / 阶段日志)
MOD  plan-doc/PROGRESS.md                            (§一 P2 → 10% 🚧；§三 加 P2 任务表；§四 加 2026-05-25 晚② 条目；§七 session 状态)
MOD  plan-doc/connectors/trino-connector.md          (当前状态/playbook/SPI 矩阵 + 进度日志)
MOD  plan-doc/HANDOFF.md                             (本文件；P1 → P2 切换状态)
```

工作树状态（本场未 commit 前）：
```
?? plan-doc/tasks/P2-trino-connector-migration.md
 M plan-doc/PROGRESS.md
 M plan-doc/connectors/trino-connector.md
 M plan-doc/HANDOFF.md
```

### 当前本地 commit 状态（catalog-spi-03 与 upstream-apache/branch-catalog-spi 同步）

```
778c5dd610f  [P1-T03-T05] route plugin-driven scans first in nereids translator (#63641)  ← P1 batch A merged
c6f056fa5bd  [feat](connector) P0 SPI baseline + DDL/Partition + import gate (T03-T27) (#63582)
63159837043  [doc](connector) add project tracking system for catalog SPI migration
```

### P2 编码涉及的目标（批 A → 批 E 顺序）

```
# 批 A
fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoConnectorProvider.java   (T01: validateProperties)
fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoDorisConnector.java      (T01: preCreateValidation)
fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoScanPlanProvider.java    (T02: applyFilter/applyProjection — 待定位置)
fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoConnectorDorisMetadata.java   (T02 备选)
fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoPredicateConverter.java  (T02: 复用)

# 批 B（fe-core）
fe/fe-core/src/main/java/org/apache/doris/persist/gson/GsonUtils.java                          (T03: line 414 后加 3 行)
fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java          (T04: gsonPostProcess line 318-341 加分支)
fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalCatalog.java                      (T05: registerCompatibleSubtype)
fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalTable.java            (T06: getEngine/getEngineTableTypeName line 196-225 加分支)

# 批 C（翻闸）
fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java                       (T07: line 53 SPI_READY_TYPES)

# 批 D（清旧）
fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java  (T08: line 779 删 instanceof 分支)
fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java                       (T09: line 147-150 删 case + 删 factory 文件)
fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/                           (T10: rm -rf 整目录)
fe/fe-core/src/main/java/org/apache/doris/persist/gson/GsonUtils.java                          (T10: 删 line 402/457/476 三个 class-token 注册)

# 批 E（测试 + 文档）
fe/fe-connector/fe-connector-trino/src/test/                                                   (T11: 新建测试包)
regression-test/suites/external_catalog/                                                       (T12: trino_connector_migration_compat)
docs-next/                                                                                     (T13: 插件安装步骤)
plan-doc/connectors/trino-connector.md                                                         (T13: 翻 100%)
plan-doc/PROGRESS.md                                                                           (T13: §一 P2 ✅)
```

### 跟踪体系（沿用不变）

```
plan-doc/  (~225K, 19 文件 [新增 P2 task])
├── 00-connector-migration-master-plan.md / 01-spi-extensions-rfc.md
├── README.md / PROGRESS.md / AGENT-PLAYBOOK.md / HANDOFF.md
├── decisions-log.md (18) / deviations-log.md (0) / risks.md (14)
├── tasks/{_template.md, P0-spi-foundation.md, P1-scan-node-cleanup.md, P2-trino-connector-migration.md}
└── connectors/{_template.md, jdbc, es, trino-connector, hudi, maxcompute, paimon, iceberg, hive}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **分支 `catalog-spi-03`** —— P2 工作就在这条分支上累积；不要新开分支
- **PR 目标分支永远是 `apache/doris:branch-catalog-spi`**；命令模板：
  ```
  gh pr create --repo apache/doris --base branch-catalog-spi \
    --head morningman:catalog-spi-03 \
    --title "[P2-Tnn] <subject>"
  ```
- **commit message 沿用** `[refactor|feat|doc](connector) [P2-Tnn] ...` 前缀
- **Maven 命令**：cwd=`fe/`；`mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false`；测试用 `-Dtest=... -DfailIfNoTests=false`
- **批 A 严格 fe-connector 侧** —— 不要混 fe-core 改动；fe-core 等到批 B 启动
- **批 D 的 T10（删旧 gson 注册）必须和批 B 的 T03（加新 redirect）在同一个 PR** —— 否则旧 FE image 反序列化会撞 ambiguity。**这是强约束，别拆开**
- **不要再 recon 一次** —— 本场已经做完 + 写进 task 文件 §阶段日志，重读它即可
- **不要乱删 `TableIf.TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` 枚举值** —— image compat 必须保留（master plan §3.3 task 2.4 明示）
- **不要试图清 P1 fallback 中除 trino 之外的分支** —— 每个分支专属于一个连接器迁移阶段；P2 只清 trino
- **必读 AGENT-PLAYBOOK §六 anti-patterns**（每个新 session 都温习一次）
