# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-25（晚 ④）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：**P2 批 A（T01+T02）+ 批 B（T03-T06）连续完成**
- **预估 context 使用**：~50%（健康；本场连做两个批次 + 三处 HANDOFF 描述校正）

---

## ✅ 本 session 完成项

### 1. 批 A — fe-connector-trino SPI 补齐（commit `31fb91c5bd3`）

3 files / +143 LOC，全 `fe-connector-trino/`，**未触 fe-core**（严守批 A 边界）。

- **T01** `TrinoConnectorProvider.validateProperties`：required-property check `trino.connector.name`（mirror ES pattern；JDBC 的多属性校验更重，不适用 Trino）
- **T01** `TrinoDorisConnector.preCreateValidation(ConnectorValidationContext)`：直接调 `ensureInitialized()`，把 plugin loading + connector factory 解析从首次 SELECT 前移到 CREATE CATALOG
- **T02** `TrinoConnectorDorisMetadata.applyFilter`：复用 `TrinoPredicateConverter` 把 `ConnectorExpression` 转 `TupleDomain<ColumnHandle>`；`tupleDomain.isAll()` 早返回；否则调 Trino native applyFilter，包装新 `TrinoTableHandle`（保留原 column maps）；**`remainingFilter` 保守返回原 expression**（匹配 legacy 不剥 conjuncts）
- **T02** `TrinoConnectorDorisMetadata.applyProjection`：从 `List<ConnectorColumnHandle>` 构 Trino `Map<String, ColumnHandle> assignments` + `List<Variable>`；调 Trino native applyProjection；包装新 handle；返回 `ProjectionApplicationResult(handle, List<ConnectorColumnRef>, List<ConnectorColumnAssignment>)`

### 2. 批 B — fe-core 桥接（commit `dfd48725c76`）

3 files / +29 LOC，全 fe-core。

- **T03** `GsonUtils.java`：**atomic replace**（不是"只加"）
  - delete 3 个 `registerSubtype(TrinoConnectorExternal{Catalog,Database,Table}.class, ...)`（lines 401-402 / 457 / 476）
  - add 3 个 `registerCompatibleSubtype(PluginDrivenExternal{Catalog,Database,Table}.class, "TrinoConnectorExternal{Catalog,Database,Table}")` 紧跟 JDBC redirect 之后
  - remove 3 个 import
- **T04** `PluginDrivenExternalCatalog.gsonPostProcess`：把 `logType.name().toLowerCase(Locale.ROOT)` 改为新 helper `legacyLogTypeToCatalogType(logType)`；helper 内 case TRINO_CONNECTOR 返 `"trino-connector"`，default 走原 `name().toLowerCase()`
- **T06** `PluginDrivenExternalTable.getEngine() / getEngineTableTypeName()`：各加一个 `case "trino-connector":` 分支。getEngine 返 `TRINO_CONNECTOR_EXTERNAL_TABLE.toEngineName()`（null — 保留 legacy 行为）；getEngineTableTypeName 返 `.name()`

### 3. HANDOFF 描述校正（recon 落到代码后才发现）

| ID | 原 HANDOFF 描述 | 实际情况 | 处理 |
|---|---|---|---|
| T03 | "只加 redirect；删旧 class-token 在 T10" | `RuntimeTypeAdapterFactory.{registerSubtype,registerCompatibleSubtype}` 都做 `labelToSubtype.containsKey(label) → throw IAE`；同时有两个 binding 同 label 抛 IAE → FE 起不来 | **必须 atomic replace 在 T03**；T10 不再碰 GsonUtils（只删目录 + CatalogFactory case） |
| T04 | "参照 ES/JDBC 已有分支 backfill `type`" | ES/JDBC 用 `name().toLowerCase()` 刚好 match `"es"`/`"jdbc"`；但 `Type.TRINO_CONNECTOR.name().toLowerCase()` = `"trino_connector"`（下划线）≠ CatalogFactory 期望 `"trino-connector"`（连字符） | 抽 helper `legacyLogTypeToCatalogType` 做 case 映射 |
| T05 | "`ExternalCatalog.registerCompatibleSubtype` 注册" | 不存在这个 API；`registerCompatibleSubtype` 只在 `RuntimeTypeAdapterFactory` 上，由 `GsonUtils` 调用 | T05 是 duplicate of T03，自动满足 |
| T06 | "返回 `TableType.TRINO_CONNECTOR_EXTERNAL_TABLE.toEngineName()`" | `TableType.toEngineName()` switch 没有 case for TRINO_CONNECTOR_EXTERNAL_TABLE → 返 null | 保留 legacy null 行为（不修 toEngineName） |

### 4. 跟踪文档同步

- `PROGRESS.md`：§一 P2 → 40% / 批 A+B ✅；§三 task 状态翻 ✅；§四 加 2026-05-25 晚 ③ + 晚 ④ 两条；§七 session 状态
- `tasks/P2-trino-connector-migration.md`：T01-T06 状态翻 ✅；阶段日志加批 A + 批 B 完成条目（含详尽 HANDOFF 校正记录）
- `connectors/trino-connector.md`：完成度 30% → 65%；Playbook 步骤 5/8/9 翻 ✅；SPI 矩阵 pushdown ✅；进度日志加批 A + 批 B 条目
- `HANDOFF.md`：**本文件**（rewrite）

---

## 🚧 本 session 进行中 / 未完成

无。批 A 和 批 B 都已完整 commit；工作树干净；本地 fe-connector + fe-core 全绿。

---

## ⚠️ 严重警告 — 中间状态有 regression window

**批 B 已删 `registerSubtype(TrinoConnectorExternalCatalog.class, ...)`，但 `CatalogFactory` 仍走 legacy `case "trino-connector"` 分支创建 `TrinoConnectorExternalCatalog` 实例（T07 翻闸前）。**

后果：在当前 HEAD（`dfd48725c76`）上启动 FE 并 `CREATE CATALOG xxx PROPERTIES ('type' = 'trino-connector', ...)`，会：
1. CatalogFactory 创建 `TrinoConnectorExternalCatalog` 实例（legacy 路径）
2. Image 持久化时 Gson 找不到该 class 的 `registerSubtype` 注册 → 序列化抛 IllegalArgumentException
3. **catalog 无法持久化**

**缓解**：批 C T07 必须紧接批 B 操作；中间状态 **不要部署 / 不要在生产 FE 上跑**。本地 smoke 测试也应直接 跑 batch C 之后的状态。

---

## 📝 关键认知 / 临时发现

### 本场新增

1. **`RuntimeTypeAdapterFactory` label 唯一性是强约束**（fe-common line 237 + 279）。任何 connector 迁移 atomic 改 GsonUtils 时，必须是一个 commit 内 delete 旧 `registerSubtype` + add 新 `registerCompatibleSubtype`。ES/JDBC 历史 commit `5c325655b8b` 就是这么做的。**P3-P7 各连接器都要遵循此 pattern**。
2. **`Type.X.name().toLowerCase()` 不通用** — ES/JDBC 是巧合 match。新连接器的 `InitCatalogLog.Type` enum 名要么和 SPI type 字符串显式映射（如本场抽的 `legacyLogTypeToCatalogType` helper），要么改 enum 让它直接 match（不推荐——破坏 image 反序列化）。**P3 Hudi 检查 `Type.HUDI` ↔ "hudi" 没问题**；**P4 MaxCompute `Type.MAX_COMPUTE` ↔ "max_compute" 也 OK（CatalogFactory 用下划线）**；**P5 Paimon、P6 Iceberg、P7 Hive 检查时务必校对**。
3. **`TableType.X.toEngineName()` 默认 case 缺失会返 null** — 不是所有 TableType 都有 case in `TableIf.java:225-273`。P3+ 连接器在 PluginDrivenExternalTable.getEngine 加分支时，要先 check `<X>_EXTERNAL_TABLE.toEngineName()` 是否返 null；如果是 null，保留为 null（legacy 行为一致），不要 silently 改成枚举名 lowercase
4. **批 B 后 → 批 C 前的 regression window**是必然的（删旧注册和翻闸不可能同 commit，会有循环依赖）。**唯一正确的策略**：把它们安排在同一 PR、且本地测试和 push 都在 batch C 之后。
5. **三处 HANDOFF 描述误差**说明：写 HANDOFF 时要么没真去读 `RuntimeTypeAdapterFactory` 源码、要么没意识到 enum/string 不通用映射。**下次写 HANDOFF 前，对每个 "easy 改动" 都先看代码确认**——尤其是涉及框架级行为（gson / image / serialization）的

### 沿用前一版（仍然有效）

1. **`tools/check-connector-imports.sh` 是 fe-core import 的隐含设计约束** — fe-core 不能 import `org.apache.doris.connector.*`；"复用 SPI 实现"唯一通道是 ConnectorPlugin 接口
2. **P1 批 A 设计了"迁移期 fallback"模式** — `PluginDrivenExternalTable` 分支前置在 `PhysicalPlanTranslator.visitPhysicalFileScan`；老 instanceof 链原地保留。**P2-T08 删 trino 那一分支**，剩余 6 个连接器分支等 P3-P7 各自迁完时删
3. **T4 (P1) `incrementalRelation` 是 P3 Hudi SPI 缺口**（不影响 P2）

---

## 🎯 下一个 session 第一件事

> **目标**：启动 P2 批 C — P2-T07（`CatalogFactory.SPI_READY_TYPES` 加 `"trino-connector"`），然后立即启动批 D（T08+T09+T10 清旧）。

```
1. 自检：
   git branch --show-current → 应为 catalog-spi-03
   git log --oneline -4 → 顶层应是 dfd48725c76 ([P2-T03-T06] batch B fe-core 桥接)
                         接着 31fb91c5bd3 ([P2-T01-T02] batch A SPI 补齐)
                         接着 e12b6c9b3af ([doc] P2 recon + task breakdown)
                         接着 778c5dd610f (P1 merge baseline)
   git status → 工作树应该干净

2. 读：
   - PROGRESS.md（§一 P2 行 40% / §三 P2 任务表 T07-T13 待办）
   - 本 HANDOFF.md（本文件）
   - tasks/P2-trino-connector-migration.md（§阶段日志倒序读"晚 ④/晚 ③"已完成项；T07-T13 任务清单）
   - connectors/trino-connector.md（playbook 进度）

3. P2-T07 编码（1 行）：
   a. 文件 `fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java`
   b. line 53 `private static final ImmutableSet<String> SPI_READY_TYPES = ImmutableSet.of("jdbc", "es");`
      → 改为 `ImmutableSet.of("jdbc", "es", "trino-connector");`
   c. **就这一行**。其他不要动
   d. 守门：`mvn -pl fe-core -am compile` + `mvn -pl fe-core checkstyle:check`（cwd=fe/，加 `-Dmaven.build.cache.enabled=false`）
   e. commit prefix：`[feat](connector) [P2-T07] enable trino-connector in SPI_READY_TYPES`

4. P2-T07 完成后立即批 D 启动（不要在 T07 后停太久 — regression window 关闭得越快越好）：

   - **T08** `PhysicalPlanTranslator.java:779`：删 `instanceof TrinoConnectorExternalTable` → `new TrinoConnectorScanNode(...)` 分支
     注意 P1 批 A 已在它之上加了 `PluginDrivenExternalTable` SPI 前置分支，所以删 trino 分支后查询自动走 SPI 路径
   - **T09** `CatalogFactory.java:147-150` 删 `case "trino-connector":` + 删整文件 `TrinoConnectorExternalCatalogFactory.java`
   - **T10** rm -rf `fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/` 整目录（10 文件 / ~1760 LOC）
     **scope 校正后 T10 不再碰 GsonUtils**（批 B T03 已经处理完）
     **保留** `TableIf.TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` 枚举值（image compat）
     **保留** `InitCatalogLog.Type.TRINO_CONNECTOR` 枚举值（image compat）

5. 守门 / 验证清单（每个 commit 之后跑）：
   - `mvn -pl fe-connector validate -Dmaven.build.cache.enabled=false`（import gate，cwd=fe/）
   - `mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false`（cwd=fe/）
   - `mvn -pl fe-core checkstyle:check -Dmaven.build.cache.enabled=false`
   - 本地 smoke（推荐 T10 之后）：手动 CREATE CATALOG ... TYPE 'trino-connector'；SHOW CATALOGS 检查走 SPI 路径

6. 批 D 完成判据：
   - `find fe/fe-core -path '*/trinoconnector/*' -name '*.java'` 返 0
   - `grep -rn "trinoconnector" fe/fe-core/src/main` 只剩 `InitCatalogLog.Type.TRINO_CONNECTOR` 枚举值 + `TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` 枚举值 + GsonUtils 的 string-name redirect（共 5 处左右；不应该有 import 或 class 引用）
   - fe-core compile + checkstyle 0
   - import gate 0

7. commit 拆分建议（HANDOFF 原本说 T08/T09/T10 各 commit；可合一个 [P2-T08-T10] batch D commit；规模适中）
   - 或：T07 单 commit；T08+T09+T10 一个 commit

8. 批 D 完成后启动批 E（T11-T13 测试 + 文档），最后开 PR
```

---

## 📋 P2 commit 节奏（branch `catalog-spi-03`）

```
dfd48725c76  [feat](connector) [P2-T03-T06] bridge trino-connector through fe-core for batch B          ← 批 B
31fb91c5bd3  [feat](connector) [P2-T01-T02] complete trino-connector SPI surface for batch A          ← 批 A
e12b6c9b3af  [doc](connector) P2 trino-connector recon + task breakdown                                ← 批 0
778c5dd610f  [P1-T03-T05] route plugin-driven scans first in nereids translator (#63641)               ← P1 baseline (merged)
```

3 个 P2 commit；本地 ahead of `upstream-apache/branch-catalog-spi` 3。

下一步 commit 预期：
- `[feat](connector) [P2-T07] enable trino-connector in SPI_READY_TYPES`（1 行）
- `[refactor](connector) [P2-T08-T10] remove legacy trino-connector code from fe-core`（~1760 LOC 删除）
- `[test](connector) [P2-T11] add fe-connector-trino unit tests`（批 E）
- `[test](connector) [P2-T12] add trino_connector_migration_compat regression test`（批 E）
- `[doc](connector) [P2-T13] add trino-connector plugin install doc + sync tracking`（批 E）

PR 开法（**批 E 完成后**）：
```
gh pr create --repo apache/doris --base branch-catalog-spi \
  --head morningman:catalog-spi-03 \
  --title "[feat](connector) P2 trino-connector migration"
```

---

## ⚠️ 开放问题 / 风险提示

### P2 in-flight 待解

1. **批 D 拆分粒度** — T08+T09+T10 一个 commit 还是 3 个？HANDOFF 原推荐分开；但 T08（PhysicalPlanTranslator 1 行）、T09（CatalogFactory case + factory 文件）、T10（rm -rf）的逻辑紧耦合，一个 commit 更原子。**待批 C 完成时定**
2. **批 E T11 单测覆盖度** — 至少 schema 解析 / predicate 转换 / type mapping / json ser-deser 4 个 test class；mock Trino plugin 是难点（Trino plugin loader 自己有 classloader 隔离）。**T11 启动时再设计**
3. **批 E T12 regression test 范围** — 至少要覆盖：旧 FE image 反序列化（含 TRINO_CONNECTOR enum value）；CREATE CATALOG 后 image 持久化 + 重启后读回。**T12 启动时再设计**

### 沿用（前一版保留）

4. **P8 待办：CDC capability 群** — 为 streaming CDC 在 SPI 上暴露 `getPrimaryKeys` / `getColumnsFromJdbc` / `listTables`；改写 PostgresResourceValidator / StreamingJobUtils / CdcStreamTableValuedFunction 走 SPI；批量删 13 个 Jdbc*Client + JdbcFieldSchema + 3 个 legacy test。**预估**：~1-2 天 SPI 设计 + ~1 天实施
5. **P8 启动前 RFC 扩展**：在 `01-spi-extensions-rfc.md` 新增 §17 章节描述 CDC capability 设计
6. **T4 (P1) `incrementalRelation`** — 待 P3 Hudi 迁移时 SPI 扩展
7. **T2 (P1) 已推迟到 P4/P5**
8. **T3 (P1) fallback 保留期跨度长**（P1 → P7 20 周）—— 每连接器在 P2-P7 迁移完成后立即删对应 fallback
9. （沿用 P0）`ColumnDefinition.defaultValue` SPI 缺位 — P5/P6 评估
10. （沿用 P0）LIST/RANGE `initialValues` flatten 缺位 — P5/P6 评估
11. （沿用 P0）`PluginDrivenExternalCatalog.createTable` 返回值丢失"已存在"信息 — P5/P6/P7 评估
12. （沿用 P0）bucket 算法名 `"doris_default"` / `"doris_random"` 占位 — Hive/Iceberg 自己推导
13. （沿用 P0）Maven build cache 误导；`mvn -pl fe-core` 必须 cwd=`fe/` + `-am`；`-Dtest=` 务必带 `-DfailIfNoTests=false`
14. （沿用 P0）`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 暂无 caller — P5/P6/P7 接通
15. （沿用 P0）`ConnectorMetaInvalidator.invalidatePartition` fallback 到 invalidateTable；`invalidateStatistics` no-op
16. （沿用 P0）`mvn -pl fe-core test` 不带 `-am` 失败

---

## 📂 当前关键文件清单

### 本场（2026-05-25 晚 ④）修改 / 新增

```
COMMIT 31fb91c5bd3 (批 A, fe-connector-trino):
  MOD  fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoConnectorProvider.java   (+11 LOC, validateProperties)
  MOD  fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoDorisConnector.java      (+9  LOC, preCreateValidation)
  MOD  fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoConnectorDorisMetadata.java (+125 LOC, applyFilter + applyProjection)
  MOD  plan-doc/PROGRESS.md
  MOD  plan-doc/tasks/P2-trino-connector-migration.md

COMMIT dfd48725c76 (批 B, fe-core):
  MOD  fe/fe-core/src/main/java/org/apache/doris/persist/gson/GsonUtils.java                       (atomic replace 3 Trino registerSubtype + remove imports)
  MOD  fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java       (+15 LOC, legacyLogTypeToCatalogType helper)
  MOD  fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalTable.java         (+6  LOC, trino-connector engine-name 分支)
  MOD  plan-doc/PROGRESS.md
  MOD  plan-doc/tasks/P2-trino-connector-migration.md
```

工作树状态：干净（all committed）。

### 下一个 session 涉及的目标文件

```
# 批 C
fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java                       (T07: line 53 SPI_READY_TYPES)

# 批 D
fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java  (T08: line 779 删 instanceof 分支)
fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java                       (T09: line 147-150 删 case + 删 factory 文件)
fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/                           (T10: rm -rf 整目录 10 文件 ~1760 LOC)

# 批 E（最后）
fe/fe-connector/fe-connector-trino/src/test/                                                   (T11: 新建测试包)
regression-test/suites/external_catalog/                                                       (T12: trino_connector_migration_compat)
docs-next/                                                                                     (T13: 插件安装步骤)
plan-doc/connectors/trino-connector.md                                                         (T13: 翻 100%)
plan-doc/PROGRESS.md                                                                           (T13: §一 P2 ✅)
```

### 跟踪体系（不变）

```
plan-doc/  (~230K, 19 文件)
├── 00-connector-migration-master-plan.md / 01-spi-extensions-rfc.md
├── README.md / PROGRESS.md / AGENT-PLAYBOOK.md / HANDOFF.md
├── decisions-log.md (18) / deviations-log.md (0) / risks.md (14)
├── tasks/{_template.md, P0-spi-foundation.md, P1-scan-node-cleanup.md, P2-trino-connector-migration.md}
└── connectors/{_template.md, jdbc, es, trino-connector, hudi, maxcompute, paimon, iceberg, hive}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **分支 `catalog-spi-03`** — P2 工作就在这条分支上累积；不要新开分支
- **PR 目标分支永远是 `apache/doris:branch-catalog-spi`**；批 E 完成后再开 PR（T07 后的中间状态不要 push）
- **commit message 沿用** `[feat|refactor|doc|test](connector) [P2-Tnn] ...` 前缀
- **Maven 命令**：cwd=`fe/`；`mvn -pl fe-core -am compile -Dmaven.build.cache.enabled=false`；测试用 `-Dtest=... -DfailIfNoTests=false`
- **批 C 必须紧接批 B 操作** — regression window 关闭得越快越好；本地启 FE 测试 / push 都应在 batch C 之后
- **T10 scope 已校正** — 不再碰 GsonUtils（批 B T03 已处理）；只删目录 + CatalogFactory case
- **保留 enum value**：`TableIf.TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` 和 `InitCatalogLog.Type.TRINO_CONNECTOR` 都保留（image compat）
- **不要乱碰 P1 fallback 中 trino 之外的分支** — 每个分支专属于一个连接器迁移；P2 只清 trino
- **必读 AGENT-PLAYBOOK §六 anti-patterns**（每个新 session 都温习一次）
- **HANDOFF 描述务必先 verify 代码再写** — 本场遇到 3 处误差（T03/T04/T05/T06）；写 HANDOFF 时如果涉及框架级行为（gson / image / serialization），先看真代码
