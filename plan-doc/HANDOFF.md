# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-04
- **本 session 主题**：**P2 批 C+D+E 连续完成**（T07 翻闸 → T08-T10 删 legacy → T11 单测 → T13 文档），**T12 推迟**，**PR 待开**（分支基线对齐由用户处理）
- **分支**：`catalog-spi-03`

---

## ✅ 本 session 完成项

> 注：用户本 session 开始前把 `catalog-spi-03` **rebase 到了新 master**，所有旧 commit hash 已变。下方为 rebase 后的新 hash。

### 批 C — T07 翻闸（commit `0fe4b8a93d6`）

`CatalogFactory.java:53` `SPI_READY_TYPES` 加 `"trino-connector"`（顺手删上方注释里过时的 trino 列举）。这一步把 `CREATE CATALOG type='trino-connector'` 路由到 SPI（`PluginDrivenExternalCatalog`），关闭了批 B→批 C 的 regression window。compile + checkstyle 绿。

### 批 D — 删 fe-core legacy trino 代码（commit `ed81a063fe8`，14 文件 / +1 −2508）

- **T08** `PhysicalPlanTranslator`：删 `instanceof TrinoConnectorExternalTable` scan 分支 + 2 import（`PluginDrivenExternalTable` SPI 前置分支接管）。
- **T09** `CatalogFactory`：删 `case "trino-connector"` + import。
- **T10**：删 `datasource/trinoconnector/` 整目录（10 文件）+ 删 legacy 测试 `TrinoConnectorPredicateTest`。
- **DV-001（HANDOFF 原计划漏项，recon 补回）**：`ExternalCatalog.java:948` `case TRINO_CONNECTOR` 改返 `PluginDrivenExternalDatabase`（照搬已迁移的 JDBC case，line 936）+ 删 import。
- **有意保留**：`MetastoreProperties.Type.TRINO_CONNECTOR` + `TrinoConnectorPropertiesFactory`（属性子系统，不引用被删目录，SPI 路径可能仍需）；`InitCatalogLog.Type.TRINO_CONNECTOR` + `TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` 枚举（image compat）；`GsonUtils` 3 个 label redirect（批 B 已处理，T10 **不碰** GsonUtils）。
- 守门：fe-core `clean test-compile`（main+test）BUILD SUCCESS、checkstyle 0、fe-connector import-gate SUCCESS。

### 批 E — T11 单测（commit `9bba12a44b2`，3 文件 / +441）

3 个 JUnit5（Jupiter）纯转换器测试，**29 测试全绿**，checkstyle 0，本地 `mvn -pl fe-connector/fe-connector-trino -am test` 可跑：
- `TrinoPredicateConverterTest`（14）— `ConnectorExpression` pushdown → Trino `TupleDomain`（EQ/range/NE/IN/NOT IN/IS [NOT] NULL/AND/OR、Slice 编码、null/unsupported 优雅降级到 `all()`）。
- `TrinoTypeMappingTest`（11）— Trino type → Doris `ConnectorType`（标量、decimal 精度/scale、timestamp 精度 clamp 到 6、array/map/struct、unknown 抛错）。
- `TrinoConnectorProviderTest`（4）— `validateProperties` 缺/空 `trino.connector.name` fail-fast（批 A T01）。
- **DV-002**：fe-connector-trino 无 Mockito、`TrinoJsonSerializer` 非纯单元（需 plugin 的 HandleResolver+TypeRegistry）→ 砍 json/schema，用 `validateProperties` 替补第 3 类；plugin 依赖路径由现有 `external_table_p0/p2` trino_connector regression 套件覆盖。

### T13 — 跟踪文档同步（本次提交）

PROGRESS / tasks/P2 / connectors/trino-connector.md / deviations-log（DV-001..004）/ 本 HANDOFF 全部翻到 P2 完成态。

---

## 🚧 未完成 / 待办

1. **PR 未开 —— 阻塞于分支基线错位（用户处理）**。`catalog-spi-03` 现基于**新 master**（含 `#63823 split fe-sql-parser`、`#64016 TLS` 等 master-only commit），而远端 `apache/doris:branch-catalog-spi` 仍停在 P1 merge `778c5dd610f`（旧 master 基线）；两者分叉于 `68d4eb308e5`（#63552）。`git rev-list --count upstream-apache/branch-catalog-spi..HEAD` = **191**（仅顶部 7 个是 P2）。**直接开 `catalog-spi-03 → branch-catalog-spi` 会是 191-commit 的错误巨型 PR**。等用户对齐分支后再开。
2. **T12 回归测试推迟**（DV-003）——`trino_connector_migration_compat`（CREATE CATALOG→image→重启读回 + 旧 image 含 `TRINO_CONNECTOR` 枚举反序列化），需有 Trino plugin + docker/集群的环境。

---

## ⚠️ 关键认知 / 临时发现

1. **rebase 后 fe-core 编译坑（非代码问题）**：本场最大时间消耗。rebase 拉入 `#63823`（nereids 语法从 fe-core 拆到新模块 `fe-sql-parser`）后，`fe-core/target/generated-sources/.../DorisParser.java` 旧生成物残留（git 不管 target/），FQCN 撞名盖过 fe-sql-parser 依赖里的新版 → `LogicalPlanBuilder` 报 `cannot find symbol HOT()/expression()`。**修法：`clean` fe-core**（旧生成物删除、fe-core 已无 grammar 不会再生成）。只 clean fe-sql-parser 不够。任何 rebase 后遇此症状先 clean fe-core，别当代码 bug 查。
2. **`MetastoreProperties` trino 条目有意保留**：它在 `property/metastore/` 子系统、不引用被删目录、删之不影响编译，但 SPI 建 catalog 可能仍走它解析属性。批 D 不动它；是否死代码留待后续评估（DV-001 后续动作）。
3. **docs-next 不在本代码仓**：用户向文档在 doris-website 仓（DV-004）。本仓只有 `docs/`。
4. （沿用）`tools/check-connector-imports.sh` import gate：fe-core 不能 import `org.apache.doris.connector.*`。
5. （沿用）P1 fallback：`PhysicalPlanTranslator` 里其余 6 个连接器的 instanceof 分支待 P3-P7 各自迁完时删；本场只清了 trino 那一支（T08）。

---

## 🎯 下一个 session 第一件事

```
1. 自检：
   git branch --show-current → catalog-spi-03
   git log --oneline -8 → 顶层应是 9bba12a44b2 (T11) → ed81a063fe8 (T08-T10)
                          → 0fe4b8a93d6 (T07) → 5e504a24883 (doc) → 9ed33f9a7a5 (批 B)
                          → 69203b6418e (批 A) → 8f0b749bd06 (recon) → 3adabcaf54b (P1)
   git status → 干净（本次文档 commit 之后）

2. 解决 PR base（核心待办）：
   - git fetch upstream-apache branch-catalog-spi
   - 确认 branch-catalog-spi 是否仍停在 778c5dd610f（P1）。
   - 推荐做法：从远端 branch-catalog-spi 拉新分支（如 catalog-spi-03-pr），
     cherry-pick 这 7 个 P2 commit（8f0b749bd06 recon → 69203b6418e A → 9ed33f9a7a5 B
     → 5e504a24883 doc → 0fe4b8a93d6 C → ed81a063fe8 D → 9bba12a44b2 E）。
     注意：branch-catalog-spi 没有 fe-sql-parser 拆分（#63823），但我们的改动与之正交，
     cherry-pick 后应能编译；在该分支上重跑 fe-core compile + fe-connector-trino test 验证。
   - 或：等 branch-catalog-spi 被刷新到 master 后直接用 catalog-spi-03。
   - PR：gh pr create --repo apache/doris --base branch-catalog-spi --head morningman:<分支>
     --title "[feat](connector) P2 trino-connector migration"

3. T12 回归测试：在有 Trino plugin + docker/集群环境补（DV-003）。

4. 之后启动 P3 Hudi 迁移（见 00-master-plan / connectors/hudi.md）。
   注意 P1-T4 incrementalRelation 是 P3 Hudi SPI 缺口。
```

---

## 📋 P2 commit 节奏（branch `catalog-spi-03`，rebase 到新 master 后）

```
9bba12a44b2  [test](connector) [P2-T11] add fe-connector-trino unit tests              ← 批 E
ed81a063fe8  [refactor](connector) [P2-T08-T10] remove legacy trino-connector code      ← 批 D
0fe4b8a93d6  [feat](connector) [P2-T07] enable trino-connector in SPI_READY_TYPES        ← 批 C
5e504a24883  [doc](connector) refresh P2 HANDOFF for batch C kickoff
9ed33f9a7a5  [feat](connector) [P2-T03-T06] bridge trino-connector through fe-core       ← 批 B
69203b6418e  [feat](connector) [P2-T01-T02] complete trino-connector SPI surface         ← 批 A
8f0b749bd06  [doc](connector) P2 trino-connector recon + task breakdown                  ← 批 0
3adabcaf54b  [P1-T03-T05] route plugin-driven scans first (#63641)                       ← P1（rebase 后新 hash）
```

本次文档 commit（T13）将追加一条 `[doc](connector) [P2-T13] sync P2 tracking docs`。

> ⚠️ 这 7 个 P2 commit 是干净的；问题只在 base（见 §未完成 1）。PR 不要在 base 对齐前开。

---

## 📂 本场修改 / 新增的关键文件

```
批 C (0fe4b8a93d6):  fe-core/.../datasource/CatalogFactory.java (SPI_READY_TYPES)
批 D (ed81a063fe8):  fe-core/.../nereids/glue/translator/PhysicalPlanTranslator.java (删 trino 分支+import)
                     fe-core/.../datasource/CatalogFactory.java (删 case+import)
                     fe-core/.../datasource/ExternalCatalog.java (TRINO_CONNECTOR db→PluginDrivenExternalDatabase, DV-001)
                     删 fe-core/.../datasource/trinoconnector/ (10 文件)
                     删 fe-core/src/test/.../trinoconnector/TrinoConnectorPredicateTest.java
批 E (9bba12a44b2):  新建 fe-connector/fe-connector-trino/src/test/.../trino/
                       TrinoPredicateConverterTest.java / TrinoTypeMappingTest.java / TrinoConnectorProviderTest.java
T13:                 plan-doc/{PROGRESS, tasks/P2, connectors/trino-connector, deviations-log, HANDOFF}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **分支 `catalog-spi-03`** 现基于 master；**开 PR 前务必先解决 base 错位**（§未完成 1），否则会是 191-commit 错误 PR。
- rebase 后 fe-core 编译失败先想到 **clean fe-core**（stale DorisParser），别查代码（§关键认知 1）。
- commit message 沿用 `[feat|refactor|test|doc](connector) [P2-Tnn] ...`。
- Maven：cwd=`fe/` 或 `-f fe/pom.xml`；`-pl fe-core -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`。
- **不要乱碰 P1 fallback 中 trino 之外的连接器分支**。
- 偏差先记 `deviations-log.md` 再改文档（本场 DV-001..004 已记）。
