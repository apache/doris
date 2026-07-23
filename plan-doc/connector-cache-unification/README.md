# 📦 调研任务空间 — 连接器缓存框架统一 (connector cache unification)

> **独立调研空间**，与 `plan-doc/HANDOFF.md`(catalog-spi 主线)及 `plan-doc/perf-hotpath-iceberg/`(iceberg 热路径修复,本报告的参考实现)**并行但不混流**。
> **一句话任务**:参考 commit `0b4f72582e7` 为 iceberg 建了一套「统一元数据缓存 + per-statement metadata funnel」框架;本调研判定**这套框架是否/如何推广到其余连接器**,目标 (1) 统一连接器缓存框架、(2) 保证热路径(查询 + 写入)的缓存性能与一致性。
> **基线**:`branch-catalog-spi` @ HEAD(2026-07-22)。参考 commit `0b4f72582e7`。

---

## 🚩 结论(先看这一句)

**框架三层里,承重的引擎接缝(Layer-2 per-statement `ConnectorMetadata` funnel + 其 build-gate + 共享底座 `fe-connector-cache` + 泛型 `ConnectorPartitionViewCache` + fe-core 通用 `shouldBypass*` 谓词)已经是连接器无关、对全部 7 个 `SPI_READY_TYPES` 生效的通用设施——"统一(goal-1)"这一半其实已经做完。** iceberg 特有的只是"填充物"(6 个连接器侧缓存)。因此推广 = **有选择地填充,不是重新造框架**:唯一 loop-amplified(iceberg-式)的 P1 真缺口是 **hudi**(缓存最薄:零连接器侧缓存、裸 `ThriftHmsClient`、metaClient 每 pass 重建 5–6 次);maxcompute / es 是少量 constant-factor(2–4x)P1 收尾;jdbc / es / trino / paimon 结构上**反指**再加重缓存(廉价透传 / 委托嵌入式引擎 / SDK 已缓存)。

> **Trino-correct 原则**(经 Trino 源码核实,见 appendix B):正确的统一粒度是**工具箱**而非"一个大缓存"——被缓存对象的类型、失效语义、授权语义天生按连接器不同。Doris 已站在这条路径上。

---

## 📄 文件索引

| 文件 | 内容 |
|---|---|
| [`00-research-report.md`](./00-research-report.md) | **主报告(交付物)** — 9 节:结论先行 / 框架解剖 / 横向矩阵 / goal-1 统一建议 / goal-2 热路径+一致性路线图 / session=user 授权跨连接器影响 / 风险与反对意见 / 需拍板决策 / 建议后续任务空间 |
| [`appendix-A-framework-taxonomy.md`](./appendix-A-framework-taxonomy.md) | 框架三层 + 共享底座的精确解剖(哪些已通用 / 哪些是 iceberg-only 模式),全部挂 HEAD `file:line` |
| [`appendix-B-trino-reference.md`](./appendix-B-trino-reference.md) | Trino 的 per-transaction metadata + 连接器缓存模型,逐条映射到 Doris 现状并指出 gap(遵循"连接器 SPI 决策前先读 Trino") |
| [`appendix-C-funnel-universality.md`](./appendix-C-funnel-universality.md) | 结构性论断核验:funnel 是否已覆盖每个连接器 + 每个连接器的 ConnectorMetadata 是否 memoize 已加载表(per-connector 表) |
| [`appendix-D-critic-punchlist.md`](./appendix-D-critic-punchlist.md) | 对主报告的矛盾/完整性对抗复核清单(5 条 minor 已在主报告修正;附通过项) |
| [`connectors/*.md`](./connectors/) | **7 份 per-connector 审计附录**(hive / hudi / paimon / maxcompute / jdbc / es / trino),各含:迁移状态、现有缓存表、funnel 参与、热路径重复加载审计表、授权/一致性、所需框架件、优先级,**末尾附对抗式复核结论(verdict + 更正表)** |
| [`data/connector-audits.json`](./data/connector-audits.json) | 7 连接器结构化审计数据(缓存清单 / hotPathFindings / 复核 verdict),矩阵与路线图的原始来源 |

---

## 🔬 本调研如何产出(方法与可信度)

- **19-agent 编排研究**(clean-room 对抗式):3 个框架 agent(taxonomy + Trino 参考 + funnel 通用性核验)‖ 7 个连接器 × (审计 → **独立对抗验证**)‖ 综合 → 矛盾/完整性 critic。共 ~2.4M token,0 error。
- **全部论断挂 HEAD `file:line`**,agent 被要求"行号信 HEAD 不信文档/commit message";7 连接器结论各经一个**默认怀疑**的验证 agent 复核:**5 CONFIRMED / 2 ADJUSTED**(hudi、es;更正已折进主报告与对应附录)。
- **⚠ 这是纯调研,未改任何产品代码,未跑 e2e。** `file:line` 为调研时(2026-07-22 HEAD)快照,落地实现时以 `grep` 为准。所有"倍数/成本"是审计估算,建议在真正立项时用调用计数守门(对齐 `perf-hotpath-iceberg` 的度量门做法)复测。

---

## 🧭 需 owner 拍板的 4 个决策(详见主报告 §8)

1. **范围**:本轮只打 hudi(唯一 P1 真缺口),还是把 maxcompute `MC-1`、es `ES-F1/F2` 两个小修一并纳入?(建议:hudi 单独立项 + mc/es 各一小 PR,余进 backlog)
2. **排序**:先建共享底座泛化,还是 per-connector-first?(建议 per-connector-first——泛型 table/handle 缓存目前只有 hudi 一个新消费者,出现第 2 个再上收,避免投机抽象 / Rule 2)
3. **`check-authz-cache-sharding.sh` 是否现在就通用化?**(现硬编码只盯 `IcebergConnector.java`;今天所有推荐新缓存都 authz-安全,故可延后,但前瞻通用化更稳)
4. **是否投入共享的"经 statement scope 解析表"helper?**(Trino 眼中"统一"的真正含义,但复核显示只有 iceberg+hudi 刚需;建议让 hudi 在自己模块内做,不提前提炼)

---

## 🔗 关系

- **参考实现**:`plan-doc/perf-hotpath-iceberg/`(iceberg 那批被删缓存的逐条修复)+ `plan-doc/perf-heavy-op-hot-path-problem-class.md`(本调研套用的"热路径重操作放大"问题类)。
- **建议后续任务空间**(见主报告 §9):`plan-doc/perf-hotpath-hudi/`(P1 旗舰)、`perf-hotpath-maxcompute/`、`perf-hotpath-es/`(均小)、`cleanup-stale-connector-docs/`(doc-only)、`perf-hotpath-backlog-p2/`。各连接器建**各自兄弟空间**,不并进本空间。
