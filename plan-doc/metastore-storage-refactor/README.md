> # 🔒 本子线已彻底 CLOSED（2026-06-22 收官，用户确认）
>
> **「属性体系重构」子项目（Storage→fe-filesystem / MetaStore→fe-connector SPI，paimon 优先）已全部完成并合入主线** —— 核心任务 15/15 + docker 真闸全过；产出 `fe-kerberos` / `fe-connector-metastore-api` / `fe-connector-metastore-spi`（含 `MetaStoreProviders.bind` + 5 provider）+ 删除 `fe-property` 孤儿模块；paimon 连接器已 cutover 到共享 SPI。合入提交：`#64446`（paimon SPI+翻闸）/ `#64653`（P5-T29 删 legacy）/ `#64655`（P3b kerberos 收口 `e5959e1b53d`）。
>
> **⛔ 后续任务（含主线 P6/P7）请勿再阅读本目录的规划/接力文档** —— 它们是已结束工作的历史留存，不再维护。需了解 metastore-spi / `MetaStoreProviders.bind` 现状请**直接读代码**：`fe/fe-connector/fe-connector-metastore-spi/`。主线接力见 [`../HANDOFF.md`](../HANDOFF.md)。

---

# 子项目：属性体系重构（Storage→fe-filesystem / MetaStore→fe-connector SPI，paimon 优先）

> 本目录是**该子项目唯一权威跟踪源**。它隶属于上层 connector 迁移项目（见 `../README.md`），并**沿用**其文档机制（决策/偏差/风险区分、ID 规则、维护规则），仅作范围裁剪与本项目特化。
> 任何讨论、评审、PR 描述都应引用本目录文件。

---

## 〇、入口（看了就懂）

| 我想做的事 | 看哪个文件 |
|---|---|
| **了解为什么做、目标架构、SPI/API 设计、接口签名** | [`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md) ★（设计权威）|
| **本项目怎么开发：流程 / 单任务循环 / 守门 / 验证 / 提交** | [`WORKFLOW.md`](./WORKFLOW.md) ★ |
| **现在做到哪一步 / 下一步是什么** | [`PROGRESS.md`](./PROGRESS.md) ★ |
| **具体任务清单（Pn-Tnn）+ 验收** | [`tasks.md`](./tasks.md) |
| **做过哪些决策、为什么** | [`decisions-log.md`](./decisions-log.md) |
| **实施中发现原计划不可行处** | [`deviations-log.md`](./deviations-log.md) |
| **风险与缓解** | [`risks.md`](./risks.md) |
| **接管上次 session** | [`HANDOFF.md`](./HANDOFF.md) ★ |

---

## 一、目录结构

```
plan-doc/metastore-storage-refactor/
├── README.md            ← 本文件（子项目入口）
├── WORKFLOW.md          ← 本项目开发流程（核心：阶段模型 / 单任务循环 / 守门 / 验证 / 维护规则）
├── PROGRESS.md          ← 仪表盘（人类+agent 入口必读）
├── tasks.md             ← Pn-Tnn 任务清单 + 验收 + 状态
├── decisions-log.md     ← 决策 ADR，append-only（本项目内编号）
├── deviations-log.md    ← 实施偏差，append-only（本项目内编号）
├── risks.md             ← 风险滚动状态
└── HANDOFF.md           ← Session 间接力（每次结束覆盖）

设计正文不放这里 → 在 ../designs/metastore-storage-property-refactor-design-2026-06-17.md
```

---

## 二、本项目范围（红线，来自用户 2026-06-17）

- ✅ **只做**：新建 `fe-connector-metastore-api/spi`（仅 paimon 用到的后端，后端用 `MetaStoreProvider` 自识别、无枚举 — D-006）；新增 `ConnectorContext.getStorageProperties()` 让 fe-core 下发已绑定 `StorageProperties`；改造 **paimon** 连接器（storage 走 fe-filesystem-api、metastore 走新 SPI、vended 仍走 `ctx.vendStorageCredentials` — D-008）；断开 paimon→`fe-property` 依赖边；**新建顶层叶子 `fe-kerberos` + paimon HMS kerberos facts 走它（P3a，D-007）**。
- 🚫 **不做**：不删 fe-core `datasource.property.{storage,metastore}` 任何类；不动 hive/hudi/iceberg/es/jdbc/mc/trino；**不动 fe-common / fe-filesystem-hdfs 既有 kerberos 路径**（其收口 = P3b follow-up）；fe-property 不物理删（仅变孤儿）；不收紧 import gate。
- 🔭 **范围外（后续）**：其它连接器迁移、**P3b**（fe-common + fe-filesystem-hdfs 收口到 fe-kerberos 全量去重）、终态删 fe-core 两包 + 删 fe-property + 收 gate。

详见设计文档 §0.1。

---

## 三、与上层 plan-doc 的关系

- **文档机制**沿用 `../README.md`（§3 决策vs偏差vs风险、§4 维护规则、§5 防腐、§6 不在范围）。
- **编号空间独立**：本目录的 `D-/DV-/R-` 与 `Pn-Tnn` 仅在本子项目内有效，**不**与 `../decisions-log.md` 等共享编号（避免跨文件碰撞）。各 log 顶部已注明。
- **Agent 接力**沿用 `../AGENT-PLAYBOOK.md` 的 context/subagent/handoff 规范；本目录 `HANDOFF.md` 是本子项目的接力点。

---

## 四、给后来者

**人类**：先读设计文档 §0/§2/§3（10 min）→ 看 `PROGRESS.md`（2 min）→ 要动手再读 `tasks.md` 对应 task + `WORKFLOW.md` 单任务循环。

**LLM agent（强制顺序）**：
1. Read `PROGRESS.md`（全局状态）
2. Read `HANDOFF.md`（上次留言）
3. Read `WORKFLOW.md`（怎么干）
4. 如 HANDOFF 指定当前 task，Read `tasks.md` 中该 task 块
5. 一句话复述确认（"上次完成 X，下一步 Y，对吗？"）→ 用户确认后开始
