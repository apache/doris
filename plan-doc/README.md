# Connector 迁移项目 — 文档与跟踪机制

> 本目录是 Doris connector 解耦迁移项目（fe-core/datasource → fe-connector/*）的**唯一权威文档源**。
> 任何讨论、评审、PR 描述都应引用本目录文件，避免事实在群聊 / 邮件中丢失。

---

## 〇、入口（看了就懂）

### 项目文档

| 我想做的事 | 看哪个文件 |
|---|---|
| **了解项目背景、整体设计、决策点** | [`00-connector-migration-master-plan.md`](./00-connector-migration-master-plan.md) |
| **了解 SPI 接口扩展细节（Java 签名）** | [`01-spi-extensions-rfc.md`](./01-spi-extensions-rfc.md) |
| **看现在做到哪一步了 / 谁在做什么** | [`PROGRESS.md`](./PROGRESS.md) ★ |
| **看具体阶段的任务清单** | [`tasks/Pn-*.md`](./tasks/) |
| **看具体连接器的迁移状态** | [`connectors/<name>.md`](./connectors/) |
| **历史上做过哪些决策、为什么** | [`decisions-log.md`](./decisions-log.md) |
| **实施中发现原计划不可行的地方** | [`deviations-log.md`](./deviations-log.md) |
| **当前项目有哪些风险，谁在缓解** | [`risks.md`](./risks.md) |

### Agent 协作（每次 session 开始必读）

| 我是 LLM agent，我想... | 看哪个文件 |
|---|---|
| **了解如何管理 context、何时用 subagent、何时 handoff** | [`AGENT-PLAYBOOK.md`](./AGENT-PLAYBOOK.md) ★ |
| **接管上次 session 的工作** | [`HANDOFF.md`](./HANDOFF.md) ★ |

---

## 一、目录结构

```
plan-doc/
├── 00-connector-migration-master-plan.md   ← WHY/WHAT 总体设计（变化少）
├── 01-spi-extensions-rfc.md                ← SPI 详细 RFC
├── README.md                               ← 本文件
├── PROGRESS.md                             ← 全局仪表盘（人类入口必读）
├── AGENT-PLAYBOOK.md                       ← Agent 协作规范（context / subagent / handoff）
├── HANDOFF.md                              ← Session 间接力文档（滚动）
├── decisions-log.md                        ← ADR，append-only
├── deviations-log.md                       ← 实施偏差日志，append-only
├── risks.md                                ← 风险滚动状态
├── tasks/                                  ← 按阶段切的任务清单
│   ├── _template.md
│   └── P0-spi-foundation.md
└── connectors/                             ← 按连接器切的迁移状态
    ├── _template.md
    └── <name>.md
```

---

## 二、文件职责矩阵

| 文件 | 内容性质 | 更新频率 | 主要读者 | 更新触发 |
|---|---|---|---|---|
| `00-master-plan.md` | 战略 / 总体设计 | 每月一次（重大架构变化）| 项目所有人 | 范围变更、阶段划分调整 |
| `01-spi-extensions-rfc.md` | 战术 / SPI 详细设计 | 每阶段一次 | connector 实现者 | SPI 接口签名变化 |
| `PROGRESS.md` | 状态快照 | **每周一次或重要变更后** | 所有人 | task 完成 / 阶段切换 |
| `AGENT-PLAYBOOK.md` | Agent 协作规范 | 不常变（v1 当前） | LLM agent | 规则失效时（DV 流程修改） |
| `HANDOFF.md` | Session 间状态接力 | **每次 session 结束**（覆盖） | 下次 agent | session 结束 |
| `tasks/Pn-*.md` | 阶段任务清单 | **每完成 task 后** | task owner | task 状态翻转 |
| `connectors/<name>.md` | 连接器迁移历史 | 该连接器有动作时 | connector owner | playbook 步骤完成 |
| `decisions-log.md` | 决策记录（ADR）| **每新增决策后**（append） | review 者 / 后来人 | 任何新决策诞生 |
| `deviations-log.md` | 偏差日志 | **每发现偏差后**（append）| review 者 | 原计划被推翻 |
| `risks.md` | 风险登记册 | 每周状态滚动 | PM / SRE | 风险等级变化、新增风险 |

---

## 三、关键概念区分（重要）

### 3.1 决策 (Decision) vs 偏差 (Deviation)

- **决策**：项目启动时或某阶段开始时**事前**确定的选择，进入 `decisions-log.md`。例：D-001 沿用 `SUPPORTS_PASSTHROUGH_QUERY`。
- **偏差**：原计划中已经记录的设计 / 实现方案，在落地中发现不可行或不必要，**事后**记录调整，进入 `deviations-log.md`。例：DV-001 原计划 callProcedure 用 Map 入参，实际改 List。

混淆这两者会让人无法判断"这是事先想清楚了还是被现实打脸了"。

### 3.2 风险 (Risk) vs 问题 (Issue)

- **风险**：可能发生的负面事件，进入 `risks.md`。状态滚动（监控中 / 缓解中 / 已闭环 / 已触发）。
- **问题**：已经发生的事，应在对应 task 上记 blocker 备注；如果是阶段性的，可在 tasks/Pn 文件的"阶段日志"中记录。

### 3.3 Task ID 编号规则

```
P0-T01    ← 阶段 P0 第 1 个任务
P6.3-T05  ← 子阶段 P6.3 第 5 个任务
```

ID 一旦分配**永不复用、永不重排**，即使任务被删除也保留 ID 占位（标 `[deleted]`）。

### 3.4 决策 / 偏差 / 风险编号规则

```
D-001, D-002, ...     决策；旧 D1-D12, U1-U6 迁入时映射到 D-001..D-018
DV-001, DV-002, ...   偏差
R-001, R-002, ...     风险；旧 R1-R8, Q1-Q6 迁入时映射到 R-001..R-014
```

---

## 四、维护规则（一定要遵守）

### 4.1 每次完成一个 task

1. 在对应 `tasks/Pn-*.md` 中把该 task 状态从 `🚧` 改为 `✅`，加完成日期 + PR 链接。
2. 在该 task 文件的"**阶段日志**"末尾追加一行：`YYYY-MM-DD: 完成 Pn-Tnn — <一句话描述>`。
3. 如果该 task 关联具体连接器，同步更新 `connectors/<name>.md` 的"进度"段。
4. 如果完成的是阶段的最后一个 task，更新 `PROGRESS.md`：
   - 进度条
   - 阶段状态
   - 当前活跃 task 列表
   - "最近 7 天动态"

### 4.2 每次产生新决策

1. 新决策**先写**到 `decisions-log.md` 顶部（时间倒序），分配 `D-NNN` 编号。
2. 在 `PROGRESS.md` "最近 7 天动态" 中加一行链接。
3. 如果决策修改了 RFC / master plan 的某节，**同步更新对应文档**，并在该节加 `（D-NNN 修订）`脚注。

### 4.3 每次发现设计偏差

1. **先在 `deviations-log.md` 顶部**记录：`DV-NNN`、原计划位置、为什么不可行、新方案、影响范围。
2. 更新被影响的 RFC / master plan / task 文件。
3. **不要**直接 silently 改 RFC——必须先记偏差，再改文档。

### 4.4 每周一例行维护

1. 滚动 `PROGRESS.md`：清"最近 7 天动态"中过期项，更新进度条。
2. 扫一遍 `risks.md`：检查每个 active 风险的状态，更新缓解措施进展。
3. 扫一遍 `tasks/` 中所有 in_progress 文件：是否有卡住的？

### 4.5 每个 PR 必带

1. PR 描述里**第一行**写：`[Pn-Tnn] <task subject>`。
2. PR merge 后，task owner 立刻按 §4.1 流程更新 task 状态。
3. 如果该 PR 引入了任何 SPI 接口签名变化，需要同步更新 `01-spi-extensions-rfc.md` 并在 PR 描述中说明。

---

## 五、防腐策略

为防止文档与代码 / 实际进度脱节，定期检查：

| 项 | 频率 | 工具 / 方法 |
|---|---|---|
| `PROGRESS.md` 上次更新日期 < 7 天 | 每周一 | 手动 / 后续可写 `tools/check-tracking-freshness.sh` |
| `tasks/` 中无"in_progress 超过 14 天"任务 | 每周一 | 同上 |
| 所有 RFC `D-NNN` 引用在 `decisions-log.md` 都有对应条目 | merge 前 | 后续可写 grep 守门 |
| `PROGRESS.md` 中阶段百分比与 tasks/ 中真实完成率一致 | 每周一 | 简单脚本可计算 |

---

## 六、不在范围

本跟踪机制**不**包含：

- 代码评审（用 GitHub PR）
- 缺陷管理（用 GitHub Issues）
- CI 状态（用 GitHub Actions）
- 工时统计（不做）
- 个人 KPI 追踪（不做）

文档只追踪"项目本身的设计与进度"，不追踪人。

---

## 七、给后来者

### 7.1 第一次接触本项目（人类）

1. 读 `00-master-plan.md` 第 §1 节、§3 节（10 分钟）
2. 看 `PROGRESS.md`（5 分钟）—— 知道现在在哪一步
3. 如果你要做某个具体阶段，再读对应 `tasks/Pn-*.md` 和 RFC 中相关章节

### 7.2 来评审某个 PR（人类）

1. 看 PR 描述中的 `[Pn-Tnn]`
2. 跳到 `tasks/Pn-*.md` 找该 task 的"备注"和"验收标准"
3. 评审完毕在 PR 中确认 task 完成

### 7.3 LLM agent 接手 session（AI）

**强制顺序**（来自 [AGENT-PLAYBOOK §4.5](./AGENT-PLAYBOOK.md)）：

1. Read `PROGRESS.md` —— 全局状态
2. Read `HANDOFF.md` —— 上次 session 留言
3. 如 HANDOFF 标记当前 task，Read 对应 `tasks/Pn-*.md` 中该 task 块
4. 用一句话复述确认："上次完成了 X，下一步是 Y，对吗？"
5. 用户确认后开始

**永远不要**在没读 HANDOFF 的情况下问"我们上次做到哪了"。
