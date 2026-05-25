# Agent 协作规范 — Context 管理与最佳实践

> 本项目是大型多阶段重构，预计跨数月、上百个 PR、可能跨数十个 LLM agent session。
> 本规范旨在让"无论哪一次 session、由哪个 agent 接手，都能高质量推进"，**核心是 context 管理**。

---

## 一、为什么需要规范

LLM agent 协作的三大失效模式：

1. **Context 中毒**：单 session 累积太多无关信息，模型注意力分散、决策质量下降、出现幻觉。
2. **认知断层**：换 session 后失去前情，重复探索 / 推翻已有决策 / 重新发明轮子。
3. **维护脱节**：代码改了文档不改，下次进 session 时基于过时文档做错误判断。

本规范用三类工具应对：
- **Context 预算与监控**（§2）—— 防失效模式 1
- **Subagent 与 Handoff**（§3、§4）—— 防失效模式 1、2
- **强制纪律**（§5）—— 防失效模式 3

---

## 二、Context 预算

### 2.1 单 session 预算

| Context 使用率 | 状态 | 推荐行为 |
|---|---|---|
| **0–40%** | 🟢 健康 | 正常工作，可以做任何任务 |
| **40–60%** | 🟢 健康偏高 | 开始倾向于把"独立的探索 / 大文件读"转给 subagent |
| **60–75%** | 🟡 警觉 | **不再读 ≥500 行的整文件**；只做精确 grep / offset+limit read；准备 handoff 草稿 |
| **75–85%** | 🟠 高危 | **停止接新任务**；完成手头 1 个原子工作；写 HANDOFF.md；通知用户切 session |
| **>85%** | 🔴 危险 | **只做记录性工作**（更新 PROGRESS / HANDOFF）；不再做任何决策 / 代码生成 |

> Claude Code 中可通过 `/context` 查看当前用量；如不可见，按"已发起的工具调用数 + 已读文件总行数"粗略估算。

### 2.2 用户对 session 的隐式预期

如果用户在一次 session 中要求"重构 X 模块 + 写文档 + 提交 PR"，agent 应：

- 评估 context 占用：单凭 RFC + 现有代码探索就可能吃掉 30-40%
- **主动报告**：在开始执行前告知 "此任务预计占用约 40% context，是否需要先写 handoff 占位以便分两个 session 完成？"

### 2.3 节省 context 的硬性技巧

1. **永远不要 `Read` 整个 >1000 行的文件** —— 用 `grep` 定位行号，再用 `offset + limit` 精读。
2. **永远不要重复 grep 同一个 pattern** —— 在 session 心智里记住结果。
3. **避免 `cat` / `find -type f -name '*.java'` 全量列举** —— 用更精准的 grep / find 加过滤。
4. **避免 `git log -p`** —— 用 `git log --oneline -20`，需要 diff 再单独 `git show <hash>`。
5. **大文件总结优先用 subagent**（见 §3）：让 subagent 读 5000 行返回 200 字总结。

---

## 三、Subagent 使用规范

### 3.1 何时**必须**用 subagent

- **跨 5+ 文件的代码搜索 / 调研**（如"找出 fe-core 中所有 instanceof HMSExternalCatalog 的地方"）
- **读取 >1000 行的单文件后只取关键信息**（如 IcebergMetadataOps.java 1247 行，只需了解 createTable 路径）
- **独立的、不影响主线决策的小重构**（如"批量改 import 路径"，给 subagent prompt + 文件列表，背景执行）
- **独立的代码评审**（如"review 这次 PR 的安全性"，需要重读大量上下文）

### 3.2 何时**不要**用 subagent

- 主线决策环节 —— subagent 给的建议你最终还是要消化，不如自己做
- 1-2 次 grep / read 就能解决的简单查找 —— 启动 subagent 的固定开销不值得
- 需要持续交互的探索（边读边问"那 X 呢"）—— subagent 一次性输出，互动不便
- 涉及"修改后立即验证"的小改动 —— 主 session 闭环更快

### 3.3 写 subagent prompt 的硬性规则

```
1. 自包含：不能假设 subagent 知道主线对话内容。明确说"working directory: /...",
   "background: 这是 XX 项目的 YY 阶段，目标是 ZZ"。
2. 输出格式约束：明确"返回 markdown 表格 / 总字数 ≤ 500 / 只列文件路径不带代码"。
3. 范围约束：明确"只看 fe-core 目录"、"忽略 test 目录"、"不读 README"。
4. 决策权约束：明确"只调研，不做任何修改"或"可以修改 X 但不能动 Y"。
5. 一次性：避免让 subagent 内部继续延伸调研——主 session 来决定下一步。
```

### 3.4 Subagent 类型选择（Claude Code 内）

| 任务类型 | 推荐 subagent | 备注 |
|---|---|---|
| 大范围代码搜索 | `Explore` | 只读、快、context 隔离 |
| 多步独立工作 | `general-purpose` | 可以执行 grep / read / edit |
| 实现计划设计 | `Plan` | 只产出方案不写代码 |
| 都不适合 | `claude`（默认）| 兜底 |

### 3.5 Background 模式

长耗时任务（如 `mvn test`、跨模块 build）使用 `run_in_background: true`，主 session 不被阻塞。完成时会自动通知，**不要 sleep 轮询**。

---

## 四、Handoff（跨 session 接管）

### 4.1 何时**必须**写 handoff

- Context 使用率 ≥ 70%（§2.1）
- 当前 P 阶段结束（如 P0 → P1 切换）
- 工作天然分段（如"下周再继续"）
- 出现长时间阻塞，等其他人 review / 等 CI 跑（≥4 小时）
- 用户主动说"今天到此为止"

### 4.2 何时**不需要**写 handoff

- 同一 session 内自然继续
- Context < 50% 且任务还很短

### 4.3 Handoff 文档结构

见 [`HANDOFF.md`](./HANDOFF.md) 模板。核心字段：

1. **本 session 完成了什么**（具体到 task ID、PR、commit）
2. **当前正在做的事是否完整**（如果中途停的，写明卡在哪个文件、哪一行）
3. **关键认知 / 临时发现**（如"刚发现 X 类的 Y 方法有意外副作用"——这种东西不写下来下次会重复踩坑）
4. **下一个 session 第一件事做什么**（精确到 task ID + 第一行代码 / 命令）
5. **当前 session 没解决但需要标记的问题**（不是 TODO 而是"开放问题"）

### 4.4 Handoff 文件存放

- 单个滚动文件 `plan-doc/HANDOFF.md`
- 每次 session 结束时**覆盖式更新**
- 历史 handoff 通过 `git log plan-doc/HANDOFF.md` 查看
- **不要**建 `handoffs/2026-05-24.md` 这种归档目录 —— git history 已经胜任

### 4.5 接管新 session 的开场流程

新 session 开始第一件事（**所有 agent 必须遵守**）：

```
1. Read plan-doc/PROGRESS.md            ← 全局状态
2. Read plan-doc/HANDOFF.md             ← 上次留言
3. 如果 HANDOFF 标记当前 task：
   Read plan-doc/tasks/Pn-*.md 中对应 task 块
4. 用一句话向用户复述："上次 session 做完了 X，下一步是 Y，对吗？"
5. 等用户确认后开始
```

**不要**在没读 HANDOFF 的情况下问"我们上次做到哪了" —— 这是失败模式。

---

## 五、强制纪律

### 5.1 文档同步纪律

每次完成 task：
1. 更新 `tasks/Pn-*.md` 对应 task 状态
2. 更新 `PROGRESS.md` §三和§四
3. 更新 `connectors/<name>.md`（如果该 task 属于某个连接器）
4. 如果产生新决策 → `decisions-log.md` 新增 D-NNN
5. 如果发现偏差 → `deviations-log.md` 新增 DV-NNN

**5 步缺一不可**。否则下次 session 看到的状态就是错的。

### 5.2 RFC 修改纪律

任何修改 `01-spi-extensions-rfc.md` 的行为：
1. 先在 `deviations-log.md` 或 `decisions-log.md` 留痕（区别见 [README §3.1](./README.md)）
2. 在 RFC 该节加 `（D-NNN / DV-NNN 修订 YYYY-MM-DD）`脚注
3. 不要 silent edit

### 5.3 Task ID 纪律

- Task ID 一旦分配**永不复用**
- 删除的 task 标 `[deleted YYYY-MM-DD]` 保留占位行
- 重命名 task 不改 ID

### 5.4 提交信息纪律

PR title 第一行必须 `[Pn-Tnn] <subject>`，例如：
```
[P0-T03] Implement ConnectorMetaInvalidator interface
```

---

## 六、Anti-Patterns（绝对禁止）

| 反模式 | 为什么禁止 | 正确做法 |
|---|---|---|
| 一个 session 又读 RFC、又改 SPI、又写实现、又跑测试 | Context 爆炸；决策质量下降 | 拆 session：阅读/设计 → handoff → 实现 → handoff → 验证 |
| 跨 session 凭记忆继续工作 | 模型完全没记忆，认知断层 | 强制读 HANDOFF |
| Subagent 也用来做"小事" | 启动开销大于收益 | <2 次 grep 直接主线做 |
| 把 RFC 当 PROGRESS 用 | RFC 是设计稳定文档，频繁更新会污染 git history | PROGRESS / tasks / handoff 才是状态文件 |
| Handoff 写得像周报 | 周报对用户有用，对下一个 agent 无用 | 写"下一步第一行命令是什么"才有用 |
| 多个 session 并发改同一 task | 重复劳动 / 文档冲突 | 同一时刻一个 task 只一个 owner |
| Decision / Deviation 直接写到 RFC 里不进 log | 失去追溯性 | 先 log 再改 RFC |

---

## 七、各类 session 的典型节奏（参考）

### 7.1 "设计 + 评审" session（高密度阅读）

```
开场：Read PROGRESS + HANDOFF                            （3% context）
主体：Read 3-5 个核心文件 + RFC 某节                     （25% context）
        ↓
       与用户来回讨论 5-10 轮                            （+30% context）
        ↓
       Edit / Write 文档（RFC 修改、decision 记录）       （+10% context）
收尾：更新 PROGRESS + 写 HANDOFF                          （+5% context）
                                                           ─────────
                                                          ~73% 健康终止
```

### 7.2 "代码实现" session（中等密度）

```
开场：Read PROGRESS + HANDOFF + 对应 task                （5%）
主体：Read 现有相关代码（精读，offset+limit）             （15%）
        ↓
       Write / Edit 实现                                  （+15%）
        ↓
       Run tests（如可），修复错误                        （+15%）
收尾：更新 task 状态 + PROGRESS + git commit + HANDOFF    （+10%）
                                                           ─────────
                                                          ~60%
```

### 7.3 "调研 / 探索" session（高度依赖 subagent）

```
开场：Read PROGRESS + HANDOFF                            （3%）
主体：dispatch subagent 做 5-10 路并行调研                （+10% 主线 +50% subagent）
        ↓
       综合 subagent 结果做决策                           （+10%）
        ↓
       Write 调研结论文档（如新 RFC）                     （+10%）
收尾：更新 PROGRESS + decisions-log + HANDOFF             （+5%）
                                                           ─────────
                                                          ~38%
```

---

## 八、Context "重启"策略

如果 context 已经超 75% 但任务还没做完：

1. **优先保存状态**：立即写 HANDOFF.md，详细到下一行代码该写什么
2. **完成原子收尾**：当前正在 Edit 的文件**改完 + 保存**，不要留半截
3. **更新 PROGRESS**：把已完成的 task 标 ✅
4. **提醒用户切 session**："Context 已 ~78%，建议开新 session 继续。HANDOFF 已写好，新 session 第一句话发 'continue from handoff' 即可。"
5. **不要硬撑**：每多用 1% context 都在降低质量

---

## 九、Multi-agent 协作的边界

本项目原则上一个 task 由一个 agent 推进，但允许：

- **并行 subagent**：调研 / 测试 / build 等独立任务并行
- **审计 subagent**：让一个 subagent 审核主线工作（如"以挑刺 reviewer 视角看这次改动"）
- **接力**：handoff 后由完全不同的 agent / 人接手

**不允许**：
- 同时两个 agent 改同一 task
- Subagent 跨阶段（subagent 只做本 session 的工作，不要让 subagent 自己写 HANDOFF）

---

## 十、面向"未来 agent"的元规则

如果你（未来 agent）发现本规范本身需要修改：

1. 不要直接改本文件 —— 先在 `deviations-log.md` 写 `DV-NNN: AGENT-PLAYBOOK 规则 X 在场景 Y 不适用`
2. 与用户讨论后再修改本文件
3. 修改时在文末 §十一 加版本号 + 变更说明

---

## 十一、版本

| 版本 | 日期 | 变更 |
|---|---|---|
| v1 | 2026-05-24 | 初版（与 README、PROGRESS、HANDOFF 同时建立） |
