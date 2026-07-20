# 📦 任务空间 — fe-connector-iceberg 热路径重操作修复

> **独立任务空间**，与 catalog-spi 主线（`plan-doc/HANDOFF.md`，当前占用者 = CI-997422 任务线）**并行但不混流**。
> 目标：把 [`../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md`](../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md) 审计出的
> **23 条确认发现（C1–C23）逐一修复**，消掉"新框架 SPI 各入口各自 loadTable/各自扫" 造成的重操作放大。
> 协作规范沿用 [`../AGENT-PLAYBOOK.md`](../AGENT-PLAYBOOK.md)。

---

## 🚩 一句话背景（2026-07-17 审计结论）

**总病灶**：新框架的 SPI 各入口"每次自己 loadTable / 自己扫"，而 legacy 有单一 source 持缓存 Table ——
同一信息在一次规划里被远程重取 **3~7 次**；另有 #64134 的 planFiles 兜底在新框架以 per-query 乘数复活。
审计分 **P0/P1/P2 三层、七簇**，55-agent 对抗审计 + 人工抽验，**23 确认 / 1 驳回 / 0 存疑**。

> ⚠️ **审计报告是"待 review"的结论草案**（报告 §5「供 review 讨论，非结论」）。故每项**立项第一步是复核**（见下 §立项流程 step 2），不是照单实现。

---

## 📂 本空间文件

| 文件 | 用途 | 更新方式 |
|---|---|---|
| [`tasklist.md`](./tasklist.md) | **Task list** —— 唯一进度总览，`PERF-NN` 勾选 + 状态表 | 每完成一项随 commit 勾 `[x]` + 填状态/commit |
| [`HANDOFF.md`](./HANDOFF.md) | **交接文档** —— 只写「下一个 session 第一件事做什么」 | 每 session 结束**覆盖式**更新 |
| [`progress.md`](./progress.md) | **进度记录** —— append-only 日志（日期 / commit / 结论 / 踩坑） | 只追加，不覆盖 |
| [`designs/`](./designs/) | **per-task 设计 + 小结** —— `FIX-PERF-NN-<slug>-design.md` / `-summary.md` | 每项**修复时**逐个建，不预建 |

> **分析/设计的权威来源不在本空间**（避免重复记录），而是：
> 审计报告 `../reviews/perf-audit-...md`（分层结论 + 各簇详述 + §5 修复优先级）
> · 证据 JSON `../reviews/perf-audit-...-findings.json`（**每条**发现的完整调用链 + 双路对抗验证意见，是复核行号的第一手材料）
> · 问题类 `../perf-heavy-op-hot-path-problem-class.md`（三要素 + A/B/C/D 变体 + 审计清单）。

---

## ▶️ 新 session 开场流程（必须遵守）

```
1. Read plan-doc/perf-hotpath-iceberg/HANDOFF.md    ← 上次留言 + 下一步
2. Read plan-doc/perf-hotpath-iceberg/tasklist.md   ← 勾到哪了、下一个 PERF-NN 是谁
3. 需要某条发现的细节时才 Read 审计报告对应簇 / findings.json 对应条目（别默认全读）
4. 用一句话向用户复述："上次做完了 X，下一步是 PERF-NN，对吗？"
5. 等用户确认后按下面「单项立项流程」开始
```

**⚠️ 行号信 HEAD 不信文档** —— 本空间与审计报告所有 `file:line` 是 **2026-07-17 / 分支 `catalog-spi-review-16`** 基线，代码动了就以 `grep` 为准。

---

## 🔁 单项立项流程（每个 PERF-NN，对齐 `step-by-step-fix` skill）

> 一次一个任务，串行推进。这些是**性能修复 → 必须行为不变**，所以复核与"证明只减不改"是重点。

1. **确认 owner 单一**：同一时刻一个任务只一个 session 在做（并发探测见 HANDOFF「构建/并发坑」）。
2. **复核发现（design subagent 先做，动码前）**：按 findings.json 的调用链**重新 grep 行号**、重新确认①重操作真实成本 ②乘数（per-query / per-split / per-file）③是否真无缓存兜住。审计是草案，复核**可能推翻或缩小**某条 —— 如推翻，在 tasklist 标 🔬 并记 progress，不硬修。
3. **写设计文档** `designs/FIX-PERF-NN-<slug>-design.md`：Problem / Root Cause / Design / Implementation Plan / Risk / Test Plan（含如何**度量收益**：loadTable/planFiles 调用计数、EXPLAIN 时延、或 debug 计数器）。
4. **设计红队**（对抗 review，符合本项目 clean-room 偏好）：至少一个独立视角挑刺后再实现。
5. **实现**：最小改动、守约束（见下铁律）、保持风格。
6. **验证**（这是性能修复，闸门 = 行为 parity + 证明减负）：
   - 相关 **UT / e2e 全绿**（parity，禁回归）；必要时补一个**调用计数断言**当守门（如"规划期 loadTable 从 N 降到 1"）。
   - build 用 `-Dmaven.build.cache.enabled=false`（否则 surefire 被静默跳过，见 HANDOFF）。
7. **独立 commit**：`[perf](catalog) fe-connector-iceberg: <subject> (PERF-NN)`，正文写 root cause / 修法 / 度量数 / 守门。
8. **写小结** `designs/FIX-PERF-NN-<slug>-summary.md`（Problem/Root Cause/Fix/Tests/Result）。
9. **更新 `tasklist.md`**（勾 `[x]` + 填 commit/状态）→ **追加 `progress.md`** → **覆盖 `HANDOFF.md`**（下一项是谁）。

---

## 🧱 约束铁律（**违反即返工，动码前记牢**）

1. **fe-core 源只出不进 + 禁 scaffolding 搬迁**（本项目删旧代码期两条铁律）：修复的缓存/memo 默认放**连接器侧**（`IcebergConnector` / handle / `fe-connector-cache`），**不得**为图省事把 Table 缓存、属性解析、派生 helper 塞进 fe-core。PERF-01/02/03 的 memo 都挂连接器。
2. **fe-core 通用节点保持 connector-agnostic**：PERF-09（fe-core 框架层微批）、PERF-11 内的 C14（通用节点 per-split）改的是**通用**逻辑 —— 允许改（惠及所有连接器、非 source-specific），但**禁**按 iceberg/源名分支；且属共享热路径，须证 **byte + cost 对所有连接器双不变**。
3. **fe-core 不解析属性**：任何属性相关派生放插件侧组装 → BE thrift → 回传，别在 fe-core 加解析。
4. **失效收窄要精确**：PERF-01 收窄 `convertPredicate` 失效时，只保留真正依赖 conjunct 的 prop（= pushdown-predicates 一个）；错杀会把簇1/簇2 成本翻回去。
5. **快照 pin 是缓存 key 的天然来源**：PERF-01/02/03 统一用 `beginQuerySnapshot` 已 pin 的 `(TableIdentifier, snapshotId)` 做 key —— 先做 PERF-01 立住这套 key/memo 模式，02/03/10 复用，别各造一套。

---

## 🔗 与其它文档/任务的关系

- **主线** `plan-doc/HANDOFF.md`：当前是 CI-997422 任务线，本任务在其「📎 并行独立任务：热路径重操作审计」小节里被引出 —— 本空间即那条线的落地执行区。**两者不混流**。
- **问题类可复用**：审计结论里「其余连接器（hive/paimon/hudi/mc）按同一问题类 + 同一 workflow 逐个审计」是后续工作 —— 届时各连接器建**各自的兄弟任务空间**（对齐本项目 per-connector workspace 惯例），不并进本空间。
- 协作规范、context 预算、subagent/handoff 纪律：一律沿用 [`../AGENT-PLAYBOOK.md`](../AGENT-PLAYBOOK.md)。
