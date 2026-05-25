# P<n>(.<m>) — <阶段主题>

> 复制本模板到 `tasks/P<n>-<slug>.md` 创建新阶段。
> 维护规则见 [README §4](../README.md)。

---

## 元信息

- **状态**：⏸ 待启动 / 🚧 进行中 / ✅ 完成 / ❌ 阻塞
- **启动日期**：YYYY-MM-DD
- **目标完成**：YYYY-MM-DD（估时 N 周）
- **实际完成**：YYYY-MM-DD（完成时填）
- **阻塞**：依赖哪些前置阶段 / 决策 / 外部条件
- **阻塞下游**：本阶段未完成会卡哪些后续阶段
- **主 owner**：@xxx

---

## 阶段目标

简述本阶段要交付什么。引用 master plan / RFC 中对应章节。

---

## 验收标准

从 master plan 或 RFC 中同步的验收清单。本阶段所有 task 完成且本清单全部勾选才算阶段完成。

- [ ] 标准 1
- [ ] 标准 2
- [ ] ...

---

## 任务清单

> ID 永不复用；删除的 task 标 `[deleted YYYY-MM-DD <原因>]` 保留占位。

| ID | 任务 | 批次 / 分组 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P<n>-T01 | <任务名> | 批 0 | @xxx | ⏳ pending / 🚧 / ✅ / ❌ | #NNN | YYYY-MM-DD | YYYY-MM-DD | 简短备注 |
| P<n>-T02 | ... | | | | | | | |

**状态图例**：
- ⏳ pending — 尚未开始
- 🚧 进行中
- ✅ 完成
- ❌ 阻塞 / 失败（在备注里写原因）
- 🚫 [deleted YYYY-MM-DD]

---

## 阶段日志（倒序）

> 每完成 / 阻塞 / 重大事件加一行；日志是追溯性的，不要回头改。

### YYYY-MM-DD
- 描述

### YYYY-MM-DD
- 描述

---

## 关联

- Master plan 章节：[§X.Y](../00-connector-migration-master-plan.md)
- RFC 章节：[§X.Y](../01-spi-extensions-rfc.md)
- 决策：D-NNN, D-MMM
- 偏差：DV-NNN（如果有）
- 风险：R-NNN
- 连接器：[connector-name](../connectors/xxx.md)（如本阶段聚焦特定连接器）

---

## 当前阻塞项（如有）

> 描述当前未解决的阻塞、谁能解、ETA。
