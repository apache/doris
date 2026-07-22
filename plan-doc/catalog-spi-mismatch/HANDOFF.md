# Catalog SPI 抽象错配修复 — HANDOFF（任务追踪入口）

> 本文件是 `catalog-spi-mismatch` 修复工作的**会话交接入口**。每个新 session 先读本文件，再读 [`TASKLIST.md`](TASKLIST.md)，然后**对照当前代码重侦察**要处理的那一条，再动手。
>
> 配套文档：[`TASKLIST.md`](TASKLIST.md)（逐条任务 + 状态）、[`analysis-00-rollup.md`](analysis-00-rollup.md)（核实总览）、`analysis-A…G`（分类深挖）。

---

## 1. 这是什么

一篇**独立 clean-room review**（对抗式两阶段：独立调查 → red-team 复核）对 catalog-SPI 迁移提出了 **28 条抽象错配发现**（#1–#28），逐条对照工作树代码核实，结论落在 `analysis-A…G` 七份分类文档、由 `analysis-00-rollup.md` 索引汇总。

本追踪空间把这 28 条**转成可执行、可勾选的任务清单**，供后续逐一处理。核实后的真实优先级远小于原报告标注的严重度——**真正待办的很少，绝大多数是死代码清理、有意 legacy parity、或已修/不成立**。

核实结论分布（28 条）：

| 结论 | 条数 | 含义 |
|---|---|---|
| CONFIRMED | 11 | 属实（但多为死代码 / vestigial / 设计如此 / 仅估计偏斜） |
| PARTIAL | 11 | 部分属实 / 严重度被高估 / 连接器被误归 |
| REFUTED | 2 | 方向性误读，不成立（#5、#17） |
| STALE_FIXED | 4 | 曾属实，当前代码已修（#3、#4、#6、#28-core） |

---

## 2. ⚠️ 动手前必读的三条前置

1. **分析基线落后于当前 HEAD。** `analysis-*` 文档基于分支 `catalog-spi-2-lvl-cache`；当前工作分支是 **`catalog-spi-review-18`**，HEAD = **`aaab68ef474`（#65893 "strip residual iceberg/hive/hudi deps & dead code, delegate DDL validation, remove hudi_meta TVF"）**。该 commit 明确"删了死代码 + 把 DDL 校验下沉连接器"，**可能已经进一步解决或移动了某些 finding**（例如 #28 的 HiveConnector "Dormant" 注释疑已被清理，见 TASKLIST）。
2. **别信行号信内容、别信蓝图信侦察。** 文档里的 `file:line` 多已漂移（如 hudi `extractLiteralValue` 已从 `:1092` 漂到 `~:1063`）。**每条任务动手前先重侦察当前代码**：确认 finding 是否仍成立（可能已 STALE）、真实归属在哪、能力是否已被连接器承接。执行蓝图常高估工作量。
3. **这是 clean-room review 的产物，不是甲方需求。** 每条"建议动作"是核实员的判断，不等于必须做。CONFIRMED ≠ 必修（很多是有意设计）。动手前用自己的判断复核，有分歧就 surface。

---

## 3. 批次总览（详见 TASKLIST）

| 批次 | 主题 | 发现 | 建议先后 |
|---|---|---|---|
| **B1** | 正确性 / 稳定性（唯一需认真排期） | #2 | 需先定架构高度再做 |
| **B2** | 活跃产错的小步低风险修 | #1、#14 | 建议**先做**（快速收益） |
| **B3** | 死代码 / 注释清理（无功能风险） | #21、#23、#25、#28 | 建议**其次**（低风险） |
| **B4** | 架构收敛（碰 fe-core 铁律，独立设计任务） | #7、#8、#9、#27 | 交 review，别顺手做 |
| **B5** | 可选增强（需产品签字，默认不动） | #10、#11、#12、#13、#16、#19 | 每项动手前先确认是否升级 |
| **B6** | 已闭环 / 不成立（仅记录） | #3、#4、#5、#6、#15、#17、#18、#20、#22、#24、#26 | 逐条快速确认即可 |

**推荐处理顺序**：`B2 → B3 → B1 → B4`；B5 逐项征询用户后再定；B6 只需逐条确认闭环并勾掉。
（顺序可变——用户驱动。B1 #2 是唯一真正的 crash 风险，但因需先定"是否碰 SPI 签名/铁律"的架构高度，故排在低风险快赢之后。）

---

## 4. 工作法（每条任务的标准流程）

对**每一条**要处理的 finding：

1. **重侦察**：读当前代码，确认 finding 是否仍成立、`file:line` 现值、真实归属。若已 STALE_FIXED → 直接在 TASKLIST 标 ☑️ 并记一句证据，跳过。
2. **定方案**：用 `step-by-step-fix` skill（逐条修：设计→实现→测试→独立 commit）。改动前若涉调试/回归用 `systematic-debugging`。
3. **小步实现**：遵循下方铁律与约定。
4. **验证**：单测钉死 + 连接器**新增能力必补 e2e**（尤其 B1 #2）。测试要编码"为什么"，不只"是什么"（Rule 9）。
5. **独立 commit**：一条 finding 一个 commit，commit message **英文**，标题 `[<type>](catalog) …`。
6. **更新追踪**：改 TASKLIST 该行状态 + 一句结果；必要时更新本 HANDOFF"当前进度"。

### 必守铁律 / 约定（本仓库）

- **fe-core 只出不进**：删旧代码期 fe-core 源相关代码只减不增；禁为"删 A 能编译过"就近把逻辑挪进 fe-core util。**B4（#7/#8/#9）直接碰这条**——#8 的 odps write-block 泄漏进 fe-core `Transaction` 比泄漏进 connector-api 更违规，收敛须在 fe-core 层做。
- **通用 SPI 节点保持 connector-agnostic**：`PluginDrivenScanNode` 禁 source-specific 代码；connector 差异走能力位 `supports*()` opt-in 或连接器自带 typed 载体。#27 若真要统一 `ReaderType` 会碰这条 + "禁 scaffolding"。
- **异常文案只差措辞就改测试，别改连接器**（除非文案真错）。
- **大改动（B1 / B4）用 clean-room 对抗 review**：多 agent、先独立判断后交叉核对历史结论。
- **commit 英文；plan-doc / HANDOFF / TASKLIST 中文。**
- **上下文用量过 30% 即交接**：找干净节点更新 HANDOFF/TASKLIST 并通知用户开新 session。

### 关键跨条依赖

- **#2 与 #15 同族**（snapshot pin 粒度）。若 B1 #2 走"框架级给 SPI 加 snapshot 形参"路线，**可一并覆盖 #15**（stats 无 snapshot）。若走 paimon 局部修则不覆盖。定 #2 高度时一并考虑 #15。
- **#7 / #9 的"最小改"是纯 javadoc 收窄**（surgical、可当作 B3 级低风险做）；"彻底改"才是重构（B4）。TASKLIST 各给两档。
- **#1 与 #14 同源**："分区值渲染口径一致性"族（DATE/null 归一、escape/unescape），可对照 hudi P3、hive #65473 已修先例。

---

## 5. 当前进度快照

**起始状态：全部未开工。** 追踪空间刚建立（本次 session），尚未处理任何一条。

- B1：⬜ #2
- B2：⬜ #1、⬜ #14
- B3：⬜ #21、⬜ #23、⬜ #25、⚠️ #28（疑已部分清理，先 verify）
- B4：⬜ #7、⬜ #8、⬜ #9、⬜ #27（均为设计任务）
- B5：🚫 #10、#11、#12、#13、#16、#19（待用户签字）
- B6：☑️ #3、#4、#5、#6、#17（已闭环）｜🚫 #15、#18、#20、#22、#24、#26（有意/潜伏，记录不动）

> 每完成一条，更新 [`TASKLIST.md`](TASKLIST.md) 对应行的状态与结论，并在此处同步 batch 级进度。
