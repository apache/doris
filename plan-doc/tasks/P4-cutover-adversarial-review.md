# P4 — MaxCompute 翻闸实现 · 对抗 Review(clean-room)

> **状态:待执行(下一 session)** · 方式:**多 agent 对抗 workflow** · 纪律:**clean-room 后交叉核对**
> 这是一份**中性 brief**:只给任务、路径与导航锚点,**不含**开发过程的任何结论/取舍/"已知没问题"的说法。这样设计是为了让本轮 review 不被历史记忆带偏。

---

## 0. 目的

MaxCompute 的功能现在通过 **connector SPI + `PluginDrivenExternalCatalog` 适配层**(以下称"翻闸实现")提供;翻闸前的 **legacy MaxCompute 实现仍完整存在于代码树中**(尚未删除)。
本轮目标:**重新、独立地审阅翻闸实现的全部功能流程**,从**设计**与**实现交付**两个角度找问题,并**逐一对照 legacy 逻辑**找差异(有意 or 意外)。

审阅对象是**当前整条路径的真实代码**(不是某一次提交的 diff)。请把每条路径当作第一次看待。

---

## 1. ⚠️ Clean-room 纪律(必须遵守 —— 本轮的核心约束)

1. **先 code,后文档**。每条路径,先只读代码(翻闸实现 + legacy),**独立**形成你自己的判断与发现,**之后**才允许打开 `decisions-log.md` / `deviations-log.md` / `HANDOFF.md` 历史结论做交叉核对。
2. **派发 review agent 时,prompt 里只放本 brief + 代码访问**;**不要**把 decisions-log / deviations-log / HANDOFF 的结论粘进 agent 的 prompt。历史结论只在最后的"交叉核对"阶段、由独立的核对 agent 读取。
3. **历史结论一律视为"待证伪的主张",不是事实**。凡是看起来"像是有意为之 / 早有定论"的地方,正是要重点质疑的地方(历史记忆最容易在此制造盲区)。
4. **不预设结论**。不要假设"翻闸已通过 gate 所以大概率没问题"。gate(编译/checkstyle/单测)只覆盖很窄的面;本轮要找的是 gate 覆盖不到的设计/语义/一致性问题。
5. 发现项必须有**证据**(`file:line`,翻闸侧 + legacy 侧各一),不接受"凭印象"。

---

## 2. 方式:多 agent 对抗 workflow

建议(下一 session 可按需调整规模):

- **Phase A — 独立审阅(per-path 并行)**:每条路径(5 条)派 1+ 个 reviewer agent,各自端到端 trace 翻闸实现 + legacy,产出该路径的发现清单(结构见 §5)。reviewer 之间互不可见彼此结果。
- **Phase B — 对抗验证(per-finding)**:对每个发现派**独立的、带不同视角的**验证 agent(例如:correctness / parity-vs-legacy / repro / 边界),**默认立场是"证伪该发现"**;多票后才保留(survives)。目的是滤掉"看似有理实则站不住"的发现。
- **Phase C — 交叉核对(clean-room 解除)**:只有到这一步,才读 `decisions-log.md` / `deviations-log.md` / `HANDOFF.md`,逐条对比:
  - 我们独立发现的问题,历史文档是否已记录?(若未记录 = 新发现)
  - 历史文档**声称**已解决/无问题/可接受的点,本轮独立审阅是否**同意**?(若不同意 = 重点分歧,优先级最高)
  - 任何"声称做了 X"但代码里查无实据的,标为 divergence。
- **Phase D — 综合**:产出最终报告(§5),按严重度排序,标注每项的"是否回归 / 是否新发现 / 与历史结论是否分歧"。

> 规模建议:ultracode 已开,token 不是约束。优先把对抗验证(Phase B)做足——这是"对抗 review"的价值所在。

---

## 3. 审阅的 5 条路径 + 导航锚点

> 下列锚点仅为**导航起点**(代码树中客观存在的类/模块),**不含**对其正确与否的任何判断。请从这些起点 trace 出完整路径,并用自己的 grep/Explore 扩展地图——不要假设这里列全了。
> 通用结构:每条路径都有 **翻闸侧**(connector SPI + `PluginDriven*` 适配)与 **legacy 侧**(`org.apache.doris.datasource.maxcompute.*`),请**两侧都读并对照**。connector 实现主要在 `fe/fe-connector/fe-connector-maxcompute/` 与 SPI 接口 `fe/fe-connector/fe-connector-api/`。

### 路径 1 — 读取(SELECT / 分区裁剪 / schema / split / 类型映射 / 投影下推)
- 翻闸:`PluginDrivenScanNode`、`PluginDrivenExternalTable`(`datasource/`)、connector 的 scan/split/schema 实现(`fe-connector-maxcompute`)。
- legacy:`datasource/maxcompute/source/MaxComputeScanNode`、`datasource/maxcompute/MaxComputeExternalTable`。
- BE 侧(如涉及):`fe/be-java-extensions/max-compute-connector/`。

### 路径 2 — 写入(INSERT / INSERT OVERWRITE / OVERWRITE PARTITION / 事务 / commit 协议 / block 分配)
- 翻闸:`nereids/.../insert/PluginDrivenInsertExecutor`、`planner/PluginDrivenTableSink`、`transaction/PluginDrivenTransactionManager`、connector 的 write/commit 实现;BE→FE block 分配 RPC `service/FrontendServiceImpl#getMaxComputeBlockIdRange`、commit 数据结构 `TMCCommitData`;BE 客户端 `be-java-extensions/max-compute-connector/.../MaxComputeFeClient`。
- legacy:`nereids/.../insert/MCInsertExecutor` 及其牵出的 legacy 写/事务路径。

### 路径 3 — DDL(CREATE/DROP TABLE、CREATE/DROP DATABASE、RENAME、IF [NOT] EXISTS / FORCE 语义)
- 翻闸:`datasource/PluginDrivenExternalCatalog`(create/drop table/db override)、SPI `connector/api/ConnectorSchemaOps`+`ConnectorTableOps`、`fe-connector-maxcompute/.../MaxComputeConnectorMetadata`、`fe-connector-maxcompute/.../McStructureHelper`。
- legacy:`datasource/maxcompute/MaxComputeExternalCatalog`、`datasource/maxcompute/MaxComputeMetadataOps`、`datasource/maxcompute/McStructureHelper`、基类 `datasource/ExternalCatalog`(create/drop 的 metadataOps 路径)。

### 路径 4 — 元数据回放(editlog → replay,master vs follower 状态重建)
- 翻闸:`datasource/ExternalCatalog#replay{CreateDb,DropDb,CreateTable,DropTable}`(注意 `metadataOps` 在翻闸路径上的取值)、`persist/EditLog` 的相关 OP 分发、`catalog/Env#replay{CreateDb,DropDb,CreateTable,DropTable}`。
- legacy:同上 replay 入口,但经 `MaxComputeMetadataOps` 的 `afterCreateDb/afterDropDb/afterCreateTable/afterDropTable`。
- 重点:**master 写路径**与 **follower 回放路径**分别如何把内存态改到与远端一致;两侧是否对称。

### 路径 5 — 元数据 cache(db/table 名单、schema、分区;失效时机与一致性)
- 翻闸:`datasource/ExternalCatalog`(`resetMetaCacheNames`/`unregisterDatabase`/`getDbForReplay`)、`datasource/ExternalDatabase`(`resetMetaCacheNames`/`unregisterTable`)、`datasource/ExternalMetaCacheMgr`、`PluginDrivenExternalTable` 的 schema/分区获取、connector 的分区列举(是否有/无连接器侧 cache)。
- legacy:`MaxComputeMetadataOps.afterX` 的失效动作、`datasource/maxcompute/MaxComputeExternalMetaCache`、legacy 分区/ schema 获取。
- 重点:DDL 后**同一 FE** 是否立即可见;**follower** 回放后是否一致;TTL/refresh;有无陈旧读窗口。

---

## 4. 每条路径的审阅维度(中性 checklist)

- **D1 正确性**:逻辑是否正确实现预期行为?参数、顺序、缺步、错误分支。
- **D2 与 legacy 的行为一致性**:trace legacy 同一操作,翻闸是否保持**可观察行为**一致?任何差异——是有意(且应有据)还是意外(=回归)?
- **D3 完整性**:翻闸是否覆盖 legacy 的全部能力?有无遗漏的操作 / 被丢弃的语义(如 `ifExists`/`force`/`ifNotExists`)/ 未处理的分支?
- **D4 边界与错误处理**:null、异常、空结果、大小写、**本地名 vs 远端名映射**、并发、超时、重试。
- **D5 一致性 / 持久化**(尤其路径 4/5):master vs follower、editlog/replay 正确性、cache 失效时机、陈旧读、HA 下的可恢复性。
- **D6 设计 vs 实现**:实现是否与其设计文档一致?(设计文档在 `plan-doc/tasks/designs/`,**仅在 Phase C 交叉核对时读**)有无未声明的偏离?

---

## 5. 产出(deliverable)

输出到 `plan-doc/reviews/P4-cutover-review-findings.md`(新建;如无 `reviews/` 目录则建)。结构:

- **逐路径小节**(读取/写入/ddl/回放/cache),每节列发现项:
  | 字段 | 说明 |
  |---|---|
  | id | 如 `READ-01` |
  | severity | blocker / major / minor / question |
  | title | 一句话 |
  | evidence | 翻闸侧 `file:line` + legacy 侧 `file:line` |
  | legacy-diff | 与 legacy 的具体行为差异 |
  | regression? | 是/否/不确定 |
  | adversarial-verdict | Phase B 的存活情况(几票证伪/几票确认) |
  | recommendation | 修 / 接受 / 待定 + 理由 |
- **交叉核对小节(Phase C)**:本轮发现 vs `decisions-log` / `deviations-log` / `HANDOFF`——分三类:① 历史未记的新发现;② 历史声称已解决但本轮**不认同**的分歧(最高优先级);③ 声称做了但查无实据。
- **总结**:按 severity 排序的 top 问题 + 建议的后续动作。

---

## 6. 边界

- 本轮是**审阅**,**不改代码**(除非另行授权)。发现 → 报告 → 由用户决定修复时机。
- legacy 代码当前仍在树中(Batch D 删除尚未执行),这正是做对照 review 的**最佳时机**——务必两侧对照,别只看翻闸侧。
- 若需要运行期佐证,可参考(但不取代代码审阅)live 验证 runbook(见 `HANDOFF.md`)。
