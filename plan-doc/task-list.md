# P4-T06d — 翻闸缺口修复 Task List

> 来源: `plan-doc/tasks/designs/P4-cutover-fix-design.md`(6 issue) + `plan-doc/reviews/P4-cutover-review-findings.md`(41 存活发现)。
> 流程(用户定): 每 issue = 独立设计文档 → 修复 → 编译+UT(无 e2e) → 对抗 review agent → review 有问题则回到设计循环(最多 5 轮)→ 记录每轮结论防矛盾 → 全过/到上限再 checkpoint。
> 每 issue 产物:
>   - 设计: `plan-doc/tasks/designs/P4-T06d-<issue>-design.md`(跨轮更新)
>   - review 轮次记录: `plan-doc/reviews/P4-T06d-<issue>-review-rounds.md`(每轮 finding+verdict+处置)
>   - summary: 写回本文件 + 设计文档尾

## 进度

| # | issue | phase | sev | layer | 设计 | 实现 | 编译+UT | review 轮次 | 状态 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | FIX-READ-DESC  | 1 read | blocker | connector | ✅ | ✅ | ✅ | 3 轮→收敛 | ✅ DONE (commit 待下方) |
| 2 | FIX-READ-SPLIT | 1 read | blocker | connector | ⬜ | ⬜ | ⬜ | — | ⬜ TODO |
| 3 | FIX-DDL-ENGINE | 2 DDL  | blocker | fe-core   | ⬜ | ⬜ | ⬜ | — | ⬜ TODO |
| 4 | FIX-DDL-REMOTE | 2 DDL  | major   | fe-core   | ⬜ | ⬜ | ⬜ | — | ⬜ TODO |
| 5 | FIX-PART-GATES | 3 part | major   | fe-core   | ⬜ | ⬜ | ⬜ | — | ⬜ TODO (⚠️OQ-6 待定) |
| 6 | FIX-WRITE-ROWS | 4 write| major   | fe-core   | ⬜ | ⬜ | ⬜ | — | ⬜ TODO |

图例: ⬜ 未开始 / 🔄 进行中 / ✅ 完成

## 关键前置决策(动手前)

- **OQ-6 (FIX-PART-GATES, issue 5)**: `partition_columns` prop 来源 —— (a) 每次调 connector `getTableSchema()` 重取(远端往返) vs (b) 新增 `PluginDrivenSchemaCacheValue` 子类持久化。**到 issue 5 前与用户确认**。
- **commit 时机**: ✅ 已定(2026-06-07)—— **每 issue 过对抗 review 后即独立 commit**(本地 catalog-spi-05,不 push)。commit message 用 `[P4-T06d] ...`。

## 跨 issue 红线(全程守)

- 🔴 勿删 `PartitionsTableValuedFunction.java:173` 的 `MaxComputeExternalCatalog` 分支(Batch-D 红线;历史"T06c 已加 PluginDriven 分支"为假)。
- 每 issue 独立 commit;改 fe-core 带 `-pl :fe-core -am`,改连接器带 `-pl :fe-connector-maxcompute`;读真实 BUILD/MVN_EXIT/CS_EXIT(勿信后台 task 通知 exit code)。
- 测试须真能 fail(Rule 9):验"业务逻辑回退能否让测试变红"。

## review 轮次累计结论(防跨轮矛盾,精简索引;详见各 issue round 文件)

- **FIX-READ-DESC (3 轮收敛)**: 生产修复(MaxComputeConnectorMetadata.buildTableDescriptor override + ctor 透传 endpoint/quota/properties;getMetadata passthrough)R1 正确性/BE-parity + R3 回归/build 两维 CLEAN。R1 R2 抓 [medium]=fe-core 调用点 wiring 无测试守门+doc 过度声明 → R2 补 `PluginDrivenExternalTableEngineTest#testToThriftPassesRemoteNamesAndNumColsToBuildTableDescriptor`(mutation 自证)→ R3 独立验证 CLEAN。结论基线: project/table 用 remote 名(OQ-7 有意修正);deprecated TMCTable 字段不 set(同 legacy,空串非 UB);连接器测试须 `-am`。
