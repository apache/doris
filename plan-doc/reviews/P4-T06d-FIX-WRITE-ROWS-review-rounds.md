# P4-T06d · FIX-WRITE-ROWS — 对抗 review 轮次记录

> issue 6 / 6(最后一个)。设计: `plan-doc/tasks/designs/P4-T06d-FIX-WRITE-ROWS-design.md`。
> 流程: clean-room 多 agent 对抗(Phase A 3 lens 仅读代码 → Phase B 3 票 refute-by-default → Phase C 交叉核对)。
> 改动: `PluginDrivenInsertExecutor.java` doBeforeCommit 加一行事务模型 `loadedRows` 回填 + `PluginDrivenInsertExecutorTest` 2 新用例。

## Round 1 — verdict: `sound`(1 轮收敛,无 real gap)

review 配置: 3 lens(correctness-parity / regression / test-quality)→ 每 finding 3 skeptic refute-by-default(≥2 confirm 存活)。workflow `wi7zu5h45`,15 agent。

4 条 raw findings 经 Phase B **全未存活**(confirms 0/0/0/1,无一 ≥2)→ 无 survivor → verdict `sound`:
- **F1**(confirms 0): 回填测试直接 stub `getUpdateCnt()`,未跑 BE-feedback→commitDataList→getUpdateCnt 链。证伪——单测边界正确(BE 累加链是 connector/BE 侧,fe-core 单测不该跨层)。
- **F2**(confirms 0): handle 模型用例断言 loadedRows 停 0 无法区分"connectorTx 分支跳过"vs"execImpl 没跑"。证伪——用例直调 doBeforeCommit、显式注 connectorTx=null + finishInsert 被调断言,区分明确。
- **F3**(confirms 0): `getUpdateCnt()` 依赖 SPI default 返 0,未来事务模型 connector 忘 override 会静默报 0 行。证伪——对 MC 今正确;未来 adopter 是其自身 override 责任,非本 fix 缺陷(投机)。
- **F4**(confirms 1,未达 2): 测试验 `loadedRows` 字段但未验其流到 reported affected-rows 表面。未存活——affected-rows 表面化是 `BaseExternalTableInsertExecutor` 既有 wiring(非本 fix),由 e2e 覆盖;字段赋值是本 fix 的唯一改动点,已 mutation 锁。

## 收敛结论
1 轮 `sound`。production code 正确(parity / 互斥分支 / 取值时点 / jdbc-es-trino 零影响 三 lens 一致 clean)。
守门(clean source,cache 关):UT 6/6 绿(4 既有 + 2 新);Checkstyle 0;BUILD SUCCESS。
mutation 总账:
- `loadedRows = connectorTx.getUpdateCnt()` → `loadedRows = 0L` → `...BackfillsLoadedRows...` 红(expected 42 was 0)。
- 删 `if (connectorTx != null)` 守卫 → `...SkipsTxnBackfillWhenNoConnectorTxn` 红(NPE: connectorTx null)。
证回填取值 + 守卫互斥 均 load-bearing。
