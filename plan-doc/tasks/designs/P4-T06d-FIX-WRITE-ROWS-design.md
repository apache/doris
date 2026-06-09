# P4-T06d · FIX-WRITE-ROWS — INSERT affected-rows 恒 0 → doBeforeCommit 回填 loadedRows

> issue 6 / 6(**最后一个**),phase 4 写回正确性,sev=major,layer=fe-core。来源: `P4-cutover-fix-design.md` §FIX-WRITE-ROWS(:394-420,parent 无 critic 块——本 issue 首次对抗 review)+ review WRITE-P1/WRITE-C1。
> 据当前代码树核实(行号校正)。

## Problem
翻闸后(SPI 事务模型,当前唯一 adopter=MaxCompute),对 PluginDriven 外表 `INSERT INTO ...` 数据被正确写入,但客户端返回 / `SHOW INSERT RESULT` / `fe.audit.log` 的 returnRows 恒为 `affected rows: 0`。触发条件:`connectorTx != null`(SPI 事务模型)的任意 INSERT。JDBC/auto-commit handle 模型(`connectorTx==null`)不受影响。可观察输出回归(数据不丢,行数判读错误)。

## Root Cause(行号据当前树)
- `PluginDrivenInsertExecutor.doBeforeCommit()`(`:146-150`,修前)只在 `writeOps != null && insertHandle != null` 时调 `finishInsert`。事务模型下 `insertHandle` 恒 null(`beforeExec():108-113` 事务模型早退,handle 仅 JDBC 分支 `:140` 创建)→ 整段跳过,`loadedRows` 永不赋值。
- `loadedRows` 字段 `AbstractInsertExecutor.java:69`(`protected long loadedRows = 0`);非事务路径在 `:222` 由 `coordinator.getLoadCounters().get(DPP_NORMAL_ALL)` 赋值。事务模型 BE 的 MaxCompute sink 只经 `TMCCommitData.row_count` 上报,不更新 `num_rows_load_success`(DPP_NORMAL_ALL)→ 取回 0。
- 下游 `BaseExternalTableInsertExecutor` 用 `loadedRows` 设 setOk/updateReturnRows → 全 0。
- legacy 基线 `MCInsertExecutor.java:74-78` doBeforeCommit:`loadedRows = transaction.getUpdateCnt()` + `transaction.finishInsert()`。翻闸 restructure 把 finishInsert 等价物(connectorTx.commit 经 txn manager,onComplete)镜像了,**漏镜像 loadedRows 赋值**。历史误判 `P4-T05-T06-cutover-design.md:114`("doBeforeCommit ... null for MC ⇒ correctly skipped")——本设计显式推翻。

## Design
在 `doBeforeCommit()` 事务模型分支回填 `loadedRows`,镜像 legacy 可观察行为。**不扩任何 SPI**:`getUpdateCnt()` 全链路已就绪——`ConnectorTransaction.getUpdateCnt()`(default `:96` 返 0)→ `MaxComputeConnectorTransaction.getUpdateCnt()`(`:158-159` = `sum(TMCCommitData.getRowCount())`)。
- 取法(a,parent 推荐):`connectorTx.getUpdateCnt()`——executor 现有字段在手,无需 `transactionManager.getTransaction(txnId)` 的可失败 lookup;值与 legacy 一致(同一 `TMCCommitData.row_count` 累加链)。
- keyed on `connectorTx != null`(SPI 事务模型),非 hardcode maxcompute——任何未来事务模型 connector 自动受益;`connectorTx == null` 的 JDBC/auto-commit 路径**字节不变**(继续走 coordinator/DPP_NORMAL_ALL)。
- 现有 `finishInsert` guard(`writeOps != null && insertHandle != null`)不动;新增分支独立。两分支**互斥**(`connectorTx != null` ⇔ `insertHandle == null`:事务模型从不开 per-statement insert handle),顺序无关。
- 无 finishInsert 调用:事务模型的提交经 txn manager(onComplete)完成,doBeforeCommit 只补行数。

## Implementation Plan
- [fe-core] `PluginDrivenInsertExecutor.java` `doBeforeCommit()`:在 finishInsert guard 后新增
  `if (connectorTx != null) { loadedRows = connectorTx.getUpdateCnt(); }` + 注释。无新 import(`ConnectorTransaction` 已 import `:30`)。
- 不改 fe-connector-maxcompute / fe-connector-api / be / thrift。守门 `-pl :fe-core -am` + checkstyle。

## Risk
- 回归极低:仅 `connectorTx != null` 分支新增一次无副作用累加器读取赋值;`connectorTx == null` 的 JDBC/ES 路径字节不变。
- `getUpdateCnt()` 时点:doBeforeCommit 在 commit 前、BE 回传 commitData 之后调用(与 legacy 同一生命周期位点,legacy 在此读 getUpdateCnt 成功)→ commitDataList 已填,值正确。
- follower/replay:`loadedRows` 是会话级返回值,非 editlog 持久化字段,无 replay 影响。
- 推翻历史:`P4-T05-T06-cutover-design.md:114` 的 "correctly skipped" 结论(只覆盖"能否写成功",漏"写成功后报告行数")——deviations/decisions-log 待 doc-sync 补更正(prior-session WIP,本 commit 不混入)。

## Test Plan(UT,fe-core,Rule 9 mutation 自证)
扩 `PluginDrivenInsertExecutorTest`(已有 CALLS_REAL_METHODS + Deencapsulation 构造基建):
- `doBeforeCommitBackfillsLoadedRowsFromConnectorTxnInTransactionModel`: 注 `connectorTx`(stub getUpdateCnt=42),调 doBeforeCommit,断言 `loadedRows==42`。mutation: `loadedRows=0L` → 红(expected 42 was 0)。
- `doBeforeCommitUsesHandleModelAndSkipsTxnBackfillWhenNoConnectorTxn`: handle 模型(connectorTx=null,注 writeOps recording + insertHandle),调 doBeforeCommit,断言 finishInsert 被调 + `loadedRows==0`(无 connectorTx 不回填、不 NPE)。mutation: 删 `connectorTx != null` 守卫 → 红(NPE)。
- E2E(live ODPS,user-run):事务模型 INSERT 后断言 `affected rows` == 实际写入行数(非 0)。CI 守门仅 UT。

## 成功标准
编译过 + Checkstyle=0 + 新 UT 绿且 mutation 自证 + 对抗 review 收敛。

## Review 轮次(1 轮收敛)
**verdict `sound`**(workflow `wi7zu5h45`,3 lens)。4 raw findings 经 Phase B 全未存活(confirms 0/0/0/1)。最接近的 F4(测 loadedRows 字段未测其流到 affected-rows 表面)未达 2 票——表面化是 `BaseExternalTableInsertExecutor` 既有 wiring,e2e 覆盖。production code 三 lens 一致 clean。mutation: `loadedRows=0L`→test1 红;删守卫→test2 红(NPE)。UT 6/6、CS=0。详见 `plan-doc/reviews/P4-T06d-FIX-WRITE-ROWS-review-rounds.md`。
