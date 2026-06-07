# P4-T06d · FIX-PART-GATES — 对抗 review 轮次记录

> issue 5 / 6。设计: `plan-doc/tasks/designs/P4-T06d-FIX-PART-GATES-design.md`。
> 流程: clean-room 多 agent 对抗(Phase A 仅读代码 5 lens → Phase B 3 票 refute-by-default → Phase C 交叉核对设计/parent critic)。
> 改动: 新 `PluginDrivenSchemaCacheValue.java` + `PluginDrivenExternalTable.java`(initSchema 填分区列 + 4 override)+ `PartitionsTableValuedFunction.java`(analyze 3 网关)+ 2 新测试。

## Round 1 — verdict: `needs-revision`(4 findings 全 test-quality,production code CLEAN)

review 配置: 5 lens(parity / pruning-invariant / cache / tvf-redline / test-quality)→ 每 finding 3 skeptic → Phase C 交叉核对。64 agent。

**关键结论(Phase B/C 一致)**: **production code 正确**(parity / pruning 不变式 / cache cast / Batch-D 红线 / 决策① gating 均 clean)。4 条存活 real-gap **全是同一处 test-quality**:`PartitionsTableValuedFunctionPluginDrivenTest` 对 SEAM-2(表类型 allow-list)覆盖 vacuous。

| id | sev | 标题 | 处置 |
|---|---|---|---|
| F6/F13/F16 | minor | TVF 测试 stub 了 `db.getTableOrMetaException(name, types...)`,绕过真实表类型 allow-list → 删 `TableType.PLUGIN_EXTERNAL_TABLE`(:189)不会令测试变红;doc 声称的 SEAM-2 覆盖不成立 | ✅ 已修 |
| F15 | **major** | 正向用例 `testAnalyzePasses` 无断言,仅"无异常";若 table 解析返 null(`null instanceof X`=false 跳所有守卫)则 vacuous 通过,且无法捕 SEAM-3 分支删除(仅捕反转) | ✅ 已修 |
| F9 | major | `getNameToPartitionItems` 每次 query bind 走未缓存 `listPartitions` 远端往返(legacy 用二级 cache)| ✅ already-registered-non-goal(设计 §决策 CACHE-P1 已登记;Phase C 判定非新 gap) |

**修复(test-only,零源码改动)**:
1. F6/F13/F16(SEAM-2)— `invokeAnalyze` 改用 `Mockito.mock(DatabaseIf.class, CALLS_REAL_METHODS)`,仅 stub 单参 `getTableOrMetaException("t")` + `table.getType()=PLUGIN_EXTERNAL_TABLE`,使**真实** allow-list 成员检查(`DatabaseIf:170-179`)执行。`PLUGIN_EXTERNAL_TABLE.getParentType()` 返自身,故从 allow-list 删 PLUGIN → list 不含 → 抛 MetaNotFound→AnalysisException → 测试红。
2. F15(正向 vacuous)— `testAnalyzePasses` 加 `Mockito.verify(table).isPartitionedTable()`:证 table 真被解析(非 null)且 SEAM-3 守卫被触达;null 解析或 SEAM-3 分支删除均令 verify 红。

**mutation 自证(Round-1 修复)**:
- M1(删 `PartitionsTableValuedFunction:189` 的 `TableType.PLUGIN_EXTERNAL_TABLE`)→ 正+负用例**双红**(正:MetaNotFound 前置使 verify 不达;负:报错文案变 "doesn't match" 非 "not a partitioned table")。
- M2(删整个 SEAM-3 PluginDriven 守卫块)→ 双红(正:`verify(isPartitionedTable)` 因分支删除不达;负:不抛)。

**Round-1 基础 mutation(修复前已验,4 业务点)**: M-A initSchema raw→mapped(用 raw)→ initSchema 测试红;M-B getNameToPartitionItems 远端名索引(错 key)→ 该测试红;M-C SEAM-3 守卫禁用 → 负用例红;M-D supportInternalPartitionPruned+isPartitionedTable 无条件 true → 非分区用例红。证 partition override + 决策① + 远端名索引 + raw→mapped 桥接 + TVF 守卫 均 load-bearing。

**Phase C 未判为 new-gap 的存活项(防跨轮矛盾)**: F9(per-call 远端往返)= already-registered-non-goal(设计 §决策 CACHE-P1)。其余 parity/pruning/cache/redline lens 的 raw findings 经 Phase B 证伪或 Phase C 判 already-addressed,无 production 改动需求。

## Round 2 — focused recheck(TVF 测试 delta)
review 配置: 3 lens(CALLS_REAL_METHODS 链真跑? / 正向非 vacuous? / 编译·mock soundness)judge SEAM-2 + 正向 vacuity 是否解决 + 新缺陷;新 finding 3 票 refute。

**verdict: `converged`**(workflow `wwxccw2i2`)。三 lens 一致:`seam2_resolved=[true,true,true]`、`positive_test_resolved=[true,true,true]`、`confirmedNew=[]`。
逐点复核(仅读代码):
- SEAM-2 非 vacuous:`CALLS_REAL_METHODS` 下 varargs(:181)→List(:170)默认方法真跑;List 内对 `this.getTableOrMetaException("t")`(单参)的 self-call 被 mockito-inline 拦截命中 stub 返 table,随后**真实** `contains` 成员检查跑在 `table.getType()=PLUGIN_EXTERNAL_TABLE` 上。单参 "t" 经 Java 定参优先(phase 1/2)无歧义绑定 `DatabaseIf:150`,非 varargs 零参形式。`getParentType()` 返自身 → 成员判定纯依赖 production allow-list 含 PLUGIN → M1 删之即 MetaNotFound→AnalysisException→双红。
- 正向非 vacuous:`isPartitionedTable()` 全仓仅 `PartitionsTableValuedFunction:215`(SEAM-3 内)一处调用 → `verify(table).isPartitionedTable()` 捕 SEAM-3 分支删除(M2)+ null 解析(`Objects.requireNonNull(table.getType())` NPE / 前置 throw 不达 verify)。
- 负用例:mock 仅 PluginDriven,instanceof HMS/MC 假,SEAM-3(:216)是唯一可达的 "not a partitioned table" throw,文案归属无歧义。
- mock soundness:`CALLS_REAL_METHODS` 执行路径不碰未 stub 抽象方法/静态/LOG → 无伪 NPE;AnalysisException 为 nereids RuntimeException,prod/test import 一致;无未用 import。

## 收敛结论
Round 1(needs-revision,4 test-quality,production CLEAN)→ 修(test-only)→ Round 2(converged)。**2 轮收敛**。production code 自始正确(parity / pruning 不变式 / cache cast / Batch-D 红线 / 决策① 两轮一致)。
最终守门(clean source,cache 关):UT 38/38 绿(含 6 partition + 2 TVF);Checkstyle 0;BUILD SUCCESS。
mutation 总账: round-1(initSchema raw→mapped / getNameToPartitionItems 远端名 / SEAM-3 守卫 / 决策① gating)4 红 + round-2(SEAM-2 allow-list 删 / SEAM-3 块删)各双红。
