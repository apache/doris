# FIX-DDL-ENGINE — 对抗 review 轮次记录

> 设计: `plan-doc/tasks/designs/P4-T06d-FIX-DDL-ENGINE-design.md`。修复: `CreateTableInfo.java` —
> `paddingEngineName` / `checkEngineWithCatalog` 各加 `PluginDrivenExternalCatalog` 分支 + 新
> `private static pluginCatalogTypeToEngine`(`"max_compute"`→`ENGINE_MAXCOMPUTE`,其余 SPI 类型返
> `null`)+ 1 import。新 UT `CreateTableInfoEngineCatalogTest`(5 例)。
>
> 流程: clean-room(4 reviewer 先独立判 code、不读 plan-doc)→ 逐 finding 对抗 verify → cross-check 交叉核对
> 设计结论 + parent critic(防开发先验 / reviewer 过度)。Workflow `wf_e8887334-53a`。

## Round 1 (4 clean-room reviewers → verify → cross-check)

修复期已折入 parent 设计 `needs-revision` critic 的 5 项更正:① import 放 `:49 InternalCatalog` 后、
`hive.*` 前;② 删 parent 错误 e2e 断言(SHOW CREATE TABLE 渲 `MAX_COMPUTE_EXTERNAL_TABLE` 非
`ENGINE=maxcompute`);③ UT 经 mock CatalogMgr 按名注册 catalog(两网关按名 re-fetch);④ 补 CTAS
(`validateCreateTableAsSelect`);⑤ 补 Rule-9"错误显式 ENGINE 被拒"测试。另 + 1 项设计精炼(Rule 7):
helper 对未映射 SPI 类型**返 null 而非 throw**,使 jdbc/es/trino 在两网关均与 legacy 逐字一致(parent 的
default-throw 会令 checkEngineWithCatalog 新拒 jdbc 显式 ENGINE)。

**4 reviewer lens(code-first,clean-room)**:correctness-parity / regression-blast / test-quality-rule9 /
build-style-redline。6 raw findings → 逐条对抗 verify → **仅 1 条 confirmed real**,其余 5 条经独立复核证伪
(invalid / not real)。

**唯一存活 finding(nit,disposition=acceptable-as-is)**
- `correctExplicitEnginePassesForPluginDriven`(test:164-170)作为**回归探测器对新分支是 vacuous**:engine=
  `maxcompute` 时,pre-fix(无 PluginDriven 分支→fall-through 不抛)与 post-fix(`pluginEngine="maxcompute"` →
  守卫 `!= null && !equals` 短路 false→不抛)**两路都不抛**,故 `assertDoesNotThrow` 移除 fix 也不会红。
- **判定 acceptable-as-is(非覆盖缺口)**:新 `checkEngineWithCatalog` 分支的真正回归守门是兄弟用例
  `wrongExplicitEngineRejectedForPluginDriven`(test:151-161,ENGINE=hive→assertThrows),verify 已确认其
  **pre-fix 必红**(无分支→不抛→assertThrows 失败),与本地 mutation 自证一致。该正向用例仍有文档价值,且能抓
  "条件写反"(若误写成 `&& equals` 会误抛)的 mutation,保留。reviewer 自身措辞为 "consider/acknowledge",
  非要求改动;其建议的 "state assertion" 不可行(成功路径 checkEngineWithCatalog 无可观察副作用)。

**cross-check:6 项设计更正全部"已在 code 落地"核实通过**(import 位置 / 错误 e2e 断言已删 / 按名注册 /
CTAS 覆盖 / Rule-9 拒测 / null-helper 两网关 parity);**code 与设计零矛盾**;无 blocker/major;无开发先验偏、无
reviewer 过度。

## 收敛结论

Round 1 → **verdict: `sound`,1 轮收敛,可 commit**。唯一 nit acceptable-as-is,不改 code/设计。

**本地守门复证**(非后台 task echo):
- UT: `mvn -pl :fe-core -am test -Dtest=CreateTableInfoEngineCatalogTest` → Tests run: 5, Failures: 0,
  Errors: 0;BUILD SUCCESS(MVN_EXIT=0)。
- Rule-9 mutation: helper `max_compute` 返 `null` → test 1(ERROR "does not support create table")/
  test 2(`expected:<maxcompute> but was:<null>`)/ test 3("nothing was thrown")三红;test 4/5 不受此
  mutation 影响(各守其它 mutation)。复原后 5/5 绿。
- Checkstyle: `mvn -pl :fe-core checkstyle:check` → 0 violations(CS_EXIT=0),import-gate clean。

**跨轮**: 单轮,无矛盾。

**红线复核**: 未触 `PartitionsTableValuedFunction.java:173` MaxCompute 分支(build-style-redline lens +
cross-check 均确认);legacy `MaxComputeExternalCatalog` import 仍在(Batch-D 删除前的 keep-set 顺序依赖,
已在设计 §Batch-D 登记)。
