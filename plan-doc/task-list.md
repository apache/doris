# P4-T06d — 翻闸缺口修复 Task List

> 来源: `plan-doc/tasks/designs/P4-cutover-fix-design.md`(6 issue) + `plan-doc/reviews/P4-cutover-review-findings.md`(41 存活发现)。
> 流程(用户定): 每 issue = 独立设计文档 → 修复 → 编译+UT(无 e2e) → 对抗 review agent → review 有问题则回到设计循环(最多 5 轮)→ 记录每轮结论防矛盾 → 全过/到上限再 checkpoint。
> 每 issue 产物:
>   - 设计: `plan-doc/tasks/designs/P4-T06d-<issue>-design.md`(跨轮更新)
>   - review 轮次记录: `plan-doc/reviews/P4-T06d-<issue>-review-rounds.md`(每轮 finding+verdict+处置)
>   - summary: 写回本文件 + 设计文档尾

## ▶ RESUME (fresh session 从这里接)

- **已完成**: Phase 1 读路径两 blocker —— commit `4dba013d514`(FIX-READ-DESC)+ `0a545d319f8`(FIX-READ-SPLIT);Phase 2 DDL blocker —— commit `316ed2abed1`(FIX-DDL-ENGINE,sound 1 轮收敛)。
- **下一个**: issue 4 **FIX-DDL-REMOTE**(needs-revision **major**,fe-core)。动手前必读其 parent critic 更正(`P4-cutover-fix-design.md` :277-294):5 个既有 drop UT 会因新增 `getDbNullable` 前置而**变红需重写**(`PluginDrivenExternalCatalogDdlRoutingTest` 的 4 drop + 1 createTable cache 用例,须 stub `dbNullableResult`/`db.getTableNullable`)/ createTable/dropTable override 由 4 个 SPI_READY_TYPES **共享**(jdbc/es/trino 也继承,DROP 新增 `getTableNullable` 远端往返,end-state 仍 throw 不回归但须登记)/ "逐字节一致"声称对未开映射的 FE 控制流不成立(改了异常类型/控制流)/ CREATE 不解析远端表名(legacy parity,显式登记为 non-goal)/ UT 不能 mock converter 否则 `req.getDbName()` 断言 vacuous(须捕 `convert()` 第二参)/ depends_on=DDL-P1(本 issue 3 已落,CREATE 路径现可达)。
- **FIX-DDL-ENGINE 落地要点**(供后续防回退): `CreateTableInfo.java` 两网关加 `PluginDrivenExternalCatalog` 分支 + helper `pluginCatalogTypeToEngine`(`max_compute`→`ENGINE_MAXCOMPUTE`,**其余 SPI 类型返 null**——精炼过 parent 的 default-throw,使 jdbc/es/trino 在两网关均 legacy parity)。Batch-D 顺序依赖:本 fix 先落 PluginDriven 分支,Batch-D 仅删 legacy MC `instanceof` 分支 + `maxcompute.MaxComputeExternalCatalog` import。
- **⚠️ issue 5 FIX-PART-GATES 前置决策 OQ-6 未定**(见下「关键前置决策」)—— 到 issue 5 前问用户。
- 每 issue 流程见顶部;commit 已定每 issue 独立。foundational docs(P4-cutover-fix-design.md / review-findings 等)仍未提交(prior session 待 doc-sync,在 disk 上可读)。

## 进度

| # | issue | phase | sev | layer | 设计 | 实现 | 编译+UT | review 轮次 | 状态 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | FIX-READ-DESC  | 1 read | blocker | connector | ✅ | ✅ | ✅ | 3 轮→收敛 | ✅ DONE (commit 待下方) |
| 2 | FIX-READ-SPLIT | 1 read | blocker | connector | ✅ | ✅ | ✅ | 1 轮→收敛 | ✅ DONE (commit 待下方) |
| 3 | FIX-DDL-ENGINE | 2 DDL  | blocker | fe-core   | ✅ | ✅ | ✅ | 1 轮→收敛(sound) | ✅ DONE (commit `316ed2abed1`) |
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

- **FIX-READ-SPLIT (1 轮收敛)**: `MaxComputeScanPlanProvider:272` byte_size 分支 `.length(splitByteSize)`→`.length(-1L)`,恢复 BE BYTE_SIZE/ROW_OFFSET sentinel(否则默认 split 策略静默读错数据)。provider-level UT mutation 自证。2 reviewer CLEAN:legacy parity 精确;3 个 getLength 消费者(含 FileSplit.length→FederationBackendPolicy/FileQueryScanNode)均 benign 且更贴 legacy。⚠️登记本批外: PluginDrivenScanNode 未 override isBatchMode(分区表不走 batch split,READ-P3 同族)。
- **FIX-READ-DESC (3 轮收敛)**: 生产修复(MaxComputeConnectorMetadata.buildTableDescriptor override + ctor 透传 endpoint/quota/properties;getMetadata passthrough)R1 正确性/BE-parity + R3 回归/build 两维 CLEAN。R1 R2 抓 [medium]=fe-core 调用点 wiring 无测试守门+doc 过度声明 → R2 补 `PluginDrivenExternalTableEngineTest#testToThriftPassesRemoteNamesAndNumColsToBuildTableDescriptor`(mutation 自证)→ R3 独立验证 CLEAN。结论基线: project/table 用 remote 名(OQ-7 有意修正);deprecated TMCTable 字段不 set(同 legacy,空串非 UB);连接器测试须 `-am`。
- **FIX-DDL-ENGINE (1 轮收敛,sound)**: `CreateTableInfo.java` `paddingEngineName`/`checkEngineWithCatalog` 各加 `PluginDrivenExternalCatalog` 分支 + helper `pluginCatalogTypeToEngine`(`max_compute`→`ENGINE_MAXCOMPUTE`,**其余返 null**)。5 项 parent critic 更正全折入(import 位/删错误 SHOW-CREATE 断言/按名注册 CatalogMgr/CTAS 覆盖/Rule-9 拒测)。**精炼(Rule 7)**: helper 返 null 而非 parent 的 default-throw,使 jdbc/es/trino 在**两网关**均 legacy parity(parent 的 throw 会令 checkEngineWithCatalog 新拒 jdbc 显式 ENGINE)。UT `CreateTableInfoEngineCatalogTest` 5 例,mutation(helper `max_compute` 返 null)令 test1/2/3 红自证;CS=0。4 reviewer clean-room→verify→cross-check:6 raw→1 confirmed=nit(`correctExplicitEnginePasses` 对新分支 vacuous,但兄弟 `wrongExplicitEngineRejected` 已 pre-fix-red 守门,acceptable-as-is),code↔design 零矛盾。⚠️Batch-D 顺序: 本 fix 先落,Batch-D 仅删 legacy MC `instanceof`+import(已在设计 §Batch-D / 待写 decisions-log 登记)。
