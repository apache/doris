# P4-T06d — 翻闸缺口修复 Task List

> 来源: `plan-doc/tasks/designs/P4-cutover-fix-design.md`(6 issue) + `plan-doc/reviews/P4-cutover-review-findings.md`(41 存活发现)。
> 流程(用户定): 每 issue = 独立设计文档 → 修复 → 编译+UT(无 e2e) → 对抗 review agent → review 有问题则回到设计循环(最多 5 轮)→ 记录每轮结论防矛盾 → 全过/到上限再 checkpoint。
> 每 issue 产物:
>   - 设计: `plan-doc/tasks/designs/P4-T06d-<issue>-design.md`(跨轮更新)
>   - review 轮次记录: `plan-doc/reviews/P4-T06d-<issue>-review-rounds.md`(每轮 finding+verdict+处置)
>   - summary: 写回本文件 + 设计文档尾

## ▶ RESUME (fresh session 从这里接)

- **已完成**: Phase 1 读路径两 blocker —— commit `4dba013d514`(FIX-READ-DESC)+ `0a545d319f8`(FIX-READ-SPLIT);Phase 2 DDL —— commit `0d95d837924`(FIX-DDL-ENGINE,sound 1 轮)+ `6c68e502662`(FIX-DDL-REMOTE,2 轮收敛)。
- **下一个**: issue 5 **FIX-PART-GATES**(needs-revision **major**,fe-core,phase 3 分区可见)。⚠️**动手前必决 OQ-6**(见下「关键前置决策」:`partition_columns` prop 来源 = 每次 connector `getTableSchema()` 重取 vs 新增 `PluginDrivenSchemaCacheValue` 子类)——**到 issue 5 前问用户**。其 parent critic(`P4-cutover-fix-design.md` :371-389)核心更正:① PROP-SOURCING load-bearing 未决(base `SchemaCacheValue` 无 properties 字段、无 PluginDriven 子类、`ConnectorColumnConverter` 丢分区标记 → "从 getFullSchema() 过滤" 不可行,必须二选一);② `isPartitionedTable()` 翻 true 但不 override `supportInternalPartitionPruned/getNameToPartitionItems` 致状态不一致(裁剪丢失,登记已知降级);③ 列名映射:`partition_columns` prop 存 raw 远端名、schema 经 `fromRemoteColumnName` 映射(MC 默认不改名故今可用,但通用性不成立);④ 🔴 **Batch-D 红线**:`PartitionsTableValuedFunction:173` MaxCompute 分支勿删(本 fix 先 **新增** PluginDriven 分支,Batch-D 再删 legacy);⑤ `partition_values()` TVF 同源缺口但仅 HMS 支持(MC 本就 throw,非回归,显式登记为区分而非漏)。
- **FIX-DDL-REMOTE 落地要点**(供后续防回退): `PluginDrivenExternalCatalog.java` createTable/dropTable 两 override 加 FE 端 local→remote 名解析(createTable `db.getRemoteName()` 喂 converter 第二参、表名不解析=legacy parity;dropTable 精确 mirror base `ExternalCatalog.dropTable:1119-1129` —— **db==null 无条件抛**[非 ifExists-gate,推翻 parent 设计文本]、table==null/handle-absent 才 ifExists)。editlog/cache 仍用本地名(follower-replay)。源码仅 fe-core 2 override;无 import 新增(同包)。Batch-D 协同:勿据 T06c §5:187 已证伪的"连接器内部解析 remote"假定行事。
- **FIX-DDL-ENGINE 落地要点**(供后续防回退): `CreateTableInfo.java` 两网关加 `PluginDrivenExternalCatalog` 分支 + helper `pluginCatalogTypeToEngine`(`max_compute`→`ENGINE_MAXCOMPUTE`,**其余 SPI 类型返 null**——精炼过 parent 的 default-throw,使 jdbc/es/trino 在两网关均 legacy parity)。Batch-D 顺序依赖:本 fix 先落 PluginDriven 分支,Batch-D 仅删 legacy MC `instanceof` 分支 + `maxcompute.MaxComputeExternalCatalog` import。
- **⚠️ issue 5 FIX-PART-GATES 前置决策 OQ-6 未定**(见下「关键前置决策」)—— 到 issue 5 前问用户。
- 每 issue 流程见顶部;commit 已定每 issue 独立。foundational docs(P4-cutover-fix-design.md / review-findings 等)仍未提交(prior session 待 doc-sync,在 disk 上可读)。

## 进度

| # | issue | phase | sev | layer | 设计 | 实现 | 编译+UT | review 轮次 | 状态 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | FIX-READ-DESC  | 1 read | blocker | connector | ✅ | ✅ | ✅ | 3 轮→收敛 | ✅ DONE (commit 待下方) |
| 2 | FIX-READ-SPLIT | 1 read | blocker | connector | ✅ | ✅ | ✅ | 1 轮→收敛 | ✅ DONE (commit 待下方) |
| 3 | FIX-DDL-ENGINE | 2 DDL  | blocker | fe-core   | ✅ | ✅ | ✅ | 1 轮→收敛(sound) | ✅ DONE (commit `0d95d837924`) |
| 4 | FIX-DDL-REMOTE | 2 DDL  | major   | fe-core   | ✅ | ✅ | ✅ | 2 轮→收敛 | ✅ DONE (commit `6c68e502662`) |
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
- **FIX-DDL-REMOTE (2 轮收敛)**: `PluginDrivenExternalCatalog.java` createTable/dropTable 两 override 加 FE 端 local→remote 名解析,mirror legacy `MaxComputeMetadataOps` + base `ExternalCatalog.dropTable`。**Rule-7 决策**: dropTable db==null **无条件抛**(精确 mirror base :1119-1129,推翻 parent 设计文本的 "ifExists-gate")。CREATE 不解析远端表名(legacy parity,non-goal);editlog/cache 用本地名(follower-replay)。**Round 1**(needs-revision,3 findings 全 test-quality,production CLEAN): 测试只锁 REMOTE 名半边,未锁 editlog/`getDbForReplay` 的 LOCAL 名半边 → 修(test-only):`ArgumentCaptor` 断言 `persist.CreateTableInfo`/`DropInfo` 携本地名 + `lastGetDbForReplayArg` 断言 + drop happy-path 分离 resolution/replay db。**Round 2** converged。mutation 总账: round-1(remote 解析 + db-null 无条件抛)5 红 + round-2(editlog/getDbForReplay LOCAL 名)2 红;UT 17/17、CS=0。⚠️登记(非本批修): createTable/dropTable 由 4 SPI_READY_TYPES 共享,jdbc/es/trino DROP 新增 `getTableNullable` 远端往返但 end-state 仍 throw 不回归;"逐字节一致" 仅对 SDK 名成立、未开映射的 FE 控制流仍变(异常层级/往返)。详见 `plan-doc/reviews/P4-T06d-FIX-DDL-REMOTE-review-rounds.md`。
- **FIX-DDL-ENGINE (1 轮收敛,sound)**: `CreateTableInfo.java` `paddingEngineName`/`checkEngineWithCatalog` 各加 `PluginDrivenExternalCatalog` 分支 + helper `pluginCatalogTypeToEngine`(`max_compute`→`ENGINE_MAXCOMPUTE`,**其余返 null**)。5 项 parent critic 更正全折入(import 位/删错误 SHOW-CREATE 断言/按名注册 CatalogMgr/CTAS 覆盖/Rule-9 拒测)。**精炼(Rule 7)**: helper 返 null 而非 parent 的 default-throw,使 jdbc/es/trino 在**两网关**均 legacy parity(parent 的 throw 会令 checkEngineWithCatalog 新拒 jdbc 显式 ENGINE)。UT `CreateTableInfoEngineCatalogTest` 5 例,mutation(helper `max_compute` 返 null)令 test1/2/3 红自证;CS=0。4 reviewer clean-room→verify→cross-check:6 raw→1 confirmed=nit(`correctExplicitEnginePasses` 对新分支 vacuous,但兄弟 `wrongExplicitEngineRejected` 已 pre-fix-red 守门,acceptable-as-is),code↔design 零矛盾。⚠️Batch-D 顺序: 本 fix 先落,Batch-D 仅删 legacy MC `instanceof`+import(已在设计 §Batch-D / 待写 decisions-log 登记)。
