# P4-T06d — 翻闸缺口修复 Task List

> 来源: `plan-doc/tasks/designs/P4-cutover-fix-design.md`(6 issue) + `plan-doc/reviews/P4-cutover-review-findings.md`(41 存活发现)。
> 流程(用户定): 每 issue = 独立设计文档 → 修复 → 编译+UT(无 e2e) → 对抗 review agent → review 有问题则回到设计循环(最多 5 轮)→ 记录每轮结论防矛盾 → 全过/到上限再 checkpoint。
> 每 issue 产物:
>   - 设计: `plan-doc/tasks/designs/P4-T06d-<issue>-design.md`(跨轮更新)
>   - review 轮次记录: `plan-doc/reviews/P4-T06d-<issue>-review-rounds.md`(每轮 finding+verdict+处置)
>   - summary: 写回本文件 + 设计文档尾

## ▶ RESUME (fresh session 从这里接)

- **已完成**: Phase 1 读路径两 blocker —— commit `4dba013d514`(FIX-READ-DESC)+ `0a545d319f8`(FIX-READ-SPLIT);Phase 2 DDL —— `0d95d837924`(FIX-DDL-ENGINE,1 轮)+ `6c68e502662`(FIX-DDL-REMOTE,2 轮);Phase 3 分区 —— FIX-PART-GATES(2 轮收敛,commit 待下方填)。
- **下一个**: issue 6 **FIX-WRITE-ROWS**(major,fe-core,phase 4 写回正确性,**最后一个**)。设计见 `P4-cutover-fix-design.md` §FIX-WRITE-ROWS(:394-420,verdict 未见 critic 块——动手前确认):`PluginDrivenInsertExecutor.doBeforeCommit()` 在事务模型分支(`connectorTx != null`)补一行 `loadedRows = connectorTx.getUpdateCnt()`,回填翻闸丢失的 affected-rows(镜像 legacy `MCInsertExecutor`)。纯 fe-core 一处赋值,`getUpdateCnt` 全链路已就绪。推荐取法 (a) `connectorTx.getUpdateCnt()`(现有字段、无 manager lookup)。推翻历史误判 `P4-T05-T06-cutover-design.md:114`("doBeforeCommit 跳过是正确的")。
- **FIX-PART-GATES 落地要点**(供防回退): 新 `PluginDrivenSchemaCacheValue`(存 partition 列 + raw 远端名)；`PluginDrivenExternalTable` initSchema 填分区列(raw→mapped 经 `fromRemoteColumnName` 桥接)+ 4 override(isPartitionedTable/getPartitionColumns/supportInternalPartitionPruned/getNameToPartitionItems);`getNameToPartitionItems` 单次 `listPartitions` + 复用 `TablePartitionValues.addPartitions`(与 legacy `MaxComputeExternalMetaCache.loadPartitionValues` 同构,ListPartitionItem/isHive=false)再 invert。**决策①**: `supportInternalPartitionPruned()` keyed on `!getPartitionColumns().isEmpty()`(非 legacy MC 无条件 true)——因 override 被 jdbc/es/trino 共享,无条件 true 会改非分区连接器行为。`PartitionsTableValuedFunction` 3 网关只增不删(🔴Batch-D 红线 :173 守住)。`PartitionValuesTableValuedFunction` 不动(仅 HMS,非回归)。per-call 远端 listPartitions 无二级 cache = CACHE-P1 既定方向(登记降级)。
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
| 5 | FIX-PART-GATES | 3 part | major   | fe-core   | ✅ | ✅ | ✅ | 2 轮→收敛 | ✅ DONE (commit 待下方) |
| 6 | FIX-WRITE-ROWS | 4 write| major   | fe-core   | ⬜ | ⬜ | ⬜ | — | ⬜ TODO |

图例: ⬜ 未开始 / 🔄 进行中 / ✅ 完成

## 关键前置决策(动手前)

- **OQ-6 (FIX-PART-GATES, issue 5)**: ✅ **已定(2026-06-07,用户)= (b) 新增 `PluginDrivenSchemaCacheValue` 子类**持久化 partition_columns(`initSchema()` 填充 + 解析 raw→mapped 列名),mirror legacy 缓存、避热路径远端往返。**另**:scope ✅ **= 一并恢复分区裁剪**(`supportInternalPartitionPruned()=true` + `getNameToPartitionItems()` 经 `listPartitions`/`listPartitionValues` 构 `PartitionItem`)——issue 5 范围扩大,非最小修。
- **commit 时机**: ✅ 已定(2026-06-07)—— **每 issue 过对抗 review 后即独立 commit**(本地 catalog-spi-05,不 push)。commit message 用 `[P4-T06d] ...`。

## 跨 issue 红线(全程守)

- 🔴 `PartitionsTableValuedFunction.java` 的 `MaxComputeExternalCatalog` 分支(catalog allow-list ~:173)(Batch-D 红线;历史"T06c 已加 PluginDriven 分支"为假)。**FIX-PART-GATES 已新增 PluginDriven 分支**(首次使该前提成真),Batch-D 删 MaxCompute 分支须**排在 FIX-PART-GATES commit 之后**。⚠️**待 doc-sync**: `P4-batchD-maxcompute-removal-design.md:70-77,:102` amendment 措辞"T06c adds"应改"FIX-PART-GATES adds" + decisions-log D-028 登记此 ordering(prior-session WIP 文件,本 issue 不混入 commit,留 doc-sync 处理)。
- 每 issue 独立 commit;改 fe-core 带 `-pl :fe-core -am`,改连接器带 `-pl :fe-connector-maxcompute`;读真实 BUILD/MVN_EXIT/CS_EXIT(勿信后台 task 通知 exit code)。
- 测试须真能 fail(Rule 9):验"业务逻辑回退能否让测试变红"。

## review 轮次累计结论(防跨轮矛盾,精简索引;详见各 issue round 文件)

- **FIX-READ-SPLIT (1 轮收敛)**: `MaxComputeScanPlanProvider:272` byte_size 分支 `.length(splitByteSize)`→`.length(-1L)`,恢复 BE BYTE_SIZE/ROW_OFFSET sentinel(否则默认 split 策略静默读错数据)。provider-level UT mutation 自证。2 reviewer CLEAN:legacy parity 精确;3 个 getLength 消费者(含 FileSplit.length→FederationBackendPolicy/FileQueryScanNode)均 benign 且更贴 legacy。⚠️登记本批外: PluginDrivenScanNode 未 override isBatchMode(分区表不走 batch split,READ-P3 同族)。
- **FIX-READ-DESC (3 轮收敛)**: 生产修复(MaxComputeConnectorMetadata.buildTableDescriptor override + ctor 透传 endpoint/quota/properties;getMetadata passthrough)R1 正确性/BE-parity + R3 回归/build 两维 CLEAN。R1 R2 抓 [medium]=fe-core 调用点 wiring 无测试守门+doc 过度声明 → R2 补 `PluginDrivenExternalTableEngineTest#testToThriftPassesRemoteNamesAndNumColsToBuildTableDescriptor`(mutation 自证)→ R3 独立验证 CLEAN。结论基线: project/table 用 remote 名(OQ-7 有意修正);deprecated TMCTable 字段不 set(同 legacy,空串非 UB);连接器测试须 `-am`。
- **FIX-PART-GATES (2 轮收敛)**: 新 `PluginDrivenSchemaCacheValue` + `PluginDrivenExternalTable` 4 分区 override + initSchema 填分区列 + `PartitionsTableValuedFunction` 3 网关。**决策①**(Rule 7): `supportInternalPartitionPruned()` = `!getPartitionColumns().isEmpty()`(非 legacy MC 无条件 true,因 override 被 4 SPI 类型共享)。**决策②**: TVF SEAM-3 守卫 keyed on `isPartitionedTable()`(分区列空)非 legacy 的分区实例空——空分区表返 0 行非抛,与 SHOW PARTITIONS 一致(登记 minor 偏差)。`getNameToPartitionItems` per-call `listPartitions`(无二级 cache）= CACHE-P1 既定。**Round 1**(needs-revision,4 findings 全 test-quality,production CLEAN): TVF 测试 SEAM-2 vacuous(stub 了 allow-list 强制方法)+ 正向用例 null 解析可 vacuous 通过 → 修 test-only(`DatabaseIf` 用 `CALLS_REAL_METHODS` 跑真 allow-list + 正向加 `verify(isPartitionedTable)`)。**Round 2** converged。mutation: round-1 4 红 + round-2 双红×2。UT 38/38、CS=0。⚠️登记: jdbc/es/trino 共享 override(决策① 规避其行为变更);`PluginDrivenSchemaCacheValue` 无条件 cast 安全(runtime cache、FE 重启重建)。详见 `plan-doc/reviews/P4-T06d-FIX-PART-GATES-review-rounds.md`。
- **FIX-DDL-REMOTE (2 轮收敛)**: `PluginDrivenExternalCatalog.java` createTable/dropTable 两 override 加 FE 端 local→remote 名解析,mirror legacy `MaxComputeMetadataOps` + base `ExternalCatalog.dropTable`。**Rule-7 决策**: dropTable db==null **无条件抛**(精确 mirror base :1119-1129,推翻 parent 设计文本的 "ifExists-gate")。CREATE 不解析远端表名(legacy parity,non-goal);editlog/cache 用本地名(follower-replay)。**Round 1**(needs-revision,3 findings 全 test-quality,production CLEAN): 测试只锁 REMOTE 名半边,未锁 editlog/`getDbForReplay` 的 LOCAL 名半边 → 修(test-only):`ArgumentCaptor` 断言 `persist.CreateTableInfo`/`DropInfo` 携本地名 + `lastGetDbForReplayArg` 断言 + drop happy-path 分离 resolution/replay db。**Round 2** converged。mutation 总账: round-1(remote 解析 + db-null 无条件抛)5 红 + round-2(editlog/getDbForReplay LOCAL 名)2 红;UT 17/17、CS=0。⚠️登记(非本批修): createTable/dropTable 由 4 SPI_READY_TYPES 共享,jdbc/es/trino DROP 新增 `getTableNullable` 远端往返但 end-state 仍 throw 不回归;"逐字节一致" 仅对 SDK 名成立、未开映射的 FE 控制流仍变(异常层级/往返)。详见 `plan-doc/reviews/P4-T06d-FIX-DDL-REMOTE-review-rounds.md`。
- **FIX-DDL-ENGINE (1 轮收敛,sound)**: `CreateTableInfo.java` `paddingEngineName`/`checkEngineWithCatalog` 各加 `PluginDrivenExternalCatalog` 分支 + helper `pluginCatalogTypeToEngine`(`max_compute`→`ENGINE_MAXCOMPUTE`,**其余返 null**)。5 项 parent critic 更正全折入(import 位/删错误 SHOW-CREATE 断言/按名注册 CatalogMgr/CTAS 覆盖/Rule-9 拒测)。**精炼(Rule 7)**: helper 返 null 而非 parent 的 default-throw,使 jdbc/es/trino 在**两网关**均 legacy parity(parent 的 throw 会令 checkEngineWithCatalog 新拒 jdbc 显式 ENGINE)。UT `CreateTableInfoEngineCatalogTest` 5 例,mutation(helper `max_compute` 返 null)令 test1/2/3 红自证;CS=0。4 reviewer clean-room→verify→cross-check:6 raw→1 confirmed=nit(`correctExplicitEnginePasses` 对新分支 vacuous,但兄弟 `wrongExplicitEngineRejected` 已 pre-fix-red 守门,acceptable-as-is),code↔design 零矛盾。⚠️Batch-D 顺序: 本 fix 先落,Batch-D 仅删 legacy MC `instanceof`+import(已在设计 §Batch-D / 待写 decisions-log 登记)。
