# FIX-WRITE-DISTRIBUTION (P0-2) — 对抗 review 轮次记录

> issue: NG-2 (F17, blocker) + NG-4 (F18, major) — 翻闸后 MaxCompute 写走通用 `PhysicalConnectorTableSink`,
> 丢失 legacy `PhysicalMaxComputeTableSink` 的动态分区 hash+local-sort（"writer has been closed"）+ 并行写退化为 GATHER。
> 设计: `plan-doc/tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md`
> review workflow: `plan-doc/reviews/P4-T06e-FIX-WRITE-DISTRIBUTION.workflow.js`（clean-room：Phase A 2 lens 只读码 → Phase B 3 票对抗 refute → Phase C 解禁先验交叉核对）
> 流程: 每轮记结论防跨轮矛盾；最多 5 轮。

---

## 编译 + UT + mutation（pre-review gate）

**改动 4 文件**:
1. `ConnectorCapability.java` — 新增 `SINK_REQUIRE_PARTITION_LOCAL_SORT`（连接器声明动态分区写需 hash+local-sort）。
2. `MaxComputeDorisConnector.java` — 新增 `getCapabilities()` override = `{SUPPORTS_PARALLEL_WRITE, SINK_REQUIRE_PARTITION_LOCAL_SORT}`（此前无 override → 空集 → GATHER）。
3. `PluginDrivenExternalTable.java` — 新增 `requirePartitionLocalSortOnWrite()`（镜像 `supportsParallelWrite()`，读新能力）。
4. `PhysicalConnectorTableSink.getRequirePhysicalProperties()` — 重写为 legacy 3 分支（动态分区→hash+local-sort / 非分区·全静态→RANDOM / 无能力→GATHER）。**关键修正 vs legacy**:分区列 → child output 索引按 **cols 位置**（通用 connector sink 的 child 投影到 cols 序），而非 legacy 的 full-schema 位置。

**blast radius**(grep 实证): `SUPPORTS_PARALLEL_WRITE`/`supportsParallelWrite` 仅 2 reader（table 方法本身 + 本 sink）；新能力仅 1 reader（新 table 方法）；唯一另一 `getCapabilities()` consumer = `QueryTableValueFunction` 查 `SUPPORTS_PASSTHROUGH_QUERY`（MaxCompute 不声明,不受影响）。→ 仅影响 `getRequirePhysicalProperties()` 及其 2 consumer（`RequestPropertyDeriver` / `ShuffleKeyPruner`）。

**编译**: 3 模块（fe-connector-api / fe-connector-maxcompute / fe-core）BUILD SUCCESS；fe-core + 连接器 checkstyle 干净。
**UT**: `PhysicalConnectorTableSinkTest` 4/4 过（dynamic→hash+sort / all-static→RANDOM / non-part→RANDOM / no-cap→GATHER），`Tests run: 4, Failures: 0, Errors: 0`，MVN_EXIT=0。
**mutation（Rule 9）**: 用 `cp` 备份产线文件,把 `getRequirePhysicalProperties()` 还原为 pre-fix 逻辑（`supportsParallelWrite ? SINK_RANDOM_PARTITIONED : GATHER`）→ 跑 4 测（`-Dcheckstyle.skip=true`,避开还原后未用 import 被 UnusedImports 挡在 test 前）。结果 `Tests run: 4, Failures: 1`,**唯 T1 `dynamicPartitionWriteRequiresHashAndLocalSort` 红**（`:82` `dynamic-partition write must hash-distribute by partition columns ==> expected: <true> but was: <false>` —— 还原后产出 RANDOM 而非 hash+sort）,T2/T3（RANDOM）/T4（GATHER）仍绿（pre-fix 逻辑对这三个 case 恰好同果）。证 T1 精确守门 NG-2 动态分区 hash+local-sort 修复。还原产线码,绿 4/4 复现。

---

## Round 1（2026-06-07）— verdict: **CONVERGED（converged-or-known）**

**review 机制**: clean-room workflow `ww1g95bba`（`P4-T06e-FIX-WRITE-DISTRIBUTION.workflow.js`，29 agents / 1.60M tokens / 11.5min）— Phase A 2 lens（parity / delivery）只读码对照 legacy + 下游 consumer → Phase B 每 finding 3 票对抗 refute → Phase C 解禁 design/history 交叉核对。

**裁决**: `rawFindings=8 → survived=3 → newGaps=0 / disagreements=0 / **mustFix=0**`。**3 存活全 `known-degradation` + `matchesDesignIntent=true`**。

**两 lens 终评（强验证）**:
- **parity**: "faithfully generalizes legacy 3-branch … index mapping is **CORRECT and self-consistent** … cols-index 与 cutover 的 cols-order child output 一致 … no wrong slot, no off-by-one, no IndexOutOfBounds（BindSink 强制 `cols.size()==child.getOutput().size()`）… `DistributionSpec/MustLocalSortOrderSpec/OrderKey` **byte-for-byte identical to legacy** … 三 case（动态/全静态/非分区）均达 legacy parity"。
- **delivery**: "correct and not a regression for the shapes it targets … **blast radius tightly contained**（仅 MaxCompute 声明两能力；两能力常量除新 table 方法+sink 外无 reader）… residual risk = pre-existing 静态分区 bind gap（NG-3，本 change surface 外）"。

**3 存活 finding（全 known-degradation，无须改本 commit）**:
| id | sev | 标题 | Phase C 处置 |
|---|---|---|---|
| F2 | major | `bindConnectorTableSink` 不剔静态分区列 → 阻断 all-static 写（本 change surface 外） | **NG-3/P0-3 耦合**,本设计已登记。归 P0-3（FIX-BIND-STATIC-PARTITION）。本 commit 无改。 |
| F4 | major | all-static 分支因 bind 不剔静态列而不可达 | 同上。Phase C **更正过度声明**:all-static 无列名形态今日在 bind `:941` 计数不符**抛错**(dormant),**不会**静默误判为 dynamic（child output 列数 < bindColumns,Consequence B 不可能发生）。 |
| F5 | major(test) | T2 手搭 cols 真 bind 路径不产出 | known-degradation。**本轮顺手澄清 T2 javadoc**:该 all-static 输入今日经 explicit-column-list 形态(`PARTITION(p='x') (data) SELECT data` → colNames=[data])可达,P0-3 后经 no-column-list 形态可达。 |

**Phase B 已退（未存活）**: ShuffleKeyPruner connector 分支缺 `enableStrictConsistencyDml` 短路（一审 regression=yes / 一审 no）→ 3 票多数 refute（确认仅 non-strict 下"少剪 = 更保守 = 无正确性损"，**默认 strict 下与 legacy MC 同果**）；RequestPropertyDeriver GATHER 短路（MC 不可达）；multi-partition order-key 序（cols 序 vs full-schema 序,grouping 等价）；co-declaration 隐性依赖（仅对假想连接器）。**均证我设计已述的 known/intended,Phase B 即退场。**

**本轮收尾改动（非 must-fix,clarity-only,不改产线逻辑/不改测试逻辑→无须再 review）**:
1. T2 javadoc 澄清 all-static 输入可达性（explicit-col-list 今日可达 / no-col-list P0-3 后可达 / 今日 no-col-list 抛错故 dormant 非误判）。
2. 设计文档 P0-3 耦合段加 forward-pointer:P0-3 落后加 all-static no-col-list 集成回归;Batch-D 删 legacy 须待本 fix + P0-3 双落。

**结论：P0-2 代码 CONVERGED（1 轮，0 must-fix）**。3 存活均 known-degradation/已登记。
**scope reminder（非缺陷,设计已述）**: 本 fix 只定 FE planner 写分发;live 真值闸 = 真实 ODPS 跨多动态分区 INSERT 无 "writer has been closed" + 非分区并行吞吐（CI 跳,须与 P0-3 一并 live 验）。
