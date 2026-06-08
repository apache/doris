# Batch-D 红线扩充 — 对抗复审查出的 gap 修复 Task List

> 来源：`plan-doc/HANDOFF.md` 横切「Batch-D 红线扩充」。2026-06-08 跑 clean-room 对抗 workflow（`wbw4xszrg`，117 agent，13 carrier-unit × inventory→adversarial-verify + 3 critic）复查 Batch-D 设计「zero survivor」声明（行为逻辑副本层面，非仅实例化链）。
> 全量结果 JSON：`/tmp/claude-1000/-mnt-disk1-yy-git-wt-catalog-spi/.../tasks/wbw4xszrg.output`（gaps/critics 摘录见 `/tmp/wf_gaps.txt` `/tmp/wf_critics.txt`，若清理见 git 历史本表）。
> 结论：11 gap + 2 critic-only finding。**Critic-2 独立复核 13 条 per-fix 等价物全部 present+wired**（前修无回退）。这些是 per-fix review 漏掉的**新**发现。
> 流程（沿用 P4-rereview 既有方法论）：每 issue = 独立设计文档（`tasks/designs/P4-T06e-<FIX>-design.md`）→ 设计验证 workflow（clean-room 对抗）→ 实现 → 守门（编译+UT+checkstyle+import-gate+mutation）→ impl-review workflow 收敛 → 独立 commit（`[P4-T06e]`）+ hash 回填 + 本表更新。

## 用户定夺（2026-06-08）

- **G8 = Fix now（repro-test 先行）**——确诊 live 静默丢行，最高优先。
- **其余 = Fix Tier 1+2，Tier 3 接受+登记 deviation**。
- **G0 = design-verify Skip + 死代码 Keep/defer Batch-D**（已 DONE `0d983a1c056`）。
- **下一新 session = 批量修复 G6 + G5 + G7**（三者独立、可并行设计；各仍独立 design doc + 独立 commit + 各自守门）。

## 进度

| # | issue (gap) | sev | 决策 | 设计 | 实现 | 守门 | review | 状态 |
|---|---|---|---|---|---|---|---|---|
| G8 | **FIX-NONPART-PRUNE-DATALOSS** (GAP8) | **blocker/correctness** | Fix（repro 先行） | ✅ | ✅ | ✅ | ✅ 设计验证`wijd3qgk0`4lens design-sound + impl-review`wza2khdb2`2lens approve | ✅ DONE (`e1760d38d86`) |
| G0 | **FIX-DATETIME-PUSHDOWN-FORMAT** (GAP0/1) | major(correctness/perf) | Fix | ✅ | ✅ | ✅ | ✅ 设计验证 skip(用户定)+impl-review 单Agent CHANGES-REQUIRED→F1(CST session 炸整查询)折入 | ✅ DONE (`0d983a1c056`) |
| G6 | **FIX-CREATE-CATALOG-VALIDATION** (GAP6) | major | Fix | ✅ | ✅ | ✅ | ✅ 单Agent APPROVE-WITH-NITS(0 must-fix) | ✅ DONE (`1fc00178484`) |
| G5 | **FIX-AGG-COLUMN-REJECT** (GAP5) | minor | Fix（用户定 Option B: SPI 字段） | ✅ | ✅ | ✅ | ✅ 单Agent APPROVE(0 must-fix) | ✅ DONE (`c5e8ba6d9e2`) |
| G7 | **FIX-VOID-TYPE-MAPPING** (GAP7) | minor | Fix | ✅ | ✅ | ✅ | ✅ 单Agent APPROVE(0 must-fix) | ✅ DONE (`49113dc7860`) |
| G2 | **FIX-PREDICATE-COLGUARD** (GAP2) | minor | Fix | ✅ | ✅ | ✅ | ✅ 单Agent APPROVE(0 must-fix) | ✅ DONE (`fefbbad391d`) |
| GC1 | **FIX-BLOCKID-CAP-CONFIG** (CRITICGAP1) | minor | Fix（用户定 Option A: 全局 Config 透传） | ✅ | ✅ | ✅ | ✅ 单Agent APPROVE-WITH-NITS(0 must-fix) | ✅ DONE (`95575a4954d`) |
| T3 | **Tier-3 DV batch** (GAP3/4/9/10) | minor | 接受+DV | ⬜ | n/a | n/a | n/a | ⬜ |
| DOC | **Batch-D redline 扩充**（design §1/§2 must-land-before-delete + scan-node 注补 LIMIT-split 第 3 副本） | — | — | ⬜ | n/a | n/a | n/a | ⬜ |

图例：⬜ 未开始 / 🔄 进行中 / ✅ 完成

## gap 速查（详见各 design + `/tmp/wf_gaps.txt`）

- **G8 GAP8**：非分区 MC 表 + WHERE → 静默 0 行。`supportInternalPartitionPruned()`=`!partCols.isEmpty()`(非分区=false) → `PruneFileScanPartition` else 支覆写 `isPruned=true,空` → `PluginDrivenScanNode.getSplits` 短路 0 split。根因=FIX-PART-GATES 坏 override（`35cfa50f988`）+ P1-4 短路（`072cd545c54`）叠加。已 5 处核码确认。单测钉错不变式、live-e2e 仅测分区表。见 auto-memory [[catalog-spi-nonpartitioned-prune-dataloss]]。
- **G0 GAP0/1** ✅ DONE (`0d983a1c056`)：DATETIME/TIMESTAMP/TIMESTAMP_NTZ 谓词下推。新路 `MaxComputePredicateConverter` 用 `LocalDateTime.toString()`（'T' 分隔）喂 `.SSS/.SSSSSS` formatter → 非 UTC 解析抛 → 整 conjunct 树降为 NO_PREDICATE（谓词永不下推=性能回归）；UTC 路推 malformed 字面量；且 source TZ 用 project-region 非 session TZ（format 修后会丢行）。legacy 用 `getStringValue(DatetimeV2Type(3|6))`（空格分隔定长）正确下推。**修**=直接 format `LocalDateTime`（逐字镜像 legacy）+ source TZ 改 `ConnectorSession.getTimeZone()`（TZ id 字符串惰性 `ZoneId.of`，使 Doris 逐字存的 `CST` 等 ZoneId 不认 id 降级 NO_PREDICATE 而非炸查询——impl-review F1 折入）。**Batch-D 死代码清理项**：`MCConnectorEndpoint.resolveProjectTimeZone` + `REGION_ZONE_MAP`（~60 行）翻闸后零调用方。
- **G6 GAP6** ✅ DONE (`1fc00178484`)：CREATE CATALOG 属性校验缺失——`MaxComputeConnectorProvider` 未 override `validateProperties`（继承 no-op）；required PROJECT/ENDPOINT、split_byte_size floor、account_format、timeout>0、`checkAuthProperties`（定义但零调用）全不在 CREATE 时校验，退化为 use-time 晚失败/静默接受非法值。**修**=override `validateProperties` 逐字镜像 legacy `checkProperties:388-457` 六校验、抛 IllegalArgumentException（→DdlException）、wire dead `checkAuthProperties`（异常类型对齐 IllegalArgumentException）。UT 19/19 + mutation 3 组向红。
- **G5 GAP5** ✅ DONE (`c5e8ba6d9e2`)：`CREATE TABLE (c INT SUM)` 聚合列拒绝丢失。ConnectorColumn 无 aggType 载体 → converter 丢 → validateColumns 不查 → nereids 非-OLAP 不拒（**证伪 P2-8「非-OLAP 路径已覆盖」**）。静默建普通列。**修**=用户定 Option B：加 SPI additive 字段 `isAggregated`（镜像 P2-8 isAutoInc）+ converter passthrough（=`Column.isAggregated()`）+ `MaxComputeConnectorMetadata.validateColumns` 加 `if(col.isAggregated())throw`（逐字镜像 legacy `:426-429`，紧邻 isAutoInc 检查）。UT 4/4/11 + mutation 3 组向红；over-rejection 已核（isOlap-gated）。
- **G7 GAP7** ✅ DONE (`49113dc7860`)：ODPS `VOID` → 新路映 `UNSUPPORTED`（legacy=`Type.NULL`）；`ConnectorColumnConverter` 无 "NULL" case + `createType("NULL")` 抛被吞。次生：未知 OdpsType legacy 硬抛、新路静默 UNSUPPORTED。**修**=连接器局部：① `MCTypeMapping` VOID token "NULL"→"NULL_TYPE"（fe-core convertScalarType default 即产 Type.NULL）；② switch default `return UNSUPPORTED`→`throw`（仅 OdpsType.UNKNOWN sentinel 落 default，legacy 亦 throw=parity，真实表零回归）。UT 5/5 + mutation 2 组向红。**out-of-scope（留待 ES 翻闸）**：ES `EsTypeMapping:191` 同款 emit "NULL" latent token bug（其 test 还钉了 buggy token），未修。
- **G2 GAP2**：列不存在守卫反转——legacy 谓词引用未知列时抛→丢谓词；新路 `formatLiteralValue` odpsType==null 静默引号化→**下推非法谓词**。实务多半不可达（bound 谓词只引真列），低。
- **GC1 CRITICGAP1**：写 block-id 上限硬编 `20000`，无视 `Config.max_compute_write_max_block_count`（legacy 可调）→ 调优部署静默回归。

## Tier-3 接受项（登记 deviation，不修）

- **GAP3** CREATE DB 非-IFNE：`ERR_DB_CREATE_EXISTS`(1007/HY000，本地预抛) → 透传 ODPS DdlException（P2-6 已注 pre-existing）。
- **GAP4** DROP TABLE 非-IF-EXISTS+远端缺：`ERR_UNKNOWN_TABLE`(1109/42S02) → 通用 DdlException（本地名）。
- **GAP9** SHOW PARTITIONS `LIMIT`：legacy paginate-then-sort → 新路 sort-then-paginate（新路更合 ORDER-BY-LIMIT 语义）。
- **GAP10** partitions() TVF：schema-分区但零实例表 legacy 抛「not partitioned」→ 新路返 0 行（已有 in-code 注释声明 intentional）。
