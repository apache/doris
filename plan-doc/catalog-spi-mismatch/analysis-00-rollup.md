# Catalog SPI 抽象错配 survey — 独立核实总览

> **修复进度跟踪见 [`HANDOFF.md`](HANDOFF.md)（会话入口）+ [`TASKLIST.md`](TASKLIST.md)（逐条任务 + 状态板）。**
>
> **本文件是 7 份分类核实文档的索引 + 全局结论。** 针对 `catalog-spi-abstraction-mismatch-survey.md` 提出的 28 条发现(#1–#26 数据粒度类 + #27 reader-type + #28 catalog-dispatch),逐条对照**当前工作树代码**独立核实,不轻信原报告。

## 方法与基线

- **基线**:当前工作树 `git worktree /mnt/disk1/yy/git/wt-catalog-spi`,分支 `catalog-spi-2-lvl-cache`(原 survey 基于"几天前"的 `apache/branch-catalog-spi`,故其部分结论可能已过时)。
- **流程**(每条发现):① 独立调查员读当前代码逐条验证事实断言 → ② 对抗复核员(red-team)默认怀疑、独立重读、尝试推翻结论 → ③ 综合两阶段以 `final_verdict` + 修正落文档。所有结论均引 `file:line`。
- **verdict 口径**:`CONFIRMED`(属实)/ `PARTIAL`(部分属实/被高估/被误归)/ `REFUTED`(不成立)/ `STALE_FIXED`(曾属实但当前代码已修)。
- **未覆盖**:原 survey 第 5 节"verified-OK"(报告自认做对的部分)未独立复核;本次只核实"提出的问题"。

## 全局统计(28 条)

| 核实结论 | 条数 | 发现 |
|---|---|---|
| **CONFIRMED**(真,但多为死代码/vestigial/设计如此/仅估计) | 11 | #2, #7, #8, #9, #15, #20, #21, #22, #23, #25, #26 |
| **PARTIAL**(部分属实 / 严重度或范围被高估 / 连接器被误归) | 11 | #1, #10, #11, #12, #13, #14, #16, #18, #24, #27 |
| **REFUTED**(方向性误读,不成立) | 2 | #5, #17 |
| **STALE_FIXED**(曾真实,当前代码已修) | 4 | #3, #4, #6, #28 |

## 关键结论

1. **原报告"5 条现行高危 🔴"经核实大幅缩水**。逐条看这 5 条:
   - **#1** → PARTIAL:hive"现行错"已修(commit `3593684715f`),真残留仅 **hudi decimal 分区分支未同步**(`HudiConnectorMetadata.extractLiteralValue` 走 `String.valueOf`),潜在剪枝失配。
   - **#2** → CONFIRMED(唯一站得住的高危):机制真实,但报告的"单列 rename 静默丢列"headline 被空集回退救活,**真实 repro 是混合投影 + time-travel rename,主导后果是 BE crash(非静默错数据)**,且 **paimon native projection 比 iceberg 更脆**。
   - **#3** → STALE_FIXED:paimon JNI/COUNT 已改 per-file 后缀 format + 回归测试钉死。
   - **#4** → STALE_FIXED:Hive 分区值 unescape 已由 #65473 完整修复(KEY+VALUE 双双 unescape + 单测)。
   - **#5** → **REFUTED**:三点全反——iceberg 行数取 `currentSnapshot().summary()`(snapshot 级)、已扣 position delete(delete-aware)、并对 equality delete 显式 gate 到 -1/UNKNOWN。
   
   即:**5 条 🔴 里,#3/#4 已修、#5 不成立、#1 仅剩 hudi 小残留,只有 #2 是真实待办(且是 crash 风险,非报告所述的静默错数)。**

2. **两条方向性误读(REFUTED)**:#5(iceberg 行数)与 #17(MVCC branch 移动窗口——被 `IcebergStatementScope` 每语句单次冻结加载证伪,plan/scan 复用同一冻结 `Table`,移动窗口不存在)。

3. **多条"现行/休眠"其实已修(STALE_FIXED)**:#3、#4、#6(压缩重映射经 `adjustFileCompressType` 已恢复)、#28(detect-and-delegate 已在 #65473 与"hms 进 SPI_READY_TYPES"**原子**落地,预测的 bug 从未出现)。**这批集中体现"报告基于旧快照"**。

4. **CONFIRMED 里真正值得动手的很少**:多数是死代码(#21 `ConnectorDeleteFile` 零调用者、#23 `ConnectorDomain` 零消费者)、纯形态 smell(#25 缺 equals/hashCode)、有意 legacy parity(#22 只丢下推、#15 只估计偏斜)、或潜伏无复现(#20)。**架构层面真实的耦合坏味集中在写/事务/DDL 三条(#7/#8/#9),但都是"接口形状不干净",非运行时缺陷。**

5. **#14 是被低估的一条**:报告标"休眠",实测经 `getNameToPartitionValues` 在 paimon `partition_values()` TVF 上**活跃产错**(DATE 显 epoch-day、null 显 `__DEFAULT_PARTITION__` 而非 SQL NULL)。

## 逐条总表

| # | 严重(原) | 核实结论 | 一句话 | 文档 |
|---|---|---|---|---|
| 1 | 🔴 | PARTIAL | 架构缺口(无 typed canonical 访问器)真实且仍在;但 hive"现行错"已修,真残留=hudi decimal 分支未同步 | [A](analysis-A-literal-predicate.md) |
| 23 | 🟡 | CONFIRMED | 字面成立但 `ConnectorDomain` 是死代码(columnDomains 恒空/零消费者),当前零影响,建议删 | [A](analysis-A-literal-predicate.md) |
| 2 | 🔴 | CONFIRMED | 机制/根因成立;但 headline 单列示例被空集回退救活,真 repro=混合投影,主导后果=BE crash 非静默错数据,paimon 更脆 | [B](analysis-B-schema-column-identity.md) |
| 10 | 🟠 | PARTIAL | 单槽/parseSchema 传 null 属实,但 initialDefault 经 #65502 独立 thrift 字段已读并下发 BE 无塌陷;真缺口仅 writeDefault 未接,低危 | [B](analysis-B-schema-column-identity.md) |
| 11 | 🟠 | PARTIAL | CHAR/VARCHAR→MAX、DATETIME scale→微秒 行为属实,但是有单测钉死的刻意 legacy parity,非 SPI 错配 | [B](analysis-B-schema-column-identity.md) |
| 12 | 🟠 | PARTIAL | nullability 已修(FIX-L13);comment 丢弃属已签字 display-only 偏差,低危 | [B](analysis-B-schema-column-identity.md) |
| 19 | 🟡 | PARTIAL | 读路径嵌套 null/comment 缺口属实但功能影响为零,legacy parity,非迁移回归 | [B](analysis-B-schema-column-identity.md) |
| 22 | 🟡 | CONFIRMED | 用 latest schema 属实且注释锁为 legacy parity;仅"改名+time-travel+谓词命中改名列"罕见交集触发,只丢下推不丢正确性 | [B](analysis-B-schema-column-identity.md) |
| 4 | 🔴 | STALE_FIXED | 曾真实且严重,已由 #65473 完整修复(KEY+VALUE 双 unescape + 单测) | [C](analysis-C-partition.md) |
| 13 | 🟠 | PARTIAL | LIST/RANGE 显式值/边界恒空属实,但有意分层+零消费者+`hasExplicitPartitionValues` 无损保留并 fail-loud,属 vestigial 未接线槽 | [C](analysis-C-partition.md) |
| 14 | 🟠 | PARTIAL | 技术事实成立,但"休眠"定性错误——经 `getNameToPartitionValues` 在 paimon `partition_values()` TVF 上活跃产错 | [C](analysis-C-partition.md) |
| 20 | 🟡 | CONFIRMED | `List<Integer>` 表达力上限真实但纯潜伏,当前无非整型参数 transform,无可复现错误 | [C](analysis-C-partition.md) |
| 3 | 🔴 | STALE_FIXED | paimon JNI/COUNT 已改 per-file 后缀 format,回归测试钉死 | [D](analysis-D-scan-split-file.md) |
| 6 | 🟠 | STALE_FIXED | 无压缩槽是有意设计,LZ4FRAME→LZ4BLOCK 重映射经 `adjustFileCompressType` 已恢复+单测 | [D](analysis-D-scan-split-file.md) |
| 21 | 🟡 | CONFIRMED | `ConnectorDeleteFile` 欠建模且零调用者=死代码,非活 bug,建议删除 | [D](analysis-D-scan-split-file.md) |
| 26 | 🟡 | CONFIRMED | residual scan-node 级下发对齐 legacy,仅极轻微 BE 冗余计算,无正确性问题 | [D](analysis-D-scan-split-file.md) |
| 5 | 🔴 | **REFUTED** | 三点全反:行数取 currentSnapshot summary(snapshot级)、已扣 position delete、对 equality delete 显式 gate 到 -1/UNKNOWN | [E](analysis-E-stats-mvcc-timetravel.md) |
| 15 | 🟠 | CONFIRMED | stats 恒取 currentSnapshot 与 schema pin 不对称,time-travel 下 CBO 估计偏斜但不错结果,legacy 忠实端口 | [E](analysis-E-stats-mvcc-timetravel.md) |
| 16 | 🟠 | PARTIAL | 微秒/毫秒命名错配属实但非 S 级;CacheAnalyzer 有 wall-clock 混算,后果=安全抑制 iceberg SqlCache | [E](analysis-E-stats-mvcc-timetravel.md) |
| 17 | 🟠 | **REFUTED** | branch 移动机制属实但 `IcebergStatementScope` 每语句单次冻结加载,plan/scan 复用同一冻结 Table,移动窗口不存在 | [E](analysis-E-stats-mvcc-timetravel.md) |
| 18 | 🟠 | PARTIAL | 数字正则当 epoch 属实但合法 datetime 字面量恒不误判,歧义定性夸大,系跨连接器有意对齐 | [E](analysis-E-stats-mvcc-timetravel.md) |
| 24 | 🟡 | PARTIAL | 一 long 两粒度属实但判别枚举始终在场,消费侧无条件分派,构造不出失败场景 | [E](analysis-E-stats-mvcc-timetravel.md) |
| 25 | 🟡 | CONFIRMED | 独缺 equals/hashCode 精确命中,纯形态 smell 零运行时影响,backlog 项 | [E](analysis-E-stats-mvcc-timetravel.md) |
| 7 | 🟠 | CONFIRMED | `getWriteContext()` 错标通用 bag(实为 INSERT 静态分区 spec) | [F](analysis-F-write-txn-ddl.md) |
| 8 | 🟠 | CONFIRMED | `ConnectorTransaction` 私有方法泄漏(odps block-alloc + iceberg-compaction rewrite,含 fe-core Transaction 双重泄漏) | [F](analysis-F-write-txn-ddl.md) |
| 9 | 🟠 | CONFIRMED | `ConnectorBucketSpec` 表分布 vs iceberg per-field bucket 类别错误,iceberg 从不消费 `iceberg_bucket` 值 | [F](analysis-F-write-txn-ddl.md) |
| 27 | 🟠 | PARTIAL | 架构观察成立;核心 hudi `force_jni_scanner` 回归 REFUTED(SPI 已完整 honor+单测),应降级为纯架构观察 | [G](analysis-G-reader-path-dispatch.md) |
| 28 | 🟠 | STALE_FIXED | detect-and-delegate 已在 #65473 与 hms 进 SPI_READY_TYPES 原子落地,预测 bug 从未出现 | [G](analysis-G-reader-path-dispatch.md) |

## 分类文档索引

| 文档 | 类别 | 覆盖发现 |
|---|---|---|
| [A — Literal / 谓词粒度](analysis-A-literal-predicate.md) | 字面量/谓词表示忠实度 | #1, #23 |
| [B — Schema / 列身份粒度](analysis-B-schema-column-identity.md) | 列身份/schema/default/嵌套 | #2, #10, #11, #12, #19, #22 |
| [C — 分区粒度](analysis-C-partition.md) | 分区值/DDL/transform | #4, #13, #14, #20 |
| [D — Scan / split 文件级](analysis-D-scan-split-file.md) | file format/compress/delete-file/residual | #3, #6, #21, #26 |
| [E — 统计 / MVCC / 时间旅行粒度](analysis-E-stats-mvcc-timetravel.md) | 行数/统计/时间旅行/branch/freshness | #5, #15, #16, #17, #18, #24, #25 |
| [F — 写 / 事务 / DDL 粒度](analysis-F-write-txn-ddl.md) | writeContext/transaction/bucket spec | #7, #8, #9 |
| [G — Reader-path / catalog-dispatch](analysis-G-reader-path-dispatch.md) | reader-type 决策/catalog 分派 | #27, #28 |

## 建议(基于核实后的真实优先级)

- **值得排期**:仅 **#2**(混合投影 + time-travel rename 下 paimon/BE crash 风险,需 e2e 覆盖 + 考虑给 `getColumnHandles` 补 snapshot 参数/统一 pinned schema 重建)。
- **小步低风险修**:**#1** 把 hive 已修的 BigDecimal `stripTrailingZeros().toPlainString()` 分支镜像进 hudi;**#14** 若 paimon `partition_values()` TVF 真被使用,归一化 DATE/null。
- **清理死代码 / 注释漂移(非缺陷)**:#21(删 `getDeleteFiles()` 零调用者)、#23(删 `ConnectorDomain` 死抽象)、#25(补 equals/hashCode 一致性)、#28(清 `HiveConnector` "Dormant" 过时注释)。
- **架构 backlog(碰"fe-core 只出不进"铁律,应作独立设计任务)**:#7/#8/#9 收连接器专属方法进 facet;#27 统一 `ReaderType` 枚举。
- **无需动**:已 STALE_FIXED 的 #3/#4/#6/#28、REFUTED 的 #5/#17,以及一批有意 legacy parity(#11/#12/#15/#18/#19/#22/#24)。
