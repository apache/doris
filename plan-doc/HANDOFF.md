# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = 修 TeamCity **CI 997422**（Doris_External_Regression）的失败用例。

---

# 🆕🆕🆕 最新一轮（2026-07-26）：rebase onto `962bd5b28c7` + 移植 #66008 的 paimon-cpp 摘除

> `git pull --rebase upstream-apache master`，61 commit onto **`962bd5b28c7`**（上游只新增 2 个 commit）。

**唯一冲突** = `#66008` 改了 `fe-core/.../paimon/source/PaimonScanNode{,Test}.java`，本分支 `1ee79930faf` 已整体删除该子系统
（modify/delete）⇒ 解法 = `git rm` 保留删除，能力另行移植进 `fe-connector-paimon`。另一个上游 commit `#66036` 纯 BE，无冲突。

**移植结论：必须移植，不是可选**。#66008 把 `PaimonScanNode.setPaimonParams` 的 paimon-cpp 臂整体删掉
（永远 `PAIMON_JNI` + `encodeObjectToString`，不再 `setPaimonTable`），原因在 BE：`_should_use_file_scanner_v2`
不再排除 `FORMAT_JNI`（`enable_file_scanner_v2` 默认 **true**）⇒ paimon JNI 扫描进 V2；而
`file_scanner_v2.cpp:is_supported_jni_table_format` 对 `reader_type == PAIMON_CPP` 返回 false ⇒
`_validate_scan_range` **直接 `Status::NotSupported` 报错**，**没有 per-range 回落 V1**（V1 的 `PaimonCppReader` 只有
`set enable_file_scanner_v2=false` 才够得着）。`enable_paimon_cpp_reader` 是 `fuzzy=true`，且 3 个上游 e2e suite
（`test_paimon_cpp_reader`、`test_paimon_partition_{pk_delete,schema_filter}_refs`）会显式 `set ...=true` ⇒ 不移植就是
paimon e2e 必红。

已交付 commit（见 `git log`）：删 `PaimonScanPlanProvider.isCppReaderEnabled` / `ENABLE_PAIMON_CPP_READER` /
`encodeSplit` 的 native-binary 臂 / `getTableLocation` / `tableLocation` 线程，删 `PaimonScanRange.cppReaderSplit` +
`paimon.table_location` + `setPaimonTable`，JNI 臂恒 `PAIMON_JNI`。测试：`PaimonScanRangeReaderTypeTest` 去 cpp 例、
`PaimonScanPlanProviderTest` 3 个 cpp 例换成 2 个（`encodeSplitAlwaysUsesJavaSerializationForDataSplit` +
新的 planScan 级 `cppReaderSessionFlagNoLongerChangesThePlan`）、`PaimonScanExplainTest` 去 `.tableLocation(...)`。
**e2e 无需改**（3 个 suite 只做 flag on/off 结果对比，flag 变 no-op 后自然通过）。

验证：模块 **382/382**（1 个既有 live-connectivity skip）、checkstyle **0**、双变异均 RED
（`PAIMON_JNI→PAIMON_CPP`；重加 `setPaimonTable`）→ 复原后 GREEN。

⏭ 下一轮注意：#66008 把 `FORMAT_JNI` 放进了 V2 **对所有连接器生效**（hudi/iceberg/max_compute/trino_connector）。
已核 hudi 侧安全（`delta_logs` 只在 `isJni` 分支写，不会造出 V2 拒收的 "parquet + delta_logs" 形状）；其余连接器未逐一核。

---

# 🆕🆕 上一轮（2026-07-25）：CI **1005291** 的 iceberg 大小写列名回归 —— 已修待 CI 验

> 任务 = TeamCity `Doris_External_Regression` **#1005291**（PR **66028** @ `7ff51a106f0`）中
> `external_table_p0/iceberg/test_iceberg_nested_schema_evolution_spark_doris_interop.groovy:273`。
> 该 build 整体 SUCCESS（603 passed），这条是 **muted** 的失败。

## 定性：本分支独有的**真回归**，不是 flaky、不是集群故障

同一用例在 pull/66011、65847、66006、65851、65126 上均 SUCCESS；另外两个 FAILURE（66007、65851）
是完全不同的原因（`can not cast from origin type STRUCT<...>`、`No backend available / not alive`），**与本问题无关**。
本地 HEAD 与 PR 66028 head 一致，`fe/fe-core/.../datasource/iceberg/` 已整体删除 ⇒ 走的一定是连接器路径。

## 根因：#65329 的**平坦（top-level）臂**只移植了一半

上游 `IcebergMetadataOps`（`git show 70a82532325:fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergMetadataOps.java`）
有 **5 处** `validateNoCaseInsensitiveSiblingCollision` 调用：flat ADD(:885)、nested ADD(:918)、
ADD COLUMNS 批量(:946→helper :1428)、flat RENAME(:1005，且前置 :1000 用 `resolveColumnPath` 大小写不敏感解析**源列名**)、nested RENAME(:1028)。
移植只带进了**两处 nested**（`IcebergNestedColumnEvolution:71/99`）。

顶层 DDL **根本到不了**那个类：fe-core `PluginDrivenExternalCatalog:908-913/949-951` 对 `!columnPath.isNested()` 直接短路回平坦 SPI，
`IcebergConnectorMetadata.addNestedColumn/renameNestedColumn` 再短路一次；终点
`CatalogBackedIcebergCatalogOps.addColumn/addColumns/renameColumn` 直接 `UpdateSchema` 零校验。
Iceberg 自己也挡不住 —— `SchemaUpdate` 构造器 `caseSensitive = true`，`findField("id")` 看不见 `Id`。
⇒ 该校验器里 `parentPath.isEmpty()` 那条顶层分支一直是**死代码**，正是漏掉调用点的信号。

## ✅ 已交付：commit `f1104a6880d`

三个平坦入口接上 nested 臂已有的校验器；`validateNoCaseInsensitiveSiblingCollision` 放宽为包内可见，
新增 `validateNoCaseInsensitiveTopLevelCollisions`（批量 + 请求内去重）与 `renameTopLevelColumn`
（大小写不敏感解析源名 + 冲突校验 + 复用 `applyRenameColumn` 的 identifier-field 修复）。**未碰 fe-core**。

验证：先 RED（7 个新用例在修前失败，症状与 CI 完全一致：ADD 是 `nothing was thrown`，RENAME 是
`Cannot rename missing column: label`）→ 后 GREEN；模块 **1145/1145**（5 个既有 live-connectivity skip）、
**全 reactor `test-compile` BUILD SUCCESS**、checkstyle 0。5 路对抗复审 + 3 skeptic 表决：**0 条成立**。
该 suite 内**唯一**的顶层列 DDL 就是 274–289 行，其余全是 dotted 嵌套路径（未受影响）；
289 行 `RENAME COLUMN label TO label` 会把列名变小写，但 `mixedCaseTable` 最后一次被引用就在 296 行，无下游影响。

## ⏭ 下一个 session

1. **重跑 CI 是唯一真闸门**。预期 273/277/281/285/289 五条全过（后四条此前从未被执行到）。
2. **同族平坦臂缺口，本轮故意未打包**（复审提出、经表决判定为既有问题且超出本次范围，非本次引入）：
   - flat `dropColumn`（`IcebergCatalogOps` 内）仍按大小写敏感解析 ⇒ 对 Spark 混合大小写表 `DROP COLUMN label` 会失败（上游 :961 用 `resolveColumnPath`）；
   - flat `modifyColumn` 用大小写敏感 `Schema.findField` ⇒ `MODIFY COLUMN id` 报 "Column id does not exist"；
   - flat `applyPosition` 的 `AFTER <ref>` 参照列未做大小写不敏感解析；
   - `reorderColumns` 既不规范化大小写也不查重复。
   以上**均未被本 suite 触发**（该 suite 的 DROP/MODIFY 全是 dotted 嵌套路径），故不阻塞本次 CI。
   另：row-lineage mutation guard 的缺失是**本分支既有的、有文档的有意偏离**（见 `IcebergConnectorMetadata:1282`），不是缺口。
3. 复审旁获（未验证、与本次无关）：有 agent 声称 hive 网关未把 5 个 ColumnPath 列操作委派给 iceberg 兄弟，
   导致 iceberg-on-HMS 的 `MODIFY COLUMN COMMENT` 恒不可用。**未经我独立核实**，要动前先自己 trace。

## 🧰 本轮新增构建坑（补充下面第 1 条）

**`-Dmaven.build.cache.enabled=false` 会连带打破 shade 依赖链**：`fe-connector-hms-hive-shade` 的 shade 插件绑在
`package` 阶段，而 `test`/`compile` 生命周期到不了 `package` ⇒ 关缓存后 `fe-connector-hms` 编译报
`package org.apache.hadoop.hive.metastore.api does not exist`（**与被改代码无关**，该 reactor 里根本没有 iceberg 模块）。
正确姿势：先 `-pl <目标模块> -am install -DskipTests -Dmaven.build.cache.enabled=false`（跑到 package 产出 shade jar），
再 `-pl <目标模块> test -Dmaven.build.cache.enabled=false`（**不带 `-am`**）。

**另注**：`fe-connector-api` 等上游模块的**已安装 jar 常落后于工作区**，只 `-pl <模块>` 不带 `-am` 会撞
"no suitable method found" / "cannot find symbol" 之类的**假编译错**（本轮撞了两次：`ConnectorType.structOf` 5 参、
`isChildCommentSpecified`）—— 那不是代码坏了，是 jar 陈旧。

---

# 🕗 上一轮 = **重跑 CI 验证 3 个修复**（997422）

> **本轮任务** = TeamCity `Doris_External_Regression` **#997422**（PR 65474 @ `6a450c9fa79`）
> **10 failed + 2 muted**（occurrence 口径 12）。
> **权威文档**：根因分析 = [`tasks/ci-997422-failure-analysis.md`](./tasks/ci-997422-failure-analysis.md)（18-agent recon + 3-lens 对抗复核 + 本人独立复核；每条证据带 file:line / 日志行号 / 实测数字）。

## 🔑 定性：**不是集群故障**，别去查宕机/OOM

BE 单次启动、优雅退出（`be.out` 仅退出时 LSAN leak summary，零 `SIGSEGV`/`SIGABRT`/`CHECK failed`）· `dmesg.txt` **无 OOM-killer**（失败时宿主机余 **19.03GB**）· 551 通过。
**12 个失败 = 4 个独立根因**（A+B / C / D / E），**A+B 是同一个 bug、占 9 个**。

## ✅ 上一轮（996541）的修复已被本轮 e2e 验证生效 —— **不是回炉**

`test_iceberg_time_travel`、`iceberg_branch_complex_queries`、`paimon_system_table`、`test_catalogs_tvf`、6 个 spec 演进用例**本轮全部未再出现**。本轮即上一版 HANDOFF 要求的 TODO 9（`4f8b35c2126` 的 e2e），**它验出了 `4f8b35c2126` 自己引入的回归**。

⚠️ **易混点**：`bd6fdf7009a` 修的是 `__DORIS_GLOBAL_ROWID_COL__`（topn lazy-mat 合成列）；本轮 A+B 是 `__DORIS_ICEBERG_ROWID_COL__`（iceberg 写路径合成列）。**两个不同的列、不同的 bug**，勿当同一个反复修。

---

## ✅ 已交付（3 个可修根因，各自独立 commit + 变异验证）

| commit | 根因 | 修的用例 | 守门 |
|---|---|---|---|
| `35cf72cce91` | **A+B** 计划路径丢连接器合成写列（`4f8b35c2126` 把 `LogicalFileScan:223` 改调**无人 override** 的 1-arg `getFullSchema(Optional<MvccSnapshot>)`） | **9 个** iceberg DML / hidden-column | 38/38；变异（删 1-arg override）红在 `expected:<3> but was:<2>` = CI 症状本身；相关 92/92；checkstyle 0 |
| `a4cba35725c` | **D** paimon shade 缺 `hive-serde` ⇒ `serdeConstants`（`9a10ece30c8` 删 hive-catalog-shade 触发 `c276e955683` 的潜伏洞） | `test_create_paimon_table` | 基线 jar serdeConstants=0 → 修后=1；**classload 冒烟**跑通 `<clinit>`；**plugin zip 端到端**加载成功且全 zip 恰好 1 份 |
| `6320389dc06` | **C** L17 guard 对 sys-table 是范畴错误（`270bd11f4da` 知情延期，**延期前提为假**） | `test_iceberg_position_deletes_sys_table` | 11/11；双变异（删排除→1 红；放宽到父类→5 红）；相关 62/62；checkstyle 0 |

**E（muted，`test_hdfs_parquet_group0`）= 有意不修**：上游 `51e44133b1d` 的 `mem_limit=35%` + 一个真含 2.000GiB 字符串列的上游 fixture（footer 实测 `total_uncompressed_size=2,147,483,749`=2³¹+101）⇒ PODArray 2GiB→4GiB 增长。`git merge-base --is-ancestor 51e44133b1d master` = **YES** ⇒ **master 同样复现**。无真 OOM、无泄漏、非过期用例。在本分支改那个 conf = 静默 revert 上游决定并掩盖真回归。**保持 mute + 记录理由**（理由全文见分析文档 E 节）。

---

# ⏭ 下一个 session 要做的

1. **重跑 CI（唯一真闸门）**。预期：A+B 的 9 个解开列数断言、C 解开 init、D 的 paimon HMS 恢复。
2. **⚠️ C 很可能需要第二轮**：去掉抛出只解开 init。真正绿还需 iceberg `$position_deletes` planner 认这个 pin（`doPlanPositionDeletesSystemTableScan` 读 `handle.hasSnapshotPin()` ← `IcebergConnectorMetadata.applySnapshot` 喂）—— **已 trace 未执行**。且该 suite `:562-568`（源表跨 ADD COLUMN 时间旅行）在本分支**从未跑过**。
3. **⚠️ A+B「修完就绿」未证**：v1 suite 原在 `:98` abort，其后 `:101` "row-id column must be populated" 与 v3 取值断言**在本分支从未执行过**。本改动只保证列回到输出。
4. **待用户裁决**：`scannedPartitionCount` 在 `$position_deletes` 上触发（= 旧 HANDOFF gap ③ 的后半、`PluginDrivenScanNode:1213`）—— 与 2026-07-13 `selectedPartitionNum` 签字冲突，**本轮故意未打包**，需先裁决。
5. **勿顺手修的潜伏洞**：A+B 的合成 row-id `uniqueId = -1`（`IcebergWritePlanProvider.buildRowIdColumn` 6-arg ctor；`ConnectorColumnConverter:89-91` 只回填 `>= 0`）⇒ 列回到输出后 L17 guard 退化成 name 匹配、无法在 pinned schema resolve。**今天不可达**（pinnedSchema 仅显式时间旅行非空，且无用例组合 show_hidden/DML + `@tag`/`@branch`/`FOR..AS OF`）。若要修，**按通用属性（schema-cache 来源）判，绝不按 iceberg 列名**。

---

# 🧰 构建/验证坑（本轮实测，下轮直接复用，别再踩）

1. **maven build cache 会静默跳过 surefire** —— 日志 `Skipping plugin execution (cached): surefire:test`，此时 **BUILD SUCCESS 是空的**（surefire 报告是上次的陈旧文件）。**所有测试必须加 `-Dmaven.build.cache.enabled=false`**。本轮第一次跑就中招（BUILD SUCCESS 但 0 测试真跑）。
2. **`mvn ... | tail` 后的 `$?` 是 `tail` 的**，不是 maven 的 —— 重定向到文件再取 `$?`，或读 `BUILD SUCCESS`/`BUILD FAILURE` 行。
3. **`surefire:test` 独立 goal 解析不了 `${revision}`** ⇒ 必须走 `test` 生命周期 + `-am`；上游模块无匹配测试时加 `-DfailIfNoTests=false`。
4. **`hive-serde` 闭包首次需联网**（`javax.servlet:servlet-api:2.4` 不在本地仓），`-o` 会失败。
5. **`-Dtest='org.apache.doris.datasource.**'` 全包 sweep > 10min**，会被 shell 超时砍；用具体类名清单。
6. **`regression-test/conf/regression-conf.groovy` 工作区本就是脏的**（session 开始前即 `M`）—— 三个 commit 均未包含它，**别顺手 `git add -A`**。
7. **`pgrep maven` 可能查到 1 天前的僵尸 until-loop**（本轮见 PID 843896，etime `1-01:58`，在轮询 Jul 15 就跑完的日志）—— **看 `etime` 再判定是否真并发**，别误判成活跃 session 而无谓停手。

---

# 🗄 被本次覆盖的旧上下文（catalog-spi 主线：删旧代码 / rebase / trino / QUIC 瘦身）

按用户 2026-07-15 指示，本文件已用 CI 任务上下文**完全覆盖**。**旧内容完整保存在 `8eb5463f769:plan-doc/HANDOFF.md`**（`git show 8eb5463f769:plan-doc/HANDOFF.md`）。其中**仍未结项、需要时去那里捞**的条目：
① 删除线 PR 收尾（拓扑多 commit → 最终 squash）+ 用户自跑翻闸 hms 全量回归；
② e2e 欠账矩阵（`tasks/hms-cutover-execution-plan-2026-07-10.md §4/§5`）+ 继承自上游的 `$position_deletes` e2e 翻闸门（**本轮 C 即其中一项，已修待验**）；
③ rebase 引入的 2 个集成缺口（`IcebergScanPlanProvider:1419` 丢 `enable.mapping.timestamp_tz`；`scannedPartitionCount` 对 `$position_deletes` 触发，语义待用户拍板 = 上面第 4 点）；
④ trino 改名 PR 收尾两笔（**需 release note**；BE 未跑全量构建 + fallback 无 e2e）；
⑤ 独立任务空间 `plan-doc/hive-catalog-shade-removal/`（**从它自己的 HANDOFF 进**）；
⑥ 并发 session 已结项的 QUIC 根治（`ae82ffd2573`）+ 插件包瘦身 Tier A（`dece64b9ff5`）明细。

---

# 📎 并行独立任务（与上面 CI 线无关）：热路径重操作审计（DORIS-27138 问题类）

> 2026-07-17 独立调研，session 自包含，不影响 CI 线。用户待 review。

- 问题类总结（三要素 + A/B/C/D 变体 + 审计清单）：`plan-doc/perf-heavy-op-hot-path-problem-class.md`
- **fe-connector-iceberg 审计报告**（23 确认/1 驳回，分 P0/P1/P2 三层七簇）：`plan-doc/reviews/perf-audit-fe-connector-iceberg-2026-07-17.md`
- 完整证据 JSON（全部调用链+双路对抗验证意见）：`plan-doc/reviews/perf-audit-fe-connector-iceberg-2026-07-17-findings.json`
- **P0 三簇**：①无 Table 对象缓存,一次规划 3~7 次远程 loadTable；②#64134 planFiles 兜底复活（`IcebergWriterHelper.getFileFormat`,每查询 1-2 次整表扫）；③分区表每查询一次 PARTITIONS 元数据表扫描（CACHE-P1 弃二级缓存的代价）。
- 旁获（与审计无关待单独处理）：`CreateDictionaryInfo.validateAndSet:164` 强转 `catalog.Table` ⇒ 外表 CREATE DICTIONARY 必 ClassCastException（功能缺口/潜在 bug）。
- 下一步（等用户 review 后）：其余连接器（hive/paimon/hudi/mc）按同一问题类+同一 workflow 模式逐个审计。
