# CI 996541 根因综合报告（Doris_External_Regression, commit fa2fcf4b246）

## 结论先行

**不是集群级故障。** BE 只启动过一次并优雅退出（`be.INFO:593309` `I20260715 18:12:22.483862 doris_main.cpp:747] All service stopped, doris main exited.`；be.out 只有 1 处 `Start time:`；be.WARNING severity 分布 678 E / 19248 W / **0 F**；pipeline 自检 `no core dump file` / `exit_flag is 0`）。

> ⚠️ 更正任务简报：`dmesg.txt` **存在**且确有 doris_be segfault，但两组都无害——16:29:05 那组 image base `562d5c060000` 属于本 BE（`55e711b59000`，16:45:09 才启动）之前的另一进程；18:13:31 那组是我们的 BE，但发生在 `doris main exited`（18:12:22）**之后 69 秒**、测试结束后 4 分钟，faulting code `48 c1 ea 03 / 48 8d 82 00 80 ff 7f` 就是 ASAN shadow 地址计算，属 LSAN at-exit 期残留线程。无 OOM-killer 记录。

**内存压力只有一次孤立瞬时事件**：全部 22 条 `Allocator sys memory check failed` 属同一个 query id `6894a8ef4a6344cc-8211cf12a0a6fd10`，集中在 17:28:18–19 两秒内；全程 201 个 daemon 采样**无一**越过 19.55 GB soft limit。其余 12 个失败的时间戳全部落在该窗口之外。

**完整性**：runner 自报 `Test 563 suites, failed 13 suites, fatal 0 scripts, skipped 0 scripts`（`RegressionTest.groovy:478`）；`RegressionTest.groovy:452-470` 中 "Fatal scripts:"/"Skipped suites:" 段仅在非空时打印，日志中两段均不存在 ⇒ 没有被静默跳过或漏报的用例。**没有任何 retry**（`ScriptContext.groovy:98-127` 无重试循环；每个 suite 名恰好出现 2 次）。注意口径：runner 把 `test_hdfs_parquet_group0` 记为 **FAILED**，"muted" 是 TeamCity 侧的，正确表述是"runner 13 失败，其中 1 个在下游被 mute"。

---

## 13 个失败 → **5 个独立根因**

| # | 根因 | 用例数 | 性质 | 本分支引入 |
|---|------|-------|------|-----------|
| A | iceberg partition spec evolution → LIST 分区 arity 不匹配 | 6 | 产品代码 bug | ✅ |
| B | L17 fail-loud guard（`22516845ca3`）三种误报 | 4 | 产品代码 bug | ✅ |
| C | paimon HMS 闭包缺 `hive-serde`（打包） | 1 | 产品代码/打包 bug | ✅ |
| D | DLF 1.0 已删除，用例未更新 | 1 | 陈旧用例 | ✅（清理漏网） |
| E | HDFS parquet 4 GB 单次分配 + ASAN 基线 | 1 (muted) | 环境敏感 / 上游既有 | ❌ |

外加 **1 个不解释任何失败、但被绿色测试掩盖的真实缺陷**（见 §F）。

---

# A. iceberg partition spec evolution → `connector supplied N partition values ... but table has M partition columns`

**用例（6）**：`test_iceberg_partition_evolution`、`test_iceberg_partition_evolution_ddl`、`test_iceberg_partition_evolution_query_write`、`test_iceberg_table_cache`、`test_iceberg_rewrite_manifests`、`test_iceberg_position_deletes_sys_table`

### 机制（已闭环）
- **列**来自**当前 spec**：`IcebergConnectorMetadata.buildTableSchema` (`IcebergConnectorMetadata.java:439-456`) 遍历 `table.spec().fields()` 并 `findField(sourceId)` 过滤后拼 `partition_columns` CSV。（"所有 spec 的并集"假设已被证伪。）
- **值**来自**每行数据文件的历史 spec**：`IcebergPartitionUtils.generateRawPartition` (`:730-737`) 读 `spec_id` → `table.specs().get(specId)` → 按**那一行的 spec** 的 field 数循环。
- 老 spec 的行因此少给值 → fe-core `PluginDrivenMvccExternalTable.listLatestPartitions:277` 的 `Preconditions.checkState(connectorValues.size() == types.size(), ...)` 抛出。
- **两种形态同一机制**：部分值（`'year=2024/month=1'` 2 值 vs 3 列，来自 `ADD PARTITION KEY day`）与零值（原表 UNPARTITIONED，spec-0 循环 0 次 → name `''`）。`IcebergPartitionUtils.java:632` 的 `table.spec().isUnpartitioned()` 早退**救不了**后者——它测的是当前 spec。
- **为何走到这条路**：6 张表都过不了 `isValidRelatedTable` (`:682-701`，要求每个 spec 恰好 1 个 year/month/day/hour transform) → `buildMvccPartitionView` 返回 `unpartitioned()` (`:533-535`) → `getMvccPartitionView` 恒 `Optional.of(...)` (`IcebergConnectorMetadata.java:1454`) → fe-core 走 `materializeLatest:175` 的**本分支新增**非-RANGE LIST 路径 → `:178` → checkState。

### 本分支引入：**是**，两个 commit 叠加
1. `1a7e071a65f`（squash 进 `442a1081e6d`）新增非-RANGE→LIST 路径（原意修 `selectedPartitionNum=0` 致 sql_block_rule 失效）。
2. `cfb0958e607`（2026-07-13）把 size checkState **移出** per-partition try/catch——commit message 自陈："The size check is moved OUT of the per-partition try/catch ... so a mis-wired connector surfaces immediately instead of being swallowed"。此前是 log-and-skip、查询**成功**（分区展示降级）；此后同一 mismatch 直接杀查询。

**master 无此问题**：`IcebergUtils.loadSnapshotCacheValue`（`442a1081e6d^`）对 `!table.isValidRelatedTable()` 直接 `IcebergPartitionInfo.empty()`，从不枚举 PARTITIONS 元数据表。

### 修复（连接器侧，fe-core checkState 保留）
**文件**：`fe/fe-connector/fe-connector-iceberg/.../IcebergPartitionUtils.java`，插入点 **:659**（在 ValidationException 降级块 `:642-658` 之后、`List<ConnectorPartitionInfo> partitions = new ArrayList<>(raws.size());` 之前）。

镜像 17 行之上已有的降级先例（dropped source column → 返回空 list，已有单测 `IcebergPartitionUtilsTest:956`），当固定 arity 的 LIST 展示无法表达异构 arity 分区时**降级为 UNPARTITIONED（空 list）**——正是 master 的行为。

> **✅ 采纳 fix-soundness reviewer 的修正 1（比原 RCA 更强，零额外成本）**：比较**名字列表**而非 arity：
> ```java
> List<String> currentNames = new ArrayList<>();
> for (PartitionField field : table.spec().fields()) {
>     NestedField source = table.schema().findField(field.sourceId());
>     if (source != null) { currentNames.add(source.name()); }
> }
> for (IcebergRawPartition raw : raws) {
>     if (!raw.columnNames.equals(currentNames)) {
>         LOG.warn("... spec evolution left partition '{}' with fields {} while current spec has {}; reporting UNPARTITIONED.", raw.name, raw.columnNames, currentNames);
>         return Collections.emptyList();
>     }
>     }
> ```
> 原 RCA 的 arity-only 版本漏掉 **same-arity/different-column** 演进（spec `[a]` → drop a → add b：old row arity 1 == currentArity 1，checkState 通过，然后 `listLatestPartitions:288-290` **按位置**把 a 的值安到列 b 上）。名字比较同价堵住这个静默错配。

> **⚠️ 必须删掉原 RCA 的一句错误理由**：`currentArity MUST use the source != null filter so it matches EXACTLY what FE derives` —— **不成立**。`PluginDrivenExternalTable.java:519-528` 会再过滤一次（`fromRemoteColumnName` → `byName.get(...)` → `col != null`），所以 `types.size() <= currentArity`。**残留漏洞**：分区源是**嵌套字段**时 `findField != null`（计入 currentNames）但 `source.name()` 是叶子名、匹配不到任何顶层 FE 列 → fe-core 丢弃 → 当前 spec 的行也不会触发降级 → **checkState 仍会抛**。6 个失败表的分区源都是顶层列，故不阻塞，但这个洞必须记录或立 issue，不要用假理由盖掉。

**否决方案**：① 放松/删除 fe-core checkState（会把 spec-evolution 的源特有知识塞进通用层，违反 connector-agnostic 铁律，且撤销对 hive/paimon/hudi 的 fail-loud 保护）；② 用 `partitionValueNullFlags` 补 NULL（**正确性陷阱**：编码"老文件 day=NULL"这个**假事实**，仅因当前 `getPartitionType` 返回 UNPARTITIONED 而暂时无害，是定时炸弹）。

### 风险 & 置信度
**LOW，且严格收窄。** 输出仅用于展示/治理（`selectedPartitionNum` + `partition_num` block-rule），从不是读取集合。关键论证（比原 RCA 更强）：**被改变行为的表 == 今天必然抛异常的表**，所以不可能命中那 550 个通过的用例。已验证 4 个受影响 suite 的 .out 里**没有** `partition=N/M` 断言；且 `IcebergScanPlanProvider.java:297-309` 的 `scannedPartitionCount` 从 scan range 的 `getScannedPartitionKey()` 独立构造，`PluginDrivenScanNode.java:330-333` 优先采用它 ⇒ **降级后 `selectedPartitionNum` 依然存活**（原 RCA 的 open question #1 已被 reviewer 关闭）。

**置信度：高（机制）**。**两位 reviewer 均未推翻。**

### ❗仍未证实
原 RCA 写"重跑 6 个 suite 应全部按现有 .out 通过"——**证据不支持，必须降级表述**。每个 suite 在**第一个** query 就 abort，后续断言在本分支**从未执行过**。`test_iceberg_partition_evolution_ddl.groovy` 共 306 行 24 个 qt_，死在 :66；`test_iceberg_partition_evolution.groovy` 死在 :52。修复只保证"**不再 crash**"，被解锁的约 22 个下游断言（含 `:68` 的 `$partitions` 系统表断言，本修复不触碰该路径）**状态未知**，须以真实 CI 结果为准。

---

# B. L17 guard `assertBoundColumnsResolveInPinnedSchema` 三种误报（同一函数，4 个用例）

**用例（4）**：`test_iceberg_time_travel`(qt_q4)、`iceberg_branch_complex_queries`(qt_order_limit)、`iceberg_query_tag_branch`(qt_sub_join_tag_with_tag_1)、`paimon_system_table`(:155)

**Provenance**：`assertBoundColumnsResolveInPinnedSchema` 在 master **不存在**，由 `22516845ca3`（2026-07-13，"[fix](catalog) fail-loud on same-table multi-version schema skew (L17)"）纯新增（59 insertions / 0 deletions），是 `fa2fcf4b246` 的祖先。**本分支引入 = 是。** 本次运行中该 guard 触发 **4 次，4/4 全是误报（0 真阳性）**。

### 触发器 1 — 合成 row-id（2 个用例）✅ 定论清晰
`LazyMaterializeTopN.java:177-180` 用 8-arg ctor（`Column.java:272-276`）注入 `__DORIS_GLOBAL_ROWID_COL__*`，`colUniqueId = Integer.MAX_VALUE` → guard 的 key 选择（`PluginDrivenScanNode.java:937-939`）走 field-id 分支 → 任何 pinned schema 都不含 `Integer.MAX_VALUE` → 抛。该列由 connector reader 合成、根本不是表列，`PluginDrivenScanNode` **已在另外两处排除它**（`:559` `classifyColumn`、`:1019` `hasTopnLazyMaterializeSlot`），guard 忘了。

**fe.audit.log 2×2 阶乘证明**：`FOR TIME AS OF` 无 lazy-mat → EOF；`order by ... limit` 无 pin → EOF；两者同时 → **ERR**。

**修复（强制，低风险）** `PluginDrivenScanNode.java:936`，循环首句：
```java
for (Column bound : boundColumns) {
    if (bound.getName().startsWith(Column.GLOBAL_ROWID_COL)) {
        continue;   // reader 合成、按构造不在任何 pinned schema 中；与 :559 / :1019 一致
    }
```
**⚠️ 非 test-neutral**：`PluginDrivenScanNodeMvccSchemaGuardTest.java` 对 GLOBAL_ROWID **零覆盖**，必须补 `rowIdColumnIsExcludedNoThrow`（bound = [id@1, `__DORIS_GLOBAL_ROWID_COL__t`@Integer.MAX_VALUE]，pinned = [id@1] → assertDoesNotThrow），否则盲区会复发。

### 触发器 2 — sys-table pin（`paimon_system_table`）
`pinMvccSnapshot:885` → `resolveSysTableSnapshotPin():1044` 只判 `instanceof PluginDrivenSysExternalTable` + `source instanceof MvccTable`，**从不查 `sysTableSupportsTimeTravel()`**，于是给 paimon 也拿了**源表**的 pin；`PluginDrivenMvccExternalTable.loadSnapshot:394-405` 的 point-in-time 分支返回**非 null 的源表 pinnedSchema**；guard 拿 sys 表的列（`snapshot_id`…）去比源表 schema → 必然抛。异常再被 `getOrLoadPropertiesResult:1742` 包成 `RuntimeException("Failed to pin MVCC snapshot for plugin-driven scan")` → 客户端看不到真因 → 用例断言的 `system tables do not support time travel` 不匹配。

**注意**：guard **确实存在且正确**（`checkSysTableScanConstraints:1073`，消息在 `:1083`/`:1087`），只是**不可达**——它只被 `getSplits:1106` / `startSplit:1454` / `:1551` 调用，全在 init 之后。代码注释 `:896-897`（"sys-table scan carries a null pinnedSchema -> no-op"）和 `:1039`（"checkSysTableScanConstraints already rejects that case"）**都被证伪**，必须一并改掉。

> **🔴 采纳 fix-soundness reviewer 的推翻，改用能力位门控（不是原 RCA 的 assert carve-out）**
>
> **文件/位置**：`PluginDrivenScanNode.java:1044` `resolveSysTableSnapshotPin()`，在取 source pin 之前插入：
> ```java
> if (!sysTableSupportsTimeTravel()) {
>     return Optional.empty();
> }
> ```
> **为什么比 carve-out 好**：paimon 走这里根本不再调 `loadSnapshot` ⇒ ①不触发 guard ②不触发 `loadSnapshot:368` 的 `notFoundMessage` **RuntimeException**（该异常**不被** `:1741` 的 `catch (UserException e)` 拦截，会带着 paimon 自己的文案冒到客户端）③省掉一次无用的 `getTableSchema` 往返。三个 block（`FOR VERSION`/`FOR TIME`/`@incr`）都**数据无关地**拿到期望文案。原 RCA 的 carve-out 让 block 2（`FOR TIME AS OF '2024-07-11 16:01:57.425'`）的成败取决于 fixture 里是否存在 at-or-before 快照——静态分析无法判定，是掷硬币。同样是 connector-agnostic（分支在通用 fe-core 类型 + 既有能力 SPI 上，不按源名）。iceberg（`IcebergScanPlanProvider.supportsSystemTableTimeTravel():340-341` = true）不受影响，仍正常拿 pin。
>
> **refute-lens 的补充（支持任一方案可行）**：`PaimonConnectorMetadata.java:715-720` `applySnapshot` 对 `paimonHandle.isSystemTable()` 直接短路返回，**不会抛**，所以跳过 guard 后 init 一定能走完到 `checkSysTableScanConstraints`。原 RCA 未证的一环由此闭合。

**测试**：`paimon_system_table.groovy` 在本分支**被改过**（`buildlog.txt:5766`），但 diff 显示三处期望只是**放宽**（`Paimon system tables do not support time travel` → `system tables do not support time travel`）以对齐 connector-agnostic 文案。期望严格弱于 master ⇒ **排除**陈旧用例假设，坐实产品 bug。

### 触发器 3 — version-blind schema binding（`iceberg_query_tag_branch`）🔴 **未解决，需产品决策**
`SELECT t1.c1, t2.c1 FROM tag_branch_table@tag(t1) t1 JOIN tag_branch_table@tag(t2) t2` → `StatementContext.getSnapshot(TableIf)`（version-**blind**，`:1015-1032`）命中自己文档的 case (3)"两个非默认版本、无默认 → 歧义 → 返回 empty" → `PluginDrivenMvccExternalTable.getSchemaCacheValue:494-503` 回退 **LATEST {c1,c2,c3}**；`LogicalFileScan` 非 `OutputPrunable`（`ColumnPruning` 只裁剪 `OutputPrunable`）⇒ scan tuple 携带未裁剪的 c1,c2,c3；而 `pinMvccSnapshot:873-874` 的 version-**aware** 查找正确解析 tag t1 → `{c1}`（`run11.sql:14` 建 t2 早于 `:16` 加 c2）→ guard 在 c2 上抛。

**guard 报的 c2 就是直接证据**：查询根本没引用 c2（原 RCA 的 open question #2 可关闭，无需 EXPLAIN VERBOSE）。

**🔴 两位 reviewer 一致推翻原 RCA 的 "Change 2"**（放宽为"只查 field-id 名字冲突"）：
1. 其核心论据 **`iceberg_query_tag_branch.out` 的 `-- !branch_1 --` = `1 \N \N` 证明 absent→NULL 是被认可语义** —— **是混淆**。BRANCH pin 经 branch-aware handle 解析（`PluginDrivenMvccExternalTable.java:387-396`），iceberg branch schema = latest = `{c1,c2,c3}`，**没有任何列 absent**；那些 NULL 来自**数据文件**的普通 schema-evolution 填充。branch 路径根本没运行过 guard，无法为 tag 路径背书。
2. `TEST EDITS: none` **是假的**：会打挂 4 个现有单测——`fieldIdRenumberBetweenBoundAndScannedVersionThrows:52`、`columnAddedAfterScannedVersionThrows:66`（**字面就是 case 3，被断言为应当抛**）、`nameMissWhenNoFieldIdThrows:77`、`fieldIdStableRenameResolvesByIdNoThrow:88`（Change 2 会**新增**对 benign rename 的误拒，与该类 javadoc `:919-921` "a rename that keeps the id is fine" 直接冲突）。
3. 其 `if (bound.getUniqueId() < 0) continue;` 会**整体关闭 paimon 的 guard**（name-matched connector），rename 从 fail-loud 退化为静默读 NULL。
4. 其 fix_risk 宣称"仍能捕获 dropped-then-re-added 的 field-id renumber"——**假的**：单测 `:52` 的 renumber 场景同名，无名字冲突，Change 2 静默放行 ⇒ **它什么都捕不到**。

**且这是本分支自造的 skew**：上游 `fbef303da5f:StatementContext.java:730-736` 是 `return Optional.ofNullable(snapshots.get(new MvccTableInfo(tableIf)))`——单 key、有 pin 时**从不返回 empty**。version-aware key + "ambiguous → empty" 是本分支 `442a1081e6d` 引入的。由于 t1 和 t2 schema 都是 `{c1}`，**上游绑定 `{c1}`，这条查询本来零 skew**。所以 guard 只是信使，真缺陷在 binding。

`.out` **不要改**：`iceberg_query_tag_branch.groovy/.out` 与 `run11.sql` 与上游 `fbef303da5f`(#51272) **字节相同**，`1 1` 是上游验证过的正确答案。

**三个选项（需用户拍板）**：
- **C1（推荐，恢复上游 parity 且保住 guard 的牙）**：修 binding——让 `StatementContext.getSnapshot(TableIf):1015-1032` 对同表多版本不再回退 LATEST（恢复上游行为返回其中一个 pinned entry），或做 per-reference version-aware binding（即已跟踪的 **D-MVCC-VERSION-SCHEMA**）。skew 真消失，guard 自然不响，`1 1` 由构造得出而非侥幸。
- **C2**：暂接受该用例红，annotate/disable 单个 case，等 C1。
- **C3（原 RCA 方向，不推荐）**：明确决定"bound field-id 不在 pinned schema 中"合法。若选它，**必须**同时改写/删除上述 4 个单测，**不得**用 `uniqueId<0` 一刀切（要保住 paimon name check），且需重新论证（branch_1 论据作废）。此举**部分推翻 2026-07-13 已签字的 "throw for now" 决策**，须用户同意。

### ❗仍未证实（B 整体）
- 无论 C1/C2/C3，**没人能静态证明**这 3 个 iceberg 用例修完变绿。C3 额外依赖"connector 的 projection push-down 能容忍请求一个在该快照 schema 中不存在的列"——**至今无人验证**，这也是偏好 C1 的理由之一。
- 已排查并**排除**的诱人窄修法："只检查查询真正读取的列"——不可行：Nereids translator 不设 materialization flag（`PhysicalPlanTranslator`/`PlanTranslatorContext` 无 `setIsMaterialized`），tuple 由未裁剪的 `fileScan.getOutput()` 经 `generateTupleDesc:3110-3117` 构造，`buildColumnHandles()`/`tryPushDownProjection:1151-1152` 推同一未裁剪集合。**guard 调用点根本拿不到"需要哪些列"的信号**——这也说明 L17 在 init 时结构性无法表达其意图。

---

# C. paimon-on-HMS：`hive-serde` 被显式排除出插件闭包

**用例（1）**：`test_create_paimon_table`（`test_paimon_table.groovy:44` `create database if not exists test_db`，catalog 用 `'paimon.catalog.type'='hms'`，line 35）

### 机制（工件级证据，非推断）
```
fe.warn.log:422991  Failed to create Paimon catalog with HMS metastore (flavor=hms)
fe.warn.log:423039  Failed to create the desired metastore client  @ RetryingMetaStoreClientFactory.createClient:150
fe.warn.log:423078  Caused by: NoClassDefFoundError: org/apache/hadoop/hive/serde/serdeConstants  @ MetaStoreUtils.<clinit>(MetaStoreUtils.java:830)
fe.warn.log:423083  Caused by: ClassNotFoundException  @ ChildFirstClassLoader.loadClass(ChildFirstClassLoader.java:81)
```
- **不是 TCCL split-brain**：栈里 `TcclPinningConnectorContext.executeAuthenticated:84` 在跑，`PaimonConnector.createCatalogFromContext:423-425` 已 `setContextClassLoader(getClass().getClassLoader())`。且 `ChildFirstClassLoader.java:81` 是 `catch (ClassNotFoundException ignored)` 里的 `return super.loadClass(name, resolve)` **fallback** —— CNFE 从 super 里冒出来 ⇒ **child 和 parent 都没有**这个类 = **缺 jar**（split-brain 会是 ClassCast/LinkageError，不是 CNFE）。
- **字节码对齐**：`javap -c -l hive-metastore-2.3.7.jar` 的 `<clinit>` LineNumberTable `line 830: 275`，offset 278 = `getstatic org/apache/hadoop/hive/serde/serdeConstants.PrimitiveTypes` —— 与日志帧精确吻合。
- **根因**：`fe/fe-connector/fe-connector-paimon-hive-shade/pom.xml:115` **显式排除** `<exclusion>org.apache.hive:hive-serde</exclusion>`（挂在 hive-metastore 2.3.7 上，而后者 pom `:38-41` 本来 compile 依赖它）。
- **为何 parent 也没有**：本分支从 fe-core 移除了 `org.apache.doris:hive-catalog-shade`（`master:fe/fe-core/pom.xml:437` 有，现只剩 `:303`/`:778` 的墓碑注释）——本地 3 个 hive-catalog-shade jar **都含** serdeConstants，它就是此前经 parent fallback 的静默供给者。
- **两处改动的交集**：shade 模块由 `c276e955683`（P5 paimon SPI 迁移）新增、master 无；fe-core 同时丢掉 hive-catalog-shade。单看任一都不可见。
- **实证**：扫描**实际构建产物** `fe-connector-paimon/target/doris-fe-connector-paimon.zip`（164 jar / 77357 class）—— **0 个** 提供 serdeConstants；`MetaStoreUtils.class` 只在 `fe-connector-paimon-hive-shade-1.2-SNAPSHOT.jar` 里。bug 物理存在于产物中。

**环境排除**：同一 HMS 上 `external_table_p0.hive` 77 个 suite **全过 0 失败**，含 `test_hive_ddl`（17:44:11，21s，同样 CREATE DATABASE/TABLE）。plain-hive 能跑正是因为 `fe-connector-hms/pom.xml:62-66` 依赖 `hive-catalog-shade`（含 serdeConstants），paimon 却从裸 hive-metastore 重新组装并砍掉 hive-serde。docker `hive2/hive3-metastore.log` 无 error/exception/refused。**确定性**（静态初始化器 CNFE 不可能 flaky），`buildlog.txt:163768` 该 F: 行只出现一次、无 retry。

### 修复
**文件**：`fe/fe-connector/fe-connector-paimon-hive-shade/pom.xml`，在 hive-metastore 块（`:104-139`）之后（约 :140）新增显式 `org.apache.hive:hive-serde:2.3.7` `<optional>true</optional>` 依赖 + exclusions，**保留 :115 的 exclusion**。

> **✅ 采纳 fix-soundness reviewer 的两处修正**：
> 1. **必须整个 jar，不能只捞 serdeConstants**。对失败路径三个类（MetaStoreUtils / HiveMetaStoreClient / RetryingMetaStoreClient）抽全部 `org/apache/hadoop/hive/**` 引用、与真实 zip 求差：**缺 12 个**，serdeConstants 只是其一，还有 `serde2/Deserializer`、`serde2/SerDeUtils`、`serde2/typeinfo/{TypeInfo,TypeInfoUtils}`、`serde2/lazy/LazySimpleSerDe`、`serde2/objectinspector/{ObjectInspector,ObjectInspector$Category,StructField,StructObjectInspector,ListObjectInspector,MapObjectInspector}`。加上 hive-serde-2.3.7 后：**0 缺失**。⇒ 任何"更外科手术"的窄版（手抄 serdeConstants / shade include filter）**会在下一个类上再挂**。注释里要写清这一点，防止后来者"优化"成坏版本。
> 2. **删掉两条 no-op exclusion**：`hadoop-common` 与 `hadoop-mapreduce-client-core` 在 hive-serde-2.3.7 的 pom 里已是 `<optional>true</optional>`，不会传递。剩余 10 条（hive-common / hive-service-rpc / hive-shims / jsr305 / commons-codec / commons-lang / avro / libthrift / opencsv / parquet-hadoop-bundle）恰好覆盖其真实 compile 闭包。
> 3. **修正 Maven 机制表述**：`<exclusions>` 是**剪枝子树**，不参与版本 mediation。直接声明只是 :115 从不触及的另一个 depth-1 节点。原 RCA 的 "nearest-wins mediation" 说法不准（结论对）。
> 4. **关闭原 open question**：版本选 2.3.7 —— serdeConstants.class 在 2.3.7 与 2.3.9 **字节相同**（md5 `8432d469fdd1048dba536c8eff5428d6`），2.3.7 匹配读它的 hive-metastore，符合本 pom "每 artifact 单一 hive 版本"的既定意图。
> 5. **重复类风险 = 0（对真实产物验证）**：hive-serde-2.3.7 的 563 个 class 与构建出的 paimon 1.3.1 插件 zip 的 77357 个 class **交集为空**。shade jar 里已有的 8 个 original-package `org/apache/hadoop/hive/serde2/io/*` 来自 **hive-storage-api-2.4.0**（Hive 2.x 把它们挪走了），与 hive-serde 不相交。原 RCA 拿本地过期的 paimon-hive-connector **1.1.1** 去比是**查错了工件**，其 open question 方向错误、可直接关闭。
> 6. 无 SPI 隐患：hive-serde-2.3.7 **无** `META-INF/services`；`META-INF/maven/**` 已被 shade config `:255` 过滤。

**不要**简单删 `:115` —— 会让 hive-serde 传递拖入 parquet-hadoop-bundle（~20MB shaded parquet+jackson）、avro、opencsv、hive-service-rpc 进 shade jar。

> **⚠️ 交叉冲突（Rule 7，须点名）**：跨领域审计（§F）的一位 reviewer 提出的方案是"**删掉 :115 的 exclusion**，让 hive-serde 被 shade 进该模块并把 thrift 重定位"。本报告**采用 C 集群自身的方案**（保留 :115 + 显式 dep + exclusions）：它经两位 reviewer 的工件级验证，且**符合该模块自己的既有约定**——`:114` 把 hive-common 从 hive-metastore 排除、`:145-151` 再显式声明并 pin 版本（Rule 11 一致性优先）。两方案在"必须把 hive-serde 弄进 shade jar"上一致，差别只在手法。

### 风险 & 置信度
**LOW。** 单文件 pom 改动、无 Java 源码改动、**fe-core 不增长**（符合 "fe-core 只出不进" 铁律）；`fe-connector-paimon-hive-shade` 只被 `fe-connector-paimon/pom.xml:130` 消费，`<optional>true</optional>` 阻断向连接器传递；BE 侧 `be-java-extensions/paimon-scanner` 独立用裸 paimon-hive-connector，不受影响。+~916 KB。**先例**：`fe-connector-paimon/pom.xml:143-170` 记录过完全同类的 bug（FIX-PAIMON-HMS-THRIFT-SHADE 把 hive-common 设 optional 导致 hadoop-hdfs-client / DistributedFileSystem 消失，CI 969249），当时的定式就是"把缺的 artifact 带 exclusions 显式加回插件闭包"。

**置信度：高。两位 reviewer 均未推翻。**

### ❗仍未证实
- **修复充分性未证**：日志只能暴露**第一个**失败。`test_paimon_table` 过了 :44 还要继续 CREATE TABLE test01–test05（int/bigint/float/double/string/date/decimal/datetime、主键、分区）。上述分析是三个类的 **depth-1** 引用闭包，**不是**建表全路径的传递闭包。**修复已证能清掉观测到的失败 + 满足 metastore-client 路径的全部直接 hive 引用；未证能把用例带绿。**只有真跑 HMS 才知道。
- **`<clinit>` 中毒是执行顺序依赖的**：失败的 `<clinit>` 会在该 ChildFirstClassLoader 里**永久毒化** MetaStoreUtils（`fe.warn.log:423097` 出现了中毒后形态 `Could not initialize class ...MetaStoreUtils`）。`test_paimon_catalog.groovy:43` 同样用 `"paimon.catalog.type"="hms"` 却通过了（17:51:13→17:54:00，未查明原因）。⇒ **该失败在别的 build 里可能表现为 flaky，尽管在本 build 里是确定的**——不要据此把它当 flake 隔离。
- **建议扫一遍 `fe-connector-hudi`**：fe-core 移除 hive-catalog-shade 是共因。已确认 hudi pom `:101` 只在注释里提到它、**未声明**依赖（不同于 `fe-connector-hms:65` 和 `fe-connector-iceberg:148`），但未深入排查。

---

# D. `test_catalogs_tvf` — DLF 1.0 已删，用例陈旧（**stale-test-case**）

**用例（1）**：`external_table_p0.tvf.test_catalogs_tvf`（groovy:82 的 CREATE CATALOG）

### 机制
`test_catalogs_tvf.groovy:85` 有 `"hive.metastore.type" = "dlf"` → `CreateCatalogCommand.run:91` → `CatalogMgr.createCatalogInternal:553` → `PluginDrivenExternalCatalog.checkProperties:199` → `HiveConnectorProvider.validateProperties:52` → `HmsClientConfig.removedMetastoreTypeError` (`:72-83`，`REMOVED_METASTORE_TYPES = {glue, dlf}` at `:54-55`) → `IllegalArgumentException` → DdlException。

```
fe.warn.log:37752-37753  DdlException: hive.metastore.type = dlf is no longer supported:
  Alibaba Cloud DLF 1.0 as an HMS thrift metastore has been removed. Supported types: hms.
  at PluginDrivenExternalCatalog.checkProperties(PluginDrivenExternalCatalog.java:199)
```
> 误导性 INFO 提醒：`fe.log:163820` 有 `Created plugin-driven catalog 'catalog_tvf_test_dlf' via SPI connector for type 'hms'`——catalog **对象**在 checkProperties 校验之前就构造了，**不代表 CREATE 成功**。

**产品行为完全符合设计**：commit `6c9b491dbcf`「删除 thrift 一代的阿里云 DLF 1.0 metastore 支持」明确记录该代在本分支实测损坏（classloader 回归）、**用户拍板不修直接删**。但 `git show --stat 6c9b491dbcf -- regression-test/` **为空** —— 它只删了 fe-core/connector 的 JUnit，**一个 regression-test 文件都没筛**，这才是本 p0 用例漏网的真实原因（不是 RCA 说的"只删了 DLF 专属测试"）。

**该用例真正测的是 `catalogs()` TVF**，DLF 只是载具：明文属性透传、敏感属性掩码为 `*XXX`、GRANT/REVOKE 权限过滤。关键洞见：**掩码被刻意保留而 feature 被删**——`DatasourcePrintableMap.java:58-69` 注释："Masking must outlive the feature: a DLF catalog created before the removal still replays from the image (rejection deliberately fires at CREATE and at client creation, never during replay) so it remains listable"。掩码按**原始 key** 判定（`CatalogMgr.getCatalogPropertiesWithPrintable:497-500`，`SENSITIVE_KEY` 为 case-insensitive TreeSet），`dlf.secret_key` 显式在 `:66`，`dlf.access_key`/`dlf.secret_key` 另经 OSS alias 进入（`OSSProperties.java:56,65` `sensitive = true`）——**与 catalog 类型无关**。

⇒ 只有两个 DLF **路由** key 是死的，.out 断言的每个属性都还活着。

### 修复 —— **一行删除，不加任何东西**

> **✅ 采纳两位 reviewer 的最小化修正**（原 RCA 的三处改动里只有一处是必需的）

**文件**：`regression-test/suites/external_table_p0/tvf/test_catalogs_tvf.groovy`，**只删 :85**：
```
        "hive.metastore.type" = "dlf",
```
可选（纯死 key 卫生，**非**修复必需，不要当成修复的一部分）：一并删 `:86` `"dlf.proxy.mode" = "DLF_ONLY",`。建议在块上方加一行注释说明 dlf.* 凭证 key 是**故意保留**的（掩码 outlive feature，见 `DatasourcePrintableMap.java:58-69`）。

**`.out` 不改。** 所有 qt 只按字面量筛 `Property ∈ {dlf.catalog.id, dlf.secret_key, dlf.access_key, dlf.uid, type}`，从不选 `hive.metastore.type` / `dlf.proxy.mode` / `hive.metastore.uris`。

> **🔴 不要加 `"hive.metastore.uris"`**（原 RCA 的建议）。URI 校验在 `HiveConnector.java:547-553` 的 `createClient()` 里，属**惰性**路径，CREATE 到不了；且 hive 的 `defaultTestConnection()` 实为 false —— 本次运行自证：同一用例 `:46-54` 用不可达的 `thrift://127.0.0.1:7004` 建 `catalog_test_hive00`，`.out:4` 记录它**成功**。
>
> **🔴 删掉 ES fallback 方案**。原 RCA 的 open question #1（"dlf.uid / dlf.catalog.id 会不会作为未知 key 被拒？"）**已被本次运行的日志证伪**：`CatalogFactory.java:164` 的 `checkWhenCreating()` 与 `:167` 的 `initAccessController()` **都在** `CatalogMgr.java:553` 的 `checkProperties` **之前**执行，而栈显示抛在 checkProperties ⇒ 连接器构造 + preCreateValidation + initAccessController **已经带着全套 dlf.* 属性通过了**。HiveConnector 既不 override `preCreateValidation`（默认 no-op，`Connector.java:272-274`）也不 override `defaultTestConnection`（默认 false，`:254-256`）；存储属性走惰性 supplier（`PluginDrivenExternalCatalog.java:185-187` / `CatalogProperty.initStorageProperties:167-189`）。**删两个 key 不可能让已经通过的更早阶段开始失败。**

**破坏面 = 0**：`rg -l catalog_tvf_test_dlf regression-test/` 只有 .groovy + .out 两个文件；零产品代码改动。单测层已有守门：`fe-connector-hms/HmsClientConfigRemovedTypeTest.java`（`6c9b491dbcf` 中 +82/-82），无需再加 e2e。

**置信度：高。两位 reviewer 均未推翻。**

### 波及面（其余 DLF/Glue 1.0 用例，本 p0 流水线不跑，但各自流水线会同样挂）
`external_table_p2/hive/test_cloud_accessible_oss.groovy:65`、`external_table_p2/refactor_catalog_param/hive_on_hms_and_dlf.groovy:634`、`aws_iam_role_p0/test_catalog_with_role.groovy:86`(glue)、`aws_iam_role_p0/test_catalog_instance_profile.groovy:70,79,89`(glue)、`external_table_p2/iceberg/test_iceberg_dlf_catalog.groovy:36`、`external_table_p2/refactor_catalog_param/iceberg_on_hms_and_filesystem_and_dlf.groovy:747,753`、`external_table_p2/paimon/test_paimon_dlf_catalog{,_miss_dlf_param,_new_param}.groovy:37/38/39`（注意 `_new_param` 名字虽新，用的仍是 **DLF 1.0** `paimon.catalog.type=dlf`，非保留的 2.0 REST 路径）。
**死代码（定义未引用，不会触发）**：`iceberg_on_hms_and_filesystem_and_dlf.groovy:721` `hive_dlf_type_properties`；`iceberg_and_hive_on_glue.groovy:369` `hms_glue_catalog_base_properties`（reviewer 补充）。注意 `iceberg_and_hive_on_glue.groovy:367` 的 `iceberg.catalog.type='glue'` **仍受支持**（`IcebergCatalogFactory.java:299-300`），只删了 glue 的 HMS-thrift metastore。
**需要 owner 拍板**：p2 的 DLF 用例是直接删除（DLF 就是它们的主题，符合 `6c9b491dbcf` 的策略）还是移植。建议删除。

---

# E. `test_hdfs_parquet_group0`（MUTED）— 4 GB 单次分配 + ASAN 基线（**pre-existing，非本分支**）

**用例（1）**：`order_qt_test_11 @ test_hdfs_parquet_group0.groovy:107`，`select count(arr) from HDFS(.../group0/large_string_map.brotli.parquet, format=parquet)`

### 机制
- fixture 磁盘 4325 B，但是著名的对抗性 parquet fixture：footer 里两个 column-chunk size = **2147483749 / 2147483827**（各略超 2^31），brotli 压缩比 ~500000:1。**4 GB 是真实数据，不是尺寸误估**（任务假设"小文件⇒尺寸误估⇒BE bug"**被证伪**）。
- `count(arr)` 本应走 BE 的 levels-only count 路径（`parquet_reader.cpp:608-672`），但 **FE 从不下发 `TPushAggOp::COUNT`**：`AggregateStrategies.java:721-727` 对 `column.isAllowNull() && checkNullSlots.contains(slot)` 直接 `return canNotPush`。
  > **reviewer 更正（结论更强）**：`arr` 可空**不是**因为 parquet footer 标 OPTIONAL。HDFS TVF 的 schema 由 `ExternalFileTableValuedFunction.java:457` `columns.add(new Column(colName, ..., true))` 构造，3-arg ctor 是 `Column.java:242 Column(String name, Type type, boolean isAllowNull)` —— **TVF 推断出的每一列、每种格式、无条件可空**。所以该门 **对 HDFS TVF 的任何列的 count() 永远不下推**。footer 的 repetition_type 根本到不了那个 `Column`。
- ⇒ 走普通读路径 → `MapColumnReader::read:46` → `load_nested_batch:55` 同时加载 key + value reader → `ColumnStr<unsigned int>::insert_data` → PODArray。chars 超 2^31 仅 ~179 B ⇒ `pod_array.h:259-262` 的 round-up-to-power-of-two ⇒ 下一步是 **2^32 = 4.00 GB**。
- 日志精确坐实：`be.WARNING:8719` `W20260715 17:28:12.593909 ... alloc large memory: 2147483648`（=2^31，**成功**）→ `:8772` `W20260715 17:28:18.751199 ... Cannot alloc:4.00 GB ... peak used 2.01 GB ... [ASAN]process memory used 18.54 GB ... limit 21.72 GB`（`be.conf:49 mem_limit=35%` × 62.06 GB）。allocator 等了 1000ms 才放弃（`allocator.cpp:136`）。查询自身只占 2.01/18.54 GB，其余是 ASAN 基线 + 并发 suite。
- **.out 期望是 `2`**（`test_hdfs_parquet_group0.out:115-116`）⇒ 空闲/非 ASAN 机器上 4 GB 分配成功。BE 基线全程剧烈波动（16:45→2.93 / 17:23→12.43 / 17:28→18.54 / 17:29→13.95 GB）⇒ **表现确实 flaky**。

### 非本分支引入（已用正确的分支范围验证）
与上游 merge-base = `c0865b021b0`（389 个分支 commit）。`git log c0865b021b0..fa2fcf4b246 -- be/src/format_v2/ be/src/format/ be/src/exec/scan/` **为空**；TVF suite/.out、`ExternalFileTableValuedFunction.java` 未动；`c0865b021b0:SessionVariable.java:1172` 已是 `enableFileScannerV2 = true`。format_v2 由上游 `3645dc94306 [feature](be) Add file scanner v2 readers (#65046)`（Gabriel, Jul 2）引入。本分支唯一动 `AggregateStrategies.java` 的是 `3d8cf2ca925`（`1 insertion / 3 deletions`：删 `LogicalHudiScan` import + 把三元收敛为 `new LogicalFileScanToPhysicalFileScan().build()`），位于**已允许下推之后**的终端 LogicalFileScan 块内；HDFS TVF 是普通 `LogicalFileScan`（`TVFScanNode extends FileQueryScanNode`），**行为字节不变**，`:721-727` 那道门未被触碰。

同一症状在更早的 build 991951 已被独立定性为与 PR 无关（`plan-doc/tasks/task-list-CLR-991951.md:11`）。mute 在 TeamCity 侧，仓库里搜不到（`regression-conf.groovy:87 excludeSuites = "test_broker_load"`）。

### 修复：**本分支不做任何改动。** 不要阻塞 PR。

> **🔴 原 RCA 的"首选上游修复"（把 null-veto 收窄为 OLAP-only）已被 fix-soundness reviewer 推翻——它会造成静默错误结果，不得落地。**
>
> 其载重前提"file scan 从 def levels 算真实非空计数"**是假的**。BE 只在 `_build_file_aggregate_request`（`be/src/format_v2/table_reader.h:1464-1484`）满足 `mappings().size() == 1` **且** `is_complex_type(...)`（`primitive_type.h:213-215`，仅 STRUCT/ARRAY/MAP）时才填 `request.columns`。否则 columns 为空，而 **`request.columns` 为空时每个 reader 都返回总行数**：`parquet_reader.cpp:601-611` 先累加 `row_group_metadata->num_rows()` **再** `if (request.columns.empty()) return Status::OK()`；`orc_reader.cpp:1984` 同理用 `stripe->getNumberOfRows()`；`delimited_text_reader.cpp:359-389` 完全忽略 `request.columns`、数**行数**。⇒ `AggregateStrategies.java:721-727` 这道门对 file scan 是**正确性守卫，不是 OLAP 遗留物**。
>
> **已证的破坏（本 build 里就跑了的 suite）**：`test_tvf_p0.groovy:36-41 qt_nested_types_parquet` 与 `:43-47 qt_nested_types_orc`，7 列 count，期望 `20926 20928 20978 23258 20962 23258 23258`（`test_tvf_p0.out:37/40`，各值不同 ⇒ 确有 NULL，总行数 23258）。改完后 7 个 mapping ⇒ size()!=1 ⇒ columns 空 ⇒ **全返回 23258**。**静默错数据**。
> 而且原 RCA 声称"已审计 parquet 且安全"——**方向反了**：parquet 自身在常见情形下就返回 row_count，早退恰好在它引用为"count path"的 `608-672` **区间之内**。所以它自己的兜底建议（"收窄到只 parquet"）也救不了。
>
> **同样否决原 RCA 的 fallback**（把用例改成 `select count(1)`）：该 fixture 的存在意义就是 >2^31 字符串处理，`count(1)` 极可能走 row-count 路径一个字符串字节都不读，等于**静默删覆盖**而非稳定它。

若将来真要上游做，正确顺序是两步（**上游工作，不是本分支的 drive-by**）：① BE：`delimited_text_reader.cpp:359` 开头 `if (!request.columns.empty()) return Status::NotSupported(...)`（`NOT_IMPLEMENTED_ERROR` 在 `table_reader.h:941` 被吞、优雅回落普通读路径），使其遵守 `file_reader.h:189-191` 的契约；② FE：只精确开洞给 BE 真正正确处理的集合（单列 + 复杂类型），并配 count(nullable_scalar) 的回归断言。

**置信度：高（诊断）。** **mute 保留**（它正确地在标记一个非确定、非本分支的失败），但**不等于无害**——它掩盖的是"单个 nullable **复杂**列的 count 在 file scan 上白白物化整列"这个（窄得多的）真实缺陷。若立 upstream issue，用**修正后的窄口径**，不要用"count(nullable) 从不走 levels 路径"的措辞（会把 reviewer 引向错数据的改法）。

### ❗仍未证实
- 无非-ASAN 对照运行，"正常机器上会过"是从 .out 期望 `2` + 21.72 GB 上限的算术**推断**，未实证。
- 未查旧 V1 reader（`be/src/format/`）对该 fixture 的内存画像 ⇒ 无法断言 format_v2 (#65046) 是否**回归**了它，还是它一直依赖环境余量。这决定 issue 该按 v2 回归还是长期 planner gap 来立。
- mute 的施加时间/施加人/针对哪个 build **无法从仓库确定**（TeamCity UI/config，需 API）。

---

# F. 【附加发现】不解释任何失败、但被绿色测试掩盖的真实缺陷

**测试状态**：`test_polaris` **PASSED**（17:29:19→17:30:07，在 Success suites 列表里）。

`IcebergWriterHelper.inferFileFormatFromDataFiles():297` 的 `catch (Exception e)` → LOG.warn → 静默 default 到 **PARQUET**（`IcebergWriterHelper.java:291-300`）。

> **🔴 两位 reviewer 一致更正了原 RCA 的头条**：该吞咽点触发 **378 次**（`grep -c 'IcebergWriterHelper.inferFileFormatFromDataFiles():297' fe.warn.log` = 378），其中：
> - **370 次（98%）= `java.lang.UnsupportedOperationException: Transaction tables do not support scans`** ← **主导缺陷**
> - **8 次（2%）= TCCL split-brain ClassCastException** ← 原 RCA 的头条
>
> 而原 RCA 把 "UnsupportedOperationException ×372 未深入调查、最可能藏着第二个吞掉的 bug" 列为 **open question** —— 那 370 条**全部来自它自己正在读的这一行**。它把 2% 的稀有信号当头条，把 98% 的主导信号搁置成未探索。

### F1（主导，370 次）— transaction table 无法 scan ⇒ 格式推断在写路径上**永久死亡**
`IcebergWritePlanProvider.buildSink:400` 传给 `IcebergWriterHelper.getFileFormat:267` 的 `table` 是 iceberg 的 `BaseTransaction$TransactionTable`，`table.newScan().planFiles()`（`IcebergWriterHelper.java:291`）**结构性地必然抛**，与 classloader 无关。⇒ 任何**同时缺** `write-format` 与 `write.format.default` 的 iceberg 表在写入时被**静默当成 PARQUET**。这正是原 RCA 只在 open question 里假设的"静默写错格式"的数据损坏场景，而它是**确定性的、频率高 46 倍、且 TCCL pin 完全治不了它**。
横跨 **37 个 catalog**，包括数个**失败**的 suite（`test_iceberg_rewrite_manifests` 16 次、`test_iceberg_partition_evolution_ddl` 1 次、`test_iceberg_partition_evolution_query_write` 1 次）——⇒ 原 RCA "只影响一个通过的测试" 的定性**事实错误**，会导致错误降级。（无证据表明吞咽**导致**了那些失败。）

**下一步（最高价值）**：查 master 的 `IcebergTableSink` 在等价点传的是不是 **base table**（非事务）。若是 ⇒ 本分支回归，修法是拿 base table 解析格式；若 master 同形 ⇒ 继承缺陷，另立 issue。**本报告未验证此点。**

### F2（8 次）— `PluginDrivenTableSink.bindDataSink:175` 缺 TCCL pin
`planWrite` 未 pin → `IcebergWritePlanProvider.planWrite:201` → `buildSink:400` → `getFileFormat:267` → `resolveFileFormatName:284` → `inferFileFormatFromDataFiles:291` → `planFiles` → S3FileIO → `HttpClientProperties.applyHttpClientConfigurations:227` 经 TCCL 反射解析 `ApacheHttpClientConfigurations`，拿到 fe-core 'app' 副本、与插件 loader 副本对撞 → CCE（`fe.warn.log:43722` 等 8 处，线程 mysql-nio-pool-20，在任何 `executeAuthenticated` pin 之外）。
**本分支引入**：`IcebergWriterHelper.java` 在 master 零 commit，由 `442a1081e6d` 新增；master 上等价逻辑全在 fe-core 单 loader 内，不可能 split-brain。
**近乎决定性的旁证**：`PluginDrivenScanNode.java:609-624` 的 javadoc **字面记录了这个机制**——"iceberg-aws's HttpClientProperties loads ApacheHttpClientConfigurations via DynMethods, whose default loader IS the TCCL..."。作者诊断出了机制、守住了 scan 路径、**漏了 write-plan 路径**。

> **🔴 原 RCA 的 fix sketch 无法编译**：`onPluginClassLoader` 在 `PluginDrivenScanNode.java:625` 是 **private static**、包 `org.apache.doris.datasource`、形参类型 `ConnectorScanPlanProvider`；而 `PluginDrivenTableSink` 在 `org.apache.doris.planner`、持有 `ConnectorWritePlanProvider`。
>
> **✅ 改用插件侧 pin（与原 RCA 相反）**：在 `fe/fe-connector/fe-connector-iceberg/.../IcebergWritePlanProvider.java:201` `planWrite` 的方法体外包既有 pin 惯用法（参照 `IcebergConnector.java:308`、`TcclPinningConnectorContext.java:101`——**iceberg 侧已有** `TcclPinningConnectorContext`，原 RCA 只提了 paimon 的）。理由：①一处覆盖整个 `buildSink→getFileFormat→planFiles` 子树；②**fe-core 零新增**，"fe-core 只出不进" 铁律根本不被触发、无需签字；③无需新增 fe-core public API、无需第三份 `onPluginClassLoader` 副本（`MetastoreEventSyncDriver.java:327` 已是第二份）。原 RCA 选 fe-core 的理由是"任何 connector 的 planWrite 都未 pin，paimon/hudi 有同样潜在暴露"——但它自己承认**没查过** paimon/hudi 的 planWrite 是否碰 TCCL 敏感库。**不要为未经验证的假设扩大 fe-core 表面**。若将来真要做通用 pin，`PluginDrivenTableSink.java:148`（`appendExplainInfo`，同类未 pin，EXPLAIN INSERT 走它）也要一起。

**关于收窄 catch**：`| LinkageError` 分支是**死代码**（`catch (Exception)` 抓不到 Error，NoClassDefFoundError/ExceptionInInitializerError 今天本来就冒出去）。且 master 有**逐字相同**的宽 catch（`master:.../datasource/iceberg/IcebergUtils.java`），所以收窄是**有意偏离 master**，不是"恢复正确行为"。**更要命的是它是载重的**：378 次里 370 次来自当前**通过**的测试，必须继续被吞。⇒ 只能窄到 `catch (ClassCastException e) { throw e; }`，且**必须在 pin 落地并确认 CCE 从 fe.warn.log 消失之后**再做，顺序不可颠倒。

**pin 的未计成本副作用**：今天 8 次 CCE 是瞬时抛错回落 PARQUET；pin 之后 `planFiles()` 会**真的对 S3/REST vended-credential catalog 执行远程 scan planning**——每次对缺格式属性表的 INSERT 都跑一遍（本次运行该点被命中数百次）。正确性中性，但原 RCA "不会改变发给 BE 的字节" 的说法**忽略了延迟/成本变化**。这本身值得单开一个成本 issue。

**验证缺口（关键）**：`test_polaris` **加不加 pin 都过**（8 次 CCE 落在它 17:29:19–17:30:07 的窗口内，它仍 `ScriptContext.groovy:120` succeed）⇒ **它不是这个 bug 的回归闸门**。需要专门构造断言——理想的是"一张既无 `write-format` 又无 `write.format.default` 的 **ORC** iceberg 表，断言实际写出的文件格式"——它能**同时**闸住 F1 和 F2。

---

## 已排查并排除的高频噪音
| 信号 | 结论 |
|---|---|
| `NullPointerException` ×1599 | **红鲱鱼**。1598 条来自 `ExternalRowCountCache.getCachedRowCount:132`，是**设计如此**：`loadRowCount:90-112` 注释 "Null may raise NPE in caller, but that is expected. We catch that NPE and return a default value -1"。fe-core 既有，master 历史（`df200d8cbe1`/`9920e9678f7`/`c31394b21d2`）。只降级为 UNKNOWN_ROW_COUNT(-1) 影响 join reorder。丑，但非 bug、非本分支。 |
| AnalysisException ×7393 / DdlException ×406 / nereids AnalysisException ×428 | 负向测试 |
| ConnectException ×976 / TTransportException ×158 / SaslException ×78 / LoginException ×39 | kerberos + HMS 负向与重试测试 |
| `No backend available as scan node` ×46 | 全部在 16:45:26–16:46:14 的 BE 注册等待循环内；测试 17:02:44 才开始。测试窗口内 0 次。 |
| docker 容器 | hive2/hive3-metastore 零 error/exception/refused；hive3-server 仅 2 条预期的 `Database does not exist`；iceberg-rest 的 5420 条分解为正常 REST 404 类响应（NoSuchView ×1288 / NoSuchTable ×456 / NoSuchNamespace ×30 / CommitFailed ×10 / AlreadyExists ×6 / NamespaceNotEmpty ×3 / Validation ×2），零 OOM / 零 Connection refused。 |
| `DorisConnectorException` ×253 | **未逐条排查**（token 预算）。见下方缺口。 |

---

## 修复优先级 / 建议下一步

### P0 — 直接绿化、无争议、可立即动手
1. **C. paimon `hive-serde`**：`fe-connector-paimon-hive-shade/pom.xml` 加显式 `hive-serde:2.3.7 <optional>` + 10 条 exclusion（去掉两条 no-op），保留 :115。注释写明"缺 12 个类、必须整 jar"。→ `test_create_paimon_table`
2. **D. DLF 陈旧用例**：删 `test_catalogs_tvf.groovy:85` 一行。.out 不动。→ `test_catalogs_tvf`
3. **B-触发器1. row-id 排除**：`PluginDrivenScanNode.java:936` 加 `startsWith(Column.GLOBAL_ROWID_COL) → continue` + 补 `rowIdColumnIsExcludedNoThrow` 单测。→ `test_iceberg_time_travel`, `iceberg_branch_complex_queries`
4. **B-触发器2. sys-table 能力门控**：`resolveSysTableSnapshotPin():1044` 加 `if (!sysTableSupportsTimeTravel()) return Optional.empty();`；同时改掉 `:896-897`、`:1039`、单测注释 `:117-118` 三处被证伪的注释。→ `paimon_system_table`
5. **A. iceberg spec evolution 降级**：`IcebergPartitionUtils.java:659` 插入**名字列表**比较版降级（非 arity 版）+ 3 个单测（arity 变化 / spec-0 零值 / 未演进表不降级的 negative guard）。→ 6 个 iceberg 用例

### P1 — 需要用户拍板，不要擅自动手
6. **B-触发器3. `iceberg_query_tag_branch`**：三选一（**C1 修 binding / C2 暂挂该 case / C3 放宽 guard**）。**推荐 C1**——skew 是本分支 `442a1081e6d` 自造的（上游 `getSnapshot(TableIf)` 单 key、从不返 empty，t1/t2 schema 相同故上游零 skew）。C3 会部分推翻 2026-07-13 已签字的决策、且必须改写 4 个单测。**`.out` 一律不改**（与上游 #51272 字节相同）。

### P2 — 独立跟进，不阻塞本 PR
7. **F1 调查**（最高价值的下一个检查）：为何 `IcebergWritePlanProvider.buildSink:400` 把事务态 Table 传进格式推断？对照 master 的 `IcebergTableSink`。这决定"ORC iceberg 表今天是否正在被静默写成 parquet"。
8. **F2 pin**：插件侧包 `IcebergWritePlanProvider.planWrite:201`。**pin 落地并确认 CCE 消失后**再考虑窄化 catch 到 `catch (ClassCastException e) { throw e; }`（去掉死的 LinkageError 分支）。补一个 ORC-无格式属性表的写格式断言，一箭双雕闸住 F1+F2。
9. **E**：本分支零改动。若立 upstream issue，用修正后的窄口径。mute 保留但给它一个 owner + 退出条件。
10. **D 波及面**：清理 p2/aws_iam_role_p0 的 9 处 DLF/Glue 1.0 用例 + 2 处死代码（需 owner 决定删还是移植）。

---

## ❗仍未证实 / 需要真实重跑（诚实清单）

1. **A / B 的"修完就绿"全部未证**。每个 suite 在第一个 query 就 abort，下游断言（`partition_evolution_ddl` 约 22 个 qt_、`$partitions` 系统表断言等）**在本分支从未执行过**。修复只保证"不再 crash"。
2. **A 的残留漏洞**：分区源为**嵌套字段**时，`currentNames` 与 fe-core 真正保留的 `types` 不一致（fe-core 在 `PluginDrivenExternalTable.java:519-528` 再过滤一次），checkState 仍会抛。6 个失败表都是顶层列故不阻塞，但**不得**用 "matches EXACTLY" 的假理由盖过它——要么明确 out-of-scope，要么立 issue。
3. **A 未审计 paimon/hudi/hive 是否可能经同一 fe-core LIST 路径产生异构 arity**。checkState 是通用的、对它们同样会触发。相信它们无此类 spec-evolution 概念，但**未证明**。
4. **B/C3 的关键未知**：若放宽 guard，connector 的 projection push-down 是否容忍"请求一个在该快照 schema 中不存在的列"（vs NPE / field not found）—— **无人验证过**，这是偏好 C1 的实质理由。
5. **C 的修复充分性未证**：只做了三个类的 depth-1 引用闭包，不是建表全路径的传递闭包。用例过了 :44 还要建 test01–test05。只有真跑 HMS 才知道。
6. **C 的顺序依赖**：`MetaStoreUtils.<clinit>` 中毒是 loader 生命周期级的，`test_paimon_catalog` 同用 hms 却通过（未查明原因）⇒ 该失败在别的 build 里可能**看起来像 flake**。不要据此隔离。
7. **F1 是否为本分支回归 未知**（见 P2 #7）。
8. **F 的 scope 未界定**：`inferFileFormatFromDataFiles` 只是**恰好会 catch** 的那个调用点。从未 pin 的 `bindDataSink → planWrite` 线程可达的其它 plan-time iceberg S3/FileIO 调用同样暴露，且会**直接抛而非被吞**——**未枚举**。
9. **`DorisConnectorException` ×253 未逐条调查**（token 预算）。在迁移分支上这是"SPI 方法未实现"的典型签名，且不被 13 个已知失败解释。**这是第二个被吞 bug 最可能的藏身处**，建议照 §F 的方法扫其 top stack frame。（`UnsupportedOperationException ×370` 已被 reviewer 解决 = 全部是 F1。）
10. **E**：无非-ASAN 对照；未查 V1 reader 的内存画像；mute 的 provenance 需 TeamCity API。
11. **iceberg-rest 的 10 条 CommitFailedException + 2 条 ValidationException** 未与 17:31–17:36 的 iceberg 失败逐行时间关联。它们是应用级 REST 响应（症状），不改变任何结论，但 iceberg 集群的 owner 应确认它们是那些失败的**后果**而非独立信号。
12. **工具告警（值得记录）**：本机的 `rg` 存在**静默改写匹配文本**的行为（把 `planWrite` 渲染成 `n`、`ConnectorWritePlanProvider` 渲染成 `ln`，并对普通 `grep` 能答的 suites 检索返回空）。**本报告中任何来自 rg 的"零命中"结论都不可信**；载重的否定结论已用 `grep` 复核。后续排查请优先 `grep`。
13. **本次运行 13 个失败中，本报告的 A/B/C/D 共解释 12 个，E 解释第 13 个（muted）。F 解释 0 个** —— 它是被绿色测试掩盖的独立缺陷，不应作为 996541 的根因呈现（原 cross-cutting RCA 的 `verdict=product-code-bug` / `confidence=high` 顶层字段在这一点上是范畴错误，已在此更正）。