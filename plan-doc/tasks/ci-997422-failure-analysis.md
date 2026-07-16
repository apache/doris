# CI 997422 失败分析（Doris_External_Regression，PR 65474 @ `6a450c9fa79`）

> **口径**：TeamCity `997422` = `10 failed (8 new) + 551 passed + 2 muted`，实际失败 occurrence **12 条**（10 failed + 2 muted）。
> **CI 测的 commit `6a450c9fa796beb405aea80abefc9c3baad52cc6` == 当时本地 HEAD**，所以「是否已被最近 commit 修过」有确定答案：**4 个根因全部 live，无一已修**。
> 方法：18 agent（5 路 recon + 每路 3 lens 对抗复核 + synthesizer），外加本人独立复核关键结论。

---

## 🔑 定性：**不是集群故障**，别去查宕机/OOM

- BE **单次启动、优雅退出**：`be.out` 仅有退出时的 LSAN leak summary（`SUMMARY: AddressSanitizer: 4584482 byte(s) leaked in 78 allocation(s)`），**零** `SIGSEGV` / `SIGABRT` / `CHECK failed`。
- `dmesg.txt` **无 OOM-killer**。失败时刻宿主机仍有 **19.03 GB** 空闲。
- 551 通过。**12 个失败 = 4 个独立根因**，其中 **9 个是同一个 bug**。

---

## ✅ 上一轮（996541）的修复**已验证生效** —— 不是回炉

`test_iceberg_time_travel`、`iceberg_branch_complex_queries`、`paimon_system_table`、`test_catalogs_tvf`、6 个 iceberg spec 演进用例**本轮全部未再出现**。
⇒ `181e7c14459` / `bd6fdf7009a` / `270bd11f4da` / `023f8d55e41` / `4f8b35c2126` 生效。本轮是给 `4f8b35c2126` 做的 e2e 验证（HANDOFF 的 TODO 9），**它验出了自己引入的回归**（下面 A+B）。

⚠️ **易混点**：`bd6fdf7009a` 处理的是 `__DORIS_GLOBAL_ROWID_COL__`（topn lazy materialization 合成列）；本轮 A+B 是 `__DORIS_ICEBERG_ROWID_COL__`（iceberg 写路径合成列）。**两个不同的列、不同的 bug**，勿当成同一个反复修。

---

## A+B（9 个用例）— 计划路径丢了连接器的合成写列 · **已修 `35cf72cce91`**

**分类**：code-bug（真实回归）。**引入**：`4f8b35c2126`。

**机制**（A、B 是同一个根因的两种门）：
`PluginDrivenExternalTable:665` 只 override **0-arg** `getFullSchema()`，合成写列的追加**只在那里**。`4f8b35c2126` 把 `LogicalFileScan.computePluginDrivenOutput():223` 从 0-arg 改调**新增的 1-arg** `getFullSchema(Optional<MvccSnapshot>)`；该重载**只声明在 `ExternalTable:188`、无任何子类 override**，是裸的 schema-cache 读 ⇒ 追加被静默绕过。二者是 **overload 而非 override**，`@Override` 抓不到。

- **A**（DML 门）：`IcebergUpdateCommand` 发 `UnboundSlot(Column.ICEBERG_ROWID_COL)`，relation 输出里没有 ⇒ `Unknown column '__DORIS_ICEBERG_ROWID_COL__' in 'table list' in PROJECT clause`（fe.warn.log:126983 起）。7 个用例（含 2 个 `test{exception}` 因错误优先级变化而失配）。
- **B**（show-hidden 门）：`SELECT *` 少**恰好一列**。runner log 实测 `expected: <6> but was: <5>`、`firstRow=[1, Alice, 25, 0, 1]` —— `_row_id`/`_last_updated_sequence_number` 仍在（走 schema cache），**只丢 request-scoped 那列**，与代码切分逐字吻合。

**判据（三重独立）**：
1. 结构：`git log -S"getFullSchema(Optional<MvccSnapshot> snapshot)"` 只返回 **1 个** commit（`4f8b35c2126`），且 1-arg 全仓**唯一调用点** = `LogicalFileScan:223` ⇒ 回归不可能早于它。
2. 运行时：同 session 相隔 116ms，**DESC（0-arg，仍 override）返回 6 列，`SELECT *`（1-arg）返回 5 列**（runner log 76905 vs 76908）。
3. `4f8b35c2126` 自陈 `未证：e2e 未跑` —— 只靠单测合入，而单测无一覆盖计划路径上的 hidden-column 门。

**修法**：补 1-arg override，两个 arity 共走私有 `appendSyntheticWriteColumns()`；snapshot 只喂 base schema 读取（合成写列是 request-scoped 非 version-scoped）。版本感知完整保留（`super.getFullSchema(snapshot)` 仍虚分发到 `PluginDrivenMvccExternalTable:516`）。

**单测选址是要点**：**不能**放 `LogicalFileScanTest` —— 它 `Mockito.mock` 并直接 stub 1-arg，mock 在真实方法体前拦截，**永远测不出「缺 override」**，这正是回归当初能绿着合入的原因。放在 `PluginDrivenExternalTableTest`（`CALLS_REAL_METHODS`）。变异（删 1-arg override）→ 红在 `expected: <3> but was: <2>`，即 CI 症状本身。

**未证**：只解开列数断言。v1 suite 在 `:98` abort，其后 `:101` "row-id column must be populated" 与 v3 取值断言**在本分支从未执行过**。

**已知潜伏洞（本次故意不修）**：合成 row-id 的 `uniqueId = -1`（`IcebergWritePlanProvider.buildRowIdColumn` 用 6-arg ctor；`ConnectorColumnConverter:89-91` 只对 `>= 0` 回填），故列回到输出后，L17 guard 会退化成按 name 匹配且无法在 pinned schema 里 resolve。今天不可达（pinnedSchema 仅在显式时间旅行非空，且无用例把 show_hidden/DML 与 `@tag`/`@branch`/`FOR ... AS OF` 组合）。若将来要修，**按通用属性（schema-cache 来源）判，绝不按 iceberg 列名**。

---

## C（1 个）— L17 guard 对 sys-table 是范畴错误 · **已修 `6320389dc06`**

**分类**：code-bug。**引入**：`22516845ca3`（guard 本身）。**先前修复不完整**：`270bd11f4da`。

**机制**：sys-table 的 pin 按构造从**源表**解析（`resolveSysTableSnapshotPin`），`pinnedSchema` = 源表 `{id,name}`，而 `desc.getSlots()` = sys 表 `{file_path,pos,row,spec_id,delete_file_path}`。两者比较**永远无法 resolve** ⇒ 抛假的 "multiple versions"，被 `getOrLoadPropertiesResult` 重新包装成 `Failed to pin MVCC snapshot for plugin-driven scan`（wrapper 盖住了真错）。

**判据（自然实验）**：runner log 68904-68924，09:45:20 同一张 sys 表 `desc` / `count(*)` / `where pos>=0` / `where spec_id=0` / `where delete_file_path is not null` / `select \`row\`` **全部成功**，只有 09:45:21.314 的 `for version as of` 失败 ⇒ **pin 是唯一变量**，排除 TCCL / schema-load / timeout。

**⚠️ 这不是重复修，是延期理由被证伪**：`270bd11f4da` 碰过这段代码，**故意只修 paimon 半边**，留 KNOWN GAP 注释（逐字预言了本次失败），理由「iceberg sys-table 时间旅行今天零 e2e 覆盖」。**该前提当时即为假** —— e2e 是上游 `0814e49bea7`(#65135) 的 `test_iceberg_position_deletes_sys_table.groovy:549-560`，早于本分支。本轮就是那次待跑的验证到来。代码把自己的 bug 写在注释里了。
（细节：该测试经 2026-07-15 rebase 进来，拓扑上早于 guard、时间上晚于 guard ⇒ 测试与 guard 很可能从未在同一次绿跑中共存 ⇒ **首次观测到的潜伏破损**，非二次破坏。）

**修法**：guard 第三参 name → `TableIf`，在 null-pinnedSchema no-op 旁加 `table instanceof PluginDrivenSysExternalTable` 排除；删除过期 KNOWN GAP 段。**拓宽签名而非在调用点判**，是为了让排除可被静态 harness 变异验证（条件若留在 private `pinMvccSnapshot()` 则测不到）。

**安全性**：`PluginDrivenSysExternalTable`(:41) 与 `PluginDrivenMvccExternalTable`(:85) 是**兄弟类** ⇒ 正常 MVCC 表不可能命中排除，guard 强度不减。paimon 能力位 false ⇒ pin 解析为空 ⇒ 本就到不了 guard ⇒ `270bd11f4da` 完好。**`applyMvccSnapshotPin`(:853) 故意不动** —— 它必须对 sys-table 保持武装，否则 pin 被静默丢弃、时间旅行读最新。

**变异**：删排除 → `sysTableIsExcludedNoThrow` 红；把排除放宽到共同父类 `PluginDrivenExternalTable` → **5 红**（含新增的 `normalMvccTableWithSameShapeStillThrows`）。
**两个 vacuity 陷阱已钉进注释**（初稿踩了第二个、以错误理由通过）：pinnedSchema 必须非 null；bound 列必须 `uniqueId == -1` —— 若给 sys 列与源表碰撞的 field-id（`file_path@1` vs `id@1`）会**按 field-id resolve 成功而永不抛**，测试即失效。按 name 匹配也才是真实情况（CI 正是在 'file_path' 上按 name 撞 `{id,name}` 而抛）。

**未证**：去掉抛出只解开 init。真正绿还需 iceberg 连接器 `$position_deletes` planner 认这个 pin（`doPlanPositionDeletesSystemTableScan` 读 `handle.hasSnapshotPin()`，由 `IcebergConnectorMetadata.applySnapshot` 喂）—— 已 trace 未执行。suite 后半段 `:562-568`（源表跨 ADD COLUMN 时间旅行）在本分支从未跑过。**可能需要第二轮**。

**故意不打包进来**：`PluginDrivenScanNode:1213`（`scannedPartitionCount` 在 `$position_deletes` 上触发，HANDOFF gap ⑤）。同文件同 suite，但与 2026-07-13 `selectedPartitionNum` 签字冲突，**需用户先裁决**。

---

## D（1 个）— paimon HMS 缺 `serdeConstants` · **已修 `a4cba35725c`**

**分类**：code-bug（打包）。**引入**：`9a10ece30c8`（2026-07-15，跑 CI 前一天）触发；潜伏前提 `c276e955683`（2026-06-20）。

**机制**：`fe-connector-paimon-hive-shade/pom.xml:115` 排除了 `hive-serde` 却**未像 `hive-common`(:145-151) 那样重新声明**。`org.apache.hadoop.hive.serde.serdeConstants` 被 `hive-metastore:2.3.7` 的 `MetaStoreUtils.<clinit>` 读取。潜伏一个月无害 —— fe-core 的 `hive-catalog-shade` 经 parent loader 兜底；`9a10ece30c8` 删掉那个 jar ⇒ **两个 loader 都取不到** ⇒ paimon `RetryingMetaStoreClientFactory` 反射逐个试 `getProxy(...)` 全部 `NoClassDefFoundError` → `ExceptionInInitializerError` → 全被 suppress ⇒ 只剩泛化文案 `Failed to create the desired metastore client`。

**⚠️ 推翻了本项目的既有先验**：**不是 TCCL split-brain**。TCCL pin 在栈里存在且正确。`ClassNotFoundException` 是**缺失**；split-brain 表现为 `ClassCastException`/`LinkageError`。也不是容器 flake（确定性，且与 `newFailure=false` 一致）。

**修法**：重新声明 `hive-serde:2.3.7`（= 排除项摘掉的那个版本），比照 hive-common 的既有范式（放 shade 模块而非连接器，使 serdeConstants / HiveConf / HiveMetaStoreClient 同处一个 hive 2.3.x 制品并共享 paimon-private thrift relocation）。

**排除项是实测得出、非拍脑袋**：
- `parquet-hadoop-bundle`（闭包大头）——修后实测 `org/apache/parquet/` **0 条**（`org/apache/paimon/shade/parquet` 是 paimon-format 自带、早于本次）。
- `servlet-api 2.4` / `jsp-api 2.0` —— hive-serde 的 web 件，对不提供 HTTP 的 metastore client 是死重（55 类 / ~64KB）；**更要紧的是** servlet-api 2.4 会把**同一个 `javax/servlet` 包**发第三份（已有 jetty-orbit 3.0.0 与 javax.servlet-api 3.1.0 经 paimon-hive-connector / hadoop-client-api 到场）⇒ 解析冲突风险。既有那两份**不动**（Rule 3）。

**验证（逐条实跑，jar 列表本身不足以证明）**：
- 基线 jar `99,235,331` B、`serdeConstants` 计数 **0** → 修后 `102,356,991` B（**+~2.98MB** = hive-serde 本体）、计数 **1**。
- **classload 冒烟**（非 zip 列表）：`Class.forName` 从 shade jar 加载 serdeConstants 并跑通 `<clinit>` → `STRING_TYPE_NAME=string`。
- **端到端**：从**构建出的 plugin zip 自己的 `lib/` jar** 加载成功；全 zip 内 serdeConstants **恰好 1 份**（无重复类隐患）。

**未证**：未跑 e2e，真正的闸门是 CI 的 `test_create_paimon_table`。本次只证「缺的类回来了且能加载」。

---

## E（1 个 muted）— **建议不在本分支修**

**分类**：infra-flake / 上游既存。**不是我们的回归。**

**机制**：`large_string_map.brotli.parquet` **真的**含一个 2.000 GiB 字符串列（footer 实测 `arr.key_value.key total_uncompressed_size = 2,147,483,749` = 2³¹+101，~642,000:1 brotli 比；`num_rows=2`）。`count(arr)` 是 count(col) 不走 row-group 计数下推 ⇒ 全量物化 ⇒ `ColumnStr<uint32_t>` 的 PODArray 必须 2GiB → **4GiB** 几何增长（`round_up_to_power_of_two_or_zero` → 2³²，与 "Cannot alloc:4.00 GB" 逐字吻合）。ASAN BE 此时已占 ~13GB / **21.72GB** 上限。

**决定性事实**：`mem_limit=35%` 来自**上游** `51e44133b1d`(#65351, 2026-07-09)，且 `git merge-base --is-ancestor 51e44133b1d master` → **YES** ⇒ **master 自己的 external ASAN pipeline 跑在同样的 cap 下，本问题在 master 同样复现**。

**不是 OOM**（宿主机余 19.03GB，只有 `mem_limit` 那一支触发）、**不是泄漏**（RSS 40s 内 18.20→13.78GB，之后 ~20min 在 14–17GB 无再失败）、**不是过期用例**（`.out` 期望 `2` = footer `num_rows`，查询是被中途 kill 而非返回错值）。

**建议**：保持 mute + 记录理由。理由：(1) 没有真的 OOM 可修；(2) 修复目标是上游 CI conf 与上游 BE reader，都在 catalog-SPI FE 迁移范围外；(3) 在本分支改 `regression-test/pipeline/external/conf/be.conf` = 在一个无关的 FE 重构 PR 里**静默 revert 上游决定**，并掩盖该 cap 本要抓的内存回归。Rule 3 + Rule 12 均指向「如实报告，不要打补丁」。

---

## 🧰 本轮踩到的构建/验证坑（下轮直接复用）

1. **maven build cache 会静默跳过 surefire**：日志里是 `Skipping plugin execution (cached): surefire:test`，此时 **BUILD SUCCESS 是空的**（surefire 报告文件是上一次的陈旧产物）。所有测试必须加 **`-Dmaven.build.cache.enabled=false`**。本轮第一次跑就中招（"BUILD SUCCESS" 但 0 个测试真跑）。
2. **`mvn ... | tail` 后的 `$?` 是 `tail` 的**，不是 maven 的。要么重定向到文件再取 `$?`，要么读 `BUILD SUCCESS`/`BUILD FAILURE` 行。
3. **`surefire:test` 独立 goal 解析不了 `${revision}`**，必须走 `test` 生命周期 + `-am`；上游模块无匹配测试时需 `-DfailIfNoTests=false`。
4. **`hive-serde` 闭包首次需要联网**（`javax.servlet:servlet-api:2.4` 不在本地仓），`-o` 会失败。
5. **动码前先探并发 session**（本轮已探：无并发、HEAD 未动）。
6. `regression-test/conf/regression-conf.groovy` 在工作区**本就是脏的**（session 开始前即 `M`），三个 commit 均**未包含**它。

---

## 📌 交接：下一轮

1. **重跑 CI**（唯一真闸门）。预期：A+B 的 9 个解开列数断言；C 解开 init；D 的 paimon HMS 恢复。
2. **C 可能要第二轮** —— 见上「未证」。
3. **E 保持 mute**，把上面的理由落到 mute 注释里。
4. **待用户裁决**：`PluginDrivenScanNode:1213` scannedPartitionCount vs `selectedPartitionNum` 签字冲突。
5. A+B 的「潜伏洞」（合成 row-id `uniqueId=-1`）已记录，勿顺手按列名修。
