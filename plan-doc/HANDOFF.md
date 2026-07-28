# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = 修 TeamCity **CI 997422**（Doris_External_Regression）的失败用例。

---

# 🆕🆕🆕 最新一轮（2026-07-28b）：rebase onto `2faf819fa89` —— 上游 #65870 是**行为中性**的 iceberg 测试补强

> `git pull --rebase upstream-apache master`，73 commit onto **`2faf819fa89`**（上游新增 **2** 个 commit）。
> 备份点 tag `backup-before-rebase-0728b` = rebase 前 HEAD `d55a4fb458c`。**未 push**。

## 上游 2 个 commit

| commit | 内容 | 与本分支 |
|---|---|---|
| `0127314e7a5` #65572 | BE `hash_map_context.h` fixed-key packing 的 `restrict` 别名优化 | 纯 BE，0 冲突 0 迁移 |
| `2faf819fa89` #65870 | `[test](iceberg)` 把 `IcebergScanNode.createScanRangeLocations` 里的 schema 初始化抽成 `@VisibleForTesting initializeIcebergSchemaInfo()`，加一段分区演进注释 + 1 个 FE 单测 | 唯一冲突源 |

## 冲突 1 处 / 2 文件（落在第 11/73 个 commit = P6 iceberg cutover `3560db37ef8`）

`fe/fe-core/.../iceberg/source/IcebergScanNode.java` + `IcebergScanNodeTest.java`，类型是 **content 而非 modify/delete**：
P6 那一版只是把这两个文件**削薄**（-578 / -518 行），真正 `git rm` 发生在更靠后的 `282f9f6a839`（#65473 hive cutover）。
所以冲突发生在一个「注定要被删掉」的中间态上。

**解法 = 整体取分支侧**（`git checkout --theirs`），并已逐字节校验 == `3560db37ef8` 的原 blob，即完全复现上一轮 rebase 的结果。
⚠️ 这里 auto-merge 已经产生了一个**编译不了的缝合怪**（上游新方法签名 `Optional<Map<..>>` + 我们旧的 4 参 `initSchemaInfoForAllColumn` 调用），
只看冲突标记内的两段会漏掉——必须整文件还原。

**守门证据**：`git diff backup-before-rebase-0728b HEAD` = **只有 `be/src/exec/common/hash_table/hash_map_context.h` 一个文件**，
即 FE 侧最终树与上一轮逐字节相同（#65870 的 fe-core 改动随文件删除一起蒸发，符合预期）。

## 移植结论：**0 能力需要迁移**（结论有实证，不是"看着像"）

#65870 自己写明 "does not change scan behavior"。它的实质是给一条不变量补文档 + 补测试：
**reader schema（`history_schema_info`）必须带全部当前表列**，理由从 #65502 的 equality-delete 扩展到**分区演进**
（新 spec 把某列变成 identity 分区列，但老 spec 写出的文件里该列仍物理存在）。

本分支连接器的字典策略是**按 requested scan slot 裁剪** + 3 个明示超集（时间旅行 pin / Top-N 惰性物化 / equality-delete key），
与上游的"全列"不同。**但分区演进不需要第 4 个超集**，链路证据：

1. `IcebergPartitionUtils.getIdentityPartitionColumns` 对 `table.specs()`（**所有** spec）取并集 → `path_partition_keys`
   （与上游 `IcebergUtils.getIdentityPartitionColumns` 逐字节同构）；
2. `FileQueryScanNode.classifyColumn`（`scan/FileQueryScanNode.java:244`）把命中 `path_partition_keys` 的列判为
   `TColumnCategory.PARTITION_KEY`；`isFileSlot(PARTITION_KEY) == false`；
3. BE 只对**要从文件里解码的 slot** 查这本字典 → identity 分区列无论投影与否都不会走字典。
4. 上游同理：`IcebergScanNode.getPathPartitionKeys()` 就是 `getOrderedPathPartitionKeys()` = 同一个并集。
   所以上游的"全列字典"对分区演进是**冗余保险**，不是本分支缺失的能力。

已在 `IcebergScanPlanProvider` 字典构造处补上等价注释（对应上游那段新注释），把上面这条推理钉在代码里。

## 测试迁移

上游唯一新增用例 `testPartitionEvolutionKeepsNonFileSlotInReaderSchema` **不能照搬**——它断言的是 fe-core 的"全列字典"，
而本分支连接器有意裁剪；照搬会写出一个断言错误不变量的测试。

改为把**本分支真正依赖的那条性质**补测（原来只有单 spec 覆盖，多 spec 并集**零覆盖**）：
`IcebergPartitionUtilsTest.identityPartitionColumnsUnionEverySpecUnderPartitionEvolution` —— 真 `InMemoryCatalog` 表
`identity(payload)` → `updateSpec().removeField("payload").addField("int_col")`，断言 `path_partition_keys == [payload, int_col]`。

- **变异 RED**：`table.specs()` → `table.spec()` ⇒ 得到 `[int_col]`，**只有新用例挂**（其余 62 例全绿，证明旧单 spec 用例查不出）。复原后全绿。
- `fe-connector-iceberg` **1152/1152**（上一轮基线 1151，5 skipped 为既有）；`-am install` BUILD SUCCESS。
- **e2e**：上游本次没加任何 regression suite，本轮**无 e2e 需要你跑**。

## ⏭ 下一个 session

1. `git push` 回 `upstream-apache/branch-catalog-spi`（**上一轮 + 本轮都未 push**），跑 External Regression。
2. 其余待办见下面 07-28 那一轮的清单（`StoragePropertiesConverter` 注释清理、上游 #66004 `NodeInfo` 双层嵌套回归上报）。

---

# 🆕🆕 上一轮（2026-07-28）：rebase onto `749290cb041` —— 移植 #65984 paimon `@options` 查询级动态选项

> `git pull --rebase upstream-apache master`，72 commit onto **`749290cb041`**（上游新增 4 个 commit）。
> 备份点 tag `backup-before-rebase-0728` = rebase 前 HEAD `bae5aae719c`。**未 push**。

## 上游 4 个 commit：只有 1 个碰外表

`#65880`(nereids repeat 去重)、`#66056`(BE parquet 稀疏 RF)、`#66118`(CI) 与本分支无交集。
唯一对手：**`d74ebad82e2` (#65984) “Support query-level dynamic options”** —— paimon 新增
`@options('scan.snapshot-id'='1', ...)` 关系级动态选项，39 文件（含 390 行新类 `PaimonScanParams`、
nereids 解析链、5 个 e2e suite）。

## 冲突 6 处 / 72 commit（range-diff 核：66 个逐字节未变，7 个改写 = 6 处冲突 + 1 处纯上下文位移）

| # | 文件 | 类型 | 解法 |
|---|---|---|---|
| 3 | `LogicalFileScan.computeOutput` | content | 上游把重复 slot 构造抽成 `computeOutput(List<Column>)`，我们同锚点加了 `computePluginDrivenOutput()`（函数体逐字节相同）→ 并集，后者委托前者 |
| 8 | `paimon_system_table.groovy` | content | 上游**删掉**了那个断言块（被它新增的 `does not support INCR/OPTIONS` 取代），我们只改了它的文案 → 取上游的删除 |
| 9 | fe-core paimon 子系统 10 文件 | modify/delete | `git rm` 保留删除，能力另行移植 |
| 11 | `BindRelation`/`LogicalFileScan`/`LogicalFileScanTest` | content | 上游给 fe-core 加 paimon 臂 vs 我们 P6 删 fe-core iceberg/paimon → 取我们的结构 |
| 29 | `StatementContextTest` | content | 上游改了 3 个 HMS 测试的上下文，我们的 hive cutover 整体删除它们 → 取删除 |
| 56 | `FileQueryScanNode.setColumnPositionMapping` | content | 两侧各加一个「列位置映射数据源」钩子：上游 `getFileColumnNames():List<String>` vs 我们 `getPinnedFullSchema():List<Column>` → 取我们的（语义更强：按本引用 pinned 快照解析），删上游那个已成死代码的钩子 |

## 移植结论：**中性层白拿，paimon 层整体搬进 fe-connector-paimon**

### A. fe-core 中性层（auto-merge 已进来，0 改动，含 3 个上游 bug 修复）
`TableScanParams`(OPTIONS 类型)、`LogicalPlanBuilder`(`@options` 解析 + 命令表引用拒绝)、
`BindRelation.rejectScanParamsOnCte`、`MaterializedViewUtils`(有 scanParams 的 relation 不做 MV 改写)、
`ExecuteCommand`(PREPARE 复用 relation 时重置已解析选项)、
**`UnboundRelation.withGroupExpression/withIndexInSql` 不再把 scanParams 置 null**（真 bug 修）、
**`ExternalTablePreloadInfo.shouldPreloadLatestSnapshot` 去掉 `&& !hasNonLatestRelation`**（真 bug 修）。

### B. `@options` 端到端（`ConnectorTimeTravelSpec` 天然承载）

架构上 `@options` 就是第 6 种时间旅行选择器，映射到既有 SPI 管道，**无需新管道**：

```
@options(...) → TableScanParams(OPTIONS)
  → PluginDrivenMvccExternalTable.toTimeTravelSpec → ConnectorTimeTravelSpec.Kind.OPTIONS(raw map)
  → PaimonConnectorMetadata.resolveTimeTravel: validateOptions + resolveOptions(把可变选择器冻结成
    不可变 pin) → ConnectorMvccSnapshot{snapshotId, schemaId, properties=已解析选项+family marker}
  → applySnapshot → handle.withScanOptions
  → PaimonScanPlanProvider.resolveScanTable → PaimonScanParams.applyOptions(table, opts)
```

新增：SPI `Kind.OPTIONS` + `options()/getOptions()`；`ConnectorCapability.SUPPORTS_SCAN_PARAM_OPTIONS`
（paimon 声明）；`PluginDrivenExternalTable.supportsScanParamOptions()`；`BindRelation.validateOptionsTarget`
改成**能力驱动**（不再 `instanceof PaimonExternalTable`）。

**⚡ 上游两条改动本分支「结构上已覆盖」，无需移植**（这是本轮最大的省力点）：
1. `LogicalFileScan.initialSelectedPartitions` → `SelectedPartitions.NOT_PRUNED`：我们的 ctor 早已按
   **本引用自己的版本** `MvccUtil.getSnapshotFromContext(table, tableSnapshot, scanParams)` 取分区，
   而显式 pin 分支返回**空分区图**（= scan-all），效果等价。
2. `LogicalFileScan.computeOutput` 的 `getFullSchema(scanParams)` 臂：`computePluginDrivenOutput()`
   已走同一条版本感知 schema 解析。
3. 上游 `TableScanParams.getOrResolveMapParams`（每 relation 只解析一次）：我们由
   `StatementContext.versionKeyOf` 提供 —— key 含 `sp.getMapParams()`，所以
   `t@options(id=1) JOIN t@options(id=2)` 两侧各自解析，同 map 的两次引用共用一次解析。

### C. paimon 系统表 scan-param **大幅放宽**（本轮真正的新增能力）

#65984 把「paimon 系统表拒绝一切 scan param」改成**按系统表类型的能力矩阵**：
`@incr` 支持 `audit_log/binlog/partitions/ro/row_tracking`；`@options` 支持
`audit_log/binlog/manifests/partitions/ro/row_tracking/table_indexes`（`files`/`buckets` 内部仍查 latest 元数据，必须拒）。

我们这边的落法（管道**本来就有**，只是 paimon 没开）：
- `ConnectorScanPlanProvider` 新增 `supportsSystemTableIncrementalRead(sysTableName)` /
  `supportsSystemTableOptions(sysTableName)`（原来只有一个连接器级 `supportsSystemTableTimeTravel()`，
  表达不了「同一连接器不同视图不同答案」）；
- `PluginDrivenScanNode.checkSysTableScanConstraints` 按 param 类型分别问，**报错文案点名缺哪个能力**；
  `resolveSysTableSnapshotPin` 的放行条件与守门**同形**（否则会先炸出 L17 假错误，掩盖真文案）；
- `PaimonConnectorMetadata.applySnapshot` 对 **sys handle** 不再原样返回，改为透传 scan options
  （@incr / @options 都经这条），并调 `validateSystemTableOptions` 拒掉系统表包装器扛不动的
  `scan.file-creation-time-millis`。

### D. 顺带的独立修复（3 项）
1. `PaimonIncrementalScanParams.applyResetsIfIncremental` 的 null 重置从 2 个键
   （`scan.snapshot-id`/`scan.mode`）扩到**整个 inherited read-state family**（含各 fallback 键）——
   base 表可以合法持久化 `scan.tag-name`/`scan.timestamp-millis`，与 `incremental-between` 合并会抛或读错版本。
2. `$binlog@incr` 的 `COUNT(*)` 下推否决：binlog reader 把 UPDATE_BEFORE/AFTER 打包成 1 行，
   DataSplit 的物理 merged 行数不是合法 `COUNT(*)`。
3. `getSysTableHandle` 的 forceJni 集合从 `binlog/audit_log` 扩到含 `row_tracking`，
   并统一到 `PaimonScanParams.requiresPaimonReader`（单一事实来源）。

### E. 3 项**有意不移植 / 反向裁决**（Rule 7/12，务必别在下一轮又"补回来"）

| 项 | 判断 | 依据 |
|---|---|---|
| `jni.enable_file_reader_async` 加进 BACKEND_PAIMON_OPTIONS | **不移植** | 全仓（**含 upstream master 自己**）无任何 BE/JNI 消费方 —— #65955 删掉了 `buildTableOptions`。上游这行是空转的。上一轮 07-27b 已就此立了守门测试 `backendOptionsDropRetiredFileReaderAsyncOptOut`，我先加后回退（它红了，守门有效） |
| `StatementContext` 的 `selectorFreePaimonOptions` preload 豁免 | **不移植** | 需要连接器在**绑定之前**回答「这些 option 是否选版本」；本分支对 `@branch/@tag/@incr` 一律按 non-latest 处理。仅影响 preload 预热**时延**，不影响正确性。已把该偏差固化成测试 `testScanParamOptionsRelationIsTreatedAsNonLatest` |
| `PaimonUtil.updatePaimonColumnMetadata`（递归补 TZ 元信息）/ `PaimonLatestSnapshotProjectionLoader.copyWithLatestSchema` | **已覆盖** | 前者：我们的 `PaimonTypeMapping.toConnectorType` 对 ARRAY/MAP/ROW 天生递归，不需要事后补。后者：`beginQuerySnapshot` 不设 schemaId ⇒ `getTableSchema(3-arg)` 落回 latest schema，正是上游修完的行为 |

另有一处**已知偏差**：paimon **系统表** 的 `@options` 在我们这边绑定的是 latest sys schema
（`PluginDrivenScanNode:1019` 对 sys 表忽略 pinnedSchema，iceberg 早已如此），上游会绑定 option 选中的 schema。
只有 `$audit_log/$binlog/$ro` 家族在跨 schema 演进时才可见差异，5 个 e2e suite 均未覆盖。

## 测试

- **单测（我跑）**：全量 FE `install` + checkstyle **BUILD SUCCESS**（两轮）；
  `fe-connector-paimon` **421/421**（1 个既有 live-connectivity skip，含新增 `PaimonScanParamsTest` 18 例、
  `PaimonScanPlanProviderCapabilityTest` +3 例）、`fe-connector-api` 110/110、`fe-connector-iceberg` 1151/1151；
  fe-core 全量 **8335 / 0 failures / 2 errors / 44 skipped** —— 2 个 error 与上一轮**同源且非本轮引入**
  （`ForwardToMasterTest` = 上游 #66004 自带 NodeInfo JSON 双层嵌套回归；`HFUtilsTest` = 要连 huggingface.co）。
- **3 个变异全部 RED**：①`isOptionsPin` 恒 false（读状态隔离+marker 剥离失效）；
  ②系统表能力答连接器级 true；③去掉 fe-core 的 `@options` 能力门。复原后全绿。
- **测试迁移**：上游 `PaimonScanParamsTest`(355 行) → 连接器版。两处适配：
  `IllegalArgumentException`→`DorisConnectorException`；**本模块无 mockito**（也没有任何 fe-connector-\* 用），
  故 resolve 类用例改成对**真实本地 FileStoreTable**（`@TempDir` + 真提交多个 snapshot/tag）断言 —— 比
  mock `TimeTravelUtil` 更强。`BindRelationTest.rejectOptionsOnUnsupportedTableType` 的文案从
  "only supported for Paimon tables" 改为能力驱动文案（我们的门不绑连接器名）。
- **e2e（你跑）**：5 个 suite 的断言逐条核过，**无需再改**：
  `does not support OPTIONS/INCR` 命中 `PluginDrivenScanNode` 新文案；
  `Unsupported Paimon query option` / `Only one Paimon startup position` / `scan.fallback-branch` /
  `is incompatible with startup position` / `OPTIONS requires a non-empty key/value map` 全部逐字保留。
  上一轮的 3 处本地文案适配（`system tables do not support time travel`、`snapshot earlier than or equal to`、
  `DROP COLUMN not supported`）auto-merge 完好保留。
  `test_paimon_schema_time_travel_matrix` 里那条 "Schema-selecting OPTIONS must bypass paimon-cpp" 断言对
  本分支**平凡成立**（07-26 已按 #66008 摘除 paimon-cpp 臂，flag 恒 no-op）。

## ⏭ 下一个 session

1. `git push` 回 `upstream-apache/branch-catalog-spi`（本轮未 push），跑 External Regression。
2. 上一轮遗留：`fe-filesystem-{oss-hdfs,s3,gcs}` 里 5 处注释仍提已删的 `StoragePropertiesConverter`，可顺手清。
3. 上游 #66004 的 Manager REST `NodeInfo` 双层嵌套回归仍在，建议向上游报，**不要改测试迎合**。

---

# 🆕 上上轮（2026-07-27c）：rebase onto `e7b7f1d1359` —— 迎面撞上上游 **#66004 存储门面大重构**

> `git pull --rebase upstream-apache master`，66 commit onto **`e7b7f1d1359`**（上游新增 5 个 commit）。
> 备份点 tag `backup-before-rebase-0727c` = rebase 前 HEAD `f9c96e1e37a`。**未 push**。
> 收尾 commit `e29884df07f`（连接器 SPI 对齐新门面）。

## 本轮的性质：**同向大重构对撞**，不是普通 rebase

上游 5 个 commit 里 4 个与外表无关（#65483 bitmap / #65781 user-property / #65700 依赖升级 / #64734 BE 指标）；
真正的对手只有一个：**`f499c78c67c` (#66004) “Migrate fe-core storage consumers onto the fe-filesystem SPI facade”**，
247 文件 ±18k 行。它做的事和本分支同向：**删掉 fe-core 自己那套 typed 存储层**
（`datasource/property/storage/*` 21 个主类 + `fs/SchemaTypeMapper` + `fs/StoragePropertiesConverter` + 34 个测试），
改成 `datasource/storage` 门面（`StorageAdapter` / `StorageTypeId` / `StorageRegistry`），
存储属性下沉进各 `fe-filesystem-*` 插件。

与本分支文件交集 **103 个**，冲突 commit **11 个 / 66**（range-diff 已核：其余 55 个逐字节未变；
11 个里 2 个（#6 P4、#55 minio）只是上游改了相邻上下文行造成的位移，无语义变化）。

## 关键判断：4 处“上游取代我们”

| 我们的实现 | 处置 | 理由 |
|---|---|---|
| `FileSystemPluginManager.bindAll`（P5 加，按注册顺序朴素收集） | **删，用上游的** | 上游版本是 legacy `StorageProperties.createAll` 的高保真复刻：优先级表、显式 `fs.<x>.support` 关闭猜测、OSS-HDFS/OSS 与 JFS/HDFS 互斥、默认 HDFS 兜底插 index 0、表外插件排在已知集合之后。**且 auto-merge 把两个同签名 `bindAll` 都留下了 → 重复方法编译错误**（git 无冲突提示） |
| `fe-filesystem-hdfs/HdfsFileSystemProperties`(+Test) | **删** | 上游 `fe-filesystem-hdfs-base/HdfsCompatibleProperties`→`HdfsProperties` 实现同样 3 个 SPI 接口，另带 `getExecutionAuthenticator`，且已接进 `bindAll`/`StorageAdapter` 并有 parity 测试 |
| P3b 把 hdfs-base 的认证指向 fe-kerberos（删 `KerberosHadoopAuthenticator`/`SimpleHadoopAuthenticator`） | **回退 hdfs-base 部分，保留 fe-kerberos 模块本身** | 上游把整个 hdfs-base 重新指向 **foundation 层 `ExecutionAuthenticator`**（无 Hadoop 类型的 doAs 抽象）——这比 P3b 更彻底：文件系统插件叶子从此**完全不需要依赖 fe-kerberos**。P3b 对 fe-common→fe-kerberos 的搬迁**全部保留** |
| 我们 3 个测试 fake provider 只 override `supports()` | **改为 override `supportsGuess()`** | 上游收紧了契约：表外 provider 现在靠 `supportsExplicit`/`supportsGuess` 选中，只有 `supports()` 会被 WARN 并跳过。真实的表外插件也必须这么写 |

## ⚠️ 3 处 git 看不见的坑（本轮教训）

1. **重复方法**：我们的 `bindAll` 与上游的 `bindAll` 同签名，auto-merge 两个都留 → 编译错。
   **同名新增 API 在两边独立演化时，rebase 后必须按签名查重。**
2. **8 字符冲突标记**：git 把 `fe-foundation/security/IOCallable.java` 与
   `fe-kerberos/SimpleAuthenticationConfig.java` 误配成一次 rename，用的是 `<<<<<<<<`（8 个）而不是 7 个，
   **`grep '^<<<<<<< '` 查不到**，两个文件互相污染，直到编译才暴露。以后一律用 `grep -E "^(<{7,8}|>{7,8}) "`。
3. **无冲突但编译崩**：`DefaultConnectorContext` 等 7 个我们自己的文件引用了被上游删掉的包，
   上游没碰过它们 ⇒ 0 冲突 ⇒ 只能靠编译发现。

## 移植结论：**上游 0 能力需要迁移到 connector**

证据：取 #66004 触碰、且在本分支已被删除的 **40 个文件**，过滤掉纯 `StorageProperties→StorageAdapter` 改名/注释/import，
**新增行为行数 = 0**。即上游对那 40 个文件做的全是机械改型，没有新能力。反向对齐则做了：
`DefaultConnectorContext` / `PluginDrivenExternalCatalog` / 6 个测试改型到新门面（commit `e29884df07f`）。

另外两项对齐（同样无冲突信号）：`fe-kerberos` 补 `fe-foundation` 依赖（上游让 `ExecutionAuthenticator`
继承 foundation 版）；3 个测试 fake 适配新 provider 契约。

## 测试

- **单测（我跑）**：全量 FE `install` **BUILD SUCCESS**；checkstyle **BUILD SUCCESS**；
  fe-filesystem 全 18 模块 + fe-kerberos + fe-foundation + 全部 fe-connector 模块 **绿**；
  直接受影响的 fe-core 测试 31/31 绿。
  fe-core 全量复跑 **8338 用例 / 0 failures / 2 errors / 44 skipped**。
  （首跑曾出现 83 个 `NoClassDefFoundError` 级联，复跑**完全消失**，确认是单 fork JVM 的 classloader 退化，非回归。）
  剩下 2 个 error 都不是本轮引入：`HFUtilsTest` = `Network is unreachable`（要连 huggingface.co）；
  `ForwardToMasterTest` = **上游 #66004 自带回归**，见下。
### ⚠️ 发现一个**上游自带的回归**（不是我们的，但会打在我们 PR 的 CI 上）

`ForwardToMasterTest.testAddBeDropBe` 在 **`f499c78c67c` (#66004) 自身**就挂，它的父 commit `0dde27390ac` 是绿的；
本分支 rebase 前（`f9c96e1e37a`）也是绿的 ⇒ **与本分支、与我的冲突解决全部无关**，纯属继承上游。

根因（已抓到实际报文）：#66004 给 fe-core/pom.xml 加了 13 个 fe-filesystem 插件依赖，改变了 Spring MVC 对
`NodeInfo` 的 JSON 序列化 —— `/rest/v2/manager/node/backends` 的响应从

```json
"data":{"columnNames":[...],"rows":[...]}
```
变成了**双层嵌套**
```json
"data":{"columnNames":{"columnNames":[...]},"rows":{"rows":[...]}}
```

这不只是测试问题：**Manager REST API 的对外响应结构被改了**，任何依赖该接口的管控面都会坏。
建议向上游报（apache/doris #66004）。我们这边**不要自己改测试去迎合**——那会把上游的 bug 固化下来。

- **e2e（你跑）**：本轮**不需要改任何 suite**。上游 #66004 自述 “Behavior changed: No” 且 0 个 regression-test 文件改动；
  它唯一可能影响用例的行为差（S3 不再静默默认 region=us-east-1）对我们无效——165 个 external suite 全部**显式**写了
  `"s3.region" = "us-east-1"`。`build.sh` 已经在打包全部 14 个 fe-filesystem 插件（含 #66004 要求的 broker）。

## ⏭ 下一个 session

1. `git push` 回 `upstream-apache/branch-catalog-spi`（本轮未 push）。
2. 跑 e2e（External Regression）确认。
3. 遗留小事：`fe-filesystem-{oss-hdfs,s3,gcs}` 里 5 处注释仍在提 `StoragePropertiesConverter`（上游已删该类），
   属于陈旧注释，不影响编译，可顺手清。

---

# 更早（2026-07-27b）：rebase onto `042e613b134` —— 移植 #65955 paimon table-option + 修一处**静默失效**

> `git pull --rebase upstream-apache master`，63 commit onto **`042e613b134`**（上游新增 6 个 commit）。
> 备份点 tag `backup-before-rebase-0727b` = rebase 前 HEAD `ceb33843d4b`。**未 push**。

## 冲突（全部来自同一个上游 commit `#65955`）

上游 6 个 commit 里只有 **`74227a80e46` (#65955) “Support Paimon table option passthrough”** 碰了 fe-core 外表，
它改的 5 个 fe-core 文件本分支 P5 全删/改光了，于是撞出两组冲突：

| 冲突 | 类型 | 解法 |
|---|---|---|
| `AbstractPaimonProperties.java` @ 我们的 P5-cutover commit | content | **并集**：上游新增 `initNormalizeAndCheckProps()`、我们新增 `initHdfsExecutionAuthenticator()`，同一锚点两个方法，无语义交叉 |
| `AbstractPaimonProperties.java` @ 我们的 P5-T29 commit | content | 取**我们的版本**（T29 的目标就是 fe-core paimon-SDK-free；上游新增的 `SupportedTableOptions`/`getTableOptionsForCopy` 全是 `org.apache.paimon.*`，必须搬走） |
| `PaimonExternalCatalog` / `PaimonScanNode` / `PaimonScanNodeTest` / `AbstractPaimonPropertiesTest` | modify/delete | `git rm` 保留删除，能力另行移植 |

`git range-diff` 63 commit → **60 个逐字节未变**，3 个改写正是上面两组冲突 + 一个纯上下文行位移
（`PaimonJniScanner` 少了一行 `import CoreOptions`）。上游本轮触碰的 22 个文件里，与我们不同的只有 6 个：
5 个是 T29 有意删除的 fe-core paimon 文件，第 6 个 `PaimonJniScanner.java` 差异**只有本分支自己的两处改动**
（P3b kerberos 包名迁移 + null-predicate backstop），上游 #65955 内容一行没丢。

## ⚠️ 抓到一处 0 冲突的**静默失效**（本轮最重要的收获）

`#65955` 把 JNI IOManager 的属性命名空间从 `paimon.doris.*` 改成 `paimon.jni.*`，**FE 与 BE 同时改**
（`be/src/format/table/paimon_jni_reader.cpp`、`be/src/format_v2/jni/paimon_jni_reader.cpp`、`PaimonJniScanner`）。
BE 侧那 3 个文件本分支没碰过 ⇒ **auto-merge 干净通过、直接变成新键名**；而 FE 侧的转发表在
`fe-connector-paimon/PaimonScanPlanProvider.BACKEND_PAIMON_JNI_OPTIONS`（legacy `PaimonScanNode` 早被删），
git 完全看不见这层对应关系 ⇒ 不改就是 **FE 发 `doris.*`、BE 只认 `jni.*`**：编译过、测试过、IOManager 永久失效，
paimon 主键合并读回到 OOM 老路。**这类跨 FE/BE 同名常量的改名，rebase 后必须逐个核对。**

## 移植结论（4 项，落在 `fe-connector-paimon`）

1. **新增 `PaimonTableOptions`** — 直译上游 `AbstractPaimonProperties` 里被 T29 删掉的
   `TABLE_OPTION_PREFIX`/`extractTableOptions`/`validateTableOption`/`getTableOptionsForCopy`/`SupportedTableOptions`。
   放 `fe-connector-paimon` 而非 `fe-connector-metastore-paimon`：三个消费点全在本模块，且与
   `PaimonCatalogFactory` 已承接 `appendCatalogOptions` 的既有惯例一致。
2. **`PaimonCatalogFactory.appendCommonOptions`** — 把 `paimon.table-option.*` + `paimon.jni.*` 排除出 catalog Options。
3. **`PaimonConnectorProvider.validateProperties`** — 调 `extract()` 做 CREATE/ALTER CATALOG 的 fail-fast
   （上游靠 `initNormalizeAndCheckProps()`，SPI 路径不再跑那条）。
4. **`PaimonCatalogOps.CatalogBackedPaimonCatalogOps.getTable`** — `table.copy(forCopy(..))`。这是连接器**唯一**的
   `Catalog.getTable`，与 legacy `PaimonExternalCatalog.getPaimonTable` 位置完全对齐，branch/时间旅行/系统表全覆盖；
   `PaimonTableResolver.resolve` 的两条路径（handle 上的 transient table、reload）都经过它 ⇒ 发给 BE 的
   `paimon.serialized_table` 必带 option。

**不需要移植的一项**：上游 `PaimonExternalCatalog.notifyPropertiesUpdated` 里新增的
`|| isTableOptionProperty(key)` 缓存失效条件。本分支 `PluginDrivenExternalCatalog` 对**任何**属性变更都
`resetToUninitialized(false)` → connector 置空 → 下次访问整体重建（`tableOptions` 在 ctor 里算），
而上游那句要清的 `PaimonExternalMetaCache` 引擎缓存本分支已随 T29 删除 ⇒ **结构上更强，无缺口**。

## 测试

- **单测（我跑）**：新增 `PaimonTableOptionsTest` 11 例（上游 `AbstractPaimonPropertiesTest` 的 table-option 用例
  一一对应 + 两条连接器专属接线 + 2 条真实 local `FileSystemCatalog` 的 getTable 覆盖验证）；
  `PaimonScanPlanProviderTest` 改键名 + 把 `backendOptionsForwardFileReaderAsyncOptOut` 换成
  `backendOptionsDropRetiredFileReaderAsyncOptOut`（#65955 连同 `buildTableOptions` 一起废掉了这个 knob，
  等价开关变成 `paimon.table-option.file-reader-async-threshold`）。
  模块 **393/393**（1 个既有 live-connectivity skip）+ checkstyle 0 + 全量 FE `install`。
  **5 个变异全部 RED**：键名回退 `doris.*`、去掉 validateProperties 的 extract、getTable 不 copy、
  去掉 catalog Options 排除、`forCopy` 改用裸 key 比较。
- **e2e（你跑）**：上游 `#66065` 新增 3 个 paimon suite，其中 2 个断言 `exception "PaimonExternalCatalog"` ——
  那是 legacy `UnboundTableSinkCreator` 的 `"Load data to " + catalog.getClass().getSimpleName()`，本分支
  paimon 是 `PluginDrivenExternalCatalog`，走 `UnboundConnectorTableSink`，拒绝改由连接器写能力裁决。已改：
  `test_paimon_write_boundary`（INSERT/INSERT-SELECT → `does not support INSERT operations`；
  INSERT OVERWRITE → `insert into overwrite only support`，它在 `InsertOverwriteTableCommand.allowInsertOverwrite`
  更早被拦）、`test_paimon_ctas_atomicity_negative`（同 INSERT 文案；该 suite 另有
  `enablePaimonKnownBugTest` 双重门控，默认不跑）。同 suite 的 UPDATE/DELETE/MERGE 三条断言**无需改**：
  paimon 连接器不声明 DELETE/MERGE ⇒ `pluginConnectorSupportsRowLevelDml` 为 false ⇒ 仍落回 legacy 那三条文案。
  第 3 个 suite `test_paimon_merge_engine_matrix` 纯读，无需改。

## 其余 5 个上游 commit：0 能力迁移

`#66049`(BE LSAN 头文件)、`#65492`… 无交集；`#65414` 是 nereids MV 规则的 2 行笔误修
（`joinCheckContext`→`scanCheckContext`），纯 fe-core 与外表无关；`#66081` 是 cloud feut 阈值；
`#65814` 纯 BE scanner。**BE 未编译**：本轮 BE 改动与本分支 BE 文件无交集。

⚠️ 坑（复用）：`mvn -pl <单模块>` 必须 `-am`（`${revision}`）；`install` 报 "did not assign a file" 加
`-Dmaven.build.cache.enabled=false`。**做变异测试时不要用 `git checkout -- <file>` 还原**——未 staged 的真实改动会
一起被丢掉（本轮踩过）；用 `cp` 备份还原。

---

# 上上上轮（2026-07-27）：rebase onto `5b3ac63f8b4` —— 0 能力迁移，白捡上游一个 BE 崩溃修复

> `git pull --rebase upstream-apache master`，62 commit onto **`5b3ac63f8b4`**（上游新增 5 个 commit）。

**冲突只有 2 处，都在 `MetadataGenerator.java` 的 import 区，纯并集**（commit 33 `#65740` 与 commit 40 perf 各撞一次）：
上游 `#65644` 加了 `import ...extension.loader.PluginRegistry`，正好夹在我们加的 `datasource.plugin.PluginDriven*` 之间，
按字母序并列即可，**无语义冲突**。

**本轮 0 能力迁移**。上游 5 个 commit：`#66073`(BE parquet V2 懒初始化)、`#65492`(BE RowKeyEncoder)、`#66053`(CODEOWNERS)
三个与本分支无交集；FE 侧两个都**不需要往 fe-connector 搬**：

- `#65987` JDBC driver_url 加固：上游 master **已有全套 `fe-connector-*` 模块**，该修复本来就打在 `fe-connector-jdbc`
  （新 `JdbcDorisConnector.checkDriverUrlSecurityRule()` 挂进 `JdbcConnectorProvider.validateProperties`）。它同时硬化的
  `fe-core/JdbcResource`（`jdbc_driver_secure_path` 结构化匹配 + 解析失败 fail-closed）是共享工具类，本分支 iceberg/paimon 的
  `driver_url` 走 `ConnectorValidationContext.validateAndResolveDriverPath()` → `JdbcResource.getFullDriverUrl()` **自动继承**。
  强制规则在上游也只覆盖 jdbc 连接器 ⇒ **作用域与上游完全一致，无缺口**。
- `#65644` `information_schema.extensions` 插件清单表：给 `ConnectorPluginManager.loadBuiltins()` 加了
  `PluginRegistry.registerBuiltin()`，**失败会 LOG.warn 后跳过 provider**（能静默丢连接器的路径）。已核：本分支 8 个连接器
  `name()` 默认返回 `getType()` = `iceberg/paimon/hudi/hms/jdbc/es/max_compute/trino-connector`，全部满足
  `PluginNames` 的 `[A-Za-z0-9._-]`/非空/≤64 且互不重名；且连接器**不在 fe-core classpath**（`fe-core/pom.xml` 只依赖
  `fe-connector-api|spi`），只走目录插件路径 ⇒ 新增的 `hasProviderNamed()` 重名 discard 不会误伤。
  `PluginRegistry.register()` 用 `putIfAbsent` 返回 boolean **不抛异常** ⇒ 重复 `loadBuiltins()` 的单测安全。
  上游 e2e `test_extensions_schema.groovy` 只断言"≥1 行 + (type,name) 唯一"，不锁清单 ⇒ **本分支多 7 个连接器不会红，无需改测试**。

**⚡ 白捡**：`#66073` 顺带加了 `RuntimeProfile::get_or_create_child`，并把
`format_v2/table/iceberg_position_delete_sys_table_reader.cpp` 的 `get_child`+`create_child` 两步换成原子一步 ——
正是 ExtReg **1005971** BE SIGABRT 的根因（多 scanner 共享 `_scanner_profile` 的 TOCTOU）。**上游已修，我们不用再自带 patch**。

**完整性验证**（不靠"没冲突"当没事）：
`git range-diff` 62 commit → **56 个逐字节未变**，6 个改写全部只是上下文行位移（逐个核过 diff，含
`JdbcDorisConnector` 里我们的 `getWritePlanProvider()` 插入锚点漂移，已确认落在类体正确位置）。
上游本轮触碰的 **56 个文件**中，我们与上游不同的只有 4 个 —— `Config.java`/`JdbcDorisConnector.java`/
`FileSystemPluginManager.java`/`MetadataGenerator.java`，差异**全部是本分支自己的改动**，上游内容一行没丢。
上游改过但本地不存在的 3 个文件（`UploadAction`/`LoadSubmitter`/`TmpFileMgr`）**正是上游自己删的**，全仓无残留引用。

**测试**（全绿）：FE 全量 `clean install` BUILD SUCCESS；`fe-common` 213/213（含上游改的 `ConfigTest`）、
`fe-connector-jdbc` 全模块（含上游新增 `JdbcDriverUrlSecurityRuleTest` 9/9、`JdbcConnectorProviderValidateTest` 24/24）、
`fe-extension-loader` 10/10（上游新增 `PluginRegistryTest` 7 + `DirectoryPluginRuntimeManagerMetadataTest` 3）、
`fe-authentication-handler` 101/101（含上游改的 `AuthenticationPluginManagerTest` 17）、
`fe-core` 定向 37/37（`JdbcResourceTest` 22 + `FileSystemPluginManagerTest` 5 + `ConnectorPluginManagerTest` 5 +
`DefaultConnectorContextSiblingTest` 3 + `MetadataGeneratorPluginDrivenTest` 2）+ checkstyle 0。
**BE 未编译**：本分支 BE 侧 7 个文件与上游 3 个 BE commit 的文件集合 `comm -12` **交集为空**，且 `#66073` 对
`RuntimeProfile` 是纯新增 API —— 这是文件级证据不是编译验证，需要时请跑 BE。

⚠️ 坑（复用）：`mvn -pl <单模块>` 会因 `${revision}` 解析失败，**必须 `-am`**；maven build-cache 扩展会导致
`install` 报 "did not assign a file to the build artifact"，加 `-Dmaven.build.cache.enabled=false`。

**未 push**。备份点 `backup-before-rebase-0727` = rebase 前 HEAD `1aa5ae9597e`。

---

# 更早（2026-07-26）：rebase onto `962bd5b28c7` + 移植 #66008 的 paimon-cpp 摘除

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

# 上上上轮（2026-07-25）：CI **1005291** 的 iceberg 大小写列名回归 —— 已修待 CI 验

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
