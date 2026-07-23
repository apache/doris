# Catalog SPI 抽象级别不一致 — 全面梳理

> **范围**:apache/doris `branch-catalog-spi` 全部外表 SPI 面(`fe-connector-api` 80+ 接口/类 + 各连接器 ScanRange/Handle + fe-core 桥)
> **基线**:结论对齐分支最新 tip `apache/branch-catalog-spi`(worktree 有 WIP drift,行号以 `git show apache/branch-catalog-spi:` 为准),与 `apache/master`(legacy)逐一对照
> **视角(LENS)**:SPI 在粒度 G_spi 建模概念 C,但数据源在不同/更细的粒度 G_real 变化 C → ①强制塌陷、②SPI 给了 G_real 的槽但连接器填成 G_spi、③粒度错配致错误/崩溃。**不局限于 file format**。
> **方法**:5 个 Opus agent 按概念簇并行梳理(schema/列身份、分区 spec&值、统计&MVCC、scan/split 文件级、写/事务/session/下推),每个区分 SPI-shape vs connector-fill、标注 verified-OK。

---

## 目录

1. [核心结论:一个反复出现的元模式](#1-核心结论一个反复出现的元模式)
2. [三种结构性子模式](#2-三种结构性子模式)
3. [全部发现(按严重度)](#3-全部发现按严重度)
4. [分类详解](#4-分类详解)
5. [验证正确的部分(verified-OK)](#5-验证正确的部分verified-ok)
6. [建议:能杀掉整类问题的 SPI-shape 修法](#6-建议能杀掉整类问题的-spi-shape-修法)

---

## 1. 核心结论:一个反复出现的元模式

**SPI 反复在表级(coarse)建模 iceberg/paimon 在 snapshot/file/field/partition-file 级(fine)变化的东西。** 这不是零散 bug,是一个贯穿整个 SPI 面的系统性张力。

关键洞察——**为什么大部分时候没炸**:

> **iceberg 是"参考实现",它靠自己的 typed per-file 机制(`populateRangeParams`、field-id dict、内部 `IcebergScanRange.DeleteFile`)绕开通用 SPI 槽。所以 iceberg 大多正确,但被绕开的通用槽常常是 vestigial(退化)的、有误导性;而真正的风险集中在两处:**
> 1. **没有 iceberg 那套严谨机制的连接器**(paimon 部分路径、hive、hudi)naively 填表级槽 → 当场中招;
> 2. **下一个信任那个 iceberg 悄悄忽略的通用槽的连接器** → 静默丢语义。

所以判断每条发现,关键不是"iceberg 有没有炸"(它通常没有),而是:**这个抽象缺口会不会咬到下一个连接器,或者已经咬到了 paimon/hive**。

---

## 2. 三种结构性子模式

所有发现落进三个结构桶:

### 模式 A — Vestigial 通用槽(iceberg 用自己的机制绕开)

通用 SPI 槽建在错误粒度,iceberg 忽略它、走自己的 per-file typed 路径。iceberg 正确,但槽是死的/误导的,**下一个信任它的连接器会丢语义**。

| Vestigial 槽 | iceberg 的绕过 | 危险 |
|---|---|---|
| `ConnectorDeleteFile`(缺 content type/序列号/位置界/field-id) | 内部 typed `IcebergScanRange.DeleteFile` + `populateRangeParams`;`getDeleteFiles()` **零生产调用者** | 下一个用通用槽的连接器丢 delete 语义 |
| `ConnectorBucketSpec` 的 `"iceberg_bucket"` 值 | iceberg CREATE 从 partition spec 读 `bucket[N]`,**从不消费这个值** | 类别错误(表分布 vs per-field transform)无正确接线 |
| `ConnectorPartitionSpec`(表级) | 读路径带 per-file `partitionSpecId`;这个类**只 DDL 用** | 表级 spec 碰不到读,但下一个连接器若在读路径信它会塌 |

### 模式 B — 右粒度槽被连接器填成错粒度(纪律缺口)

SPI 给了 per-X 槽,但连接器在部分代码路径填成粗粒度值。**这是已经咬到 paimon/hive 的一类。**

| 槽 | 谁填错 | 后果 |
|---|---|---|
| `ConnectorScanRange.getFileFormat()`(per-split) | paimon JNI/COUNT 路径填**表级** `file.format` | ORC 数据无 option → paimon-cpp 误读(🔴) |
| `getColumnHandles`(应 per-snapshot) | 从 **latest** schema 建、按**名字** key | rename + time-travel → 列被丢(🔴) |
| Hive 分区值 | `part.substring(eq+1)` **不 unescape** | 含转义字符分区值剪光 → 丢行(🔴) |
| `listPartitionValues`(应 typed) | paimon 返回**原始** spec map(DATE=epoch-day、NULL=未归一 sentinel) | TVF 类型错乱(休眠) |

### 模式 C — 缺失整个粒度轴(SPI-shape 洞)

SPI 根本没有承载某个变化维度的槽。**这些是能一次性杀掉整类问题的 SPI-shape 修法所在。**

| 缺失的轴 | 后果 |
|---|---|
| `ConnectorLiteral` **无 typed `getStringValue()`** | 逼每连接器从裸 Object 重推 canonical string → Hive 推错、iceberg 手写矩阵推对(🔴 根因) |
| `ConnectorScanRange` **无 `getCompressType()`** | 引擎只按后缀推;LZ4FRAME→LZ4BLOCK 重映射丢失 → BE 解码失败 |
| `getTableStatistics` / `getColumnHandles` **无 snapshot 参数** | time-travel 下 stats/schema 用 latest、scan 用 pinned → 偏斜 |
| `ConnectorColumn` **单个 `defaultValue`** | iceberg v3 的 initial-default vs write-default 塌成一个 |
| `ConnectorMvccSnapshot` 是**定点**(snapshotId) | branch 是**移动引用**;scan 跟 head、schema/stats 定点 → 契约破 |

---

## 3. 全部发现(按严重度)

> 🔴 高 / 🟠 中 / 🟡 低。kind:**S**=SPI-shape(抽象本身错)/ **F**=connector-fill(纪律缺口)。状态:**现行**/**休眠**(连接器未 cutover)/**潜伏**(咬下一个连接器/未来消费者)/**已知**(前次 review 报过)。

| # | 严重 | kind | 概念 | 粒度错配(SPI → 真实) | 连接器 | 状态 |
|---|---|---|---|---|---|---|
| 1 | 🔴 | S | `ConnectorLiteral` 无 typed `getStringValue()` | 裸 Object → 类型化 canonical string | hive 错/iceberg 对 | 现行+根因 |
| 2 | 🔴 | S | `getColumnHandles` 无 snapshot 参数 | 表(latest 名字) → snapshot(field-id) | paimon 脆/iceberg 幸免 | 现行 |
| 3 | 🔴 | F | paimon JNI/COUNT file format | split → per-file | paimon | 现行·已知 |
| 4 | 🔴 | F | Hive 分区值不 unescape | 转义路径文本 → 逻辑值 | hive | 休眠(P7) |
| 5 | 🔴 | F | iceberg 行数丢 equality-delete gate | 表级公式 → snapshot·delete-aware | iceberg | 现行·已知(P6-3) |
| 6 | 🟠 | S | `ConnectorScanRange` 无 `getCompressType()` | scan-node(后缀) → per-file | hive | 休眠(P7) |
| 7 | 🟠 | S | `getWriteContext()` 错标通用 bag | 通用 map → INSERT 静态分区 spec | 全部写 | 现行·已知(P4-D) |
| 8 | 🟠 | S | `ConnectorTransaction` 私有方法泄漏 | 通用接口 → odps block-alloc + iceberg-compaction rewrite | mc/iceberg | 现行 |
| 9 | 🟠 | S | `ConnectorBucketSpec` 表分布 vs per-field bucket | 表级单 numBuckets/字符串算法 → iceberg per-field transform | iceberg | 潜伏 |
| 10 | 🟠 | S/F | 列 DEFAULT 未读 + initial/write 塌陷 | 单 defaultValue → per-kind | iceberg | 现行 |
| 11 | 🟠 | F | paimon CREATE 丢 per-列类型参数 | column → CHAR 长度/datetime scale | paimon | 现行 |
| 12 | 🟠 | F | paimon 嵌套 null/注释(写) | column → per 嵌套 field | paimon | 现行·已知(P5-5) |
| 13 | 🟠 | F | `initialValues` LIST/RANGE 丢弃 | 有槽 → DDL 显式值/边界 | 全部 DDL | 现行·已知(P5-7) |
| 14 | 🟠 | S | `listPartitionValues` 无类型 | `List<List<String>>` → typed | paimon | 潜伏 |
| 15 | 🟠 | S | `getTableStatistics` 无 snapshot 参数 | 表级 → per-snapshot | 全部 | 现行 |
| 16 | 🟠 | S | `getNewestUpdateTimeMillis` 名说毫秒实微秒 | millis 名 → 源定义微秒 | iceberg | 现行·已知 |
| 17 | 🟠 | F | MVCC branch ref 定点 vs 移动 | 定点 snapshot → 移动 branch head | iceberg | 现行 |
| 18 | 🟠 | F | `FOR TIME AS OF` 数字串当 epoch | 数字 regex → date/epoch 歧义 | iceberg | 现行 |
| 19 | 🟡 | F | 嵌套 null/注释(读) | column → per 嵌套 field | iceberg/paimon | 现行 |
| 20 | 🟡 | S | transform 参数只 `List<Integer>` | int → 任意 transform 参数 | 全部 DDL | 潜伏 |
| 21 | 🟡 | S | `ConnectorDeleteFile` 欠建模/vestigial | 缺 content/seq/bounds/fieldId | iceberg 绕开 | 潜伏 |
| 22 | 🟡 | F | 谓词下推用 latest 非 pinned schema | 表 → snapshot | iceberg | 现行(仅丢下推) |
| 23 | 🟡 | S | `ConnectorDomain` 无 CHAR padding | Comparable → CHAR(n) 定宽 | 全部 | 潜伏 |
| 24 | 🟡 | S | `getFreshnessValue` 一 long 两粒度 | long → snapshot-id 或 millis | iceberg/paimon | 潜伏 |
| 25 | 🟡 | S | `ConnectorMvccSnapshot` 无 equals/hashCode | 独缺(4 个兄弟类都有) | 全部 | 潜伏(P6-2 同源) |
| 26 | 🟡 | S | iceberg residual 不 per-range 下发 | scan-node → per-file | iceberg | 现行(对齐 legacy) |

**统计**:5 现行高危、若干中危洞、大量潜伏槽。其中 **3 条根因级 SPI-shape**(#1 Literal、#2/#15 无 snapshot 参数、#6 无 compress 槽)修一处能杀一整类。

---

## 4. 分类详解

### A. Literal / 谓词粒度(#1, #18, #23)

**#1 `ConnectorLiteral` 无 typed `getStringValue()`(🔴 根因)**——SPI 只揣裸 `Object` + `toString()==value.toString()`,逼每个连接器自己从 Java 值重推 canonical string。`LocalDateTime.toString()` 出 ISO `T`(`2021-01-01T00:00:00`)、`Boolean` 出 `true`/`false`、`BigDecimal` 出科学计数——都不等于 Doris/Hive canonical 形式。**同一缺口,divergent fill**:`HiveConnectorMetadata.extractLiteralValue:381` 用 `String.valueOf(val)` 推错;`IcebergPredicateConverter:310+` 手写 `dorisDateTimeString()` + bool `'1'/'0'` 推对。更讽刺:canonical string 在 `ExprToConnectorExpressionConverter:305` 边界拿得到,却为 datetime 丢弃(存裸 `LocalDateTime`)。这是 hudi/paimon/hive 一系列分区匹配 bug 的**共同 SPI 根因**。

### B. Schema / 列身份粒度(#2, #10, #11, #12, #19, #22)

**#2 `getColumnHandles` 无 snapshot 参数(🔴)**——从 `table.schema()`(latest)建,handle 按**名字** key(揣的 fieldId 对身份判等是死的)。`buildColumnHandles` 做 `allHandles.get(slot.getColumn().getName())`,miss 就静默丢列。time-travel 跨 RENAME:query slot 带 pin 的旧名,latest map 里没有 → 列被丢出 `columns`。**iceberg 只因在 pin 下忽略 `columns`、从完整 pinned schema 重建 field-id dict 才幸免**(`IcebergScanPlanProvider:1268` 注释明说这防"BE StructNode DCHECK crash");**paimon native projection 按名消费 `columns`,无此 fallback → 更脆**。这是 P6-1 那个 crash 的 schema-grain 兄弟。

**#10 列 DEFAULT**:iceberg `parseSchema` 硬编码 `null`,从不读 `initialDefault()/writeDefault()`;且 `ConnectorColumn` 单 `defaultValue` 槽,连 iceberg v3 的 initial(填历史行)vs write(新写)两 default 都塌成一个。(P0 转换器传 null 已在 tip 修)

**#11 paimon CREATE per-列类型参数**:CHAR/VARCHAR 长度塌成 `VarChar(MAX)`、DATETIME scale 塌成默认微秒;decimal 精度保留。

### C. 分区粒度(#4, #13, #14, #20)

**#4 Hive 分区值不 unescape(🔴)**——`parsePartitionName` 取 `part.substring(eq+1)` 无 `FileUtils.unescapePathName`,legacy `HiveUtil:190` 有。含 `/ 空格 : % 非ASCII` 的分区值 → escaped actualValue 永不等于 unescaped 谓词 → 分区剪光丢行。和 hudi P3 同族(hive 副本)。

**#14 `listPartitionValues` 无类型**:paimon 返回原始 `partition.spec()` map(DATE=epoch-day `19723`、NULL=未归一 `__DEFAULT_PARTITION__`),与 `getPartitionName()`(格式化日期、归一 null)不一致;iceberg 无 override。休眠(TVF 还走 legacy HMS,零 fe-core 消费者)。

### D. Scan / split 文件级(#3, #6, #21, #26)

**#6 无 `getCompressType()` 槽(🟠)**——`ConnectorScanRange` 有 `getFileFormat()` 无压缩槽,引擎只 `Util.inferFileCompressTypeByPath` 按后缀推,`PluginDrivenScanNode` 丢了 legacy `HiveScanNode` 的 LZ4FRAME→LZ4BLOCK 重映射。真编码与后缀不符(hadoop block-lz4、无后缀压缩文本、表属性编码)→ BE 解码失败/错行。休眠(hive 未 cutover;活跃的 orc/parquet 内部压缩)。

**#21 `ConnectorDeleteFile` vestigial**:只建模 `(path, format, recordCount, properties)`——缺 content type(position/equality/DV)、data/delete 序列号、位置界、equality field-id。iceberg 绕开用内部 typed 类,`getDeleteFiles()` 零生产调用者。

### E. 统计 / MVCC 时间粒度(#5, #15, #16, #17, #18, #24, #25)

**#15 `getTableStatistics` 无 snapshot 参数(🟠)**——`fetchRowCount` 解析新 handle 不带 MvccSnapshot,iceberg `computeRowCount` 用 `table.currentSnapshot()`。`FOR VERSION AS OF <old>` 查询喂优化器 latest 行数、scan 读 pinned old → stats/scan 偏斜(仅估计,不错结果)。**schema 已原子 pin,stats 没有**——P6-1 的 stats-grain 兄弟。

**#17 MVCC branch ref 定点 vs 移动(🟠)**——`ConnectorMvccSnapshot` 是定点(snapshotId+schemaId),但 branch 是**移动引用**(head 会进);`applySnapshot` 路由到 `scan.useRef(name)`,scan 跟 branch head(忽略 pin 的 snapshotId),而 schema/stats 定点。plan→scan 之间对 branch 的 schema-changing commit → BE 用旧 schemaId 读新 schema 数据 → 列错配/错行/崩溃。破了 pin 的"query 内一致版本"契约(legacy-parity 机制,但 SPI 把这个定点-移动分裂固化了)。

**#25 `ConnectorMvccSnapshot` 无 equals/hashCode**——4 个 MVCC/stats 兄弟类(Partition/View/TimeTravelSpec/TableStatistics)都定义了 equals+hashCode,唯它没有 → 不能按 (table,version) value-key。和 P6-2(version-blind context pin)同源。显式 time-travel 各 relation 各带 pin 作方法参数,故无确诊塌陷——shape smell。

### F. 写 / 事务粒度(#7, #8)

**#8 `ConnectorTransaction` 私有方法泄漏(🟠)**——通用事务接口被两个连接器塑形:`allocateWriteBlockRange(writeSessionId, count)` 是纯 odps write-session 语义(其他连接器 default 返 false/throw),`registerRewriteSourceFiles(Set<String>)`/`getRewriteAddedDataFilesCount()` 是 iceberg-compaction 专属(default throw)。和 P4 报的 block-allocation 同模式,又新增一对 iceberg 的。(txn-id 粒度本身 OK——都从 `session.allocateTransactionId()` 取)

---

## 5. 验证正确的部分(verified-OK)

**重要:SPI 大体是 sound 的,iceberg 的读侧工程尤其扎实。** 这些是明确查过、做对了的,证明上面的问题是"细节洞"而非"架构塌":

- **iceberg 分区 spec 演进**:scan range 带 per-file `partitionSpecId` + `partitionDataJson`,union 所有 `table.specs()`,`isPartitionBearing()` 处理 spec-evolved/物理无分区文件。表级 `ConnectorPartitionSpec` 仅 DDL 用、碰不到读。
- **iceberg delete→data-file 序列号**:SDK `DeleteFileIndex.forDataFile(dataSequenceNumber, dataFile)` 在建 FileScanTask 前算好,per-range 只带适用的 delete。
- **time-travel schema pinning**:`getTableSchema` 有 snapshot 重载,pin 到 `snapshot.getSchemaId()`(iceberg `table.schemas().get(schemaId)`、paimon `schemaAt`),与 latest 路径共享 `buildTableSchema` 不会漂。
- **P6-1 缓解**:latest-snapshot 缓存里**原子** pin `snapshotId+schemaId`(`IcebergConnectorMetadata:1508-1526`)。(残留 P6-1 风险是独立缓存 TTL 那条,见前次 P6 review。)
- **RENAME 在 latest 读**:`Column.uniqueId=field.fieldId()`,BE field-id dict 按 id 匹配老文件,不按名。
- **count 下推**:单 collapsed range 带总数,BE `CountReader` 跨 range 求和;非 count range 保 -1 哨兵。
- **paimon native 子 split**:per sub-range start/length;DV 按全局行位置附到每个 sub-range(文档化不变量)。
- **iceberg identity 分区值 / columns-from-path**:per-file 从 `dataFile.partition()/specId()` 读。
- **per-user vended 凭证(#63068)**:`getDelegatedCredential()` per-connection OIDC/JWT + `expiresAtMillis` fail-closed,只对 `SUPPORTS_USER_SESSION` 连接器填;`ExternalDatabaseSessionContextTest` 证跨用户不走共享缓存(Trino CVE-2026-34214 泄漏防护)。
- **MTMV per-partition freshness**:真 per-partition(各分区自己的 latest snapshot id),非表级塌陷。
- **TIMESTAMPTZ 标记**:`ConnectorColumn.withTimeZone()` 正交于映射类型(即便映成 plain DATETIME 也保留)。
- **ConnectorDomain NULL + 开闭界**:`nullsAllowed`(all/none/onlyNull/singleValue)+ `ConnectorRange` inclusive/exclusive/unbounded 忠实建模。
- **decimal/datetime 精度读**、**ConnectorSortField 表级(DDL 默认排序正确粒度)**。

---

## 6. 建议:能杀掉整类问题的 SPI-shape 修法

按"一处修法杀一整类"排序:

1. **给 `ConnectorLiteral` 加 typed `getStringValue()`(杀 #1，连带 hudi/hive/paimon 一系列分区匹配 bug)**。在 `ExprToConnectorExpressionConverter` 边界就把 Doris-canonical string 塞进 literal(源头拿得到,却丢了),连接器不再各自重推。**最高性价比**——一处 SPI 修法消除模式 B/C 里最痛的一类。

2. **给读路径 SPI 补 snapshot 参数(杀 #2/#15/#22)**:`getColumnHandles(session, handle, snapshot)`、`getTableStatistics(session, handle, snapshot)`、谓词转换用 pinned schema。让 schema/stats/handle 和 scan 用**同一个 pin**,消除所有 time-travel 偏斜(而不是靠 iceberg 各自绕过)。

3. **`ConnectorScanRange` 补 `getCompressType()` 槽(杀 #6)**——per-file 压缩编码,P7 hive cutover 前必需(否则 hadoop block-lz4 等静默解码失败)。

4. **强制 per-split 语义 or 兜底(收 #3 及同类)**:要么 javadoc 明确 `getFileFormat()` 必须 per-file、加校验;要么 BE 侧在 range 无明确 format 时按**文件后缀**兜底(iceberg/native paimon 已是真格式,只救 JNI/COUNT 塌陷)。

5. **清理 vestigial 通用槽(治 #21/#9/模式 A)**:要么把 `ConnectorDeleteFile` 建模到能真用的粒度(content/seq/bounds/fieldId),要么删掉零调用者的 `getDeleteFiles()` / `iceberg_bucket` 值,免得下一个连接器信任一个 iceberg 悄悄忽略的槽。

6. **MVCC 值类型补齐(治 #16/#24/#25)**:`getNewestUpdateTimeMillis` 改名 `getNewestUpdateMarker`(或边界归一到 millis);`ConnectorMvccSnapshot` 加 equals/hashCode 以支持 (table,version) value-key(顺带对齐 P6-2)。

7. **收连接器专属方法进 facet(治 #8/#7)**:odps block-allocation、iceberg-compaction rewrite、writeContext 的静态分区语义——从通用 `ConnectorTransaction`/`ConnectorWriteHandle` 移进可选 facet 接口(像 `getProcedureOps()` 那样能力发现),别堆根接口。

**一句话**:SPI 的骨架是对的(iceberg 读侧证明了),问题是**反复在表级建模细粒度概念,靠 iceberg 各自绕过而非 SPI 统一保证**。三条根因级修法(Literal typed string、读路径 snapshot 参数、compress 槽)+ 清理 vestigial 槽,能把这份清单从 26 条压到个位数,并且让 P7 hive / 下一个连接器不必重新踩 paimon 已经踩过的坑。

---

## 7. Reader-path / catalog-dispatch 维度(不同格式经不同路径读)

除了上面"数据粒度"错配,SPI 还有两个**路径级**的抽象不一致——同一份数据经不同代码路径读。范围收敛到 **HMS-多格式三元组(hive/hudi/iceberg)+ 有双 reader 的连接器(iceberg/paimon/hudi)**,不是全体外表通病,且都是**分阶段迁移产物**(P7 前应统一,尚未建)。

### 7.1 native/JNI reader 选择 = 3 份手搓副本(#27, 🟠 含一个真回归)

reader-type 决策(native / JNI / format-cpp)**每连接器各写一套**,只共享薄传输 seam(`ConnectorScanRange.getFileFormat()`/`getTableFormatType()`/`populateRangeParams()`),**无共享 reader-type 枚举、无共享决策、无共享开关点**:

| 连接器 | 决策处 | JNI 触发 | 线上编码 | honor `force_jni_scanner`? |
|---|---|---|---|---|
| paimon | `PaimonScanPlanProvider.shouldUseNativeReader:1012` | 无 raw file(PK MOR)/ 非 orc-parquet / forceJni / 会话开关 | typed `TPaimonReaderType{NATIVE,JNI,CPP}` | ✅ |
| iceberg | 隐式于 `buildRange:966` | **data 永不 JNI**;仅 `$sys` 表 JNI;delete 全走 native 描述符;不支持格式**抛错**非降级 | `serialized_split + FORMAT_JNI` 约定 | ❌(对齐 legacy) |
| hudi | `collectMorSplits:249` **且** `HudiScanRange.populateRangeParams:166`(两处) | MOR 有 delta log → JNI | 魔法字符串 `"jni"` in `file_format` | ❌ **legacy 有、SPI 丢了** |

**核心问题**:
- **🔴 真回归**:hudi 的 `force_jni_scanner` 在 legacy `HudiScanNode` 3 处 honor,SPI `HudiScanPlanProvider` 零处。`SET force_jni_scanner=true` 在 hudi COW 上 legacy 走 JNI、SPI 静默继续 native。根因=决策被拆到 `planScan`(有 session 不读开关)+ `populateRangeParams`(降级但无 session),**无一处能读开关**。
- **一个概念 3 种线上编码**(typed 枚举 / 魔法字符串 / serialized-split 约定);`THudiFileDesc`/`TIcebergFileDesc` 无 reader_type 字段。
- **"有 delete ⇒ JNI"三家判断相反**:iceberg NATIVE(描述符)、paimon JNI 但 DV→NATIVE、hudi JNI。用户无法用一个心智模型推断。
- **开关不统一**:`force_jni_scanner`(全局)+ `enable_paimon_cpp_reader`(paimon 专属)。
- **连接器内部两路径漂移**:paimon `buildNativeRange`(per-file 后缀 format、set schemaId、显式 DV) vs `buildJniScanRange`(表级 format=P5-2、不 set schemaId、DV 编码进 blob);hudi 决策点重复两处。
- **粒度本身对**:paimon per-split、hudi per-file-slice、iceberg data-native/systable-JNI 都能混——缺陷在决策一致性,不在粒度。

**正确的共享不是决策而是契约+传输**(iceberg native 吃 delete 描述符、paimon/hudi 不能,"有 delete"含义相反,强行统一 `shouldUseNative()` 是错的):① SPI 级 typed `ReaderKind{NATIVE,JNI,FORMAT_CPP}` 取代 `"jni"` 字符串 + paimon 专属枚举 + 统一 thrift `reader_type` 字段;②框架层统一 honor `force_jni_scanner`(连接器声明 `supportsForceJni()`,hudi 回归不复发);③各连接器保留 native-eligibility 谓词返回 `ReaderKind`(共享形状、连接器 body)。

### 7.2 同格式不同 catalog = 不同路径(#28, 🟠 分阶段债)

**两个易混情形必须拆开**:

- **Case A — `type=iceberg` + `iceberg.catalog.type=hms`**(iceberg catalog 用 HMS 做元数据后端,7 flavor 之一):**与 REST 统一**,同一 `IcebergScanPlanProvider`,flavor 参数化(`IcebergCatalogFactory.resolveFlavor:261`)。8 个 legacy `Iceberg*ExternalCatalog` flavor 类塌成 1 个 `IcebergConnector` + 共享 metastore SPI——**真正的跨 flavor 统一**。
- **Case B — `type=hms` catalog 里躺一张 iceberg 表**(寄生,`table_type=ICEBERG` 检测):**分裂**——走 legacy `HMSExternalTable` DLA=ICEBERG → **legacy `IcebergScanNode` + IcebergHMSSource**,**从不碰 SPI iceberg 连接器**。

所以**同一 iceberg 格式,取决于 catalog 声明方式,被两条完全不同的路径读**(SPI `IcebergScanPlanProvider` vs legacy `IcebergScanNode`)。cutover 反而**磨锋利**了:`IcebergScanNode` 构造器以前处理两种源、现在只处理 `HMSExternalTable`。

**dispatch map**(`CatalogFactory.SPI_READY_TYPES` 主开关):

| catalog type | 表格式 | 路径 | SPI/legacy |
|---|---|---|---|
| `type=iceberg`(7 flavor 含 hms 后端) | iceberg | `PluginDrivenScanNode`→`IcebergScanPlanProvider` | **SPI** |
| `type=paimon`(含 hms 后端) | paimon | `PluginDrivenScanNode`→`PaimonConnector` | **SPI** |
| `type=hms` | hive 表 | `HMSExternalTable` DLA=HIVE→`HiveScanNode` | legacy |
| `type=hms` | **iceberg 表** | `HMSExternalTable` DLA=ICEBERG→**legacy `IcebergScanNode`** | legacy |
| `type=hms` | **hudi 表** | DLA=HUDI→**legacy `HudiScanNode`** | legacy |
| `type=hudi`(独立) | hudi | 不在 SPI_READY_TYPES、无 legacy 分支→**抛错** | 休眠 |

**三处重复 + 一个更危险的缺口**:
1. 格式检测重复:`HiveTableFormatDetector` javadoc 自认"mirrors HMSExternalTable.supportedIcebergTable/..."(`HiveTableType{HIVE,HUDI,ICEBERG}` ↔ `HMSExternalTable.DLAType`)。
2. iceberg/hudi 读逻辑两份:legacy `IcebergScanNode`/`HudiScanNode` ↔ SPI `IcebergScanPlanProvider`/`HudiScanPlanProvider`(hudi SPI 全套写好但休眠)。
3. **⚠️ hive 连接器检测格式却不委托**:`HiveConnector.getScanPlanProvider():60` **无条件**返回 `HiveScanPlanProvider`,不管检测到什么。detect→delegate 交接(`HiveTableType` javadoc 承诺)**没实现**——一旦 `type=hms` 进 SPI_READY_TYPES,HMS 里的 iceberg/hudi 表会被当**原始 hive 文件**读。

**范围**:只 iceberg + hudi 是"双重身份"(有 `DLAType`、能寄生 HMS);paimon 无 DLAType 不寄生;jdbc/mc/es/trino 只 catalog 形态;hive 只 HMS 形态。**定性**:真实、当前在架的债,但分阶段产物——设计意图是 P7 hive cutover 时 SPI hive 连接器**检测并委托**给 iceberg/hudi 连接器,把 Case B 收进 Case A 的 SPI 路径。**尚未建**。

### 7.3 建议(接第 6 节)

8. **统一 reader-type 契约(治 #27)**:SPI 级 typed `ReaderKind` + 框架层统一 honor `force_jni_scanner`(先修 hudi 回归)+ 各连接器 eligibility 谓词返回 `ReaderKind`。这是 P7 前的独立小修,不必等 hive cutover。
9. **P7 hive cutover 必须实现 detect-and-delegate(治 #28)**:`HiveConnector` 按 `HiveTableFormatDetector` 结果委托给 iceberg/hudi 连接器,collapse Case B 进 Case A,删掉 legacy `IcebergScanNode`/`HudiScanNode` 和重复的格式检测。否则 `type=hms` 进 allowlist 当天,HMS 里的 iceberg/hudi 表会被误读成 hive 文件。

---

*本梳理基于 7 个并行 agent(5 个数据粒度 + 2 个 reader-path/dispatch)对全 SPI 面的系统性审查,全部对齐 `apache/branch-catalog-spi` tip 并与 master 对照。每条发现区分 SPI-shape(抽象本身)vs connector-fill(纪律),标注现行/休眠/潜伏/已知,可直接定位 file:line。与前两份报告(`catalog-spi-review-65185.md` 九-PR 评审、`catalog-spi-65126-analysis.md` 缓存重构)互补:那两份查"这次改动对不对",本份查"抽象本身在哪些维度粒度/路径错配"。*

