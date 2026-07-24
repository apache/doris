# 类别 D — Scan / split 文件级

## 范围说明

本类别汇集 catalog SPI 迁移中,与「扫描规划(scan planning)/ split 构造 / 文件级元数据下发」相关的四条发现:paimon per-split file format 来源(#3)、`ConnectorScanRange` 缺压缩类型槽(#6)、`ConnectorDeleteFile` 欠建模/冗余(#21)、iceberg residual 表达式的下发粒度(#26)。它们共同触及一个主题:通用 fe-core 扫描节点(`PluginDrivenScanNode` / `ConnectorScanRange`)与各连接器之间,文件级(per-file / per-split)属性(格式、压缩、删除文件、残差谓词)如何建模与传递。

## 基线声明

本核实基于当前工作树 branch `catalog-spi-2-lvl-cache`(复核时 HEAD = `b20ffb3bd5c`, 2026-07-21,相关文件无未提交改动),对每条发现逐条独立读取当前代码 + 对抗复核(stage-1 investigate 与 stage-2 verify 双阶段),不轻信原 survey 的结论或行号。所有结论均引 file:line。凡两阶段有分歧,文中明确指出以哪一阶段为准及理由。

## 总表

| # | 严重 | 原报告结论 | 核实结论 | 一句话 |
|---|------|-----------|---------|--------|
| 3 | 🔴 | paimon JNI/COUNT 路径把表级 file.format 填进 per-split,per-file format 丢,ORC 数据被 paimon-cpp 误读 | STALE_FIXED | bug 曾存在,已由 P7-1(#65473 系列 commit)修复,JNI/COUNT 两路径均改用 per-file 后缀 format,并有回归测试钉死 |
| 6 | 🟠 | `ConnectorScanRange` 无压缩槽,`PluginDrivenScanNode` 丢了 legacy 的 LZ4FRAME→LZ4BLOCK 重映射 | STALE_FIXED | 无压缩槽是有意设计(走 SPI 钩子);LZ4 重映射经 `adjustFileCompressType` 已完整恢复,随 hive cutover 同 commit 落地并配单测 |
| 21 | 🟡 | `ConnectorDeleteFile` 欠建模(缺 content-type/序列号/bounds/field-id),iceberg 绕开自带 typed 载体,getter 零调用者 | CONFIRMED | 三条事实全部成立;是死且欠建模的 SPI 表面,非活 bug;建议清理删除 |
| 26 | 🟡 | iceberg residual 表达式在 scan-node 级而非 per-range 下发,对齐 legacy | CONFIRMED | 属实且对齐 legacy;非 bug,仅极轻微的 BE 冗余计算,无正确性影响 |

---

## #3 paimon JNI/COUNT file format 填表级

**核实结论**:STALE_FIXED,置信度 high。两阶段一致(stage-1 与 stage-2 verify 均判 STALE_FIXED)。verify 仅纠正一处非实质性偏差:stage-1 引用 commit `3593684715f`(2026-07-16),而复核时 HEAD 已推进到 `b20ffb3bd5c`(2026-07-21),文件内容与行号仍逐行吻合,修复照旧在位,不影响 verdict。以两阶段共识为准。

**原报告主张**:paimon 的 JNI 与 COUNT 扫描路径把「表级 `file.format`」填进 per-split 的 `getFileFormat()`,而只有 native 路径(`buildNativeRange`)用 per-file 后缀;当 ORC 数据文件所在表的 `file.format` option 与磁盘后缀不符(或缺 option)时,paimon-cpp 会按错误格式误读,per-file format 丢失。

**核实过程**:两阶段均亲读当前 `PaimonScanPlanProvider.java` 三条 range 构造路径的 format 来源。

- `buildNativeRange`(`PaimonScanPlanProvider.java:674`):`getFileFormatBySuffix(file.path()).orElse(defaultFileFormat)` —— per-file 后缀,fallback 表级。主张对此段的陈述至今仍准确。
- `buildJniScanRange`(`PaimonScanPlanProvider.java:948-950`):`String fileFormat = isDataSplit ? dataSplitFileFormat((DataSplit) split, defaultFileFormat) : defaultFileFormat;` —— DataSplit 走 per-file 后缀,只有非 DataSplit(本就无实体数据文件)才回退表级。**与主张所说「用表级 format」相反。**
- `buildCountRange`(`PaimonScanPlanProvider.java:989`):`.fileFormat(dataSplitFileFormat(dataSplit, defaultFileFormat))` —— per-file 后缀。
- `dataSplitFileFormat`(`PaimonScanPlanProvider.java:1331-1337`):取 `dataSplit.dataFiles().get(0).fileName()` 后缀,经 `getFileFormatBySuffix`(识别 .avro/.orc/.parquet/.parq),空 files 或后缀不识别才 fallback `defaultFileFormat`。精确复刻 legacy `PaimonScanNode.getFileFormat(getPathString())`。
- 代码内 FIX 注释(`PaimonScanPlanProvider.java:941-947 / 986-987 / 1322-1330`)明确点名:"HEAD had regressed to the bare table-level file.format default (wrong when the option differs from the on-disk files, e.g. an altered/mixed-format table)" —— 与主张描述的 bug 形态一一对应。
- 回归测试(`PaimonScanPlanProviderTest.java:967-1042`):构造 orc 数据文件 + `table.copy(file.format=parquet)` overlay 的 altered/mixed 场景,断言 JNI data range(:1025-1026)与 COUNT range(:1041-1042)的 `getFileFormat()` 必须 == `"orc"`(真实后缀),并带 mutation 注释(改回 default/`"jni"` 即变红);另 :894-961 有 orc-only 场景断言不得为 `"jni"`。

**为何已修**:主张描述的 bug 确曾存在(某个 HEAD 回归阶段,JNI/COUNT 路径把表级 `file.format` 填进 per-split format),但已由 P7-1(commit `3593684715f`,作者 Mingyu Chen)修复,当前工作树即修复后状态。核心指控「JNI/COUNT 填表级 → per-file format 丢 / ORC 误读」在现行代码不再成立:两条路径对有实体数据文件的 DataSplit 均已改用 per-file 后缀 format,且退化路径(改回 default/`"jni"`)被回归测试钉死为不可再回归。属于报告基于旧代码、并低估了已存在的修复与测试覆盖。无需再改代码。

---

## #6 ConnectorScanRange 无 getCompressType()

**核实结论**:STALE_FIXED,置信度 high。两阶段一致。stage-2 verify 额外从 legacy 源码(`3593684715f~1:.../datasource/hive/source/HiveScanNode.java` 第 677-683 行)取证,更硬地佐证了「无后缀文本/表属性编码」老实现同样不处理这一非回归结论;并纠正 stage-1 一处路径笔误(legacy 实为 `datasource/hive/source/HiveScanNode.java`,多一层 `source` 子包)。以 verify 为准,不影响 verdict。

**原报告主张**:`ConnectorScanRange` 有 `getFileFormat()` 但无压缩槽,引擎只用 `Util.inferFileCompressTypeByPath` 按后缀推压缩类型,`PluginDrivenScanNode` 丢了 legacy `HiveScanNode` 的 LZ4FRAME→LZ4BLOCK 重映射;当真编码与后缀不符(hadoop block-lz4、无后缀压缩文本、表属性编码)时 BE 解码失败/错行。标注「休眠(P7,hive 未 cutover)」。

**核实过程**:

1. 结构性事实成立:`ConnectorScanRange.java:67` 只有 `default String getFileFormat()`,:194 序列化为 `connector_file_format`;全文件 grep 无 `getCompressType`/`CompressType`。
2. 但引擎已不再「只按后缀推、无从修正」。新增 SPI 钩子 `ConnectorScanPlanProvider.adjustFileCompressType(TFileCompressType inferred)`,默认 identity(`ConnectorScanPlanProvider.java:125-127`),javadoc(:118-120)解释 hadoop block-lz4 场景。
3. `PluginDrivenScanNode.getFileCompressType(FileSplit)` 已 override(`PluginDrivenScanNode.java:651-659`):先跑 `super.getFileCompressType`(= `FileQueryScanNode.java:634-635` 的 `Util.inferFileCompressTypeByPath` 后缀推断)得 inferred,`scanProvider==null` 回退 inferred,否则 `onPluginClassLoader(scanProvider, () -> scanProvider.adjustFileCompressType(inferred))`(钉 TCCL)交连接器定夺。
4. Hive override 恢复了重映射:`HiveScanPlanProvider.java:211-214` `return inferred == LZ4FRAME ? LZ4BLOCK : inferred;`,javadoc(:203-209)明述「hadoop/hive 把 .lz4 写成 LZ4 block 编码,后缀推出的 frame 会让 BE 文本/CSV reader 报 LZ4F_getFrameInfo ERROR_frameType_unknown」—— 正是主张所述失败场景。Hudi 亦 override(`HudiScanPlanProvider.java:134-135`)。
5. 多重 parity 单测护住:`HiveScanBatchModeTest.java:290-300`(只 remap LZ4FRAME,GZ/ZSTD/SNAPPYBLOCK/PLAIN 透传)、`HudiBackendDescriptorTest.java:122-132`、`ConnectorScanPlanProviderCompressTypeTest.java:57-58`(default 对所有类型 identity)。
6. `git log -S adjustFileCompressType` = `3593684715f`(#65473),与 hive cutover 同一 commit。
7. verify 亲验 legacy 源(`3593684715f~1:.../datasource/hive/source/HiveScanNode.java` 第 677-683 行):legacy 也仅 `super.getFileCompressType` + 单条 `if (LZ4FRAME) → LZ4BLOCK`,其余全靠 super = `inferFileCompressTypeByPath`。

**为何非缺陷 / 已修**:

- 结构性观察「`ConnectorScanRange` 无压缩槽」至今仍真(`ConnectorScanRange.java:67`),但这是**有意设计**,不是缺陷:修复者刻意不在 scan-range 上加压缩槽,而走连接器能力位 `adjustFileCompressType`,让通用节点保持 connector-agnostic(与本仓库 `PluginDrivenScanNode` 禁 source-specific 代码的铁律一致)。
- 核心指控「丢失 LZ4FRAME→LZ4BLOCK 重映射」**曾成立、已修复**:该重映射经 `HiveScanPlanProvider.adjustFileCompressType` + `PluginDrivenScanNode.getFileCompressType` 完整恢复,随 hive cutover 同一 commit(#65473)落地并配单测。
- 报告状态「休眠(hive 未 cutover)」**已过时**:hive 已 cutover(legacy `datasource/hive/source/HiveScanNode.java` 已删),修复随之在位。
- 主张附带的「无后缀压缩文本、表属性编码」等情形属**夸大/非回归**:verify 已从 legacy 源码直接证实,老实现本身也只做 LZ4 一项重映射、其余同样落到按后缀的 `inferFileCompressTypeByPath`;这些情形老实现同样不处理,不构成 SPI 迁移引入的回归。

无需再改动。

---

## #21 ConnectorDeleteFile 欠建模/vestigial

**核实结论**:CONFIRMED,置信度 high。两阶段一致,verify 无 corrections。

**原报告主张**:`ConnectorDeleteFile` 只建模 (path, format, recordCount, properties),缺 content type(position/equality/DV)、data/delete 序列号、position 上下界、equality field-id;iceberg 绕开它用内部 typed `IcebergScanRange.DeleteFile`;返回 `ConnectorDeleteFile` 的 `getDeleteFiles()` 零生产调用者。

**核实过程**:三条事实断言逐一复核成立。

1. `ConnectorDeleteFile.java:35-38` 确实只有 4 字段 path、fileFormat、recordCount、properties;缺 content-type、序列号、position bounds、equality field-id、DV puffin offset/size。verify 复读 :31-79 确认。
2. 全树 `grep -rn ConnectorDeleteFile fe/ --include=*.java` 仅 5 命中,全落在两文件:`ConnectorDeleteFile.java` 自身(声明 + 2 构造 + toString)与 `ConnectorScanRange.java:149`。**无第三方引用、无 override、无 caller。**
3. `ConnectorScanRange.java:149-151` 的 default `getDeleteFiles()` 返回 `List<ConnectorDeleteFile>`,恒 `Collections.emptyList()`。从声明到消费全线空置。
4. Iceberg 完全绕开,自带内部 typed 载体 `IcebergScanRange.DeleteFile`(`IcebergScanRange.java:617-676`):Serializable 静态类,字段 content(1=position/2=equality/3=DV)、fileFormat、positionLowerBound/UpperBound、fieldIds(equality)、contentOffset/contentSizeInBytes(deletion vector),序列化进 `TIcebergDeleteFileDesc`(`IcebergScanRange.java:306-308, 413-414`)—— 正好补齐 `ConnectorDeleteFile` 所缺维度。

**须避免的误读**(主张指认精确,不要混淆):另有一个同名但签名不同的方法 `ConnectorScanPlanProvider.getDeleteFiles(TTableFormatFileDesc)`(返回路径字符串列表 `List<String>`,`ConnectorScanPlanProvider.java:506`),它是生产在用的(`FileScanNode.java:140` / `PluginDrivenScanNode.java:722-727` 调用,`IcebergScanPlanProvider.java:1816`、`PaimonScanPlanProvider.java:1499` override),但仅用于 EXPLAIN 回读删除文件路径,与返回 `ConnectorDeleteFile` 类型的那个 getter 是两回事。

**背景**:相比 iceberg MOR 删除文件所需信息,`ConnectorDeleteFile` 缺 content type、序列号、position 上下界、equality field-id 列表、DV 的 puffin blob offset/size。iceberg 因此不用它,自带完整 typed 载体读取,删除语义完整、结果正确。

**影响**(为何判 CONFIRMED 而非活 bug):这是抽象错配/冗余,**不产出错误查询结果**,无某表某查询到错误结果的可复现路径。iceberg 走自己的 typed DeleteFile,读结果正确;返回 `ConnectorDeleteFile` 的 `getDeleteFiles()` 无人 override、无人调用,是死代码,不影响任何运行时行为。真正负面影响在架构层——SPI 里躺着一个既欠建模又无人用的删除文件抽象,误导后来者。具体示例:若未来某新连接器(如给 paimon DV 或某湖格式补 MOR)天真地去 override `ConnectorScanRange.getDeleteFiles()` 并返回 `ConnectorDeleteFile`,会发现 (a) 引擎侧根本无消费端,返回值被丢弃、删除不生效;(b) 即便接上消费端,`ConnectorDeleteFile` 也表达不了 position/equality/DV 的区分与 field-id/bounds,无法驱动 BE 的 MOR 读取,只能推倒重来。这就是「潜伏」的含义。

**修复方案**(清理性质,非紧急):

1. **推荐**:直接删除死代码——移除 `ConnectorScanRange.getDeleteFiles()` 默认方法与 `ConnectorDeleteFile` 类(全树无 override 无 caller,删除零运行时风险),消除误导性 SPI 表面,最贴合本仓库删旧代码、减少 fe-core 表面的方向。
2. 若打算把 delete-file 建模提升为跨连接器通用 SPI,则需把 `ConnectorDeleteFile` 扩成带 content-type、bounds、fieldIds、DV offset/size 的完整模型,让 iceberg 用它替换内部 `DeleteFile`、同时接通引擎消费端;但这与「每连接器各自 typed 载体 + 直发 thrift」的既有范式冲突,属较大重构,须先定架构高度,不建议清理阶段顺手做。推荐方案 1,方案 2 留作独立设计议题。

---

## #26 iceberg residual 不 per-range 下发

**核实结论**:CONFIRMED,置信度 high。两阶段一致,verify 复核 buildRange、advance、`IcebergScanRange` 字段及 residual grep 后确认 stage-1,无 corrections。

**原报告主张**:iceberg 的 residual expression 在 scan-node 级(整套下压 conjuncts)而非 per-file/per-range 下发;对齐 legacy。

**核实过程**:每个事实断言均成立。

1. 每文件 residual 仅用于 FE 侧剪枝:`IcebergScanPlanProvider.java:1997-1999` 建 `residualEvaluators`;:2111 `currentResidual.residualFor(dataFile.partition()).equals(Expressions.alwaysFalse())` 命中即 skip 该文件 —— 这是 residual 的唯一用途(FE 剪枝)。
2. 生成 BE range 的 `buildRange`(`IcebergScanPlanProvider.java:1241-1278`)完全不读 `task.residual()`,只写 path/start/length/fileFormat/formatVersion/partitionSpecId/partitionDataJson/partitionValues/deleteFiles 等,无任何 per-range filter/residual 字段。
3. `IcebergScanRange.java` 无 residual/conjunct/predicate/filter 字段(grep 零命中)。
4. 全连接器 grep:`FileScanTask.residual()` 从未被读取/序列化下发;residual 相关仅 `ResidualEvaluator`/`residualFor` 的 FE 剪枝。
5. legacy `IcebergScanNode` 已从 fe-core 删除,但 `buildRange`/`computePerFileInvariants` 注释(:1230-1234)明示忠实镜像 legacy `createIcebergSplit`/`setIcebergParams`,legacy 同样不下发 per-file residual。

结论:BE 收到的过滤是 scan-node 级的整套下压 conjuncts(对每个文件逐行 apply),而非 iceberg 每文件裁剪后的 residual。

**背景**:iceberg 规划时,SDK 会为每个 `FileScanTask` 算一个 residual(残差谓词):把整体过滤按该文件的分区值绑定后,剩下 BE 仍需逐行验证的部分。理论上,若某文件的分区值已完全满足某分区谓词,该谓词可从该文件的 residual 中剔除,BE 无需对这些行重复验证。Doris(连接器与 legacy 一致)不利用这一 per-file 裁剪,而是把整套下压 conjuncts 挂在 scan-node 上。

**影响**:无正确性影响,仅极轻微的 BE 冗余计算。具体示例:一张按 `dt` 分区的 iceberg 表,查询 `WHERE dt='2026-01-01' AND val>10`。命中 `dt='2026-01-01'` 分区的数据文件,其 SDK per-file residual 会退化为仅 `val>10`(dt 谓词已由分区值满足);但 Doris 下发给 BE 的是整套 `dt='2026-01-01' AND val>10`,BE 对该分区每个文件的每一行仍会重复评估恒真的 `dt='2026-01-01'`。**结果完全正确**,只是多做一次恒真判断,开销可忽略。与 legacy 行为一致。

**是否需修复**:不需要作为缺陷修复。这是有意的、与 legacy 对齐的行为,severity 🟡/S 合理。若未来要做 per-file residual 下压优化,需在 `IcebergScanRange`/thrift 增 per-range residual 字段、BE 支持 per-split 覆盖 scan-node conjuncts —— 属大改且收益薄(仅省去恒真分区谓词的逐行判断),不建议在本迁移分支内做。

---

## 本类别小结

**真问题(需处理)**:仅 #21(`ConnectorDeleteFile` 欠建模/死代码)与 #26(iceberg residual 非 per-range 下发)判 CONFIRMED,但二者**均非活 bug、均无正确性影响**:

- #21 是死且欠建模的 SPI 表面,属架构清理项(推荐直接删除),风险为「误导后来者」而非「产出错误结果」。
- #26 是与 legacy 对齐的有意设计,仅带极轻微 BE 冗余计算,收益薄,不建议在迁移分支内优化。

**已修/伪问题**:#3、#6 均判 STALE_FIXED —— 报告描述的 bug 形态确曾在某回归阶段存在,但都已由 hive/paimon 的 P7 系列 cutover commit(`3593684715f` / #65473 一线)修复并配回归测试。二者的报告状态标注(#3 的「现行·已知」、#6 的「休眠·未 cutover」)均已过时。

**共性根因**:四条发现共同折射 SPI 迁移的一个核心张力——**文件级(per-file/per-split)语义如何在保持通用节点 connector-agnostic 的前提下正确传递**。项目选定的范式是「通用节点不含 source-specific 逻辑,per-connector 差异经能力位 SPI(`adjustFileCompressType`)或连接器自带 typed 载体(`IcebergScanRange.DeleteFile`)+ 直发 thrift 表达」。#3/#6 正是「回归到表级默认值、丢失 per-file 精度」的两次翻车与修复,#6 的修法(能力位钩子而非在 scan-range 加字段)恰是范式的正面示例;#21 则暴露该范式的副作用——通用 SPI 上留下了未随范式清理的欠建模死抽象(`ConnectorDeleteFile`),而真实删除语义早已由连接器 typed 载体承载。

**与其他类别的关联**:#6 的 `adjustFileCompressType` 能力位设计,与「`PluginDrivenScanNode` 禁 source-specific 代码 / connector-agnostic 铁律」「按大小的通用特性须能力 opt-in」一脉相承,可与「能力 SPI / opt-in」相关类别互参。#3/#6 的「回归到表级默认」模式,与其他类别中「per-connector 精度在迁移中退化为通用默认」的发现同源,可对照排查。
