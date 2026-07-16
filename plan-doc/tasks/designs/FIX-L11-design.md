# FIX-L11 — paimon JNI/COUNT range uses table-level default file format, not per-file suffix

> reverify §1 L11 (P5-2). 严重度 🟡低。模块 = fe-connector-paimon（连接器局部，不碰 fe-core）。

## Problem

翻闸后的 paimon 连接器在为 **JNI 序列化的 DataSplit** 和 **COUNT(*) 折叠 range** 打 `file_format` 时，
用的是**表级默认** `table.options().file.format`（缺省 `parquet`），而不是**该 split 首个数据文件的实际后缀**。
当表的当前 `file.format` 选项与磁盘上历史数据文件的实际格式不一致时（例如表先以 orc 写入、后 `ALTER` 改
选项为 parquet；或混格式表），FE 会给 BE 发**错误**的 `file_format`。

影响面窄：默认 JNI reader 不消费 `file_format`（JNI 路由由 `paimon.split` 属性决定）；只有 opt-in 的
paimon-cpp reader 会把该字段回填进 `FILE_FORMAT/MANIFEST_FORMAT`，错值会破坏它的 manifest 读。故为 🟡低。

## HEAD recon（对 HEAD `ca77ea774f7` 复核，行号已核）

- `PaimonScanPlanProvider.java:411-412` — `defaultFileFormat = table.options().getOrDefault(FILE_FORMAT.key(), "parquet")`。
- `:447` — nonDataSplits 臂 `buildJniScanRange(..., defaultFileFormat, ..., isDataSplit=false, ...)`。
- `:504-505` — native 臂 `buildNativeRanges(...)` → `buildNativeRange:540` 已经用
  `getFileFormatBySuffix(file.path()).orElse(defaultFileFormat)`（**native 臂已正确**）。
- `:509-511` — DataSplit-forced-JNI 臂 `buildJniScanRange(dataSplit, ..., defaultFileFormat, ..., isDataSplit=true, ...)`。
- `:520-521` — COUNT 折叠臂 `buildCountRange(countRepresentative, ..., defaultFileFormat, ...)`。
- `buildJniScanRange:795-823` — `.fileFormat(defaultFileFormat)`（硬用表级默认）。
- `buildCountRange:843-860` — `.fileFormat(defaultFileFormat)`（硬用表级默认）。
- `getFileFormatBySuffix:1182-1193` — 私有 static，`.orc`→orc、`.parquet`/`.parq`→parquet、否则 empty。

## Legacy parity target（legacy 已删，取 git `dbc38a265e5^`）

- `PaimonScanNode.setPaimonParams:268` — `String fileFormat = getFileFormat(paimonSplit.getPathString());`，
  **所有臂**（JNI `split!=null` 与 native `split==null`）都用它，末尾 `fileDesc.setFileFormat(fileFormat)`。
- `PaimonScanNode.getFileFormat:645-646` — `FileFormatUtils.getFileFormatBySuffix(path).orElse(source.getFileFormatFromTableProperties())`。
- `PaimonSplit`（ctor）— DataSplit 的 `path = LocationPath.of("/" + dataFiles().get(0).fileName())`；
  非 DataSplit 无文件路径 → `getFileFormatBySuffix` empty → 回退表级默认。

结论：legacy 对 **DataSplit** 按**首数据文件后缀**取格式（回退表级默认）；对**非 DataSplit** 天然回退表级默认。

## Design（surgical，仿 native 臂 `:540` + legacy）

新增一个私有 static 助手（对齐已抽出的 `isCountPushdownSplit`/`computeFileSplitOffsets`/`encodeSplit` 风格，
便于单测）：

```java
/** DataSplit 的实际数据文件格式：取首个数据文件后缀（legacy PaimonSplit path = "/"+dataFiles().get(0).fileName()），
 *  回退表级默认。空文件列表（不应发生）时 fail-safe 回退默认。 */
private static String dataSplitFileFormat(DataSplit dataSplit, String defaultFileFormat) {
    List<DataFileMeta> files = dataSplit.dataFiles();
    if (files == null || files.isEmpty()) {
        return defaultFileFormat;
    }
    return getFileFormatBySuffix("/" + files.get(0).fileName()).orElse(defaultFileFormat);
}
```

改两处发射点，只对 DataSplit 生效：

1. `buildJniScanRange`：把 `.fileFormat(defaultFileFormat)` 改为
   ```java
   String fileFormat = isDataSplit
           ? dataSplitFileFormat((DataSplit) split, defaultFileFormat)
           : defaultFileFormat;
   ...
   .fileFormat(fileFormat)
   ```
   （非 DataSplit 保持 `defaultFileFormat` = legacy parity；`(DataSplit) split` 的 cast 与本方法既有的
   `computeSplitWeight((DataSplit) split)`（isDataSplit 臂）同一守卫，安全。）

2. `buildCountRange`（参数恒为 DataSplit）：`.fileFormat(dataSplitFileFormat(dataSplit, defaultFileFormat))`。

注释更新：把两处 `FIX-JNI-FILE-FORMAT` 注释从「emit real data-file format ... 目前用 defaultFileFormat」
更正为「按首数据文件后缀取（legacy getFileFormat(getPathString())），回退表级默认」。

**不动**：native 臂（已正确）、nonDataSplits 臂（无文件、legacy 亦回退默认）、`getFileFormatBySuffix`。

## Risk

- Cast 安全：`buildJniScanRange` 仅在 `isDataSplit=true` 时 cast，与既有 weight 逻辑同守卫。
- 空 `dataFiles()`：DataSplit 恒 ≥1 数据文件；仍加 fail-safe 守卫回退默认（比 legacy 更稳，happy-path 不变）。
- 多文件格式不一：legacy 只看**首**文件；本 fix 同（同一 DataSplit 内文件同格式是 paimon 不变式）。
- 行为向后兼容：当表默认 == 文件实际格式（绝大多数表），输出逐字节不变。

## Test Plan

### Unit（RED-able）— `PaimonScanPlanProviderTest`

- **新** `dataSplitFileFormatUsesFileSuffixOverTableDefault`：用 `buildRealDataSplit` 变体建 **orc** 数据文件的
  DataSplit（`.option("file.format","orc")`），断言 `dataSplitFileFormat(split, "parquet") == "orc"` 且
  `dataSplitFileFormat(split, "zzz") == "orc"`。**MUTATION**：助手直接返回 `defaultFileFormat` → 返回 "parquet"/"zzz" → RED。
  （为此把 `dataSplitFileFormat` 设为 package-private static。）
- 现有 `...RealFileFormat`（~933/960）：表默认与文件同为 orc，本 fix 后仍 == "orc"，**回归守卫**（保持绿）。

### E2E（live-gated，登记）

paimon-cpp reader（`enable_paimon_cpp_reader=true`）over 一张 `file.format` 与历史文件格式不一致的表：
断言 cpp 读不因 `file_format` 错值失败。默认 JNI reader 不受影响。→ 真集群回归（memory `hms-iceberg-delegation-needs-e2e`）。

---

## 设计红队结论（`wf_05574ccb-bd2`，3 lens · 全 SOUND / SOUND_WITH_CHANGES，无 UNSOUND）

- **MAJOR（test-build）已折入**：原「helper 孤立单测」不守护**接线**（emission point 仍发 defaultFileFormat 也能过）——
  现有 `jniAndCountRangesCarryRealFileFormatNotJni` 表默认==后缀==orc,无法区分。**加 call-site RED 测**
  `jniAndCountRangesUseFileSuffixNotAlteredTableDefault`：`Table.copy(file.format=parquet)` 令表默认=parquet 而磁盘文件仍 .orc,
  断言 JNI + COUNT range 均携 "orc"(后缀)——对 pre-fix `.fileFormat(defaultFileFormat)`(=parquet)必 RED。
- **MINOR（legacy-parity）已折入**：连接器 `getFileFormatBySuffix` 缺 legacy `FileFormatUtils` 的 `.avro` 臂→avro 数据文件
  在表默认≠avro 时偏离 legacy。**加 `.avro` 臂**(仅 native 臂+新 helper 调用;avro 永不到 native 臂,inert)。保留既有 `.parq`(非 L11 范围)。
- **MINOR（test-build）已折入**：helper 须 **package-private static**(非 `private`,否则同包测试不可见)——已按 package-private 实现。
- 三 lens 均确认:cast 受 `isDataSplit` 守卫、空 `dataFiles()` fail-safe 回退默认(比 legacy 更稳)、默认路径逐字节不变、
  RED-able 经 paimon 1.3.1 字节码证实(orc/parquet 数据文件名恒以 `.orc`/`.parquet` 结尾)。
