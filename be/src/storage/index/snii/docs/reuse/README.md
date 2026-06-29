# SNII 倒排索引 复用/解耦 审计索引（Reuse / Decouple Audit）

本目录汇总 SNII 倒排索引各组件相对 Doris 自有实现的「复用 vs 保留」逐项审计结论。总原则有二：(1) SNII 必须与 CLucene **完全解耦**（格式 / 存储 / 查询路径零 CLucene）；(2) 凡 Doris 已有等价实现，**优先复用**，仅当 Doris 实现对 SNII 明确次优（API 契合度、性能、const/uint8_t 字节语义、依赖重量）时才保留 SNII 自有代码。**红线**：任何触碰**在盘字节**（varint / zstd / crc32c / pfor / section framing / checksum）的组件，只有在 Doris 实现产出与已发布 on-disk format v2 **逐字节一致**时方可替换；否则属于「格式变更」而非「实现替换」，必须保留 SNII 代码并以黄金测试（golden test）钉死字节等价。

## 判定汇总表（Verdict Table）

| 组件 | Doris 等价 | 判定 | 碰在盘字节 | 字节兼容 | 工作量 | 调用点 |
|------|-----------|------|-----------|---------|-------|--------|
| [R01-status](R01-status.md) — snii::Status | 是 (doris::Status) | reuse-with-extension | 否 | n/a | L | 1400 |
| [R02-slice](R02-slice.md) — Slice 只读视图 | 是 (doris::Slice) | keep-snii-doris-suboptimal | 否 | n/a | S | 277 |
| [R03-varint](R03-varint.md) — varint LEB128 | 是 (util/coding.h) | keep-snii-doris-suboptimal | 是 | byte-identical | S | 17 |
| [R04-zstd](R04-zstd.md) — zstd 编解码 | 是 (ZstdBlockCompression) | keep-snii-doris-suboptimal | 是 | incompatible | S | 9 |
| [R05-crc32c](R05-crc32c.md) — crc32c 校验 | 是 (thirdparty crc32c) | **reuse-doris** | 是 | byte-identical | S | 28 |
| [R06-byte-sink-source](R06-byte-sink-source.md) — ByteSink/ByteSource | 部分 (faststring+coding.h) | keep-snii-doris-suboptimal | 是 | byte-identical | L | 344 |
| [R07-pfor](R07-pfor.md) — PFOR 位打包 | 否 (Doris 是纯 FOR) | keep-snii-doris-suboptimal | 是 | incompatible | S | 7 |
| [R08-section-framer](R08-section-framer.md) — SectionFramer | 否 | keep-snii-no-equivalent | 是 | incompatible | S | 10 |
| [R09-file-rw-abstraction](R09-file-rw-abstraction.md) — FileReader/Writer 抽象 | 是 (io::FileReader/Writer) | keep-snii-doris-suboptimal | 否 | n/a | S | 70 |
| [R10-io-local-s3](R10-io-local-s3.md) — Local/S3 后端 | 是 (io::Local*/S3*) | **reuse-doris** | 否 | n/a | M | 2 |
| [R11-io-batch-metered](R11-io-batch-metered.md) — BatchRangeFetcher/Metered/Metrics | 部分 (MergeRangeFileReader) | keep-snii-doris-suboptimal | 否 | n/a | S | 50 |
| [R12-writer-infra](R12-writer-infra.md) — TempDir/MemoryReporter/SpillBuffer | 近似 (Spill/MemTracker) | keep-snii-doris-suboptimal | 否 | n/a | M | 8 |
| [R13-clucene-decoupling](R13-clucene-decoupling.md) — CLucene 解耦核查 | 是 (InvertedIndexAnalyzer) | **reuse-doris** | 否 | n/a | S | 8 |

## 计数汇总（Summary Counts）

- **#reuse = 4**：R01（reuse-with-extension，扩展 ErrorCode 后归一到 doris::Status）、R05（crc32c 复用 thirdparty）、R10（Local/S3 复用 Doris io）、R13（分词设施复用 Doris）。
- **#keep = 9**：R02、R03、R04、R06、R07、R08、R09、R11、R12。
- **#drop = 0（含 1 处子级死代码删除）**：无整组件丢弃；但 R10 内的 SNII standalone S3 后端在产品构建中已被 CMake 排除、零调用方，属纯重复死代码，应随 R10 复用一并删除。

## 基础涟漪（Foundational Ripples）— 优先落地

- **R01 Status（1400 调用点）** 与 **R02 Slice（277 调用点）** 是贯穿整个 SNII 核心的基础类型。Status 判定为复用 + 扩展，改动面最广；Slice 判定为本期保留。两者一旦变动会波及几乎所有 format / query / io 文件，属「宽涟漪」。**Status 的复用应尽早落地**（机械替换为主，分文件分批迁移），避免后续组件迁移时反复触碰错误传播代码；**Slice 本期不动**，避免无收益的 277 处机械迁移污染 diff。详见 [sequencing.md](sequencing.md)。
