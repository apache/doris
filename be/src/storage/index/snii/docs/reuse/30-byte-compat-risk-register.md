# 在盘字节兼容 风险登记册（On-Disk Byte-Compat Risk Register）

覆盖所有触碰在盘字节的组件（R03/R04/R05/R06/R07/R08）。每项列出：风险、缓解黄金测试、若非字节一致的 KEEP fallback。

> 总闸门：任何在盘组件的复用 PR 必须先让 byte-identity golden test 变绿；红则一律 KEEP fallback、不合入。

---

## R03 — varint LEB128 ｜ byte_compat = byte-identical ｜ 判定 KEEP

- **风险**：SNII varint 与 Doris `coding.h` varint 同为标准 LEB128（7-bit + 0x80 continuation，小端），当前字节一致。但 SNII 未复用（次优：缺 zigzag、防御式带 `end` 边界 + `Status::Corruption` 诊断、encode 返回写入字节数、依赖更轻）。**主风险是未来两者各自演进导致 on-disk 格式静默漂移**。
- **缓解黄金测试**：cross-decode 黄金测试——同一组数值用 SNII 编码、用 Doris 解码（及反向）必须等价；并对固定数值向量断言字节序列不变。钉死字节等价。
- **KEEP fallback**：已是 KEEP，保留 `varint.{h,cpp}`（含 zigzag 辅助）。无需迁移即天然安全；测试仅用于演进守护。

---

## R04 — zstd 编解码 ｜ byte_compat = incompatible ｜ 判定 KEEP

- **风险**：写路径字节不一致——Doris 写端置 `ZSTD_c_checksumFlag=1`（额外 4 字节 XXH64 帧尾）且用 `ZSTD_compressStream2` 流式编码、帧头无 content-size；SNII 用一次性 `ZSTD_compress`（帧头含 content-size、无 checksum）。级别同为 3 但 framing 字节不同。**换 Doris 写端 = 改 format v2 字节 = 格式变更（红线）**。次要：Doris 解压每次走 `std::mutex` 上下文池，与 SNII const/无锁只读热路径冲突。
- **缓解黄金测试**：cross-decode 测试 + **字节差异断言**——锁定「两端解码互通、但写端字节不同」这一结论，防止未来误判为可直接换写端。
- **KEEP fallback**：保留 `zstd_codec.{h,cpp}`。禁止复用切换。

---

## R05 — crc32c 校验 ｜ byte_compat = byte-identical ｜ 判定 REUSE-DORIS ✅

- **风险**：唯一采纳的在盘复用。风险点是「校验值是否逐字节一致」。SNII 用 Castagnoli 多项式（0x1EDC6F41 / 位反射 0x82F63B78）+ 标准 `~crc` 预/后取反，与 Google crc32c 库（RocksDB/LevelDB 同源）规范值一致。次要：`crc32c_extend` 的链式语义——SNII 入口/出口取反，Google `Extend` 遵循相同约定；当前 SNII 全部调用点均为一次性 `crc32c(slice)=Extend(0,...)`（grep 未见非零 crc 的 extend），链式差异不触发。
- **缓解黄金测试**：crc32c 黄金向量测试 + 既有 on-disk 校验回放，证明 `crc32c::Crc32c(data)` 与 SNII 实现对同一字节串结果完全一致。**测试绿后方可切换**。
- **KEEP fallback**：若任一向量不一致即属格式变更，立即恢复 `crc32c.cpp`，header API（`snii::crc32c(Slice)` wrapper）不变，零成本回退。

---

## R06 — ByteSink/ByteSource 序列化缓冲 ｜ byte_compat = byte-identical（写侧）｜ 判定 KEEP

- **风险**：**最高风险在盘组件**——是所有 section 序列化/反序列化的唯一通道（47 文件、约 344 调用点），输出经 framer/CRC/zstd 后落盘构成 format v2。写侧字节可与 `coding.h` 对齐（LEB128 / `encode_fixedN_le` 一致），但 Doris 缺 `put_fixed16_le` 且无字节对齐 zigzag（zigzag 仅在 `bit_stream_utils.h` 位流，paradigm 不同）；读侧 `ByteSource`（position/remaining/eof/slice_from + `Status::Corruption` 逐字段诊断）**无 Doris 等价物**，`coding.h get_varint*` 返回 bool、按 `remove_prefix` 消费、无绝对偏移游标、无诊断。**任何字节回归都会破坏已发布格式且难以局部回滚**；read 侧若改 bool 语义将丢失 Corruption 诊断、放大解析期排障成本。
- **缓解黄金测试**：cross-decode 黄金测试钉死写侧字节等价（SNII 写 / Doris 解；固定字段集字节断言），防演进漂移。
- **KEEP fallback**：已是 KEEP，保留 `byte_sink.{h,cpp}` / `byte_source.{h,cpp}`（含 put_fixed16/put_zigzag、读游标、Status 诊断）。不迁移。

---

## R07 — PFOR 位打包 ｜ byte_compat = incompatible ｜ 判定 KEEP

- **风险**：Doris 是纯 FOR（无异常表、强制 128 值/帧、整帧 MinValue 减法 + 三种 StorageFormat、参数置尾部 footer），SNII 是 patched-frame-of-reference（选最小总字节 bit_width、超宽值入 (index_delta,value) varint 异常表、每 block 头内联 `[u8 width][varint n_exc]`）。**算法不同、在盘布局不同、无法字节对齐**。`BitPacking` 只有 `UnpackValues`（无 encoder、要求 32 值对齐边界），无法承担编码侧。**强行换 Doris FOR = 改 format v2 在盘字节 = 历史 SNII 索引无法解码（红线越界）**。
- **缓解黄金测试**：N/A（不复用）。如需防护，可对 SNII pfor 自身做 round-trip + 历史样本回放（含 n=1/255/256 边界）。
- **KEEP fallback**：保留 `pfor.{h,cpp}`（含 `bitunpack_w1..w8` 快速路径与 `pfor_skip`）。不在本工作流范围内替换。

---

## R08 — SectionFramer 段封装 ｜ byte_compat = incompatible ｜ 判定 KEEP（无等价物）

- **风险**：Doris **无等价的段封装工具**。SNII framing `[u8 type][varint64 len][payload][fixed32 crc32c(type+len+payload)]` 是 format v2 定义本身。最接近的 `PageIO` 用 protobuf `PageFooterPB` + `footer_length(uint32)` + checksum，字节布局、校验范围、类型分发机制全不同，无法承载 SNII 的 type 分发 + 跳过未知可选段。换用 = 格式变更。
- **缓解黄金测试**：SectionFramer round-trip + 历史样本回放（验证 type/len/crc framing 字节不变）；并绑定 R05 crc32c 字节一致性（framer 经 `snii::crc32c(framed.view())` 委托校验）。
- **KEEP fallback**：保留 `section_framer.{h,cpp}`。唯一外部依赖是 `snii::crc32c`（R05）——只要 R05 维持 KEEP 或做到字节一致替换，本组件不受影响。
