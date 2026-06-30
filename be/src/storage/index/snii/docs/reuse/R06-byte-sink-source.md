# R06-byte-sink-source — ByteSink/ByteSource 序列化缓冲

## 结论与依据
判定 **keep-snii-doris-suboptimal**：保留 SNII 自有 ByteSink/ByteSource。

依据：
1. 该组件已完全 CLucene-free（`be/src/storage/index/snii/encoding/byte_sink.h`、`byte_source.h` 纯 `snii` 命名空间，0 CLucene 引用），解耦目标已达成，无需为解耦而迁移。
2. 写侧字节虽可与 Doris 对齐（见下「字节兼容性」），但读侧 ByteSource 在 Doris 中**无等价游标**：
   - `byte_source.h:25-30` 暴露 `position()/remaining()/eof()/slice_from(start,len)`，`section_framer` 依赖其绝对偏移与 `slice_from` 回退重算 CRC 覆盖区；
   - `byte_source.cpp:8/14/23/32/64` 以 `Status::Corruption(<逐字段消息>)` 报错；
   - Doris `coding.h:175-200` 的 `get_varint32/64` 返回 `bool` 且 `remove_prefix` 消费 `Slice`，无偏移游标、无 Status、无带界定长 LE 读取器。基于 Doris 原语重建 = 重写 ByteSource。
3. 写侧 Doris 覆盖不完整：`coding.h` 仅有 `put_fixed32_le/64_le/128_le`，**缺 `put_fixed16_le`**；且无字节对齐 zigzag（zigzag 仅见于 `bit_stream_utils.h` 的位打包读写，paradigm 不符），而 SNII 需要 `put_fixed16`/`put_zigzag`(byte_sink.h:15,20)。
4. 类型与迁移成本：`take()`→`std::vector<uint8_t>`、`view()`→`doris::snii::Slice` 被 53 处直接消费；改用 `faststring::build()`→`doris::OwnedSlice` 将跨约 344 调用点改动返回/切片类型，收益仅边际。

## Doris 等价物
- 后端缓冲：`doris::faststring`（`be/src/util/faststring.h`）——可替代 `std::vector<uint8_t> buf_`。
- 写原语：`be/src/util/coding.h` 的 `encode_varint32/64`、`encode_fixed32/64/128_le`、`put_varint32/64`、`put_fixed32_le/64_le`。
- 读原语：`coding.h` 的 `get_varint32/64`、`decode_fixed*_le`（自由函数，无游标语义）。
- 缺口：`put_fixed16_le`、字节对齐 `put_zigzag/get_zigzag`、带 position/Status/slice_from 的读游标——均无等价。

## 是否最优
次优。Doris 仅提供散点原语，无法整体替换本组件而不重写 source 侧并扩展 sink 侧，且不带来字节或显著性能收益。

## 字节兼容性结论（byte-identical）
- varint：SNII `encode_varint64`(varint.cpp:14-22) 与 Doris `encode_varint64`(coding.h:114-125) 均为标准 LEB128，低 7 位 + 0x80 续位，逐字节一致；varint32 同（均委派 64 实现）。
- 定长：SNII `put_fixed16/32/64`(byte_sink.cpp:7-17) 的 `v>>(8*i)` 小端写出，与 `encode_fixedN_le`(coding.h:32-45) 在大小端平台均产出小端字节，一致。
- zigzag：SNII 公式 `(v<<1)^(v>>63)`(varint.h:19) 为规范 protobuf zigzag，与任何标准实现一致（但 Doris 无字节对齐版本）。
- u8/bytes：`push_back`/`append` 直拷，一致。
- 结论：若仅做「内部缓冲 std::vector→faststring」的等价替换，输出严格 byte-identical；但仍**必须**以黄金测试守住（见 TDD）。

## 迁移设计（保留前提下的可选低风险优化）
本判定为 KEEP，不做强制迁移。若后续要消化 faststring 的分配收益，建议**仅内部替换、保持公共 API 不变**：
- 改动面：仅 `byte_sink.h`/`byte_sink.cpp`，`buf_` 由 `std::vector<uint8_t>` 换为 `doris::faststring`；`put_*` 改走 `coding.h` 的 `put_fixed*_le`/`put_varint*`（fixed16 与 zigzag 保留 SNII 行内实现以补缺口）。
- 签名不变：`size()/buffer()/view()/clear()/take()` 对外语义保持；`take()` 用 `faststring::build()` 转 `std::vector` 或改造调用方接 `OwnedSlice`（后者属破坏性，不在本轮）。
- ByteSource 不动（无等价物）。
- 风险/回滚：改动隔离在 sink 两文件；以 byte-identity 黄金测试 + cross-decode 把关，回滚即还原两文件。

## 若 KEEP 的明确理由
ByteSource 在 Doris 无任何游标等价物（bool 语义、无 position/Status/slice_from）；ByteSink 在 Doris 缺 fixed16 与字节对齐 zigzag；且 take/view 的 vector/doris::snii::Slice 类型贯穿 344 调用点。整体替换需重写 source、扩展 sink、并做全量类型迁移，换来的仅是边际分配收益与零字节收益——不划算，故保留。

---

## TDD

## 测试目标 target
`be/test`，二进制 `doris_be_test`；新增 `be/test/storage/index/snii/snii_byte_codec_test.cpp`（与既有 `be/test/storage/index/snii_query_test.cpp` 同 target）。

## RED → GREEN → REFACTOR

### 1. 功能验证（RED 先写断言，当前实现应直接 GREEN）
- `Sink_PutGet_RoundTrip`：对 u8 / fixed16 / fixed32 / fixed64 / varint32 / varint64 / zigzag(含负数与边界 INT64_MIN/MAX) / bytes 逐一 put 后用 ByteSource get，断言值与 `position()/remaining()/eof()` 推进正确。
- `Source_Overrun_ReturnsCorruption`：截断缓冲，断言 `get_*` 返回 `Status::Corruption` 且 `position()` 不前移。
- `Sink_ReuseAfterClear`：`clear()` 后容量保留、再次编码字节与全新 sink 一致。
- `Source_SliceFrom_CRCWindow`：构造前缀+载荷+CRC 布局，断言 `slice_from(start,len)` 取回的覆盖区与手算一致（守住 framer 依赖）。

### 2. 等价性验证（新旧/Doris-原语 结果一致）
- `Varint_Equiv_Doris`：对一组确定性样本（0,1,0x7F,0x80,0x3FFF,0x4000,UINT32_MAX,UINT64_MAX 及随机固定种子）比较 SNII `encode_varint64` 与 Doris `coding.h::encode_varint64` 输出，`memcmp==0`。
- `Fixed_Equiv_Doris`：比较 `ByteSink::put_fixed16/32/64` 与 `encode_fixed16/32/64_le` 字节一致。

### 3. 黄金字节测试（on-disk 必备）
- `ByteSink_Golden_V2`：用固定脚本序列写一段混合记录（多字段顺序固定），与硬编码的 format v2 期望字节数组逐字节断言 `ASSERT_EQ(memcmp,0)`。该黄金值在任何「内部缓冲换 faststring」改动前后必须保持不变。
- `ByteSink_Faststring_Identity`（若执行可选迁移）：同一序列分别走「旧 std::vector 路径」与「新 faststring 路径」，断言 `view()` 字节完全一致。

### 4. cross-decode 互解
- `CrossDecode_SniiWrite_DorisRead`：ByteSink 写出的 varint/fixed 用 Doris `get_varint64`/`decode_fixed*_le` 解出，值一致。
- `CrossDecode_DorisWrite_SniiRead`：Doris `put_varint64`/`encode_fixed*_le` 写出的字节用 ByteSource 解，值一致且 `eof()` 命中。

全部断言均为确定性（固定样本/固定种子），不依赖随机或时间。