# R03-varint — varint LEB128

## R03-varint 决策文档

### 结论与依据
**Verdict: keep-snii-doris-suboptimal（保留 SNII，Doris 等价物次优）。**

SNII 的 `encode_varint64`（core/src/encoding/varint.cpp:14-22）与 Doris `doris::encode_varint64`（be/src/util/coding.h:114-125）是同一套标准 LEB128：每次取低 7 位、置 0x80 continuation 位、`v >>= 7`，小端排列。逐行对照：SNII `out[i++] = static_cast<uint8_t>(v) | 0x80` 等价于 Doris `*(dst++) = uint8_t(v | B)`（B=128），末字节 `static_cast<uint8_t>(v)` 同 `static_cast<unsigned char>(v)`。Doris 的 `encode_varint32`（coding.cpp:13）是 LevelDB 展开写法，但输出与循环写法逐字节相同；SNII 的 `encode_varint32` 直接委托 `encode_varint64`（varint.cpp:24-26），输出亦相同。解码侧两者均为标准 LEB128（SNII varint.cpp:28-43；Doris coding.cpp:58-73）。**结论：on-disk 字节完全一致（byte-identical）。**

既然字节一致，按红线这不是「格式变更」，复用在格式层面是允许的。但按「优先复用、仅在次优时保留」的原则，本组件判定为**次优保留**，原因见下。

### Doris 等价物
- 编码：`doris::encode_varint64`（coding.h:114）、`doris::encode_varint32`（coding.cpp:13）
- 解码：`doris::decode_varint64_ptr`（coding.cpp:58）、`doris::decode_varint32_ptr`（coding.h:130）、`get_varint32/64`（coding.h:175/190）
- 长度：`doris::varint_length`（coding.h:95）≡ SNII `varint_len`（varint.cpp:5）
- **zigzag：无对应物**（Doris coding.h 不含 zigzag）

### 是否最优
不最优。具体（均含 file:line）：
1. **缺 zigzag**：SNII `zigzag_encode/decode`（varint.h:19-24）被 byte_sink.cpp:31、byte_source.cpp:56、spimi_term_buffer.cpp:162/353 使用；Doris 无，复用后仍须自带。
2. **解码 API 语义更优**：SNII 解码带显式 `end` 边界并返回 `Status::Corruption`（varint.cpp:40/42），面向不可信 on-disk/远端缓存字节做防御式校验；Doris 截断仅返回 nullptr，错误信息缺失。
3. **编码调用约定**：SNII 返回写入字节数 + 写入调用方 tmp 缓冲（byte_sink.cpp:21/27、spill_run_codec.cpp:37、spimi_term_buffer.cpp:135）；Doris 返回 end 指针，迁移需改每个调用点。
4. **依赖重量**：coding.h 连带 storage/olap_common.h、util/slice.h、exec/common/endian.h；SNII varint.h 仅依赖 status.h，复用会污染轻量核心 encoding 头。

可复用的「核心」仅约 10 行 LEB128 循环，收益远小于上述耦合成本。

### 字节兼容性结论
**byte-identical**（已逐行证明，覆盖 0 / 2^7 / 2^14 / 2^21 / 2^28 / 2^35.../2^63 / UINT64_MAX 全部边界，因两者均为无前缀压缩的标准 LEB128，编码唯一）。

### 迁移设计
不迁移（KEEP）。仅做一项加固：新增 cross-decode 黄金测试，把「SNII 写出的字节能被 Doris 解、Doris 写出的字节能被 SNII 解，且与黄金向量逐字节相等」固化为回归用例，防止两套 varint 未来各自演进导致 on-disk 格式静默漂移。无签名变更、无调用点变更、无回滚需求。

### 为何 Doris 次优（KEEP 理由汇总）
Doris 有字节一致的等价物，但（a）缺 zigzag、（b）解码无 Status/边界友好语义、（c）encode 调用约定不同需改 17 处调用点、（d）头文件依赖更重。复用净收益为负，故保留 SNII 的 50 行自包含实现。

---

## TDD

## R03-varint TDD 测试计划（加固型，非迁移）

目标：保留 SNII 实现的同时，用黄金 + cross-decode 测试钉死「SNII varint ≡ Doris varint ≡ format v2 字节」。
测试目标：`be/test`（gtest，二进制 `doris_be_test`）。建议新增 `be/test/storage/index/snii/snii_varint_test.cpp`，与现有 `be/test/storage/index/snii_query_test.cpp`、`snii_doris_adapter_test.cpp` 同套构建。

### RED -> GREEN -> REFACTOR

1. 功能验证（round-trip）
   - `EncodeDecodeRoundTrip64`：对边界集 V = {0, 1, 0x7F, 0x80, 0x3FFF, 0x4000, 0x1FFFFF, 0x200000, 0xFFFFFFFF, 0x100000000, (1ull<<63), UINT64_MAX} 调 `doris::snii::encode_varint64` 后 `doris::snii::decode_varint64`，断言值与消耗字节数相等。
   - `EncodeDecodeRoundTrip32` / `Varint32Overflow`：>0xFFFFFFFF 的 varint 经 `decode_varint32` 返回 `Status::Corruption`。
   - `ZigzagRoundTrip`：对 {0,-1,1,INT64_MIN,INT64_MAX,...} 验证 `zigzag_decode(zigzag_encode(x))==x`。
   - `DecodeTruncated` / `DecodeOverflow`：截断输入与 >10 字节连续位返回 Corruption（确定性断言错误码）。

2. 等价性验证（新旧/双实现结果一致）
   - `SniiEqualsDorisLength`：对 V 集断言 `doris::snii::varint_len(v) == doris::varint_length(v)`。
   - `SniiEqualsDorisEncode`：同一 v，`doris::snii::encode_varint64(v,a)` 与 `doris::encode_varint64(b,v)` 产生的字节序列逐字节相等且长度相等。

3. on-disk 黄金测试（字节逐字节）
   - `GoldenByteVectors`：硬编码 format v2 期望字节，例如 `0x80 -> {0x80,0x01}`、`0x4000 -> {0x80,0x80,0x01}`、`UINT64_MAX -> {0xFF*9,0x01}`、`zigzag_encode(-1)=1 -> {0x01}`，断言 `doris::snii::encode_varint64` 输出与黄金数组 `memcmp==0`。这是红线要求的字节一致黄金锚点。

4. cross-decode（互解）
   - `DorisDecodesSniiBytes`：`doris::snii::encode_varint64` 写出的缓冲交给 `doris::decode_varint64_ptr` 解出原值，且返回指针位移等于写入长度。
   - `SniiDecodesDorisBytes`：`doris::encode_varint64` 写出的缓冲交给 `doris::snii::decode_varint64` 解出原值与 next 指针正确。
   - `CrossDecode32`：同上覆盖 32 位路径（`doris::decode_varint32_ptr` 与 `doris::snii::decode_varint32`）。

全部为确定性断言，无随机/时间依赖。若仅纯保留不加测试，本项可视为可选；但建议落地以防格式漂移。