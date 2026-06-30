# R07-pfor — PFOR 位打包

## R07-pfor 决策：KEEP（Doris 等价物次优 / 字节不兼容）

### 结论与依据
- **Verdict: keep-snii-doris-suboptimal**。SNII 的 PFOR 编解码（`be/src/storage/index/snii/encoding/pfor.h:18-20`，实现 `be/src/storage/index/snii/encoding/pfor.cpp`）触碰**在盘字节**（format v2），Doris 没有产生**字节一致**输出的等价物 → 触发红线，必须 KEEP。
- SNII 已完全 CLucene-free（本组件 0 处 CLucene 引用），保留它**不违背**解耦原则；红线优先级高于「优先复用 Doris」。

### Doris 等价物
1. `be/src/util/frame_of_reference_coding.h` — `ForEncoder<T>` / `ForDecoder<T>`（128 值/帧、footer 元数据、MinValue 减法、三种 StorageFormat）。
2. `be/src/util/bit_packing.h` — `BitPacking::UnpackValues`（仅 unpack、batch-of-32、来自 Impala，无 encoder）。
3. `be/src/storage/segment/frame_of_reference_page.h` — 上述 FOR 的 page 封装。

### 是否最优
**次优**。两点本质差异（详见 optimal_assessment）：
- **算法不同**：SNII = patched-FOR + 异常表（`pfor.cpp:36-57` choose_width 最小化「packed+异常」总字节；`pfor.cpp:298-315` 写 (index_delta,value) 异常表，低位补 0）；Doris = 纯帧式 FOR，无异常表、强制 128 值帧、整帧 Min 减法/delta（`frame_of_reference_coding.h:81-93,139`）。
- **布局不同**：SNII 每 block 头 `[u8 width][varint n_exc]`+packed+异常表（`pfor.h:13-15`）；Doris 元数据集中在尾部 footer（`.h:70-78`）。
- **原语缺口**：`BitPacking` 只有解包、需 32 值对齐，无编码侧、无 `pfor_skip`（SNII 需 `pfor.cpp:338-358`）。
- **API/依赖**：SNII 用自有 `ByteSink/ByteSource`+`Status`+const uint8_t 字节语义、w1..w8 专用解包快路径（`pfor.cpp:96-231`），为 .frq/.prx 热路径定制；Doris 用 faststring/模板/bool，依赖更重。

### 字节兼容性结论
**incompatible**。Doris FOR 的帧切分、footer、Min 减法、delta 存储格式与 SNII 的「width 头 + 位打包 + varint 异常表」布局根本不同，无法字节对齐。换用 = format change，出范围 → 拒绝复用。

### 迁移设计
不迁移。保留 `be/src/storage/index/snii/encoding/pfor.h` + `core/src/encoding/pfor.cpp` 原样。生产调用点 7 处（`format/frq_pod.cpp:38,47`；`format/prx_pod.cpp:106,115,406,444,448`）维持现状，签名 `pfor_encode/pfor_decode/pfor_skip` 不变。

### 为何 Doris 次优 / 无等价
Doris 既无「PFOR+异常表」算法，其 FOR 又与 SNII format v2 字节不兼容；`BitPacking` 仅能覆盖解包子集且不可承担编码与 skip。SNII 自带实现在算法选择、在盘紧凑度、热路径性能（专用宽度快路径、8 字节 LE load）三方面对其 use case 最优。

---

## TDD

n/a（保留现状，无迁移）。

说明：本组件已存在守护测试，无需新增迁移测试，仅确认其持续 GREEN：
- Round-trip 功能/边界：`be/test/storage/index/snii_query_test.cpp:812-818`（pfor_encode→pfor_decode）；目标 `doris_be_test`（filter 形如 `--gtest_filter='*Pfor*'`）。
- 字节级黄金测试与等价/op-count 计划已记录于 `be/src/storage/index/snii/docs/perf/T11-pfor-choose-width-histogram.md`（FW-03 GoldenByteIdentical：`sink.buffer()` 逐字节 `EXPECT_EQ` 内联 golden）与 `T22-window-framing-single-copy.md`（BITPACK-RT：各 w∈[0,32] 含异常值、n=1/255/256 的 round-trip）。
- 若未来真要评估 Doris 复用，则必须新增 cross-decode 黄金测试（Doris 解 SNII 写的字节、反之），预期 RED——以此固化 incompatible 结论、阻止误改格式。