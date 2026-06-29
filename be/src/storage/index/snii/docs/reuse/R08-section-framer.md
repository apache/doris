# R08-section-framer — SectionFramer 段封装

## R08 SectionFramer 段封装 — 决策文档

### 结论与依据
**Verdict: keep-snii-no-equivalent（保留 SNII 实现，无 Doris 等价物）。**

SectionFramer 是 SNII on-disk 格式 v2 的**格式定义组件**，而非可替换的通用实现。其封装契约为：

- 写：`[u8 type][varint64 len][payload][fixed32 crc32c(type+len+payload)]`（section_framer.cpp:7-16），crc 覆盖 type+len+payload 整体。
- 读：解析 type/len/payload 后，对 `[start, framed_len)` 区间重算 crc32c 并与尾部 fixed32 比对，不一致返回 `Status::Corruption("section crc mismatch")`（section_framer.cpp:18-37）。

设计意图（section_framer.h:18-20）：全部 full-format 段统一复用此 encode/checksum 路径，避免各处手写校验和拼装；未知可选段由调用方按 type 分发，读路径仍会验 CRC 并跳过 payload。

### Doris 等价物
**无。** 在 be/src/util、be/src/olap、be/src/io 下未发现任何 `type+len+crc32c` 形式的通用段/区域封装工具（grep 结果为空）。最接近的 be/src/olap/rowset/segment_v2/page_io.h 中的 `PageIO` 采用 protobuf `PageFooterPB` + footer 长度 + checksum 的布局，校验范围与字节编排都与 SNII 不同，属于不同的磁盘格式，不能作为等价替换。

### 是否最优
不适用"最优/次优"判断——这是格式定义本身。SNII 的 type 分发 + 跳过未知可选段 + 单一 crc32c 包络是 v2 格式契约，Doris 的 protobuf footer 方案无法在不改变磁盘字节的前提下承载该契约。

### 字节兼容性结论（on-disk）
**incompatible。** 不存在可产生**字节一致**输出的 Doris 实现；任何用 Doris 方案替换都会改变 v2 磁盘字节，构成格式变更（超出"实现替换"范围）。按红线规则，必须 KEEP。

### 调用点（约 10 处，7 个 format 模块）
- be/src/storage/index/snii/core/src/format/dict_block_directory.cpp:67,74
- per_index_meta.cpp:36,99,178（write 一处 + read 两处）
- logical_index_directory.cpp:71,81
- stats_block.cpp:34,39
- norms_pod.cpp:18,25
- sampled_term_index.cpp:64,116
- null_bitmap.cpp:41,58

注意：be/src/snii/format/tail_pointer.h:29 明确**不**使用 SectionFramer（固定布局，需在无前置知识时可解析，见 bootstrap_header.h:18 同理），这是格式有意为之，不应"统一"到 framer。

### 为何 Doris 次优 / 无等价（KEEP 理由）
1. Doris 无 type+len+crc32c 通用封装；唯一相近的 PageIO 是 protobuf footer 布局，字节不兼容。
2. 该组件直接定义 on-disk 格式 v2 字节，替换 = 格式变更，触碰红线。
3. 依赖关系：仅依赖 snii::crc32c（R05）。SectionFramer 自身无需改动；其命运绑定 R05——若 R05 保留或做到字节一致替换，本组件零影响。

### 迁移设计
无迁移。保持现状。后续若 R05 对 crc32c 做字节一致替换，SectionFramer 因仅调用 `crc32c(Slice)` 接口而无需任何签名或调用点改动。

---

## TDD

n/a（保留现状，无迁移）。

补充建议（非本次工作范围，仅记录覆盖缺口）：当前 be/test 下未发现 section_framer 的独立 gtest（grep 为空）。若 R05 后续触发 crc32c 字节一致替换，应在 doris_be_test（be/test/CMakeLists.txt:154 add_executable(doris_be_test)）下补一个 golden 测试 `SectionFramerGoldenTest`：固定 type/payload，断言 write 产出的字节序列逐字节等于 v2 基准十六进制，并验证 read 往返与 crc 篡改后返回 Corruption。