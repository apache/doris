# R04-zstd — zstd 编解码

## 结论与依据
**verdict = keep-snii-doris-suboptimal。** SNII 的 zstd 封装（be/src/storage/index/snii/encoding/zstd_codec.h:13-14 + .../core/src/encoding/zstd_codec.cpp:9-30）是一个 30 行、零 CLucene 耦合、仅依赖系统 libzstd 的薄封装：一次性 `ZSTD_compress`（zstd_codec.cpp:12，level=3）/ `ZSTD_decompress`（zstd_codec.cpp:22，由调用方传入 expected_uncomp_len 精确预分配并校验长度）。Doris 存在功能等价物，但对 SNII 的使用场景次优，且换写端会改动已发布的 on-disk format v2 字节，触红线。

## Doris 等价物
`ZstdBlockCompression`（be/src/util/block_compression.cpp:1032），经 be/src/util/block_compression.h:84 的 `get_block_compression_codec(segment_v2::CompressionTypePB::ZSTD, ...)` 获取单例（分发于 block_compression.cpp:1600-1601）。compress：block_compression.cpp:1081/1088；decompress：block_compression.cpp:1177。

## 是否最优 —— 否，理由具体
1. **写端字节不一致（决定性）**：Doris 写端在 block_compression.cpp:1126 显式 `ZSTD_c_checksumFlag=1`（多 4 字节帧尾 XXH64），并用流式 `ZSTD_compressStream2`（:1143）且未 `setPledgedSrcSize`（帧头无 content-size）；SNII 一次性 `ZSTD_compress`（srcSize 已知→帧头含 content-size、默认无 checksum）。两端压缩级别同为 3，但 framing 字节不同。
2. **读端加锁退化**：Doris `decompress` 每次经 `_acquire_decompression_ctx` 取 std::mutex 保护的上下文池（:1180,1233+）；SNII reader 为 const 无锁只读，dict-block/prx/frq 解压是热点（logical_index_reader.cpp:101、prx_pod.cpp:702/722/743、frq_pod.cpp:118）。
3. **类型/语义不匹配**：doris::snii::Slice(const uint8_t*, slice.h:20)+std::vector<uint8_t>* vs doris::Slice(char*)+faststring；SNII 由 block 头 uncomp_len 精确定长并校验 n==expected（zstd_codec.cpp:26）。

## 字节兼容性结论（on-disk）
**byte_compat = incompatible（就 re-compression 而言）。** 用 Doris 写端替换 SNII 写端，会因 checksum flag + 流式 framing 产生与 format v2 不同的字节 → 属格式变更，本工作流范围外，拒绝。**注意**：解码方向两端互通（双方都是标准 zstd 帧；Doris 用 ZSTD_decompressDCtx、SNII 用 ZSTD_decompress，均能读对方写的帧），即 `doris-decode(snii-encode(x))==x` 与 `snii-decode(doris-encode(x))==x` 均成立。但“解码可互通”不等于“可换写端保持字节一致”，后者不成立，故保留。

## 为何保留（Doris 次优）
- 仅换“读端”到 Doris：在 const 无锁热路径上引入 mutex 上下文池，是性能退化，且需 doris::snii::Slice↔doris::Slice/faststring 适配层；收益≈0（SNII 解码已是一行 ZSTD_decompress）。
- 换“读+写端”到 Doris：改 on-disk 字节（checksum/framing）= 格式变更，触红线。
- SNII 封装已 CLucene-free，依赖重量与 Doris 相同（同一 libzstd），无去耦动机。
结论：保留 SNII zstd_codec，不迁移。共 9 处调用点（compress 4：prx_pod.cpp:248/605、frq_pod.cpp:84、logical_index_writer.cpp:510；decompress 5：prx_pod.cpp:702/722/743、frq_pod.cpp:118、logical_index_reader.cpp:101）维持现状。

---

## TDD

## TDD（守护“保留 + 字节不可换写端”的经验性结论；非迁移，但需测试锁定判据）
目标 gtest target：`doris_be_test`，落点 be/test/storage/index/snii_query_test.cpp（已有 SniiPrxPodTest 套件，line 771 处有 zstd 相关用例可邻接扩展）。优先确定性断言。

### RED -> GREEN -> REFACTOR
1. **功能验证（FV-zstd-roundtrip）**：对一组样本（空串、随机 64 字节、可压缩重复串、64KB 量级模拟 dict-block）调用 `doris::snii::zstd_compress(x,3,&c)` 后 `doris::snii::zstd_decompress(c, x.size(), &y)`，`EXPECT_EQ(y, x)` 且返回 Status::OK。空输入与长度不符的损坏输入应返回 Corruption（覆盖 zstd_codec.cpp:23-28 分支）。

2. **等价性验证 / cross-decode（CD-1，决定性证据）**：
   - CD-1a `doris-decode(snii-encode(x))==x`：用 SNII 写端得 c，构造 doris::Slice 输入 + 预分配 faststring(x.size())，经 get_block_compression_codec(ZSTD) 的 decompress 解出，`EXPECT_EQ` 原文。
   - CD-1b `snii-decode(doris-encode(x))==x`：用 Doris ZstdBlockCompression::compress 得 d，`doris::snii::zstd_decompress(d, x.size(), &y)`，`EXPECT_EQ(y,x)`。
   两向通过即证明解码互通（标准帧）。

3. **字节差异断言（BD-1，锁定“不可换写端”）**：对同一可压缩样本，`EXPECT_NE(snii_encode(x), doris_encode(x))`，并断言 Doris 输出末尾 4 字节为 XXH64 checksum（由 checksumFlag=1 引入），以文档化“换 Doris 写端=格式变更”。此用例失败即提示有人误改写端或参数。

4. **on-disk 黄金字节测试（GT-1）**：对固定种子样本写出 SNII zstd 帧，`EXPECT_EQ(bytes, golden_zstd)`（与现有 T22 PRX-BYTE-ZSTD 黄金一致）。保证保留期内 SNII 写端字节逐字节稳定，等同 format v2。任何对 zstd_codec.cpp 的“无害重构”必须保持 GT-1 GREEN。

5. **REFACTOR**：本组为 KEEP 守护测试，不改动实现；若未来 T19（resize_uninitialized）等优化触及 zstd_codec.cpp，须先保证 GT-1 + FV-zstd-roundtrip + CD-1 全绿再合入。