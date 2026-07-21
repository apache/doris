// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

class ByteOutput;

// SPIMI PFOR (Patched Frame Of Reference) encoder for `.frq` / `.prx`
// blocks of uint32_t deltas. The format is **NOT** byte-compatible with
// CLucene's existing `vp4.h` / TurboPFor blocks — this is a deliberate
// design choice (see SPIMI_DESIGN.md § 9.1b). The trade-off:
//
//   - Doris's current PFOR (`contrib/clucene/src/ext/for/vp4*`) is
//     TurboPFor, which is rejected per project direction.
//   - This implementation references the algorithms used in production
//     database / search systems that the user pointed at:
//       * **FastPFor** (Daniel Lemire, BSD-licensed). Used by DuckDB,
//         Apache Lucene 4.6.x, LinkedIn Pinot, GMAP/GSNAP, the zsearch
//         engine. https://github.com/lemire/FastPFor
//       * **Apache Lucene 9.x `PForUtil.java`** in
//         `org.apache.lucene.codecs.lucene90`. Used by Elasticsearch,
//         OpenSearch, Solr.
//       * **Tantivy bitpacking** (Quickwit, Meilisearch). Like Lucene
//         but with the patch list removed; per the Lucene blog,
//         dropping patches gave Tantivy a small latency win at a small
//         compression cost.
//   - The bitpacking primitive is `arrow::bit_util::BitWriter` from
//     Apache Arrow (already in `thirdparty/installed`). Arrow is the
//     canonical bit-stream layer for production data systems
//     (Parquet ⇒ Snowflake, BigQuery, Athena, ClickHouse).
//
// Format per block of N uint32 values (N ≤ kBlockSize = 128):
//
//   byte    bit_width        // bits needed to encode max value (1..32)
//   bytes   bitpacked        // N × bit_width bits, ceil-byte aligned
//
// The block does NOT self-describe its value count N: the caller always
// knows N implicitly. A run is exactly `count` values (the window/term
// doc_count from the outer header); within a run, every block holds
// kBlockSize values except the final block, which holds the remainder.
// So the decoder derives N = min(kBlockSize, count - collected) per block
// — no leading VInt(n) is stored. This removes the per-block VInt(n)
// overhead (a guaranteed win: every block except the last had n == 128,
// costing 2 VInt bytes each for nothing).
//
// Reasoning for this minimal-FOR (no exception patches) base shape: it
// mirrors what Tantivy emits (per the Lucene team's benchmark), it
// uses Arrow's bitpacker without any extra abstractions, and the
// per-block overhead is constant (1 byte width). On adversarial input
// the worst case is 1 value of full 32-bit width in a block of small
// values — we pay 32×N bits instead of e.g. 4×N + 1×(32-4); the
// FastPFor / Lucene patched variant (below) wins on these inputs.
//
// PATCHED variant (opt-in, freq region only). The 0x80 bit in the
// width byte — previously reserved — now signals a trailing patch
// list (FastPFor / Lucene90 "split high bits into a patch list"
// shape, simplified to byte-indexed positions because a block is
// ≤128 values). Patched block layout (N is implicit, as above):
//
//   byte    base_width | 0x80   // low 6 bits = base width (read masks &0x3F)
//   bytes   bitpacked           // N × base_width bits — the LOW base_width
//                               //   bits of every value, ceil-byte aligned
//   --- patch trailer (only present when 0x80 set) ---
//   byte    num_exceptions      // k, 1..N (k==0 is never emitted)
//   byte    except_width        // bits for the HIGH part, 1..(32-base_width)
//   bytes   except_positions    // k bytes: each exception's index 0..N-1
//   bytes   except_highbits     // k × except_width bits, ceil-byte aligned
//
//   value[pos] = low_base_width_bits | (except_high << base_width)
//
// The patched form is emitted ONLY when it is strictly smaller than the
// plain form; otherwise the encoder leaves 0x80 clear and emits the
// exact legacy bytes (byte-identical). The patch path is opt-in per
// call (`allow_patch`). Both freq AND doc-delta PFOR runs enable it: a
// doc-delta block with a few large-gap outliers packs at a narrower base
// width with the gaps split into the trailer, and a no-outlier block
// stays byte-identical to the plain form (the flag is only set on a
// strict win). The decoder is the SAME patch-aware DecodePforRun for
// both regions, so doc-delta patches round-trip via the freq decode path.
//
// 常数块（CONST，b=0，opt-in：`allow_const`）。delta≡1 / freq≡1 的稠密
// term（df≈N）是 V4 .frq 相对 V2 TurboPFor（delta-1 域 + 0-bit 常数块）
// 唯一普遍反向的来源：旧格式位宽下限 1 bit/值，全等值的 128 值块仍要
// 1 + 16 = 17 字节。常数块布局（N 同样隐式，由调用方供给）：
//
//   byte    0x00                // 常数块标记（旧格式中 0x00 非法 ⇒ 无歧义）
//   VInt    constant            // 块内 N 个值的公共值，零比特 payload
//
// 仅当块内全部值相等且 1 + VIntLen(constant) 严格小于普通块
// （1 + ceil(N*width/8)）时发射；平手或更大保持普通块，逐字节稳定。
// 窗口可寻址性不变：常数块只是窗口 payload 内部的一种子块形态，skip 表、
// 窗口边界、range-GET framing 与 kWinRaw/kWinZstd 字节语义都不受影响。
// 解码侧三处（DecodeBlockFromBytes 与两份 DecodePforRun 副本）同步识别
// 0x00 标记；0x80（patch 标志 + 宽度 0）仍按损坏字节硬失败。
class SpimiPforEncoder {
public:
    static constexpr size_t kBlockSize = 128;

    // Encodes `values[0..count)` into `out`. `count` must be
    // ≤ kBlockSize. The value count is NOT stored in the block — the
    // decoder is told `count` out-of-band. `values` is read-only (all
    // scratch lives in stack buffers), so callers can pass slices of
    // their staging arrays directly with no per-block copy. When
    // `allow_patch` is true AND a patched encoding is strictly smaller,
    // emits the patched form (0x80 set); otherwise emits the plain
    // block. `allow_const` 为 true 且块内全部值相等且严格更小时发射常数块
    // （0x00 标记 + VInt 常数，见文件头）；windowed V4 路径开启，legacy
    // 路径保持默认 false（字节稳定）。Returns bytes written.
    static size_t EncodeBlock(const uint32_t* values, size_t count, ByteOutput* out,
                              bool allow_patch = false, bool allow_const = false);

    // Test seam: encodes into a vector (bypasses ByteOutput). The
    // resulting bytes are exactly what `EncodeBlock` writes (modulo
    // the ByteOutput wrapping).
    static std::vector<uint8_t> EncodeBlockToBytes(const std::vector<uint32_t>& values,
                                                   bool allow_patch = false,
                                                   bool allow_const = false);
};

// Decoder counterpart. The decoder lives in the same translation unit
// as the encoder so the bit-width interpretation can never drift
// between writer and reader.
class SpimiPforDecoder {
public:
    // Decodes a single block of exactly `count` values from `in` into
    // `out`. The block does not self-describe its count (see header note),
    // so the caller supplies it. `count` must be ≥ 1 and ≤ kBlockSize.
    // `out` is resized to `count`. Returns the number of values decoded
    // (== count).
    static size_t DecodeBlockFromBytes(const std::vector<uint8_t>& in, size_t count,
                                       std::vector<uint32_t>* out);
};

} // namespace doris::segment_v2::inverted_index::spimi
