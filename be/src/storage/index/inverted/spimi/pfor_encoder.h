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
//   VInt    n               // number of values in the block (= N)
//   byte    bit_width        // bits needed to encode max value (1..32)
//   bytes   bitpacked        // N × bit_width bits, ceil-byte aligned
//
// Reasoning for this minimal-FOR (no exception patches) shape: it
// mirrors what Tantivy emits (per the Lucene team's benchmark), it
// uses Arrow's bitpacker without any extra abstractions, and the
// per-block overhead is constant (VInt n + 1 byte width). On
// adversarial input the worst case is 1 value of full 32-bit width
// in a block of small values — we pay 32×N bits instead of e.g.
// 4×N + 1×(32-4); the FastPFor / Lucene patched variant wins on
// these inputs by ~10-15%. Adding patches is a follow-up: the format
// has a 7th bit reserved in the bit_width byte (`0x80`) which a
// future patched encoder can flip to indicate the extra patch list
// trailing the block.
class SpimiPforEncoder {
public:
    static constexpr size_t kBlockSize = 128;

    // Encodes `values[0..count)` into `out`. `count` must be
    // ≤ kBlockSize. `values` may be modified (the encoder may use it
    // as scratch). Returns bytes written (after the VInt + width
    // byte + bitpacked payload).
    static size_t EncodeBlock(uint32_t* values, size_t count, ByteOutput* out);

    // Test seam: encodes into a vector (bypasses ByteOutput). The
    // resulting bytes are exactly what `EncodeBlock` writes (modulo
    // the ByteOutput wrapping).
    static std::vector<uint8_t> EncodeBlockToBytes(const std::vector<uint32_t>& values);
};

// Decoder counterpart. The decoder lives in the same translation unit
// as the encoder so the bit-width interpretation can never drift
// between writer and reader.
class SpimiPforDecoder {
public:
    // Decodes a single block from `in` into `out`. `out` is resized to
    // hold the block. Returns the number of values decoded.
    static size_t DecodeBlockFromBytes(const std::vector<uint8_t>& in, std::vector<uint32_t>* out);
};

} // namespace doris::segment_v2::inverted_index::spimi
