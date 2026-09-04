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
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/prx_decode_stats.h"

// .prx position window (PrxPod): stores term position information for several
// docs within one window.
//
// Single-window on-disk byte layout (see docs/design SNII "prx design"):
//   u8   codec        # PrxCodec: 0=raw / 1=zstd / 2=pfor (bit7 cont-reserved)
//   VInt uncomp_len   # payload length (raw/pfor: on-disk payload bytes; zstd:
//   plaintext) VInt comp_len     # present only when codec==zstd u32  crc32c #
//   covers header (codec..comp_len) + payload bytes payload     # raw: varint
//   plaintext; zstd: compressed; pfor: bit-packed
//
// raw/zstd plaintext payload (self-describing per-doc boundaries):
//   VInt doc_count
//   per doc: VInt pos_count, followed by pos_count position deltas (VInt)
//   positions within a doc are ascending, stored as deltas (first absolute).
//
// pfor payload (one auto-build candidate; no entropy coding):
//   VInt doc_count
//   VInt total_pos                   # sum of all pos_counts
//   PFOR_runs(pos_counts)            # doc_count values
//   PFOR_runs(position_deltas)       # total_pos deltas, kFrqBaseUnit per run,
//                                    #   flat doc order (first per doc
//                                    absolute)
//
// Multi-byte fixed-length fields are little-endian; variable-length integers
// reuse snii/encoding/varint. crc32c checksum at window tail detects
// corruption.
namespace doris::snii::format {

struct PrxWindowLimits {
    uint32_t max_docs;
    uint32_t max_positions;
    uint32_t max_uncomp_bytes;
};

enum class PrxWindowBuildOutcome : uint8_t {
    kBuilt = 0,
    kNeedsSplit = 1,
};

inline constexpr PrxWindowLimits kReaderPrxWindowLimits {
        .max_docs = 1U << 24,
        .max_positions = 1U << 26,
        .max_uncomp_bytes = 256U * 1024 * 1024,
};

// Writer policies may tighten, but never exceed, the reader's allocation
// guards. Keeping this validation in the format layer makes it impossible for
// a caller to configure a writer that emits frames the current reader rejects.
Status validate_prx_window_limits(const PrxWindowLimits& limits);

// Build a .prx window and append it to sink.
// per_doc_positions[d] is the position list for the d-th doc within this
// window; must be ascending (duplicates allowed).
// zstd_level_or_negative_for_auto:
//   <0  → auto: use direct RAW for a single-doc/freq=1 window; otherwise use
//         PFOR below the compression threshold and choose the smallest
//         PFOR/ZSTD/RAW frame once raw plaintext is already required (default
//         zstd level).
//   0   → force raw varint payload.
//   >0  → force ZSTD with the given level.
// Non-ascending positions within a doc return InvalidArgument.
Status build_prx_window(std::span<const std::vector<uint32_t>> per_doc_positions,
                        int zstd_level_or_negative_for_auto, ByteSink* sink);
Status build_prx_window(std::span<const std::vector<uint32_t>> per_doc_positions,
                        int zstd_level_or_negative_for_auto, const PrxWindowLimits& limits,
                        ByteSink* sink);

// Vector convenience overload (forwards a span view over the window's per-doc
// lists; the writer can pass a slice of its flat positions WITHOUT deep-copying
// the inner vectors into a fresh std::vector<std::vector<uint32_t>> per
// window).
inline Status build_prx_window(const std::vector<std::vector<uint32_t>>& per_doc_positions,
                               int zstd_level_or_negative_for_auto, ByteSink* sink) {
    return build_prx_window(std::span<const std::vector<uint32_t>>(per_doc_positions),
                            zstd_level_or_negative_for_auto, sink);
}

// FLAT-positions builder: byte-identical output to build_prx_window above, but
// reads the window's positions from a single flat span partitioned per-doc by
// `freqs` (doc d owns the next freqs[d] entries; freqs.size() == doc count and
// sum(freqs) == positions_flat.size()). Lets the writer pass a subspan of the
// term's flat positions/freqs with NO vector-of-vectors materialization.
Status build_prx_window_flat(std::span<const uint32_t> positions_flat,
                             std::span<const uint32_t> freqs, int zstd_level_or_negative_for_auto,
                             ByteSink* sink);
Status build_prx_window_flat(std::span<const uint32_t> positions_flat,
                             std::span<const uint32_t> freqs, int zstd_level_or_negative_for_auto,
                             const PrxWindowLimits& limits, ByteSink* sink);

// Writer-facing exact attempt. kNeedsSplit is returned only when the current
// multi-document window exceeds a shape or encoded-byte limit and every
// standalone document is representable, so retrying at document boundaries is
// guaranteed to resolve the limit. An unsplittable document, malformed input
// and codec failures remain errors. The sink is unchanged unless the outcome is
// kBuilt.
Status try_build_prx_window_flat(std::span<const uint32_t> positions_flat,
                                 std::span<const uint32_t> freqs,
                                 int zstd_level_or_negative_for_auto, const PrxWindowLimits& limits,
                                 ByteSink* sink, PrxWindowBuildOutcome* outcome);

// Read and verify a .prx window from source, reconstructing the per-doc
// position list. CRC mismatch / invalid codec / truncation / decompression
// failure all return a non-OK Status.
Status read_prx_window(ByteSource* source, std::vector<std::vector<uint32_t>>* per_doc_positions);

// CSR variant of read_prx_window: decodes ALL docs' positions into one flat
// buffer `pos_flat` with per-doc offsets `pos_off` (size doc_count+1,
// pos_off[0]==0), so doc d's positions are pos_flat[pos_off[d] ..
// pos_off[d+1]). Avoids the per-doc std::vector allocation of read_prx_window
// -- both output vectors are flat uint32 buffers whose capacity a caller can
// retain (clear()) across windows/queries.
Status read_prx_window_csr(ByteSource* source, std::vector<uint32_t>* pos_flat,
                           std::vector<uint32_t>* pos_off);
Status read_prx_window_csr(ByteSource* source, std::vector<uint32_t>* pos_flat,
                           std::vector<uint32_t>* pos_off, PrxDecodeContext* context);

// Decode every physical document while reporting the supplied ordinals as the
// logical selection. Used by the density fallback after docid narrowing.
Status read_prx_window_csr_for_selection(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                         std::vector<uint32_t>* pos_flat,
                                         std::vector<uint32_t>* pos_off, PrxDecodeContext* context);

// Selective CSR variant: decodes positions only for the requested local doc
// ordinals within this PRX window. `doc_ordinals` must be strictly ascending.
// The output uses the same CSR shape, but has doc_ordinals.size()+1 offsets.
Status read_prx_window_csr_selective(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                     std::vector<uint32_t>* pos_flat,
                                     std::vector<uint32_t>* pos_off);
Status read_prx_window_csr_selective(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                     std::vector<uint32_t>* pos_flat,
                                     std::vector<uint32_t>* pos_off, PrxDecodeContext* context);

} // namespace doris::snii::format

// Test-only instrumentation seam. prx_raw_build_count() returns a process-global
// count of zstd-candidate plaintext payloads materialized by the auto-mode (.prx)
// window builders. Sub-threshold RAW fallback after an unreadable PFOR is not a
// zstd candidate and is intentionally not counted. In ordinary auto mode the
// small windows that dominate a Zipfian corpus skip the throwaway plaintext and
// second delta walk entirely, so tests assert the count is 0 for sub-threshold
// PFOR windows and exactly 1 per zstd candidate.
// Counters use relaxed atomics and are reset between tests; the writer's segment
// build is single-threaded, so the atomic adds introduce no data race.
namespace doris::snii::format::testing {

uint64_t prx_raw_build_count();
void reset_prx_raw_build_count();
void note_prx_raw_build();
#ifdef BE_TEST
uint64_t prx_clock_read_count();
void reset_prx_clock_read_count();
void note_prx_clock_read();
uint64_t prx_delta_materialization_count();
void reset_prx_delta_materialization_count();
void note_prx_delta_materialization();
uint8_t select_auto_prx_codec_for_test(size_t pfor_payload_size, size_t plain_payload_size,
                                       size_t compressed_payload_size, uint32_t max_uncomp_bytes);
#endif

} // namespace doris::snii::format::testing
