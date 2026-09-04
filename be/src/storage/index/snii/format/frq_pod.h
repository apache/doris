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

#include <cstdint>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

// .frq region codec (FrqPod): doc-delta (dd) and freq postings, columnar + PFOR
// (see docs/design SNII "frq design" and the read-byte-optimizations
// design 1.6).
//
// PHASE D (posting-level dd/freq grouping): windows are NO LONGER
// self-describing. A windowed .frq payload is laid out as
//   [prelude][dd-block][freq-block]
// where the dd-block concatenates every window's dd_region and the freq-block
// concatenates every window's freq_region. Each region is independently encoded
// (raw or zstd, chosen by size) and the per-window codec metadata (mode,
// lengths, crc, offsets) is hoisted into the frq_prelude rows -- the region
// bytes carry NO header. This makes the docs-only prefix ([prelude][dd-block])
// ONE contiguous run a docid-only / phrase reader can fetch in a single range,
// skipping the freq-block entirely.
//
// dd_region plaintext   = VInt n ++ PFOR_runs(doc_delta)   # n = doc count
//   dd[0] = first_docid - win_base; dd[i] = docid[i] - docid[i-1]; win_base is
//   the previous window's last docid (first window = 0).
// freq_region plaintext = PFOR_runs(freq)                  # present iff
// has_freq PFOR runs are segmented at 256 docs (kFrqBaseUnit); a partial
// segment writes the remainder. Variable-length integers reuse
// snii/encoding/varint; PFOR reuses snii/encoding/pfor; crc32c covers each
// region's ON-DISK bytes.
namespace doris::snii::format {

class PrxCsrAllocationGate;

// Codec metadata for ONE encoded region (dd or freq), hoisted into the prelude.
// The region's on-disk bytes are pure payload (no header); these fields drive
// the decode. crc covers the on-disk (disk_len) bytes.
struct FrqRegionMeta {
    bool zstd = false;       // true => disk bytes are zstd(plaintext); false => raw
    uint64_t uncomp_len = 0; // plaintext byte length (== disk_len when raw)
    uint64_t disk_len = 0;   // on-disk byte length of this region
    uint32_t crc = 0;        // crc32c of the on-disk (disk_len) bytes
    // When false, decode_*_region SKIPS the per-region crc check (and the writer
    // omits the 4-byte crc from the dict entry). Set false for INLINE entries:
    // their region bytes live inside the dict block, whose own block-level crc32c
    // already covers them, so a per-region crc is fully redundant. POD-ref
    // regions (slim/windowed) live in the separately-fetched .frq POD -- their
    // crc stays.
    bool verify_crc = true;
};

// Encodes a window's dd_region plaintext (VInt n ++ PFOR_runs(doc_delta)) into
// raw or zstd (per zstd_level_or_neg_for_auto), APPENDS the on-disk bytes to
// out, and fills meta (mode/uncomp_len/disk_len/crc). The region carries no
// header. docids_ascending: ascending docids in this window (single doc or
// empty allowed). win_base: previous window's last docid (first window = 0);
// requires docids[0] >= win_base. zstd_level_or_neg_for_auto: <0 auto (zstd
// when large enough, else raw); 0 force
//   raw; >0 force zstd at that level.
// Non-ascending docids / first_docid < win_base / null out returns
// InvalidArgument.
Status build_dd_region(std::span<const uint32_t> docids_ascending, uint64_t win_base,
                       int zstd_level_or_neg_for_auto, ByteSink* out, FrqRegionMeta* meta);

// Trusted writer fast path for a window whose doc deltas have already been
// produced and validated by the posting stream. Appends the same
// VInt n ++ PFOR_runs(doc_delta) bytes as build_dd_region(..., level=0), without
// reconstructing absolute docids or scanning the deltas. Only raw level 0 is
// supported; any other level returns InvalidArgument without modifying out or
// meta.
Status build_dd_region_from_deltas(std::span<const uint32_t> doc_deltas,
                                   int zstd_level_or_neg_for_auto, ByteSink* out,
                                   FrqRegionMeta* meta);

// Vector convenience overload (forwards a span view; no copy of the elements).
inline Status build_dd_region(const std::vector<uint32_t>& docids_ascending, uint64_t win_base,
                              int zstd_level_or_neg_for_auto, ByteSink* out, FrqRegionMeta* meta) {
    return build_dd_region(std::span<const uint32_t>(docids_ascending), win_base,
                           zstd_level_or_neg_for_auto, out, meta);
}

// Encodes a window's freq_region plaintext (PFOR_runs(freq)) into raw or zstd,
// APPENDS the on-disk bytes to out, and fills meta. Empty freqs yields a
// zero-length region. Null out returns InvalidArgument.
Status build_freq_region(std::span<const uint32_t> freqs, int zstd_level_or_neg_for_auto,
                         ByteSink* out, FrqRegionMeta* meta);

// Vector convenience overload (forwards a span view; no copy of the elements).
inline Status build_freq_region(const std::vector<uint32_t>& freqs, int zstd_level_or_neg_for_auto,
                                ByteSink* out, FrqRegionMeta* meta) {
    return build_freq_region(std::span<const uint32_t>(freqs), zstd_level_or_neg_for_auto, out,
                             meta);
}

// Decodes a dd_region from its on-disk slice (exactly disk_len bytes) + meta +
// win_base, reconstructing ascending docids. Verifies meta.crc against the
// slice. crc mismatch / wrong slice length / truncation / decompression /
// oversized count all return a non-OK Status. The freq region is irrelevant
// here (docs-only path).
Status decode_dd_region(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                        std::vector<uint32_t>* docids);
// Compaction variant: validates the encoded count before resizing `docids`, so
// a corrupt frame cannot grow a pre-reserved destination buffer past its
// metadata-derived hard budget.
Status decode_dd_region(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                        uint32_t expected_doc_count, std::vector<uint32_t>* docids);
// Compaction variant that charges a zstd decompression buffer before allocation
// and may reuse the caller's bounded decoder workspace.
Status decode_dd_region(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                        uint32_t expected_doc_count, PrxCsrAllocationGate* allocation_gate,
                        std::vector<uint32_t>* docids);

// Decodes a freq_region from its on-disk slice (exactly disk_len bytes) + meta,
// producing doc_count freqs. Verifies meta.crc. doc_count == 0 yields empty
// freqs (and requires a zero-length region). crc mismatch / wrong slice length
// / etc. return a non-OK Status.
Status decode_freq_region(Slice freq_disk, const FrqRegionMeta& meta, size_t doc_count,
                          std::vector<uint32_t>* freqs);

} // namespace doris::snii::format

// Test-only work counters for the level-0 writer path. They expose redundant
// work that a direct raw encoder must eliminate without making it part of the
// production codec API.
namespace doris::snii::testing {
void reset_frq_raw_encode_work();
uint64_t frq_dd_validation_doc_visits();
uint64_t frq_dd_materialized_values();
uint64_t frq_raw_region_copy_bytes();
} // namespace doris::snii::testing
