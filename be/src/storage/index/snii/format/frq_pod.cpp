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

#include "storage/index/snii/format/frq_pod.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <limits>
#include <span>
#include <utility>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/common/uninitialized_buffer.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/prx_decode_stats.h"

namespace doris::snii::testing {
void note_frq_dd_validation_doc_visits(uint64_t count);
void note_frq_dd_materialized_values(uint64_t count);
void note_frq_raw_region_copy_bytes(uint64_t count);
} // namespace doris::snii::testing

namespace doris::snii::format {

namespace {

// Auto-compression threshold: use raw when a region is smaller than this byte
// count (zstd gain is negligible and metadata overhead is relatively large).
inline constexpr size_t kAutoZstdMinBytes = 512;
// Default zstd level for auto mode.
inline constexpr int kDefaultZstdLevel = 3;
// Maximum decompressed byte size for a single region. Guards against a
// corrupted uncomp_len read from S3 that inflated to a huge value: sanity-check
// before allocating/decompressing to avoid GB-scale allocations. Windows are
// 256-doc aligned and normally far smaller than this.
inline constexpr uint32_t kMaxRegionUncompBytes = 256u * 1024 * 1024;
// Maximum doc count per .frq window (guards against a corrupted n). Window
// baseline is 256, practical combined cap is 2048, so this is a loose but
// astronomically-large-number-blocking upper bound.
inline constexpr uint32_t kMaxWindowDocs = 1u << 24;

// Encode a uint32 array into multiple PFOR runs, each of 256 (kFrqBaseUnit)
// elements. n / run count is not written: the number of runs is derived from
// total length n and kFrqBaseUnit, and the decoder computes it the same way.
void encode_pfor_runs(std::span<const uint32_t> values, ByteSink* out) {
    size_t n = values.size();
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        pfor_encode(values.data() + off, run, out);
    }
}

// Decode n uint32 values from source (multiple PFOR runs of 256 each).
Status decode_pfor_runs(ByteSource* src, size_t n, std::vector<uint32_t>* out) {
    // Sized then fully overwritten by pfor_decode below; no zero-fill needed.
    resize_uninitialized(*out, n);
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        RETURN_IF_ERROR(pfor_decode(src, run, out->data() + off));
    }
    return Status::OK();
}

// Verifies docids are ascending and the first entry is not below win_base.
Status validate_docs(std::span<const uint32_t> docs, uint64_t win_base) {
    if (docs.empty()) return Status::OK();
#ifdef BE_TEST
    ::doris::snii::testing::note_frq_dd_validation_doc_visits(1);
#endif
    if (static_cast<uint64_t>(docs.front()) < win_base) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: first docid below win_base");
    }
    for (size_t i = 1; i < docs.size(); ++i) {
#ifdef BE_TEST
        ::doris::snii::testing::note_frq_dd_validation_doc_visits(1);
#endif
        if (docs[i] < docs[i - 1]) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "frq: docids must be ascending");
        }
    }
    return Status::OK();
}

// Decision: given level and plaintext length, determine whether to compress.
bool should_compress(int level, size_t plain_len) {
    if (level == 0) return false;          // force raw
    if (level > 0) return true;            // force zstd
    return plain_len >= kAutoZstdMinBytes; // auto
}

// Encodes one region's plaintext into raw or zstd, appends the on-disk bytes to
// out, and fills meta (mode/uncomp_len/disk_len/crc). The region carries no
// header.
Status emit_region(Slice plain, int level, ByteSink* out, FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: null region out");
    }
    meta->uncomp_len = plain.size();
    if (should_compress(level, plain.size())) {
        // zstd needs its own buffer: the compressed bytes differ from `plain`.
        std::vector<uint8_t> disk;
        meta->zstd = true;
        RETURN_IF_ERROR(zstd_compress(plain, level > 0 ? level : kDefaultZstdLevel, &disk));
        meta->disk_len = static_cast<uint64_t>(disk.size());
        meta->crc = crc32c(Slice(disk));
        out->put_bytes(Slice(disk));
        return Status::OK();
    }
    // Raw: the on-disk bytes ARE `plain` (a view over the caller's contiguous
    // ByteSink), so crc and emit straight from it -- no temp `disk` alloc/copy.
    // disk_len MUST stay == plain.size(): open_region enforces uncomp_len ==
    // disk_len for raw regions. Byte-identical to the former disk.assign() path
    // (disk == plain, so crc32c(disk) == crc32c(plain), put_bytes(disk) == same
    // bytes).
    meta->zstd = false;
    meta->disk_len = static_cast<uint64_t>(plain.size());
    meta->crc = crc32c(plain);
#ifdef BE_TEST
    ::doris::snii::testing::note_frq_raw_region_copy_bytes(plain.size());
#endif
    out->put_bytes(plain);
    return Status::OK();
}

void finish_raw_region(ByteSink* out, size_t begin, FrqRegionMeta* meta) {
    const size_t length = out->size() - begin;
    const Slice appended = length == 0 ? Slice() : Slice(out->buffer().data() + begin, length);
    meta->zstd = false;
    meta->uncomp_len = static_cast<uint64_t>(length);
    meta->disk_len = static_cast<uint64_t>(length);
    meta->crc = crc32c(appended);
}

void rollback_raw_region(ByteSink* out, size_t begin) {
    ByteSink restored;
    if (begin != 0) {
        restored.put_bytes(Slice(out->buffer().data(), begin));
    }
    *out = std::move(restored);
}

Status append_raw_dd_region(std::span<const uint32_t> docs, uint64_t win_base, ByteSink* out,
                            FrqRegionMeta* meta) {
    if (!docs.empty() && static_cast<uint64_t>(docs.front()) < win_base) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: first docid below win_base");
    }

    const size_t begin = out->size();
    out->put_varint32(static_cast<uint32_t>(docs.size()));
    std::array<uint32_t, kFrqBaseUnit> deltas;
    uint64_t previous = win_base;
    for (size_t offset = 0; offset < docs.size(); offset += kFrqBaseUnit) {
        const size_t count = std::min(docs.size() - offset, static_cast<size_t>(kFrqBaseUnit));
        for (size_t i = 0; i < count; ++i) {
            const uint32_t doc = docs[offset + i];
            if (static_cast<uint64_t>(doc) < previous) {
                rollback_raw_region(out, begin);
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "frq: docids must be ascending");
            }
            deltas[i] = static_cast<uint32_t>(static_cast<uint64_t>(doc) - previous);
            previous = doc;
        }
        pfor_encode(deltas.data(), count, out);
    }
    finish_raw_region(out, begin, meta);
    return Status::OK();
}

// Materializes a region's plaintext (raw borrows the view; zstd decompresses)
// and verifies its crc + slice length against meta.
Status open_region(Slice disk, const FrqRegionMeta& meta, std::vector<uint8_t>* holder,
                   PrxCsrAllocationGate* allocation_gate, Slice* plain) {
    if (disk.size() != static_cast<size_t>(meta.disk_len)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: region slice length mismatch");
    }
    if (meta.uncomp_len > kMaxRegionUncompBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: region uncomp_len exceeds sane cap");
    }
    // Inline entries (verify_crc=false) carry no per-region crc: their on-disk
    // bytes are covered by the enclosing dict block's block-level crc32c, so the
    // region crc would be redundant. POD-ref regions keep their own crc check.
    if (meta.verify_crc && crc32c(disk) != meta.crc) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: region crc mismatch");
    }
    if (!meta.zstd) {
        if (meta.uncomp_len != meta.disk_len) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq: raw region length inconsistent");
        }
        *plain = disk;
        return Status::OK();
    }
    if (allocation_gate != nullptr) {
        RETURN_IF_ERROR(allocation_gate->reserve_decompression(static_cast<size_t>(meta.uncomp_len),
                                                               &holder));
        DCHECK(holder != nullptr);
    }
    RETURN_IF_ERROR(zstd_decompress(disk, static_cast<size_t>(meta.uncomp_len), holder));
    *plain = Slice(*holder);
    return Status::OK();
}

} // namespace

Status build_dd_region(std::span<const uint32_t> docids_ascending, uint64_t win_base,
                       int zstd_level_or_neg_for_auto, ByteSink* out, FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: null dd region out");
    }
    if (zstd_level_or_neg_for_auto == 0) {
        return append_raw_dd_region(docids_ascending, win_base, out, meta);
    }
    RETURN_IF_ERROR(validate_docs(docids_ascending, win_base));
    ByteSink plain; // VInt n ++ PFOR_runs(doc_delta)
    std::vector<uint32_t> dd(docids_ascending.size());
#ifdef BE_TEST
    ::doris::snii::testing::note_frq_dd_materialized_values(dd.size());
#endif
    uint64_t prev = win_base;
    for (size_t i = 0; i < docids_ascending.size(); ++i) {
        dd[i] = static_cast<uint32_t>(static_cast<uint64_t>(docids_ascending[i]) - prev);
        prev = docids_ascending[i];
    }
    plain.put_varint32(static_cast<uint32_t>(docids_ascending.size()));
    encode_pfor_runs(dd, &plain);
    return emit_region(plain.view(), zstd_level_or_neg_for_auto, out, meta);
}

Status build_dd_region_from_deltas(std::span<const uint32_t> doc_deltas,
                                   int zstd_level_or_neg_for_auto, ByteSink* out,
                                   FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: null dd region out");
    }
    if (zstd_level_or_neg_for_auto != 0) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "frq: direct doc-delta encoding requires raw level 0");
    }

    const size_t begin = out->size();
    out->put_varint32(static_cast<uint32_t>(doc_deltas.size()));
    encode_pfor_runs(doc_deltas, out);
    finish_raw_region(out, begin, meta);
    return Status::OK();
}

} // namespace doris::snii::format

namespace doris::snii::testing {
#ifdef BE_TEST
namespace {
std::atomic<uint64_t> validation_visits {0};
std::atomic<uint64_t> materialized_values {0};
std::atomic<uint64_t> raw_copy_bytes {0};
} // namespace
#endif

void note_frq_dd_validation_doc_visits(uint64_t count) {
#ifdef BE_TEST
    validation_visits.fetch_add(count, std::memory_order_relaxed);
#endif
}
void note_frq_dd_materialized_values(uint64_t count) {
#ifdef BE_TEST
    materialized_values.fetch_add(count, std::memory_order_relaxed);
#endif
}
void note_frq_raw_region_copy_bytes(uint64_t count) {
#ifdef BE_TEST
    raw_copy_bytes.fetch_add(count, std::memory_order_relaxed);
#endif
}
void reset_frq_raw_encode_work() {
#ifdef BE_TEST
    validation_visits.store(0, std::memory_order_relaxed);
    materialized_values.store(0, std::memory_order_relaxed);
    raw_copy_bytes.store(0, std::memory_order_relaxed);
#endif
}
uint64_t frq_dd_validation_doc_visits() {
#ifdef BE_TEST
    return validation_visits.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t frq_dd_materialized_values() {
#ifdef BE_TEST
    return materialized_values.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t frq_raw_region_copy_bytes() {
#ifdef BE_TEST
    return raw_copy_bytes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
} // namespace doris::snii::testing

namespace doris::snii::format {

Status build_freq_region(std::span<const uint32_t> freqs, int zstd_level_or_neg_for_auto,
                         ByteSink* out, FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: null freq region out");
    }
    if (zstd_level_or_neg_for_auto == 0) {
        const size_t begin = out->size();
        encode_pfor_runs(freqs, out);
        finish_raw_region(out, begin, meta);
        return Status::OK();
    }
    ByteSink plain;
    encode_pfor_runs(freqs, &plain);
    return emit_region(plain.view(), zstd_level_or_neg_for_auto, out, meta);
}

namespace {

Status decode_dd_region_impl(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                             const uint32_t* expected_doc_count,
                             PrxCsrAllocationGate* allocation_gate, std::vector<uint32_t>* docids) {
    if (docids == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: null docids out");
    std::vector<uint8_t> holder;
    Slice plain;
    RETURN_IF_ERROR(open_region(dd_disk, meta, &holder, allocation_gate, &plain));
    ByteSource src(plain);
    uint32_t n = 0;
    RETURN_IF_ERROR(src.get_varint32(&n));
    if (n > kMaxWindowDocs)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: doc count exceeds sane cap");
    if (expected_doc_count != nullptr && n != *expected_doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: encoded doc count differs from metadata");
    }
    RETURN_IF_ERROR(decode_pfor_runs(&src, n, docids));
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: trailing bytes after dd region payload");
    }
    uint64_t cur = win_base;
    if (n != 0 && cur > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: window base exceeds uint32 docid range");
    }
    for (uint32_t i = 0; i < n; ++i) {
        const uint32_t delta = (*docids)[i];
        if (i != 0 && delta == 0) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq: zero docid delta");
        }
        if (delta > std::numeric_limits<uint32_t>::max() - cur) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq: docid accumulation overflow");
        }
        cur += delta;
        (*docids)[i] = static_cast<uint32_t>(cur);
    }
    return Status::OK();
}

} // namespace

Status decode_dd_region(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                        std::vector<uint32_t>* docids) {
    return decode_dd_region_impl(dd_disk, meta, win_base, nullptr, nullptr, docids);
}

Status decode_dd_region(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                        uint32_t expected_doc_count, std::vector<uint32_t>* docids) {
    return decode_dd_region_impl(dd_disk, meta, win_base, &expected_doc_count, nullptr, docids);
}

Status decode_dd_region(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                        uint32_t expected_doc_count, PrxCsrAllocationGate* allocation_gate,
                        std::vector<uint32_t>* docids) {
    if (allocation_gate == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "frq: allocation gate must be non-null");
    }
    return decode_dd_region_impl(dd_disk, meta, win_base, &expected_doc_count, allocation_gate,
                                 docids);
}

Status decode_freq_region(Slice freq_disk, const FrqRegionMeta& meta, size_t doc_count,
                          std::vector<uint32_t>* freqs) {
    if (freqs == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("frq: null freqs out");
    std::vector<uint8_t> holder;
    Slice plain;
    RETURN_IF_ERROR(open_region(freq_disk, meta, &holder, nullptr, &plain));
    if (doc_count == 0) {
        if (meta.uncomp_len != 0) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq: empty freq region expected");
        }
        freqs->clear();
        return Status::OK();
    }
    ByteSource src(plain);
    RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, freqs));
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: trailing bytes after freq region payload");
    }
    return Status::OK();
}

} // namespace doris::snii::format
