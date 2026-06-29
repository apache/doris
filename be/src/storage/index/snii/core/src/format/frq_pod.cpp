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

#include "snii/format/frq_pod.h"

#include <cstddef>
#include <span>

#include "snii/common/slice.h"
#include "snii/common/uninitialized_buffer.h"
#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"
#include "snii/encoding/pfor.h"
#include "snii/encoding/zstd_codec.h"
#include "snii/format/format_constants.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status
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
doris::Status decode_pfor_runs(ByteSource* src, size_t n, std::vector<uint32_t>* out) {
    // Sized then fully overwritten by pfor_decode below; no zero-fill needed.
    snii::resize_uninitialized(*out, n);
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        RETURN_IF_ERROR(pfor_decode(src, run, out->data() + off));
    }
    return doris::Status::OK();
}

// Verifies docids are ascending and the first entry is not below win_base.
doris::Status validate_docs(std::span<const uint32_t> docs, uint64_t win_base) {
    if (docs.empty()) return doris::Status::OK();
    if (static_cast<uint64_t>(docs.front()) < win_base) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq: first docid below win_base");
    }
    for (size_t i = 1; i < docs.size(); ++i) {
        if (docs[i] < docs[i - 1]) {
            return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                    "frq: docids must be ascending");
        }
    }
    return doris::Status::OK();
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
doris::Status emit_region(Slice plain, int level, ByteSink* out, FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq: null region out");
    }
    meta->uncomp_len = plain.size();
    std::vector<uint8_t> disk;
    if (should_compress(level, plain.size())) {
        meta->zstd = true;
        RETURN_IF_ERROR(zstd_compress(plain, level > 0 ? level : kDefaultZstdLevel, &disk));
    } else {
        meta->zstd = false;
        disk.assign(plain.data(), plain.data() + plain.size());
    }
    meta->disk_len = static_cast<uint64_t>(disk.size());
    meta->crc = crc32c(Slice(disk));
    out->put_bytes(Slice(disk));
    return doris::Status::OK();
}

// Materializes a region's plaintext (raw borrows the view; zstd decompresses)
// and verifies its crc + slice length against meta.
doris::Status open_region(Slice disk, const FrqRegionMeta& meta, std::vector<uint8_t>* holder,
                          Slice* plain) {
    if (disk.size() != static_cast<size_t>(meta.disk_len)) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: region slice length mismatch");
    }
    if (meta.uncomp_len > kMaxRegionUncompBytes) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: region uncomp_len exceeds sane cap");
    }
    // Inline entries (verify_crc=false) carry no per-region crc: their on-disk
    // bytes are covered by the enclosing dict block's block-level crc32c, so the
    // region crc would be redundant. POD-ref regions keep their own crc check.
    if (meta.verify_crc && crc32c(disk) != meta.crc) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: region crc mismatch");
    }
    if (!meta.zstd) {
        if (meta.uncomp_len != meta.disk_len) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq: raw region length inconsistent");
        }
        *plain = disk;
        return doris::Status::OK();
    }
    RETURN_IF_ERROR(zstd_decompress(disk, static_cast<size_t>(meta.uncomp_len), holder));
    *plain = Slice(*holder);
    return doris::Status::OK();
}

} // namespace

doris::Status build_dd_region(std::span<const uint32_t> docids_ascending, uint64_t win_base,
                              int zstd_level_or_neg_for_auto, ByteSink* out, FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq: null dd region out");
    }
    RETURN_IF_ERROR(validate_docs(docids_ascending, win_base));
    ByteSink plain; // VInt n ++ PFOR_runs(doc_delta)
    std::vector<uint32_t> dd(docids_ascending.size());
    uint64_t prev = win_base;
    for (size_t i = 0; i < docids_ascending.size(); ++i) {
        dd[i] = static_cast<uint32_t>(static_cast<uint64_t>(docids_ascending[i]) - prev);
        prev = docids_ascending[i];
    }
    plain.put_varint32(static_cast<uint32_t>(docids_ascending.size()));
    encode_pfor_runs(dd, &plain);
    return emit_region(plain.view(), zstd_level_or_neg_for_auto, out, meta);
}

doris::Status build_freq_region(std::span<const uint32_t> freqs, int zstd_level_or_neg_for_auto,
                                ByteSink* out, FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq: null freq region out");
    }
    ByteSink plain;
    encode_pfor_runs(freqs, &plain);
    return emit_region(plain.view(), zstd_level_or_neg_for_auto, out, meta);
}

doris::Status decode_dd_region(Slice dd_disk, const FrqRegionMeta& meta, uint64_t win_base,
                               std::vector<uint32_t>* docids) {
    if (docids == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq: null docids out");
    std::vector<uint8_t> holder;
    Slice plain;
    RETURN_IF_ERROR(open_region(dd_disk, meta, &holder, &plain));
    ByteSource src(plain);
    uint32_t n = 0;
    RETURN_IF_ERROR(src.get_varint32(&n));
    if (n > kMaxWindowDocs)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: doc count exceeds sane cap");
    RETURN_IF_ERROR(decode_pfor_runs(&src, n, docids));
    if (!src.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: trailing bytes after dd region payload");
    }
    uint64_t cur = win_base;
    for (uint32_t i = 0; i < n; ++i) {
        cur += (*docids)[i];
        (*docids)[i] = static_cast<uint32_t>(cur);
    }
    return doris::Status::OK();
}

doris::Status decode_freq_region(Slice freq_disk, const FrqRegionMeta& meta, size_t doc_count,
                                 std::vector<uint32_t>* freqs) {
    if (freqs == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq: null freqs out");
    std::vector<uint8_t> holder;
    Slice plain;
    RETURN_IF_ERROR(open_region(freq_disk, meta, &holder, &plain));
    if (doc_count == 0) {
        if (meta.uncomp_len != 0) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq: empty freq region expected");
        }
        freqs->clear();
        return doris::Status::OK();
    }
    ByteSource src(plain);
    RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, freqs));
    if (!src.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq: trailing bytes after freq region payload");
    }
    return doris::Status::OK();
}

} // namespace snii::format
