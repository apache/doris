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

#include "snii/format/prx_pod.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <span>
#include <vector>

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

// Auto-compression threshold: use raw when payload is smaller than this (zstd
// gain is negligible and metadata overhead is relatively large).
inline constexpr size_t kAutoZstdMinBytes = 512;
// Default zstd level in auto mode.
inline constexpr int kDefaultZstdLevel = 3;
// Maximum decompressed byte size for a single .prx window. Guards against a
// corrupted uncomp_len read from S3 inflated to a huge value: sanity-check
// before allocating/decompressing to avoid GB-scale allocations. Windows are
// 256-doc aligned and normally far below this limit.
inline constexpr uint32_t kMaxWindowUncompBytes = 256u * 1024 * 1024;
// Anti-DoS cap on position count decoded from a single window before
// allocation.
inline constexpr uint32_t kMaxWindowPositions = 1u << 26; // 64M positions/window
// Anti-DoS cap on doc count decoded from a single window before allocation. A
// corrupt doc_count is otherwise fed straight to assign()/reserve() ->
// bad_alloc.
inline constexpr uint32_t kMaxWindowDocs = 1u << 24; // 16M docs/window

// Writer-side precondition for the FLAT builders: the per-doc partition `freqs`
// must address exactly the positions present in `flat`. If sum(freqs) overruns
// flat.size() a (positions_flat, freqs) mismatch would index flat[off+i] past
// the span end -- an out-of-bounds read on caller-supplied data. Reject it as
// InvalidArgument BEFORE any indexing so the bug surfaces as a clean doris::Status,
// never UB. (sum < size leaves trailing positions unused, which is also a
// writer bug, so we require exact equality.) Uint64 accumulation cannot
// overflow for uint32 freqs.
doris::Status check_flat_partition(std::span<const uint32_t> flat,
                                   std::span<const uint32_t> freqs) {
    uint64_t sum = 0;
    for (uint32_t fc : freqs) sum += fc;
    if (sum != flat.size()) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "prx: sum(freqs) does not match positions_flat size");
    }
    return doris::Status::OK();
}

// Encode per-doc position lists into a self-describing plain payload (doc_count
// + per-doc delta stream).
doris::Status encode_payload(std::span<const std::vector<uint32_t>> per_doc, ByteSink* out) {
    out->put_varint32(static_cast<uint32_t>(per_doc.size()));
    for (const auto& doc : per_doc) {
        out->put_varint32(static_cast<uint32_t>(doc.size()));
        uint32_t prev = 0;
        for (size_t i = 0; i < doc.size(); ++i) {
            uint32_t pos = doc[i];
            if (i > 0 && pos < prev) {
                return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: positions within a doc must be ascending");
            }
            out->put_varint32(i == 0 ? pos : pos - prev);
            prev = pos;
        }
    }
    return doris::Status::OK();
}

// FLAT-positions encoder: identical wire output to encode_payload above, but
// reads positions from a single flat span partitioned per-doc by `freqs` (doc d
// owns the next freqs[d] entries). This avoids materializing a
// vector-of-vectors for the window; freqs.size() is the doc count and
// sum(freqs) == flat.size().
doris::Status encode_payload_flat(std::span<const uint32_t> flat, std::span<const uint32_t> freqs,
                                  ByteSink* out) {
    RETURN_IF_ERROR(check_flat_partition(flat, freqs));
    out->put_varint32(static_cast<uint32_t>(freqs.size()));
    size_t off = 0;
    for (uint32_t fc : freqs) {
        out->put_varint32(fc);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t pos = flat[off + i];
            if (i > 0 && pos < prev) {
                return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: positions within a doc must be ascending");
            }
            out->put_varint32(i == 0 ? pos : pos - prev);
            prev = pos;
        }
        off += fc;
    }
    return doris::Status::OK();
}

// Encode a uint32 array into PFOR runs of kFrqBaseUnit (256) elements each. The
// run count is derived by the decoder from the total length, so it is not
// stored.
void encode_pfor_runs(std::span<const uint32_t> values, ByteSink* out) {
    const size_t n = values.size();
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        const size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        pfor_encode(values.data() + off, run, out);
    }
}

// Decode n uint32 values (multiple PFOR runs of kFrqBaseUnit each) into out.
doris::Status decode_pfor_runs(ByteSource* src, size_t n, std::vector<uint32_t>* out) {
    // Sized then fully overwritten by pfor_decode below (every [0, n) slot is
    // written); no zero-fill needed beyond what std::vector mandates.
    snii::resize_uninitialized(*out, n);
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        const size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        RETURN_IF_ERROR(pfor_decode(src, run, out->data() + off));
    }
    return doris::Status::OK();
}

// PFOR window payload (self-describing; no entropy coding):
//   VInt doc_count
//   VInt total_pos             # sum of all pos_counts
//   PFOR_runs(pos_counts)      # doc_count values (bit-packed; mostly 1 -> ~1
//   bit) PFOR_runs(position_deltas) # total_pos deltas, flat across docs (first
//   per
//                              #   doc absolute, rest delta-within-doc)
// Bit-packing the per-doc pos_counts (vs one varint each) is the size win: in a
// uniform corpus most docs have freq 1, so the count column packs to ~1
// bit/doc. Builds the payload from a flat positions span partitioned per-doc by
// `freqs`.
doris::Status encode_pfor_payload_flat(std::span<const uint32_t> flat,
                                       std::span<const uint32_t> freqs, ByteSink* out) {
    RETURN_IF_ERROR(check_flat_partition(flat, freqs));
    out->put_varint32(static_cast<uint32_t>(freqs.size()));
    out->put_varint32(static_cast<uint32_t>(flat.size()));
    encode_pfor_runs(freqs, out);
    std::vector<uint32_t> deltas;
    deltas.reserve(flat.size());
    size_t off = 0;
    for (uint32_t fc : freqs) {
        uint32_t prev = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t pos = flat[off + i];
            if (i > 0 && pos < prev) {
                return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: positions within a doc must be ascending");
            }
            deltas.push_back(i == 0 ? pos : pos - prev);
            prev = pos;
        }
        off += fc;
    }
    encode_pfor_runs(deltas, out);
    return doris::Status::OK();
}

// Builds the PFOR payload from per-doc lists (delegates through a flat view).
doris::Status encode_pfor_payload(std::span<const std::vector<uint32_t>> per_doc, ByteSink* out) {
    std::vector<uint32_t> flat, freqs;
    freqs.reserve(per_doc.size());
    for (const auto& doc : per_doc) {
        freqs.push_back(static_cast<uint32_t>(doc.size()));
        flat.insert(flat.end(), doc.begin(), doc.end());
    }
    return encode_pfor_payload_flat(flat, freqs, out);
}

// Decode per-doc position lists from a PFOR payload.
doris::Status decode_pfor_payload(Slice plain, std::vector<std::vector<uint32_t>>* out) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    std::vector<uint32_t> pos_counts;
    RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, &pos_counts));
    uint64_t sum = 0;
    for (uint32_t d = 0; d < doc_count; ++d) sum += pos_counts[d];
    if (sum != total_pos) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: pos_count sum mismatch");
    }
    std::vector<uint32_t> deltas;
    RETURN_IF_ERROR(decode_pfor_runs(&src, total_pos, &deltas));
    out->clear();
    out->reserve(doc_count);
    size_t off = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        std::vector<uint32_t> doc;
        doc.reserve(pos_counts[d]);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_counts[d]; ++i) {
            prev = (i == 0) ? deltas[off + i] : prev + deltas[off + i];
            doc.push_back(prev);
        }
        off += pos_counts[d];
        out->push_back(std::move(doc));
    }
    if (!src.eof())
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after pfor payload");
    return doris::Status::OK();
}

// Writes a PFOR window: codec=pfor, payload, crc(header+payload).
void write_pfor(Slice payload, ByteSink* sink) {
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kPfor));
    framed.put_varint32(static_cast<uint32_t>(payload.size()));
    framed.put_bytes(payload);
    sink->put_bytes(framed.view());
    sink->put_fixed32(crc32c(framed.view()));
}

size_t varint32_size(uint32_t value) {
    size_t bytes = 1;
    while (value >= 128) {
        value >>= 7;
        ++bytes;
    }
    return bytes;
}

size_t pfor_frame_size(size_t payload_size) {
    return 1 + varint32_size(static_cast<uint32_t>(payload_size)) + payload_size + sizeof(uint32_t);
}

size_t zstd_frame_size(size_t plain_size, size_t compressed_size) {
    return 1 + varint32_size(static_cast<uint32_t>(plain_size)) +
           varint32_size(static_cast<uint32_t>(compressed_size)) + compressed_size +
           sizeof(uint32_t);
}

void write_zstd_compressed(Slice plain, Slice compressed, ByteSink* sink) {
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kZstd));
    framed.put_varint32(static_cast<uint32_t>(plain.size()));
    framed.put_varint32(static_cast<uint32_t>(compressed.size()));
    framed.put_bytes(compressed);
    sink->put_bytes(framed.view());
    sink->put_fixed32(crc32c(framed.view()));
}

doris::Status write_auto_pfor_or_zstd(Slice pfor_payload, Slice plain_payload, ByteSink* sink) {
    if (plain_payload.size() >= kAutoZstdMinBytes) {
        std::vector<uint8_t> compressed;
        RETURN_IF_ERROR(zstd_compress(plain_payload, kDefaultZstdLevel, &compressed));
        if (zstd_frame_size(plain_payload.size(), compressed.size()) <
            pfor_frame_size(pfor_payload.size())) {
            write_zstd_compressed(plain_payload, Slice(compressed), sink);
            return doris::Status::OK();
        }
    }
    write_pfor(pfor_payload, sink);
    return doris::Status::OK();
}

// Decode per-doc position lists from a plain payload.
doris::Status decode_payload(Slice plain, std::vector<std::vector<uint32_t>>* out) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    out->clear();
    out->reserve(doc_count);
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        RETURN_IF_ERROR(src.get_varint32(&pos_count));
        std::vector<uint32_t> doc;
        doc.reserve(pos_count);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t delta = 0;
            RETURN_IF_ERROR(src.get_varint32(&delta));
            prev = (i == 0) ? delta : prev + delta;
            doc.push_back(prev);
        }
        out->push_back(std::move(doc));
    }
    if (!src.eof())
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after payload");
    return doris::Status::OK();
}

// CSR decode of a PFOR payload: all docs' positions into one flat buffer +
// per-doc offsets, with NO per-doc std::vector allocation. `pos_off` has
// doc_count+1 entries (pos_off[0]==0); doc d's positions are
// pos_flat[pos_off[d] .. pos_off[d+1]).
doris::Status decode_pfor_payload_csr(Slice plain, std::vector<uint32_t>* pos_flat,
                                      std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    pos_off->clear();
    pos_off->reserve(static_cast<size_t>(doc_count) + 1);
    RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, pos_off));
    uint64_t sum = 0;
    for (uint32_t d = 0; d < doc_count; ++d) sum += (*pos_off)[d];
    if (sum != total_pos)
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: pos_count sum mismatch");
    // pos_flat is sized to total_pos by decode_pfor_runs (resize_uninitialized);
    // a separate reserve is redundant. pos_off keeps its reserve (push_back below).
    RETURN_IF_ERROR(decode_pfor_runs(&src, total_pos, pos_flat));
    size_t off = 0;
    uint32_t next_off = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        const uint32_t pos_count = (*pos_off)[d];
        (*pos_off)[d] = next_off;
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t& value = (*pos_flat)[off + i];
            prev = (i == 0) ? value : prev + value;
            value = prev;
        }
        off += pos_count;
        next_off += pos_count;
    }
    pos_off->push_back(next_off);
    if (!src.eof())
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after pfor payload");
    return doris::Status::OK();
}

doris::Status validate_doc_ordinals(std::span<const uint32_t> doc_ordinals, uint32_t doc_count) {
    uint32_t prev = 0;
    for (size_t i = 0; i < doc_ordinals.size(); ++i) {
        const uint32_t doc = doc_ordinals[i];
        if (doc >= doc_count) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "prx: selected doc ordinal out of range");
        }
        if (i != 0 && doc <= prev) {
            return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: selected doc ordinals must be strictly ascending");
        }
        prev = doc;
    }
    return doris::Status::OK();
}

struct SelectedRange {
    SelectedRange(uint32_t begin_, uint32_t end_, uint32_t out_begin_)
            : begin(begin_), end(end_), out_begin(out_begin_) {}

    uint32_t begin;
    uint32_t end;
    uint32_t out_begin;
};

uint32_t count_covered_pfor_runs(std::span<const SelectedRange> selected, uint32_t total_pos) {
    if (selected.empty() || total_pos == 0) {
        return 0;
    }
    uint32_t runs = 0;
    uint32_t next_run = 0;
    for (const SelectedRange& range : selected) {
        if (range.begin == range.end) {
            continue;
        }
        const uint32_t first_run = range.begin / kFrqBaseUnit;
        const uint32_t last_run = (range.end - 1) / kFrqBaseUnit;
        const uint32_t counted_first = std::max(first_run, next_run);
        if (counted_first <= last_run) {
            runs += last_run - counted_first + 1;
            next_run = last_run + 1;
        }
    }
    return runs;
}

bool should_decode_full_prx_positions(std::span<const SelectedRange> selected,
                                      uint32_t selected_pos_count, uint32_t total_pos) {
    if (selected.empty() || total_pos == 0) {
        return false;
    }
    if (selected_pos_count * 2 >= total_pos) {
        return true;
    }
    const uint32_t total_runs = (total_pos + kFrqBaseUnit - 1) / kFrqBaseUnit;
    const uint32_t covered_runs = count_covered_pfor_runs(selected, total_pos);
    return covered_runs * 4 >= total_runs * 3;
}

doris::Status decode_selected_pfor_count_ranges(ByteSource* src, uint32_t doc_count,
                                                std::span<const uint32_t> doc_ordinals,
                                                std::vector<SelectedRange>& selected,
                                                std::vector<uint32_t>& pos_off,
                                                uint64_t* total_pos_count,
                                                uint32_t* selected_pos_count) {
    selected.clear();
    selected.reserve(doc_ordinals.size());
    pos_off.clear();
    pos_off.reserve(doc_ordinals.size() + 1);
    pos_off.push_back(0);

    *selected_pos_count = 0;
    uint32_t delta_begin = 0;
    size_t next_doc = 0;
    *total_pos_count = 0;
    std::array<uint32_t, kFrqBaseUnit> run_buf {};
    for (uint32_t run_begin = 0; run_begin < doc_count; run_begin += kFrqBaseUnit) {
        const uint32_t run_len = std::min<uint32_t>(kFrqBaseUnit, doc_count - run_begin);
        RETURN_IF_ERROR(pfor_decode(src, run_len, run_buf.data()));
        for (uint32_t i = 0; i < run_len; ++i) {
            const uint32_t d = run_begin + i;
            const uint32_t count = run_buf[i];
            *total_pos_count += count;
            if (*total_pos_count > kMaxWindowPositions) {
                return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "prx: pos_count sum exceeds sane cap");
            }
            if (next_doc < doc_ordinals.size() && doc_ordinals[next_doc] == d) {
                selected.emplace_back(delta_begin, delta_begin + count, *selected_pos_count);
                *selected_pos_count += count;
                pos_off.push_back(*selected_pos_count);
                ++next_doc;
            }
            delta_begin += count;
        }
    }
    if (next_doc != doc_ordinals.size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: selected doc ordinal was not decoded");
    }
    return doris::Status::OK();
}

doris::Status decode_selected_pfor_positions(ByteSource* src, uint32_t total_pos,
                                             std::span<const SelectedRange> selected,
                                             bool decode_all_runs, std::span<uint32_t> pos_flat) {
    std::array<uint32_t, kFrqBaseUnit> run_buf {};
    size_t range_idx = 0;
    uint32_t prev = 0;
    for (uint32_t run_begin = 0; run_begin < total_pos; run_begin += kFrqBaseUnit) {
        const uint32_t run_len = std::min<uint32_t>(kFrqBaseUnit, total_pos - run_begin);
        const uint32_t run_end = run_begin + run_len;
        while (range_idx < selected.size() && selected[range_idx].end <= run_begin) {
            ++range_idx;
            prev = 0;
        }
        if (!decode_all_runs &&
            (range_idx == selected.size() || selected[range_idx].begin >= run_end)) {
            RETURN_IF_ERROR(pfor_skip(src, run_len));
            continue;
        }

        RETURN_IF_ERROR(pfor_decode(src, run_len, run_buf.data()));
        while (range_idx < selected.size() && selected[range_idx].begin < run_end) {
            const SelectedRange& range = selected[range_idx];
            const uint32_t copy_begin = std::max(range.begin, run_begin);
            const uint32_t copy_end = std::min(range.end, run_end);
            if (copy_begin == range.begin) {
                prev = 0;
            }
            uint32_t dst = range.out_begin + copy_begin - range.begin;
            for (uint32_t off = copy_begin; off < copy_end; ++off) {
                const uint32_t delta = run_buf[off - run_begin];
                prev = (off == range.begin) ? delta : prev + delta;
                pos_flat[dst++] = prev;
            }
            if (copy_end < range.end) {
                break;
            }
            ++range_idx;
            prev = 0;
        }
    }
    return doris::Status::OK();
}

doris::Status decode_pfor_payload_csr_selective(Slice plain, std::span<const uint32_t> doc_ordinals,
                                                std::vector<uint32_t>* pos_flat,
                                                std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    RETURN_IF_ERROR(validate_doc_ordinals(doc_ordinals, doc_count));

    pos_flat->clear();

    std::vector<SelectedRange> selected;
    uint64_t sum = 0;
    uint32_t selected_pos_count = 0;
    RETURN_IF_ERROR(decode_selected_pfor_count_ranges(&src, doc_count, doc_ordinals, selected,
                                                      *pos_off, &sum, &selected_pos_count));
    if (sum != total_pos) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: pos_count sum mismatch");
    }

    pos_flat->resize(selected_pos_count);
    RETURN_IF_ERROR(decode_selected_pfor_positions(
            &src, total_pos, selected,
            should_decode_full_prx_positions(selected, selected_pos_count, total_pos),
            std::span<uint32_t>(pos_flat->data(), pos_flat->size())));
    if (!src.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after pfor payload");
    }
    return doris::Status::OK();
}

// CSR decode of a plain (raw) payload. See decode_pfor_payload_csr.
doris::Status decode_payload_csr(Slice plain, std::vector<uint32_t>* pos_flat,
                                 std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    pos_flat->clear();
    pos_off->clear();
    pos_off->reserve(static_cast<size_t>(doc_count) + 1);
    pos_off->push_back(0);
    uint64_t total_pos = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        RETURN_IF_ERROR(src.get_varint32(&pos_count));
        total_pos += pos_count;
        if (total_pos > kMaxWindowPositions) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "prx: position count exceeds sane cap");
        }
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t delta = 0;
            RETURN_IF_ERROR(src.get_varint32(&delta));
            prev = (i == 0) ? delta : prev + delta;
            pos_flat->push_back(prev);
        }
        pos_off->push_back(static_cast<uint32_t>(pos_flat->size()));
    }
    if (!src.eof())
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after payload");
    return doris::Status::OK();
}

doris::Status decode_payload_csr_selective(Slice plain, std::span<const uint32_t> doc_ordinals,
                                           std::vector<uint32_t>* pos_flat,
                                           std::vector<uint32_t>* pos_off) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    RETURN_IF_ERROR(validate_doc_ordinals(doc_ordinals, doc_count));
    pos_flat->clear();
    pos_off->clear();
    pos_off->reserve(doc_ordinals.size() + 1);
    pos_off->push_back(0);
    size_t next_doc = 0;
    uint64_t total_pos = 0;
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        RETURN_IF_ERROR(src.get_varint32(&pos_count));
        total_pos += pos_count;
        if (total_pos > kMaxWindowPositions) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "prx: position count exceeds sane cap");
        }
        const bool selected = next_doc < doc_ordinals.size() && doc_ordinals[next_doc] == d;
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t delta = 0;
            RETURN_IF_ERROR(src.get_varint32(&delta));
            if (!selected) continue;
            prev = (i == 0) ? delta : prev + delta;
            pos_flat->push_back(prev);
        }
        if (selected) {
            pos_off->push_back(static_cast<uint32_t>(pos_flat->size()));
            ++next_doc;
        }
    }
    if (!src.eof())
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after payload");
    return doris::Status::OK();
}

// Decision: given level and plain length, determine whether to compress.
bool should_compress(int level, size_t plain_len) {
    if (level == 0) return false;          // force raw
    if (level > 0) return true;            // force zstd
    return plain_len >= kAutoZstdMinBytes; // auto
}

// Write a raw window: codec=raw, uncomp_len, crc(header+payload), payload.
void write_raw(Slice plain, ByteSink* sink) {
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kRaw));
    framed.put_varint32(static_cast<uint32_t>(plain.size()));
    framed.put_bytes(plain);
    sink->put_bytes(framed.view());
    sink->put_fixed32(crc32c(framed.view()));
}

// Write a zstd window: codec=zstd, uncomp_len, comp_len, crc(header+payload),
// payload.
doris::Status write_zstd(Slice plain, int level, ByteSink* sink) {
    std::vector<uint8_t> comp;
    RETURN_IF_ERROR(zstd_compress(plain, level > 0 ? level : kDefaultZstdLevel, &comp));
    write_zstd_compressed(plain, Slice(comp), sink);
    return doris::Status::OK();
}

// Read header + payload, verify crc in retrospect, and return the payload view
// and uncomp_len to the caller.
doris::Status read_framed(ByteSource* src, uint8_t* codec, uint32_t* uncomp_len, Slice* payload) {
    size_t start = src->position();
    RETURN_IF_ERROR(src->get_u8(codec));
    if (*codec != static_cast<uint8_t>(PrxCodec::kRaw) &&
        *codec != static_cast<uint8_t>(PrxCodec::kZstd) &&
        *codec != static_cast<uint8_t>(PrxCodec::kPfor)) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: unknown codec");
    }
    RETURN_IF_ERROR(src->get_varint32(uncomp_len));
    if (*uncomp_len > kMaxWindowUncompBytes) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: uncomp_len exceeds sane window cap");
    }
    size_t payload_len = *uncomp_len;
    if (*codec == static_cast<uint8_t>(PrxCodec::kZstd)) {
        uint32_t comp_len = 0;
        RETURN_IF_ERROR(src->get_varint32(&comp_len));
        payload_len = comp_len;
    }
    RETURN_IF_ERROR(src->get_bytes(payload_len, payload));
    size_t framed_len = src->position() - start;
    uint32_t stored = 0;
    RETURN_IF_ERROR(src->get_fixed32(&stored));
    if (crc32c(src->slice_from(start, framed_len)) != stored) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: window crc mismatch");
    }
    return doris::Status::OK();
}

} // namespace

doris::Status build_prx_window(std::span<const std::vector<uint32_t>> per_doc_positions,
                               int zstd_level_or_negative_for_auto, ByteSink* sink) {
    if (sink == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("prx: null sink");
    // Forced legacy codecs (level 0 = raw varint, level > 0 = zstd) are kept so
    // the test/legacy paths still exercise them; the auto path (< 0) now emits
    // PFOR bit-packed deltas -- no entropy coding, far cheaper build CPU than
    // zstd-3.
    if (zstd_level_or_negative_for_auto >= 0) {
        ByteSink plain;
        RETURN_IF_ERROR(encode_payload(per_doc_positions, &plain));
        Slice plain_view = plain.view();
        if (!should_compress(zstd_level_or_negative_for_auto, plain_view.size())) {
            write_raw(plain_view, sink);
            return doris::Status::OK();
        }
        return write_zstd(plain_view, zstd_level_or_negative_for_auto, sink);
    }
    ByteSink payload;
    RETURN_IF_ERROR(encode_pfor_payload(per_doc_positions, &payload));
    ByteSink plain;
    RETURN_IF_ERROR(encode_payload(per_doc_positions, &plain));
    return write_auto_pfor_or_zstd(payload.view(), plain.view(), sink);
}

doris::Status build_prx_window_flat(std::span<const uint32_t> positions_flat,
                                    std::span<const uint32_t> freqs,
                                    int zstd_level_or_negative_for_auto, ByteSink* sink) {
    if (sink == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("prx: null sink");
    if (zstd_level_or_negative_for_auto >= 0) {
        ByteSink plain;
        RETURN_IF_ERROR(encode_payload_flat(positions_flat, freqs, &plain));
        Slice plain_view = plain.view();
        if (!should_compress(zstd_level_or_negative_for_auto, plain_view.size())) {
            write_raw(plain_view, sink);
            return doris::Status::OK();
        }
        return write_zstd(plain_view, zstd_level_or_negative_for_auto, sink);
    }
    ByteSink payload;
    RETURN_IF_ERROR(encode_pfor_payload_flat(positions_flat, freqs, &payload));
    ByteSink plain;
    RETURN_IF_ERROR(encode_payload_flat(positions_flat, freqs, &plain));
    return write_auto_pfor_or_zstd(payload.view(), plain.view(), sink);
}

doris::Status read_prx_window(ByteSource* source,
                              std::vector<std::vector<uint32_t>>* per_doc_positions) {
    if (source == nullptr || per_doc_positions == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("prx: null arg");
    }
    uint8_t codec = 0;
    uint32_t uncomp_len = 0;
    Slice payload;
    RETURN_IF_ERROR(read_framed(source, &codec, &uncomp_len, &payload));
    if (codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        return decode_pfor_payload(payload, per_doc_positions);
    }
    if (codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        return decode_payload(payload, per_doc_positions);
    }
    std::vector<uint8_t> plain;
    RETURN_IF_ERROR(zstd_decompress(payload, uncomp_len, &plain));
    return decode_payload(Slice(plain), per_doc_positions);
}

doris::Status read_prx_window_csr(ByteSource* source, std::vector<uint32_t>* pos_flat,
                                  std::vector<uint32_t>* pos_off) {
    if (source == nullptr || pos_flat == nullptr || pos_off == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("prx: null arg");
    }
    uint8_t codec = 0;
    uint32_t uncomp_len = 0;
    Slice payload;
    RETURN_IF_ERROR(read_framed(source, &codec, &uncomp_len, &payload));
    if (codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        return decode_pfor_payload_csr(payload, pos_flat, pos_off);
    }
    if (codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        return decode_payload_csr(payload, pos_flat, pos_off);
    }
    std::vector<uint8_t> plain;
    RETURN_IF_ERROR(zstd_decompress(payload, uncomp_len, &plain));
    return decode_payload_csr(Slice(plain), pos_flat, pos_off);
}

doris::Status read_prx_window_csr_selective(ByteSource* source,
                                            std::span<const uint32_t> doc_ordinals,
                                            std::vector<uint32_t>* pos_flat,
                                            std::vector<uint32_t>* pos_off) {
    if (source == nullptr || pos_flat == nullptr || pos_off == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("prx: null arg");
    }
    uint8_t codec = 0;
    uint32_t uncomp_len = 0;
    Slice payload;
    RETURN_IF_ERROR(read_framed(source, &codec, &uncomp_len, &payload));
    if (codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        return decode_pfor_payload_csr_selective(payload, doc_ordinals, pos_flat, pos_off);
    }
    if (codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        return decode_payload_csr_selective(payload, doc_ordinals, pos_flat, pos_off);
    }
    std::vector<uint8_t> plain;
    RETURN_IF_ERROR(zstd_decompress(payload, uncomp_len, &plain));
    return decode_payload_csr_selective(Slice(plain), doc_ordinals, pos_flat, pos_off);
}

} // namespace snii::format
