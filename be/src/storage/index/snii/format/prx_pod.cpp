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

#include "storage/index/snii/format/prx_pod.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <limits>
#include <span>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/common/uninitialized_buffer.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {

void PrxDecodeStats::merge(const PrxDecodeStats& other) {
    raw_frames += other.raw_frames;
    zstd_frames += other.zstd_frames;
    pfor_frames += other.pfor_frames;
    plaintext_bytes += other.plaintext_bytes;
    total_docs += other.total_docs;
    selected_docs += other.selected_docs;
    total_positions += other.total_positions;
    selected_positions += other.selected_positions;
    fetch_ns += other.fetch_ns;
    decode_ns += other.decode_ns;
    phrase_verify_ns += other.phrase_verify_ns;
}

Status validate_prx_window_limits(const PrxWindowLimits& limits) {
    if (limits.max_docs == 0 || limits.max_positions == 0 || limits.max_uncomp_bytes == 0) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "prx: window limits must be non-zero");
    }
    if (limits.max_docs > kReaderPrxWindowLimits.max_docs ||
        limits.max_positions > kReaderPrxWindowLimits.max_positions ||
        limits.max_uncomp_bytes > kReaderPrxWindowLimits.max_uncomp_bytes) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "prx: writer window limits exceed reader limits");
    }
    return Status::OK();
}

namespace {

using PrxClock = std::chrono::steady_clock;

PrxClock::time_point prx_clock_now() {
#ifdef BE_TEST
    testing::note_prx_clock_read();
#endif
    return PrxClock::now();
}

uint64_t elapsed_ns(PrxClock::time_point start) {
    const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(prx_clock_now() - start).count();
    return std::max<uint64_t>(1, static_cast<uint64_t>(elapsed));
}

// Auto-compression threshold: use raw when payload is smaller than this (zstd
// gain is negligible and metadata overhead is relatively large).
inline constexpr size_t kAutoZstdMinBytes = 512;
// Default zstd level in auto mode.
inline constexpr int kDefaultZstdLevel = 3;
// Maximum decompressed byte size for a single .prx window. Guards against a
// corrupted uncomp_len read from S3 inflated to a huge value: sanity-check
// before allocating/decompressing to avoid GB-scale allocations. Windows are
// 256-doc aligned and normally far below this limit.
inline constexpr uint32_t kMaxWindowUncompBytes = kReaderPrxWindowLimits.max_uncomp_bytes;
// Anti-DoS cap on position count decoded from a single window before
// allocation.
inline constexpr uint32_t kMaxWindowPositions =
        kReaderPrxWindowLimits.max_positions; // 64M positions/window
// Anti-DoS cap on doc count decoded from a single window before allocation. A
// corrupt doc_count is otherwise fed straight to assign()/reserve() ->
// bad_alloc.
inline constexpr uint32_t kMaxWindowDocs = kReaderPrxWindowLimits.max_docs; // 16M docs/window

// Writer-side precondition for the FLAT builders: the per-doc partition `freqs`
// must address exactly the positions present in `flat`. If sum(freqs) overruns
// flat.size() a (positions_flat, freqs) mismatch would index flat[off+i] past
// the span end -- an out-of-bounds read on caller-supplied data. Reject it as
// InvalidArgument BEFORE any indexing so the bug surfaces as a clean Status,
// never UB. (sum < size leaves trailing positions unused, which is also a
// writer bug, so we require exact equality.) Uint64 accumulation cannot
// overflow for uint32 freqs.
Status check_flat_partition(std::span<const uint32_t> flat, std::span<const uint32_t> freqs) {
    size_t sum = 0;
    for (uint32_t fc : freqs) {
        if (fc > flat.size() - sum) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: sum(freqs) exceeds positions_flat size");
        }
        sum += fc;
    }
    if (sum != flat.size()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "prx: sum(freqs) does not match positions_flat size");
    }
    return Status::OK();
}

Status validate_flat_positions(std::span<const uint32_t> flat, std::span<const uint32_t> freqs) {
    size_t off = 0;
    for (uint32_t fc : freqs) {
        uint32_t previous = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t position = flat[off + i];
            if (i != 0 && position < previous) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: positions within a doc must be ascending");
            }
            previous = position;
        }
        off += fc;
    }
    return Status::OK();
}

Status validate_per_doc_window(std::span<const std::vector<uint32_t>> per_doc,
                               const PrxWindowLimits& limits, size_t* total_positions) {
    RETURN_IF_ERROR(validate_prx_window_limits(limits));
    if (per_doc.size() > limits.max_docs) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "prx: doc count exceeds writer window limit");
    }
    uint64_t total = 0;
    for (const auto& positions : per_doc) {
        total += positions.size();
        if (total > limits.max_positions) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: position count exceeds writer window limit");
        }
    }
    *total_positions = static_cast<size_t>(total);
    return Status::OK();
}

// Encode per-doc position lists into a self-describing plain payload (doc_count
// + per-doc delta stream).
Status encode_payload(std::span<const std::vector<uint32_t>> per_doc, ByteSink* out) {
    out->put_varint32(static_cast<uint32_t>(per_doc.size()));
    for (const auto& doc : per_doc) {
        out->put_varint32(static_cast<uint32_t>(doc.size()));
        uint32_t prev = 0;
        for (size_t i = 0; i < doc.size(); ++i) {
            uint32_t pos = doc[i];
            if (i > 0 && pos < prev) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: positions within a doc must be ascending");
            }
            out->put_varint32(i == 0 ? pos : pos - prev);
            prev = pos;
        }
    }
    return Status::OK();
}

// FLAT-positions encoder: identical wire output to encode_payload above, but
// reads positions from a single flat span partitioned per-doc by `freqs` (doc d
// owns the next freqs[d] entries). The public entry point has already validated
// that sum(freqs) == flat.size(). This avoids materializing a vector-of-vectors
// for the window.
Status encode_payload_flat(std::span<const uint32_t> flat, std::span<const uint32_t> freqs,
                           ByteSink* out) {
    out->put_varint32(static_cast<uint32_t>(freqs.size()));
    size_t off = 0;
    for (uint32_t fc : freqs) {
        out->put_varint32(fc);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t pos = flat[off + i];
            if (i > 0 && pos < prev) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: positions within a doc must be ascending");
            }
            out->put_varint32(i == 0 ? pos : pos - prev);
            prev = pos;
        }
        off += fc;
    }
    return Status::OK();
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
Status decode_pfor_runs(ByteSource* src, size_t n, std::vector<uint32_t>* out) {
    // Sized then fully overwritten by pfor_decode below (every [0, n) slot is
    // written); no zero-fill needed beyond what std::vector mandates.
    resize_uninitialized(*out, n);
    for (size_t off = 0; off < n; off += kFrqBaseUnit) {
        const size_t run = (n - off < kFrqBaseUnit) ? (n - off) : kFrqBaseUnit;
        RETURN_IF_ERROR(pfor_decode(src, run, out->data() + off));
    }
    return Status::OK();
}

size_t varint32_size(uint32_t value) {
    size_t bytes = 1;
    while (value >= 128) {
        value >>= 7;
        ++bytes;
    }
    return bytes;
}

// Derive the per-doc position deltas ONCE into `deltas` (flat, in doc order: the
// first position of each doc is absolute, the rest are deltas within the doc),
// enforcing the ascending-position precondition after the public entry point
// validated the exact (flat, freqs) partition. The loop is identical to the delta
// derivation the old encode_pfor_payload_flat ran inline, lifted out so the auto
// path can feed BOTH the PFOR payload and (only when needed) the raw plaintext
// payload from one buffer instead of walking `flat` twice. Accumulate the exact
// raw payload size in the same pass so codec selection does not rescan deltas.
Status compute_flat_deltas(std::span<const uint32_t> flat, std::span<const uint32_t> freqs,
                           std::vector<uint32_t>* const deltas, size_t* const plain_size) {
#ifdef BE_TEST
    testing::note_prx_delta_materialization();
#endif
    deltas->clear();
    deltas->reserve(flat.size());
    *plain_size = varint32_size(static_cast<uint32_t>(freqs.size()));
    size_t off = 0;
    for (uint32_t fc : freqs) {
        *plain_size += varint32_size(fc);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < fc; ++i) {
            const uint32_t pos = flat[off + i];
            if (i > 0 && pos < prev) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: positions within a doc must be ascending");
            }
            const uint32_t delta = i == 0 ? pos : pos - prev;
            deltas->push_back(delta);
            *plain_size += varint32_size(delta);
            prev = pos;
        }
        off += fc;
    }
    return Status::OK();
}

// PFOR window payload (self-describing; no entropy coding):
//   VInt doc_count
//   VInt total_pos             # sum of all pos_counts
//   PFOR_runs(pos_counts)      # doc_count values (bit-packed; mostly 1 -> ~1
//   bit) PFOR_runs(position_deltas) # total_pos deltas, flat across docs (first
//   per
//                              #   doc absolute, rest delta-within-doc)
// Bit-packing the per-doc pos_counts (vs one varint each) is the size win: in a
// uniform corpus most docs have freq 1, so the count column packs to ~1 bit/doc.
// Emits byte-for-byte the same payload the old encode_pfor_payload_flat produced
// (doc_count == freqs.size(), total_pos == deltas.size() == sum(freqs)), but
// reads the already-derived `deltas` instead of re-walking the positions.
void encode_pfor_payload_from_deltas(std::span<const uint32_t> freqs,
                                     std::span<const uint32_t> deltas, ByteSink* out) {
    out->put_varint32(static_cast<uint32_t>(freqs.size()));
    out->put_varint32(static_cast<uint32_t>(deltas.size()));
    encode_pfor_runs(freqs, out);
    encode_pfor_runs(deltas, out);
}

// Raw plaintext payload (self-describing per-doc boundaries):
//   VInt doc_count
//   per doc: VInt pos_count, then pos_count position deltas (VInt)
// Emits byte-for-byte the same payload the old encode_payload_flat produced, but
// reads the already-derived `deltas` instead of re-walking the positions and
// re-running the partition/ascending checks.
void encode_payload_from_deltas(std::span<const uint32_t> freqs, std::span<const uint32_t> deltas,
                                ByteSink* out) {
    out->put_varint32(static_cast<uint32_t>(freqs.size()));
    size_t off = 0;
    for (uint32_t fc : freqs) {
        out->put_varint32(fc);
        for (uint32_t i = 0; i < fc; ++i) {
            out->put_varint32(deltas[off + i]);
        }
        off += fc;
    }
}

// Decode per-doc position lists from a PFOR payload.
Status decode_pfor_payload(Slice plain, std::vector<std::vector<uint32_t>>* out) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    std::vector<uint32_t> pos_counts;
    RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, &pos_counts));
    uint64_t sum = 0;
    for (uint32_t d = 0; d < doc_count; ++d) sum += pos_counts[d];
    if (sum != total_pos) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
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
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after pfor payload");
    return Status::OK();
}

// Writes a PFOR window: codec=pfor, payload, crc(header+payload).
void write_pfor(Slice payload, ByteSink* sink) {
    // Single-copy framing: write [codec][varint len][payload] straight into the
    // caller's sink, then crc exactly those bytes. view() is taken AFTER the
    // payload and BEFORE the crc, so subslice([start, framed_len)) is over a
    // settled, contiguous buffer with no pending realloc/aliasing. Byte-identical
    // to the former temp-ByteSink assembly, minus one heap alloc + one payload copy.
    const size_t start = sink->size();
    sink->put_u8(static_cast<uint8_t>(PrxCodec::kPfor));
    sink->put_varint32(static_cast<uint32_t>(payload.size()));
    sink->put_bytes(payload);
    const size_t framed_len = sink->size() - start;
    const uint32_t crc = crc32c(sink->view().subslice(start, framed_len));
    sink->put_fixed32(crc);
}

void write_raw(Slice plain, ByteSink* sink);

// Emit a RAW frame directly from the already-derived deltas. This is used by
// the singleton fast path so choosing RAW does not allocate a temporary plain
// payload or encode/copy a losing PFOR payload first.
void write_raw_from_deltas(std::span<const uint32_t> freqs, std::span<const uint32_t> deltas,
                           size_t plain_size, ByteSink* sink) {
    const size_t start = sink->size();
    sink->put_u8(static_cast<uint8_t>(PrxCodec::kRaw));
    sink->put_varint32(static_cast<uint32_t>(plain_size));
    const size_t payload_start = sink->size();
    encode_payload_from_deltas(freqs, deltas, sink);
    DCHECK_EQ(sink->size() - payload_start, plain_size);
    const size_t framed_len = sink->size() - start;
    const uint32_t crc = crc32c(sink->view().subslice(start, framed_len));
    sink->put_fixed32(crc);
}

Status validate_single_doc_byte_limits(std::span<const uint32_t> positions,
                                       std::span<const uint32_t> freqs, bool auto_codec,
                                       uint32_t max_uncomp_bytes) {
    size_t offset = 0;
    std::vector<uint32_t> one_doc_deltas;
    for (size_t doc = 0; doc < freqs.size(); ++doc) {
        const uint32_t frequency = freqs[doc];
        const auto one_freq = freqs.subspan(doc, 1);
        const auto one_doc_positions = positions.subspan(offset, frequency);
        size_t plain_size = varint32_size(1) + varint32_size(frequency);
        uint32_t previous = 0;
        for (size_t i = 0; i < one_doc_positions.size(); ++i) {
            const uint32_t position = one_doc_positions[i];
            plain_size += varint32_size(i == 0 ? position : position - previous);
            previous = position;
        }
        if (plain_size > max_uncomp_bytes) {
            if (!auto_codec) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: one document exceeds the writer window byte limit");
            }
            if (one_doc_deltas.capacity() < frequency) {
                std::vector<uint32_t>().swap(one_doc_deltas);
                one_doc_deltas.reserve(frequency);
            } else {
                one_doc_deltas.clear();
            }
            previous = 0;
            for (size_t i = 0; i < one_doc_positions.size(); ++i) {
                const uint32_t position = one_doc_positions[i];
                one_doc_deltas.push_back(i == 0 ? position : position - previous);
                previous = position;
            }
            ByteSink pfor_payload;
            encode_pfor_payload_from_deltas(one_freq, one_doc_deltas, &pfor_payload);
            if (pfor_payload.size() > max_uncomp_bytes) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: one document exceeds the writer window byte limit");
            }
        }
        offset += frequency;
    }
    return Status::OK();
}

size_t pfor_frame_size(size_t payload_size) {
    return 1 + varint32_size(static_cast<uint32_t>(payload_size)) + payload_size + sizeof(uint32_t);
}

size_t raw_frame_size(size_t plain_size) {
    return 1 + varint32_size(static_cast<uint32_t>(plain_size)) + plain_size + sizeof(uint32_t);
}

size_t zstd_frame_size(size_t plain_size, size_t compressed_size) {
    return 1 + varint32_size(static_cast<uint32_t>(plain_size)) +
           varint32_size(static_cast<uint32_t>(compressed_size)) + compressed_size +
           sizeof(uint32_t);
}

struct AutoPrxCodecChoice {
    PrxCodec codec = PrxCodec::kPfor;
    bool readable = false;
};

// Select the smallest complete reader-safe frame among the candidates already
// materialized by the zstd/fallback path. Preserve the existing codec on equal
// sizes by considering PFOR, then ZSTD, then RAW and replacing the winner only
// on a strict size reduction. Sub-threshold singleton RAW selection happens
// before PFOR materialization in build_prx_window_auto_from_flat.
AutoPrxCodecChoice select_auto_prx_codec(size_t pfor_payload_size, size_t plain_payload_size,
                                         size_t compressed_payload_size, bool has_compressed,
                                         uint32_t max_uncomp_bytes) {
    const bool pfor_readable = pfor_payload_size <= max_uncomp_bytes;
    const bool plain_readable = plain_payload_size <= max_uncomp_bytes;
    AutoPrxCodecChoice choice;
    size_t selected_frame_size = 0;
    if (pfor_readable) {
        choice = {.codec = PrxCodec::kPfor, .readable = true};
        selected_frame_size = pfor_frame_size(pfor_payload_size);
    }
    if (has_compressed && plain_readable) {
        const size_t frame_size = zstd_frame_size(plain_payload_size, compressed_payload_size);
        if (!choice.readable || frame_size < selected_frame_size) {
            choice = {.codec = PrxCodec::kZstd, .readable = true};
            selected_frame_size = frame_size;
        }
    }
    if (plain_readable) {
        const size_t frame_size = raw_frame_size(plain_payload_size);
        if (!choice.readable || frame_size < selected_frame_size) {
            choice = {.codec = PrxCodec::kRaw, .readable = true};
        }
    }
    return choice;
}

void write_zstd_compressed(Slice plain, Slice compressed, ByteSink* sink) {
    // Single-copy framing (see write_pfor): assemble [codec][uncomp_len][comp_len]
    // [compressed] in the caller's sink and crc that span before appending the crc.
    const size_t start = sink->size();
    sink->put_u8(static_cast<uint8_t>(PrxCodec::kZstd));
    sink->put_varint32(static_cast<uint32_t>(plain.size()));
    sink->put_varint32(static_cast<uint32_t>(compressed.size()));
    sink->put_bytes(compressed);
    const size_t framed_len = sink->size() - start;
    const uint32_t crc = crc32c(sink->view().subslice(start, framed_len));
    sink->put_fixed32(crc);
}

// Shared auto-mode path for BOTH .prx builders. A single-doc/freq=1 window has a
// provably smaller RAW frame: RAW stores three payload varints, while PFOR adds
// two run headers and is always 4-5 bytes larger. Emit that RAW frame directly
// before any PFOR work. Other sub-threshold windows retain the existing PFOR
// policy so an unmeasured second payload encode cannot regress import CPU. At
// and above the zstd threshold the raw plaintext is already required for
// compression, so all materialized candidates participate in the exact complete
// frame-size comparison at no additional encoding cost.
Status build_prx_window_auto_from_flat(std::span<const uint32_t> positions_flat,
                                       std::span<const uint32_t> freqs, int zstd_level,
                                       const PrxWindowLimits& limits, ByteSink* sink,
                                       PrxWindowBuildOutcome* outcome) {
    if (freqs.size() == 1 && freqs.front() == 1) {
        const size_t plain_size =
                varint32_size(1) + varint32_size(1) + varint32_size(positions_flat.front());
        if (plain_size > limits.max_uncomp_bytes) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: one document exceeds the writer window byte limit");
        }
        write_raw_from_deltas(freqs, positions_flat, plain_size, sink);
        *outcome = PrxWindowBuildOutcome::kBuilt;
        return Status::OK();
    }

    std::vector<uint32_t> deltas;
    size_t plain_size = 0;
    RETURN_IF_ERROR(compute_flat_deltas(positions_flat, freqs, &deltas, &plain_size));
    const bool plain_readable = plain_size <= limits.max_uncomp_bytes;

    ByteSink payload;
    encode_pfor_payload_from_deltas(freqs, deltas, &payload);
    const bool pfor_readable = payload.size() <= limits.max_uncomp_bytes;
    if (!pfor_readable && !plain_readable) {
        if (freqs.size() <= 1) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: one document exceeds the writer window byte limit");
        }
        RETURN_IF_ERROR(validate_single_doc_byte_limits(positions_flat, freqs, true,
                                                        limits.max_uncomp_bytes));
        *outcome = PrxWindowBuildOutcome::kNeedsSplit;
        return Status::OK();
    }
    if (plain_readable && (plain_size >= kAutoZstdMinBytes || !pfor_readable)) {
        ByteSink plain;
        encode_payload_from_deltas(freqs, deltas, &plain);
        DCHECK_EQ(plain.size(), plain_size);
        std::vector<uint8_t> compressed;
        const bool has_compressed = plain_size >= kAutoZstdMinBytes;
        if (plain_size >= kAutoZstdMinBytes) {
            testing::note_prx_raw_build();
            RETURN_IF_ERROR(zstd_compress(plain.view(), zstd_level, &compressed));
        }
        const AutoPrxCodecChoice choice =
                select_auto_prx_codec(payload.size(), plain.size(), compressed.size(),
                                      has_compressed, limits.max_uncomp_bytes);
        DCHECK(choice.readable);
        if (choice.codec == PrxCodec::kZstd) {
            write_zstd_compressed(plain.view(), Slice(compressed), sink);
        } else if (choice.codec == PrxCodec::kRaw) {
            write_raw(plain.view(), sink);
        } else {
            write_pfor(payload.view(), sink);
        }
        *outcome = PrxWindowBuildOutcome::kBuilt;
        return Status::OK();
    }
    DCHECK(pfor_readable);
    write_pfor(payload.view(), sink);
    *outcome = PrxWindowBuildOutcome::kBuilt;
    return Status::OK();
}

// Decode per-doc position lists from a plain payload.
Status decode_payload(Slice plain, std::vector<std::vector<uint32_t>>* out) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    out->clear();
    out->reserve(doc_count);
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        RETURN_IF_ERROR(src.get_varint32_fast(&pos_count));
        std::vector<uint32_t> doc;
        doc.reserve(pos_count);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < pos_count; ++i) {
            uint32_t delta = 0;
            RETURN_IF_ERROR(src.get_varint32(&delta));
            if (i != 0 && delta > std::numeric_limits<uint32_t>::max() - prev) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "prx: position accumulation overflow");
            }
            prev = (i == 0) ? delta : prev + delta;
            doc.push_back(prev);
        }
        out->push_back(std::move(doc));
    }
    if (!src.eof())
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after payload");
    return Status::OK();
}

// CSR decode of a PFOR payload: all docs' positions into one flat buffer +
// per-doc offsets, with NO per-doc std::vector allocation. `pos_off` has
// doc_count+1 entries (pos_off[0]==0); doc d's positions are
// pos_flat[pos_off[d] .. pos_off[d+1]).
Status decode_pfor_payload_csr(Slice plain, std::vector<uint32_t>* pos_flat,
                               std::vector<uint32_t>* pos_off,
                               PrxCsrAllocationGate* allocation_gate, uint32_t* max_frequency,
                               bool* has_zero_frequency) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    if (allocation_gate != nullptr) {
        RETURN_IF_ERROR(allocation_gate->reserve_csr(pos_flat, total_pos, pos_off,
                                                     static_cast<size_t>(doc_count) + 1));
    }
    pos_off->clear();
    pos_off->reserve(static_cast<size_t>(doc_count) + 1);
    RETURN_IF_ERROR(decode_pfor_runs(&src, doc_count, pos_off));
    uint64_t sum = 0;
    uint32_t decoded_max_frequency = 0;
    bool decoded_zero_frequency = false;
    for (uint32_t d = 0; d < doc_count; ++d) {
        sum += (*pos_off)[d];
        decoded_max_frequency = std::max(decoded_max_frequency, (*pos_off)[d]);
        decoded_zero_frequency |= (*pos_off)[d] == 0;
    }
    if (sum != total_pos)
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
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
            if (i != 0 && value > std::numeric_limits<uint32_t>::max() - prev) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "prx: position accumulation overflow");
            }
            prev = (i == 0) ? value : prev + value;
            value = prev;
        }
        off += pos_count;
        next_off += pos_count;
    }
    pos_off->push_back(next_off);
    if (!src.eof())
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after pfor payload");
    *max_frequency = decoded_max_frequency;
    *has_zero_frequency = decoded_zero_frequency;
    return Status::OK();
}

Status validate_doc_ordinals(std::span<const uint32_t> doc_ordinals, uint32_t doc_count) {
    uint32_t prev = 0;
    for (size_t i = 0; i < doc_ordinals.size(); ++i) {
        const uint32_t doc = doc_ordinals[i];
        if (doc >= doc_count) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "prx: selected doc ordinal out of range");
        }
        if (i != 0 && doc <= prev) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: selected doc ordinals must be strictly ascending");
        }
        prev = doc;
    }
    return Status::OK();
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

Status decode_selected_pfor_count_ranges(ByteSource* src, uint32_t doc_count,
                                         std::span<const uint32_t> doc_ordinals,
                                         std::vector<SelectedRange>& selected,
                                         std::vector<uint32_t>& pos_off, uint64_t* total_pos_count,
                                         uint32_t* selected_pos_count, uint32_t* max_frequency,
                                         bool* has_zero_frequency) {
    selected.clear();
    selected.reserve(doc_ordinals.size());
    pos_off.clear();
    pos_off.reserve(doc_ordinals.size() + 1);
    pos_off.push_back(0);

    *selected_pos_count = 0;
    uint32_t delta_begin = 0;
    size_t next_doc = 0;
    *total_pos_count = 0;
    *max_frequency = 0;
    *has_zero_frequency = false;
    std::array<uint32_t, kFrqBaseUnit> run_buf {};
    for (uint32_t run_begin = 0; run_begin < doc_count; run_begin += kFrqBaseUnit) {
        const uint32_t run_len = std::min<uint32_t>(kFrqBaseUnit, doc_count - run_begin);
        RETURN_IF_ERROR(pfor_decode(src, run_len, run_buf.data()));
        for (uint32_t i = 0; i < run_len; ++i) {
            const uint32_t d = run_begin + i;
            const uint32_t count = run_buf[i];
            *max_frequency = std::max(*max_frequency, count);
            *has_zero_frequency |= count == 0;
            *total_pos_count += count;
            if (*total_pos_count > kMaxWindowPositions) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
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
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: selected doc ordinal was not decoded");
    }
    return Status::OK();
}

Status decode_selected_pfor_positions(ByteSource* src, uint32_t total_pos,
                                      std::span<const SelectedRange> selected, bool decode_all_runs,
                                      std::span<uint32_t> pos_flat) {
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
                if (off != range.begin && delta > std::numeric_limits<uint32_t>::max() - prev) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                            "prx: position accumulation overflow");
                }
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
    return Status::OK();
}

Status decode_pfor_payload_csr_selective(Slice plain, std::span<const uint32_t> doc_ordinals,
                                         std::vector<uint32_t>* pos_flat,
                                         std::vector<uint32_t>* pos_off,
                                         uint32_t* decoded_doc_count,
                                         uint64_t* decoded_total_positions, uint32_t* max_frequency,
                                         bool* has_zero_frequency) {
    ByteSource src(plain);
    uint32_t doc_count = 0, total_pos = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    RETURN_IF_ERROR(src.get_varint32(&total_pos));
    if (total_pos > kMaxWindowPositions) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: position count exceeds sane cap");
    }
    if (doc_count > kMaxWindowDocs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    RETURN_IF_ERROR(validate_doc_ordinals(doc_ordinals, doc_count));

    pos_flat->clear();

    std::vector<SelectedRange> selected;
    uint64_t sum = 0;
    uint32_t selected_pos_count = 0;
    RETURN_IF_ERROR(decode_selected_pfor_count_ranges(&src, doc_count, doc_ordinals, selected,
                                                      *pos_off, &sum, &selected_pos_count,
                                                      max_frequency, has_zero_frequency));
    if (sum != total_pos) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: pos_count sum mismatch");
    }

    const bool decode_all_runs =
            should_decode_full_prx_positions(selected, selected_pos_count, total_pos);
    pos_flat->resize(selected_pos_count);
    RETURN_IF_ERROR(decode_selected_pfor_positions(
            &src, total_pos, selected, decode_all_runs,
            std::span<uint32_t>(pos_flat->data(), pos_flat->size())));
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after pfor payload");
    }
    *decoded_doc_count = doc_count;
    *decoded_total_positions = total_pos;
    return Status::OK();
}

// CSR decode of a plain (raw) payload. See decode_pfor_payload_csr.
Status scan_payload_csr_shape(Slice plain, uint32_t* doc_count, uint32_t* total_positions) {
    ByteSource src(plain);
    RETURN_IF_ERROR(src.get_varint32(doc_count));
    if (*doc_count > kMaxWindowDocs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    uint64_t total_pos = 0;
    for (uint32_t d = 0; d < *doc_count; ++d) {
        uint32_t pos_count = 0;
        RETURN_IF_ERROR(src.get_varint32_fast(&pos_count));
        total_pos += pos_count;
        if (total_pos > kMaxWindowPositions) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "prx: position count exceeds sane cap");
        }
        RETURN_IF_ERROR(src.skip_varints(pos_count));
    }
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after payload");
    }
    *total_positions = static_cast<uint32_t>(total_pos);
    return Status::OK();
}

Status decode_payload_csr(Slice plain, std::vector<uint32_t>* pos_flat,
                          std::vector<uint32_t>* pos_off, PrxCsrAllocationGate* allocation_gate,
                          uint32_t* max_frequency, bool* has_zero_frequency) {
    if (allocation_gate != nullptr) {
        uint32_t preflight_doc_count = 0;
        uint32_t preflight_total_positions = 0;
        RETURN_IF_ERROR(
                scan_payload_csr_shape(plain, &preflight_doc_count, &preflight_total_positions));
        RETURN_IF_ERROR(allocation_gate->reserve_csr(pos_flat, preflight_total_positions, pos_off,
                                                     static_cast<size_t>(preflight_doc_count) + 1));
    }
    ByteSource src(plain);
    uint32_t doc_count = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    pos_flat->clear();
    pos_off->clear();
    pos_off->reserve(static_cast<size_t>(doc_count) + 1);
    pos_off->push_back(0);
    uint64_t total_pos = 0;
    uint32_t decoded_max_frequency = 0;
    bool decoded_zero_frequency = false;
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        RETURN_IF_ERROR(src.get_varint32_fast(&pos_count));
        decoded_max_frequency = std::max(decoded_max_frequency, pos_count);
        decoded_zero_frequency |= pos_count == 0;
        total_pos += pos_count;
        if (total_pos > kMaxWindowPositions) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "prx: position count exceeds sane cap");
        }
        // Tight inline prefix-sum decode (single-byte fast path) -- see
        // decode_delta_run. Shared with the selective reader below.
        RETURN_IF_ERROR(src.decode_delta_run(pos_count, pos_flat));
        pos_off->push_back(static_cast<uint32_t>(pos_flat->size()));
    }
    if (!src.eof())
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after payload");
    *max_frequency = decoded_max_frequency;
    *has_zero_frequency = decoded_zero_frequency;
    return Status::OK();
}

Status decode_payload_csr_selective(Slice plain, std::span<const uint32_t> doc_ordinals,
                                    std::vector<uint32_t>* pos_flat, std::vector<uint32_t>* pos_off,
                                    uint32_t* decoded_doc_count, uint64_t* decoded_total_positions,
                                    uint32_t* max_frequency, bool* has_zero_frequency) {
    ByteSource src(plain);
    uint32_t doc_count = 0;
    RETURN_IF_ERROR(src.get_varint32(&doc_count));
    if (doc_count > kMaxWindowDocs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: doc count exceeds sane cap");
    }
    RETURN_IF_ERROR(validate_doc_ordinals(doc_ordinals, doc_count));
    pos_flat->clear();
    pos_off->clear();
    pos_off->reserve(doc_ordinals.size() + 1);
    pos_off->push_back(0);
    size_t next_doc = 0;
    uint64_t total_pos = 0;
    uint32_t decoded_max_frequency = 0;
    bool decoded_zero_frequency = false;
    for (uint32_t d = 0; d < doc_count; ++d) {
        uint32_t pos_count = 0;
        RETURN_IF_ERROR(src.get_varint32_fast(&pos_count));
        decoded_max_frequency = std::max(decoded_max_frequency, pos_count);
        decoded_zero_frequency |= pos_count == 0;
        total_pos += pos_count;
        if (total_pos > kMaxWindowPositions) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "prx: position count exceeds sane cap");
        }
        const bool selected = next_doc < doc_ordinals.size() && doc_ordinals[next_doc] == d;
        if (!selected) {
            // Skip this doc's position deltas without decoding them -- the CSR
            // layout is sequential so we must advance past them, but only the
            // candidate (selected) docs' positions are ever used. With a sparse
            // candidate set (the common phrase / phrase-prefix case after docid
            // narrowing) most docs in a window are skipped, so this avoids the
            // dominant varint-decode cost.
            RETURN_IF_ERROR(src.skip_varints(pos_count));
            continue;
        }
        // Selected doc: decode its `pos_count` ascending position deltas with a
        // tight inline prefix-sum decoder (single-byte fast path, no per-value
        // get_varint32/Status call chain). This is the CPU hotspot for narrowed
        // phrase/phrase-prefix candidate sets, where each selected doc's varint
        // run dominates after non-selected docs are skipped.
        RETURN_IF_ERROR(src.decode_delta_run(pos_count, pos_flat));
        pos_off->push_back(static_cast<uint32_t>(pos_flat->size()));
        ++next_doc;
    }
    if (!src.eof())
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: trailing bytes after payload");
    *decoded_doc_count = doc_count;
    *decoded_total_positions = total_pos;
    *max_frequency = decoded_max_frequency;
    *has_zero_frequency = decoded_zero_frequency;
    return Status::OK();
}

// Decision: given level and plain length, determine whether to compress.
bool should_compress(int level, size_t plain_len) {
    if (level == 0) return false;          // force raw
    if (level > 0) return true;            // force zstd
    return plain_len >= kAutoZstdMinBytes; // auto
}

// Write a raw window: codec=raw, uncomp_len, crc(header+payload), payload.
void write_raw(Slice plain, ByteSink* sink) {
    // Single-copy framing (see write_pfor): assemble [codec][uncomp_len][payload]
    // in the caller's sink and crc that span before appending the crc.
    const size_t start = sink->size();
    sink->put_u8(static_cast<uint8_t>(PrxCodec::kRaw));
    sink->put_varint32(static_cast<uint32_t>(plain.size()));
    sink->put_bytes(plain);
    const size_t framed_len = sink->size() - start;
    const uint32_t crc = crc32c(sink->view().subslice(start, framed_len));
    sink->put_fixed32(crc);
}

// Write a zstd window: codec=zstd, uncomp_len, comp_len, crc(header+payload),
// payload.
Status write_zstd(Slice plain, int level, ByteSink* sink) {
    std::vector<uint8_t> comp;
    RETURN_IF_ERROR(zstd_compress(plain, level > 0 ? level : kDefaultZstdLevel, &comp));
    write_zstd_compressed(plain, Slice(comp), sink);
    return Status::OK();
}

struct FramedPrxWindow {
    uint8_t codec = 0;
    uint32_t uncomp_len = 0;
    Slice payload;
};

// Read header + payload and verify the frame CRC.
Status read_framed(ByteSource* src, FramedPrxWindow* frame) {
    const size_t start = src->position();
    RETURN_IF_ERROR(src->get_u8(&frame->codec));
    if (frame->codec != static_cast<uint8_t>(PrxCodec::kRaw) &&
        frame->codec != static_cast<uint8_t>(PrxCodec::kZstd) &&
        frame->codec != static_cast<uint8_t>(PrxCodec::kPfor)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("prx: unknown codec");
    }
    RETURN_IF_ERROR(src->get_varint32(&frame->uncomp_len));
    if (frame->uncomp_len > kMaxWindowUncompBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: uncomp_len exceeds sane window cap");
    }
    size_t payload_len = frame->uncomp_len;
    if (frame->codec == static_cast<uint8_t>(PrxCodec::kZstd)) {
        uint32_t comp_len = 0;
        RETURN_IF_ERROR(src->get_varint32(&comp_len));
        payload_len = comp_len;
    }
    RETURN_IF_ERROR(src->get_bytes(payload_len, &frame->payload));
    const size_t framed_len = src->position() - start;
    uint32_t stored = 0;
    RETURN_IF_ERROR(src->get_fixed32(&stored));
    if (crc32c(src->slice_from(start, framed_len)) != stored) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "prx: window crc mismatch");
    }
    return Status::OK();
}

void initialize_frame_stats(const FramedPrxWindow& encoded, PrxDecodeStats* stats) {
    if (encoded.codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        stats->raw_frames = 1;
        stats->plaintext_bytes = encoded.payload.size();
    } else if (encoded.codec == static_cast<uint8_t>(PrxCodec::kZstd)) {
        stats->zstd_frames = 1;
        stats->plaintext_bytes = encoded.uncomp_len;
    } else {
        stats->pfor_frames = 1;
        stats->plaintext_bytes = encoded.uncomp_len;
    }
}

Status decode_csr_frame(const FramedPrxWindow& encoded, std::span<const uint32_t> doc_ordinals,
                        bool decode_all_docs, bool all_docs_selected,
                        std::vector<uint32_t>* pos_flat, std::vector<uint32_t>* pos_off,
                        PrxDecodeStats* stats, PrxDecodedShape* shape,
                        PrxCsrAllocationGate* allocation_gate) {
    if (!decode_all_docs && allocation_gate != nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "prx: selective decode cannot use an allocation gate");
    }
    if (stats != nullptr) {
        initialize_frame_stats(encoded, stats);
    }
    uint32_t total_docs = 0;
    uint64_t total_positions = 0;
    uint32_t max_frequency = 0;
    bool has_zero_frequency = false;

    std::vector<uint8_t> local_decompressed;
    Slice plain = encoded.payload;
    if (encoded.codec == static_cast<uint8_t>(PrxCodec::kZstd)) {
        std::vector<uint8_t>* decompressed = &local_decompressed;
        if (allocation_gate != nullptr) {
            RETURN_IF_ERROR(
                    allocation_gate->reserve_decompression(encoded.uncomp_len, &decompressed));
            DCHECK(decompressed != nullptr);
            DCHECK_GE(decompressed->capacity(), encoded.uncomp_len);
        }
        RETURN_IF_ERROR(zstd_decompress(encoded.payload, encoded.uncomp_len, decompressed));
        plain = Slice(*decompressed);
    }

    if (decode_all_docs) {
        if (encoded.codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
            RETURN_IF_ERROR(decode_pfor_payload_csr(plain, pos_flat, pos_off, allocation_gate,
                                                    &max_frequency, &has_zero_frequency));
        } else {
            RETURN_IF_ERROR(decode_payload_csr(plain, pos_flat, pos_off, allocation_gate,
                                               &max_frequency, &has_zero_frequency));
        }
        total_docs = static_cast<uint32_t>(pos_off->size() - 1);
        total_positions = pos_flat->size();
        if (!all_docs_selected) {
            RETURN_IF_ERROR(validate_doc_ordinals(doc_ordinals, total_docs));
        }
    } else if (encoded.codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        RETURN_IF_ERROR(decode_pfor_payload_csr_selective(plain, doc_ordinals, pos_flat, pos_off,
                                                          &total_docs, &total_positions,
                                                          &max_frequency, &has_zero_frequency));
    } else {
        RETURN_IF_ERROR(decode_payload_csr_selective(plain, doc_ordinals, pos_flat, pos_off,
                                                     &total_docs, &total_positions, &max_frequency,
                                                     &has_zero_frequency));
    }
    if (shape != nullptr) {
        shape->total_docs = total_docs;
        shape->total_positions = total_positions;
        shape->max_frequency = max_frequency;
        shape->has_zero_frequency = has_zero_frequency;
    }
    if (stats != nullptr) {
        stats->total_docs = total_docs;
        stats->total_positions = total_positions;
        if (all_docs_selected) {
            stats->selected_docs = total_docs;
            stats->selected_positions = total_positions;
        } else if (!decode_all_docs) {
            stats->selected_docs = doc_ordinals.size();
            stats->selected_positions = pos_flat->size();
        }
    }
    return Status::OK();
}

Status read_prx_window_csr_impl(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                bool decode_all_docs, bool all_docs_selected,
                                std::vector<uint32_t>* pos_flat, std::vector<uint32_t>* pos_off,
                                PrxDecodeContext* context) {
    if (source == nullptr || pos_flat == nullptr || pos_off == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("prx: null arg");
    }
    const bool collect_stats = context != nullptr && context->stats != nullptr;
    PrxDecodeStats frame_stats;
    PrxDecodeStats* stats = collect_stats ? &frame_stats : nullptr;

    PrxClock::time_point decode_start;
    if (collect_stats) {
        decode_start = prx_clock_now();
    }
    FramedPrxWindow encoded;
    RETURN_IF_ERROR(read_framed(source, &encoded));
    RETURN_IF_ERROR(decode_csr_frame(encoded, doc_ordinals, decode_all_docs, all_docs_selected,
                                     pos_flat, pos_off, stats,
                                     context == nullptr ? nullptr : context->shape,
                                     context == nullptr ? nullptr : context->allocation_gate));
    if (collect_stats) {
        // Stop inclusive decode timing before the logical-selection scan. Phrase
        // execution wraps this call in PhraseVerifyTimer, so that scan remains
        // part of verification rather than format decode.
        frame_stats.decode_ns = elapsed_ns(decode_start);
        if (decode_all_docs && !all_docs_selected) {
            uint64_t selected_positions = 0;
            for (uint32_t ordinal : doc_ordinals) {
                DCHECK_LT(static_cast<size_t>(ordinal) + 1, pos_off->size());
                selected_positions += (*pos_off)[ordinal + 1] - (*pos_off)[ordinal];
            }
            frame_stats.selected_docs = doc_ordinals.size();
            frame_stats.selected_positions = selected_positions;
        }
        // Format decode is complete. Caller-level CSR invariants are validated
        // afterwards, so a later phrase error intentionally retains this work.
        context->stats->merge(frame_stats);
    }
    return Status::OK();
}

} // namespace

Status build_prx_window(std::span<const std::vector<uint32_t>> per_doc_positions,
                        int zstd_level_or_negative_for_auto, ByteSink* sink) {
    return build_prx_window(per_doc_positions, zstd_level_or_negative_for_auto,
                            kReaderPrxWindowLimits, sink);
}

Status build_prx_window(std::span<const std::vector<uint32_t>> per_doc_positions,
                        int zstd_level_or_negative_for_auto, const PrxWindowLimits& limits,
                        ByteSink* sink) {
    if (sink == nullptr) return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("prx: null sink");
    size_t total_positions = 0;
    RETURN_IF_ERROR(validate_per_doc_window(per_doc_positions, limits, &total_positions));
    // Forced legacy codecs (level 0 = raw varint, level > 0 = zstd) are kept so
    // the test/legacy paths still exercise them; the auto path (< 0) now emits
    // PFOR bit-packed deltas -- no entropy coding, far cheaper build CPU than
    // zstd-3.
    if (zstd_level_or_negative_for_auto >= 0) {
        ByteSink plain;
        RETURN_IF_ERROR(encode_payload(per_doc_positions, &plain));
        if (plain.size() > limits.max_uncomp_bytes) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: encoded payload exceeds writer window byte limit");
        }
        Slice plain_view = plain.view();
        if (!should_compress(zstd_level_or_negative_for_auto, plain_view.size())) {
            write_raw(plain_view, sink);
            return Status::OK();
        }
        return write_zstd(plain_view, zstd_level_or_negative_for_auto, sink);
    }
    // Auto mode: flatten the per-doc lists into (positions_flat, freqs) exactly as
    // the former encode_pfor_payload did, then run the shared single-encode path so
    // this builder stays byte-identical to build_prx_window_flat.
    std::vector<uint32_t> flat, freqs;
    freqs.reserve(per_doc_positions.size());
    flat.reserve(total_positions);
    for (const auto& doc : per_doc_positions) {
        freqs.push_back(static_cast<uint32_t>(doc.size()));
        flat.insert(flat.end(), doc.begin(), doc.end());
    }
    // G16-h: level < -1 is auto mode at zstd level |level| (-1 stays the default).
    const int auto_level = zstd_level_or_negative_for_auto == -1 ? kDefaultZstdLevel
                                                                 : -zstd_level_or_negative_for_auto;
    PrxWindowBuildOutcome outcome = PrxWindowBuildOutcome::kBuilt;
    RETURN_IF_ERROR(
            build_prx_window_auto_from_flat(flat, freqs, auto_level, limits, sink, &outcome));
    if (outcome == PrxWindowBuildOutcome::kNeedsSplit) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "prx: encoded payload exceeds writer window byte limit");
    }
    return Status::OK();
}

Status build_prx_window_flat(std::span<const uint32_t> positions_flat,
                             std::span<const uint32_t> freqs, int zstd_level_or_negative_for_auto,
                             ByteSink* sink) {
    return build_prx_window_flat(positions_flat, freqs, zstd_level_or_negative_for_auto,
                                 kReaderPrxWindowLimits, sink);
}

Status build_prx_window_flat(std::span<const uint32_t> positions_flat,
                             std::span<const uint32_t> freqs, int zstd_level_or_negative_for_auto,
                             const PrxWindowLimits& limits, ByteSink* sink) {
    PrxWindowBuildOutcome outcome = PrxWindowBuildOutcome::kBuilt;
    RETURN_IF_ERROR(try_build_prx_window_flat(
            positions_flat, freqs, zstd_level_or_negative_for_auto, limits, sink, &outcome));
    if (outcome == PrxWindowBuildOutcome::kNeedsSplit) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "prx: window exceeds writer limits");
    }
    return Status::OK();
}

Status try_build_prx_window_flat(std::span<const uint32_t> positions_flat,
                                 std::span<const uint32_t> freqs,
                                 int zstd_level_or_negative_for_auto, const PrxWindowLimits& limits,
                                 ByteSink* sink, PrxWindowBuildOutcome* outcome) {
    if (sink == nullptr || outcome == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("prx: null arg");
    }
    RETURN_IF_ERROR(validate_prx_window_limits(limits));
    RETURN_IF_ERROR(check_flat_partition(positions_flat, freqs));
    if (freqs.size() > limits.max_docs || positions_flat.size() > limits.max_positions) {
        RETURN_IF_ERROR(validate_flat_positions(positions_flat, freqs));
        for (uint32_t frequency : freqs) {
            if (frequency > limits.max_positions) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: one document exceeds the writer window position limit");
            }
        }
        if (freqs.size() <= 1) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "prx: one document exceeds the writer window shape limit");
        }
        RETURN_IF_ERROR(validate_single_doc_byte_limits(positions_flat, freqs,
                                                        zstd_level_or_negative_for_auto < 0,
                                                        limits.max_uncomp_bytes));
        *outcome = PrxWindowBuildOutcome::kNeedsSplit;
        return Status::OK();
    }
    if (zstd_level_or_negative_for_auto >= 0) {
        ByteSink plain;
        RETURN_IF_ERROR(encode_payload_flat(positions_flat, freqs, &plain));
        if (plain.size() > limits.max_uncomp_bytes) {
            if (freqs.size() <= 1) {
                return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "prx: one document exceeds the writer window byte limit");
            }
            RETURN_IF_ERROR(validate_single_doc_byte_limits(positions_flat, freqs, false,
                                                            limits.max_uncomp_bytes));
            *outcome = PrxWindowBuildOutcome::kNeedsSplit;
            return Status::OK();
        }
        Slice plain_view = plain.view();
        if (!should_compress(zstd_level_or_negative_for_auto, plain_view.size())) {
            write_raw(plain_view, sink);
            *outcome = PrxWindowBuildOutcome::kBuilt;
            return Status::OK();
        }
        RETURN_IF_ERROR(write_zstd(plain_view, zstd_level_or_negative_for_auto, sink));
        *outcome = PrxWindowBuildOutcome::kBuilt;
        return Status::OK();
    }
    // Auto mode: shared path with a direct singleton RAW fast path, then PFOR,
    // with raw plaintext materialized only for zstd or a tightened-limit fallback.
    // G16-h: level < -1 is auto mode at zstd level |level| (-1 stays the default).
    const int auto_level = zstd_level_or_negative_for_auto == -1 ? kDefaultZstdLevel
                                                                 : -zstd_level_or_negative_for_auto;
    return build_prx_window_auto_from_flat(positions_flat, freqs, auto_level, limits, sink,
                                           outcome);
}

Status read_prx_window(ByteSource* source, std::vector<std::vector<uint32_t>>* per_doc_positions) {
    if (source == nullptr || per_doc_positions == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("prx: null arg");
    }
    FramedPrxWindow frame;
    RETURN_IF_ERROR(read_framed(source, &frame));
    if (frame.codec == static_cast<uint8_t>(PrxCodec::kPfor)) {
        return decode_pfor_payload(frame.payload, per_doc_positions);
    }
    if (frame.codec == static_cast<uint8_t>(PrxCodec::kRaw)) {
        return decode_payload(frame.payload, per_doc_positions);
    }
    std::vector<uint8_t> plain;
    RETURN_IF_ERROR(zstd_decompress(frame.payload, frame.uncomp_len, &plain));
    return decode_payload(Slice(plain), per_doc_positions);
}

Status read_prx_window_csr(ByteSource* source, std::vector<uint32_t>* pos_flat,
                           std::vector<uint32_t>* pos_off) {
    return read_prx_window_csr_impl(source, {}, true, true, pos_flat, pos_off, nullptr);
}

Status read_prx_window_csr(ByteSource* source, std::vector<uint32_t>* pos_flat,
                           std::vector<uint32_t>* pos_off, PrxDecodeContext* context) {
    return read_prx_window_csr_impl(source, {}, true, true, pos_flat, pos_off, context);
}

Status read_prx_window_csr_for_selection(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                         std::vector<uint32_t>* pos_flat,
                                         std::vector<uint32_t>* pos_off,
                                         PrxDecodeContext* context) {
    return read_prx_window_csr_impl(source, doc_ordinals, true, false, pos_flat, pos_off, context);
}

Status read_prx_window_csr_selective(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                     std::vector<uint32_t>* pos_flat,
                                     std::vector<uint32_t>* pos_off) {
    return read_prx_window_csr_impl(source, doc_ordinals, false, false, pos_flat, pos_off, nullptr);
}

Status read_prx_window_csr_selective(ByteSource* source, std::span<const uint32_t> doc_ordinals,
                                     std::vector<uint32_t>* pos_flat,
                                     std::vector<uint32_t>* pos_off, PrxDecodeContext* context) {
    return read_prx_window_csr_impl(source, doc_ordinals, false, false, pos_flat, pos_off, context);
}

} // namespace doris::snii::format

namespace doris::snii::format::testing {
namespace {
std::atomic<uint64_t>& prx_raw_build_atomic() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}

#ifdef BE_TEST
std::atomic<uint64_t>& prx_clock_read_atomic() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}

std::atomic<uint64_t>& prx_delta_materialization_atomic() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
#endif
} // namespace

uint64_t prx_raw_build_count() {
    return prx_raw_build_atomic().load(std::memory_order_relaxed);
}

void reset_prx_raw_build_count() {
    prx_raw_build_atomic().store(0, std::memory_order_relaxed);
}

void note_prx_raw_build() {
    prx_raw_build_atomic().fetch_add(1, std::memory_order_relaxed);
}

#ifdef BE_TEST
uint64_t prx_clock_read_count() {
    return prx_clock_read_atomic().load(std::memory_order_relaxed);
}

void reset_prx_clock_read_count() {
    prx_clock_read_atomic().store(0, std::memory_order_relaxed);
}

void note_prx_clock_read() {
    prx_clock_read_atomic().fetch_add(1, std::memory_order_relaxed);
}

uint64_t prx_delta_materialization_count() {
    return prx_delta_materialization_atomic().load(std::memory_order_relaxed);
}

void reset_prx_delta_materialization_count() {
    prx_delta_materialization_atomic().store(0, std::memory_order_relaxed);
}

void note_prx_delta_materialization() {
    prx_delta_materialization_atomic().fetch_add(1, std::memory_order_relaxed);
}

uint8_t select_auto_prx_codec_for_test(size_t pfor_payload_size, size_t plain_payload_size,
                                       size_t compressed_payload_size, uint32_t max_uncomp_bytes) {
    const AutoPrxCodecChoice choice =
            select_auto_prx_codec(pfor_payload_size, plain_payload_size, compressed_payload_size,
                                  plain_payload_size >= kAutoZstdMinBytes, max_uncomp_bytes);
    DCHECK(choice.readable);
    return static_cast<uint8_t>(choice.codec);
}
#endif

} // namespace doris::snii::format::testing
