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

#include "storage/index/inverted/spimi/prox_reader.h"

// `_CLTHROWA` for byte-parser hard-fail on untrusted .prx bytes.
// StdHeader.h sets up the CLUCENE_EXPORT / CL_NS_DEF macros that
// debug/error.h depends on.

#include <algorithm>

#include "common/logging.h"
#include "gen_cpp/segment_v2.pb.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "util/block_compression.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

// The .prx block mode markers are the on-disk format contract owned by the
// writer; reuse its public constants (as term_docs_reader does for
// kCodeModeZstd) so a marker change can never leave reader and writer out of
// sync.
inline constexpr uint8_t kProxRaw = FreqProxEncoder::kProxRaw;
inline constexpr uint8_t kProxZstd = FreqProxEncoder::kProxZstd;
inline constexpr uint8_t kProxWindowed = FreqProxEncoder::kProxWindowed;

namespace {

// VInt decoder over the .prx bytes. Hard-fails on underflow because
// `.prx` is part of the on-disk segment and therefore attacker-
// influenceable; DCHECK is no-op in release builds and would let a
// VInt continuation-byte chain extend past `len` reading arbitrary
// heap.
int32_t ReadVInt(const uint8_t* data, size_t len, size_t* pos) {
    uint32_t v = 0;
    uint32_t shift = 0;
    while (true) {
        if (*pos >= len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx VInt underflow");
        }
        const uint8_t b = data[(*pos)++];
        v |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
        if (shift >= 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx VInt: shift overflow on crafted input");
        }
    }
    return static_cast<int32_t>(v);
}

// Inflates one V4 window payload tuple (win_mode, VInt(uncomp), optional
// VInt(comp), bytes) at *pos and appends its inner bytes to `out`. All reads
// bounds-checked against the untrusted buffer.
void AppendWindowPayload(const uint8_t* data, size_t len, size_t* pos, std::vector<uint8_t>* out) {
    if (*pos >= len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx window: truncated payload header");
    }
    const uint8_t win_mode = data[(*pos)++];
    const auto uncomp = static_cast<uint32_t>(ReadVInt(data, len, pos));
    if (win_mode == 0 /*raw*/) {
        if (*pos + uncomp > len || *pos + uncomp < *pos) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx window: raw payload exceeds block");
        }
        out->insert(out->end(), data + *pos, data + *pos + uncomp);
        *pos += uncomp;
        return;
    }
    if (win_mode == 1 /*zstd*/) {
        const auto comp = static_cast<uint32_t>(ReadVInt(data, len, pos));
        if (*pos + comp > len || *pos + comp < *pos) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx window: compressed payload exceeds block");
        }
        const size_t base = out->size();
        out->resize(base + uncomp);
        BlockCompressionCodec* codec = nullptr;
        if (!get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok() || codec == nullptr)
                [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx window: ZSTD codec unavailable");
        }
        Slice in(reinterpret_cast<const char*>(data + *pos), comp);
        Slice slice_out(reinterpret_cast<char*>(out->data() + base), uncomp);
        if (!codec->decompress(in, &slice_out).ok() || slice_out.size != uncomp) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx window: ZSTD decompress failed");
        }
        *pos += comp;
        return;
    }
    SPIMI_THROW_CORRUPT("SPIMI .prx window: unknown win_mode");
}

} // namespace

std::vector<std::vector<int32_t>> SpimiProxReader::ReadPositions(
        const uint8_t* prx_data, size_t prx_length, const std::vector<int32_t>& freqs_per_doc) {
    std::vector<std::vector<int32_t>> out;
    out.reserve(freqs_per_doc.size());
    if (freqs_per_doc.empty()) {
        return out;
    }
    // The term's prox block begins with a 1-byte mode header: raw VInt deltas,
    // or a ZSTD-compressed payload (uncompressed-len, compressed-len, bytes).
    // After resolving it, `data`/`len` point at the VInt position-delta stream.
    const uint8_t* data = prx_data;
    size_t len = prx_length;
    std::vector<uint8_t> decompressed;
    if (prx_length == 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx: empty block but freqs present");
    }
    const uint8_t mode = prx_data[0];
    if (mode == kProxZstd) {
        size_t hpos = 1;
        const auto uncomp = static_cast<uint32_t>(ReadVInt(prx_data, prx_length, &hpos));
        const auto comp = static_cast<uint32_t>(ReadVInt(prx_data, prx_length, &hpos));
        if (hpos + comp > prx_length) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx: compressed length exceeds block");
        }
        decompressed.resize(uncomp);
        BlockCompressionCodec* codec = nullptr;
        const Status cs = get_block_compression_codec(CompressionTypePB::ZSTD, &codec);
        if (!cs.ok() || codec == nullptr) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx: ZSTD codec unavailable");
        }
        Slice in(reinterpret_cast<const char*>(prx_data + hpos), comp);
        Slice slice_out(reinterpret_cast<char*>(decompressed.data()), uncomp);
        const Status ds = codec->decompress(in, &slice_out);
        if (!ds.ok() || slice_out.size != uncomp) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx: ZSTD decompress failed");
        }
        data = decompressed.data();
        len = uncomp;
    } else if (mode == kProxRaw) {
        data = prx_data + 1;
        len = prx_length - 1;
    } else if (mode == kProxWindowed) {
        // V4 windowed .prx: byte mode, VInt(W), VInt(num_windows), a per-window
        // skip table (4 VInts/window), then per-window payloads. This eager path
        // is framing-agnostic — it concatenates the inflated per-window PART_POS
        // bytes into the term's contiguous VInt position stream and slices by the
        // caller's whole-term freqs — so it only needs to STEP OVER the skip table.
        size_t hpos = 1;
        (void)ReadVInt(prx_data, prx_length, &hpos); // W
        const int32_t num_windows = ReadVInt(prx_data, prx_length, &hpos);
        if (num_windows <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx windowed: num_windows out of range");
        }
        // Skip the skip table: doc_count, byte_offset, min_docid, max_docid_delta.
        for (int32_t w = 0; w < num_windows; ++w) {
            for (int s = 0; s < 4; ++s) {
                (void)ReadVInt(prx_data, prx_length, &hpos);
            }
        }
        for (int32_t w = 0; w < num_windows; ++w) {
            AppendWindowPayload(prx_data, prx_length, &hpos, &decompressed);
        }
        data = decompressed.data();
        len = decompressed.size();
    } else [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx: unknown prox block mode");
    }
    size_t pos = 0;
    for (const int32_t freq : freqs_per_doc) {
        if (freq <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx: non-positive freq in freqs_per_doc");
        }
        std::vector<int32_t> positions;
        // Cap reserve against the same DoS-bounding limit as
        // term_docs_reader.cpp. Per-doc positions have a much
        // smaller realistic ceiling than docs-per-term, so the
        // cap is 1<<16 (~65k positions per doc).
        constexpr size_t kPosReserveCap = 1U << 16;
        positions.reserve(std::min(static_cast<size_t>(freq), kPosReserveCap));
        int32_t last = 0;
        for (int32_t i = 0; i < freq; ++i) {
            last += ReadVInt(data, len, &pos);
            positions.push_back(last);
        }
        out.push_back(std::move(positions));
    }
    return out;
}

} // namespace doris::segment_v2::inverted_index::spimi
