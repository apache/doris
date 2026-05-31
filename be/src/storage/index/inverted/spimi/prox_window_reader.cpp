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

#include "storage/index/inverted/spimi/prox_window_reader.h"

#include <algorithm>

#include "common/exception.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/frq_window_decode_internal.h"

namespace doris::segment_v2::inverted_index::spimi {

using frq_internal::ByteStream;
using frq_internal::ReadWindowPayload;

namespace {

inline constexpr uint8_t kProxWindowed = FreqProxEncoder::kProxWindowed;

// Header probe: [byte mode][VInt W][VInt num_windows]. Mirrors the `.frq`
// reader's bounded-prefix approach. A VInt is at most 5 bytes.
constexpr size_t kHeaderProbe = 1 + 2 * 5;
// Per-window payload tuple header is at most [win_mode byte][VInt uncomp][VInt comp]
// = 1 + 2*5 = 11 bytes. We self-frame each tuple by reading just its header.
constexpr size_t kPayloadHeaderProbe = 1 + 2 * 5;
constexpr size_t kPosReserveCap = 1U << 16; // ~65k positions/doc DoS bound (matches prox_reader)

// Decodes one VInt from `data[*pos..len)`, accumulating per-doc deltas. Hard-
// fails on underflow (untrusted `.prx` bytes). Shared shape with
// SpimiProxReader::ReadVInt, kept local so this TU has no cross-dependency.
int32_t ReadVInt(const uint8_t* data, size_t len, size_t* pos) {
    uint32_t v = 0;
    uint32_t shift = 0;
    while (true) {
        if (*pos >= len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx window VInt underflow");
        }
        const uint8_t b = data[(*pos)++];
        v |= static_cast<uint32_t>(b & 0x7FU) << shift;
        if ((b & 0x80U) == 0) {
            break;
        }
        shift += 7;
        if (shift >= 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx window VInt: shift overflow on crafted input");
        }
    }
    return static_cast<int32_t>(v);
}

// Self-frames a `.prx` payload tuple at absolute `payload_pos`: reads just its
// header to compute the EXACT tuple byte length (header_bytes + body). `body`
// is `uncomp` for raw (win_mode 0) and `comp` for ZSTD (win_mode 1). `max_len`
// is the upper bound from the file end (clamps the read probe and the result).
int64_t FrameTupleLen(PostingStore* store, int64_t payload_pos, int64_t max_len) {
    if (max_len <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx window: payload tuple past end of file");
    }
    const size_t probe_n = static_cast<size_t>(std::min<int64_t>(kPayloadHeaderProbe, max_len));
    std::vector<uint8_t> hdr(probe_n);
    store->read_at(payload_pos, hdr.data(), hdr.size());
    ByteStream cur(hdr.data(), hdr.size());
    const uint8_t win_mode = cur.ReadByte();
    int64_t body = 0;
    if (win_mode == 0 /*raw*/) {
        body = static_cast<uint32_t>(cur.ReadVInt());
    } else if (win_mode == 1 /*zstd*/) {
        (void)cur.ReadVInt();                         // uncomp
        body = static_cast<uint32_t>(cur.ReadVInt()); // comp
    } else {
        SPIMI_THROW_CORRUPT("SPIMI .prx window: unknown win_mode in self-frame");
    }
    const int64_t exact = static_cast<int64_t>(cur.pos()) + body;
    if (exact <= 0 || exact > max_len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx window: self-framed tuple length out of bounds");
    }
    return exact;
}

} // namespace

bool SpimiWindowedTermPositions::Open(PostingStore* prx_store, int64_t prox_pointer,
                                      const SpimiWindowedTermDocs& frq_lazy) {
    // Reset so Open is re-callable on a fresh seek.
    _windows.clear();
    _cache_positions.clear();
    _cache_win = -1;
    _windows_inflated = 0;
    _W = 0;
    _prx_store = prx_store;

    if (prx_store == nullptr || prox_pointer < 0 || prox_pointer > prx_store->length())
            [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed Open: bad store / prox_pointer");
    }
    const int64_t file_len = prx_store->length();
    const int64_t avail = file_len - prox_pointer; // bytes from prox_pointer to file end
    if (avail <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed Open: prox_pointer at/past end of file");
    }

    // --- (1) Read the fixed header [mode][VInt W][VInt num_windows]. ---
    const size_t hdr_n = static_cast<size_t>(std::min<int64_t>(kHeaderProbe, avail));
    std::vector<uint8_t> hdr(hdr_n);
    prx_store->read_at(prox_pointer, hdr.data(), hdr.size());
    ByteStream hcur(hdr.data(), hdr.size());
    const uint8_t mode = hcur.ReadByte();
    if (mode != kProxWindowed) {
        // Legacy raw / whole-term ZSTD `.prx` — no per-window framing; the
        // caller falls back to the eager SpimiProxReader path.
        return false;
    }
    _W = hcur.ReadVInt();
    const int32_t num_windows = hcur.ReadVInt();
    if (num_windows <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: num_windows out of range");
    }
    // 1:1 alignment contract: `.prx` window w covers `.frq` window w. A
    // mismatch means the two streams desynced (future encoder bug or crafted
    // segment) — fail loudly rather than slice positions with wrong freqs.
    if (num_windows != frq_lazy.windows_total()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: num_windows disagrees with .frq");
    }

    // --- (2) Self-frame each window payload tuple to fill the byte-offset
    //         table. The first tuple sits at `payload_base`; each subsequent
    //         tuple immediately follows the previous (no skip table in .prx),
    //         so we walk them in order, framing each by its header only. ---
    const int64_t payload_base = prox_pointer + static_cast<int64_t>(hcur.pos());
    if (payload_base > file_len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: payload base past end of file");
    }
    _windows.resize(static_cast<size_t>(num_windows));
    int64_t cursor = payload_base;
    for (int32_t w = 0; w < num_windows; ++w) {
        PrxWinEntry& e = _windows[static_cast<size_t>(w)];
        const int64_t remain = file_len - cursor;
        const int64_t tuple_len = FrameTupleLen(prx_store, cursor, remain);
        e.payload_pos = cursor;
        e.payload_len = tuple_len;
        // Doc partition copied from the aligned `.frq` window (NOT re-derived).
        e.doc_count = frq_lazy.window_doc_count(w);
        e.doc_index_start = frq_lazy.window_doc_index_start(w);
        if (e.doc_count <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx windowed: non-positive .frq window doc_count");
        }
        cursor += tuple_len;
        if (cursor > file_len || cursor < e.payload_pos) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx windowed: window payload past end of file");
        }
    }
    return true;
}

void SpimiWindowedTermPositions::DecodeWindow(int32_t w, SpimiWindowedTermDocs& frq_lazy) {
    if (w == _cache_win) {
        return; // already cached
    }
    if (w < 0 || static_cast<size_t>(w) >= _windows.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: window index out of range");
    }
    const PrxWinEntry& e = _windows[static_cast<size_t>(w)];

    // Pull the per-doc freqs of this window from the `.frq` lazy reader. It
    // decodes the covering `.frq` window on demand; for phrase queries the
    // doc cursor already advanced through it, so this is usually a cache hit.
    const auto& frq_docs = frq_lazy.WindowDocsForDoc(e.doc_index_start);
    if (static_cast<int32_t>(frq_docs.size()) != e.doc_count) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: .frq window doc_count desync");
    }

    // Range-read EXACTLY this window's framed payload, then inflate it with the
    // SAME ReadWindowPayload primitive the eager path uses (byte-identical).
    std::vector<uint8_t> win_bytes(static_cast<size_t>(e.payload_len));
    _prx_store->read_at(e.payload_pos, win_bytes.data(), win_bytes.size());
    ByteStream wcur(win_bytes.data(), win_bytes.size());
    const std::vector<uint8_t> inner = ReadWindowPayload(wcur);

    // Slice the window's contiguous VInt position-delta stream into per-doc
    // vectors, exactly as SpimiProxReader::ReadPositions does — but confined to
    // this window's docs. The per-doc delta accumulator resets to 0 per doc.
    _cache_positions.clear();
    _cache_positions.resize(static_cast<size_t>(e.doc_count));
    const uint8_t* data = inner.data();
    const size_t len = inner.size();
    size_t pos = 0;
    for (int32_t i = 0; i < e.doc_count; ++i) {
        const int32_t freq = frq_docs[static_cast<size_t>(i)].second;
        if (freq <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx windowed: non-positive freq slicing positions");
        }
        std::vector<int32_t>& positions = _cache_positions[static_cast<size_t>(i)];
        positions.reserve(std::min(static_cast<size_t>(freq), kPosReserveCap));
        int32_t last = 0;
        for (int32_t k = 0; k < freq; ++k) {
            last += ReadVInt(data, len, &pos);
            positions.push_back(last);
        }
    }
    // Defence-in-depth: the window payload must be consumed EXACTLY (sum of
    // this window's freqs VInts). Trailing bytes mean the `.frq` freqs and the
    // `.prx` payload disagree (crafted/desynced segment) — fail loudly rather
    // than return silently-wrong positions for a later doc.
    if (pos != len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: window payload not fully consumed");
    }
    _cache_win = w;
    ++_windows_inflated;
}

const std::vector<int32_t>& SpimiWindowedTermPositions::PositionsForDoc(
        int32_t p, SpimiWindowedTermDocs& frq_lazy) {
    if (p < 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: PositionsForDoc negative doc index");
    }
    // Covering window = same index the `.frq` reader uses (metadata-only lookup).
    const int32_t w = frq_lazy.WindowIndexForDoc(p);
    DecodeWindow(w, frq_lazy);
    const PrxWinEntry& e = _windows[static_cast<size_t>(w)];
    const int32_t local = p - e.doc_index_start;
    if (local < 0 || local >= e.doc_count) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: doc index outside covering window");
    }
    return _cache_positions[static_cast<size_t>(local)];
}

} // namespace doris::segment_v2::inverted_index::spimi
