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
    const int64_t header_bytes = static_cast<int64_t>(hcur.pos());

    // --- (2) Parse the `.prx`-OWN per-window skip table (4 VInts/window):
    //         [doc_count][byte_offset][min_docid][max_docid - min_docid]. The
    //         framing is INDEPENDENT of `.frq`; the doc partition is the prefix
    //         sum of doc_count, validated against the term's .frq doc_freq. ---
    const int64_t skip_start = prox_pointer + header_bytes;
    const int64_t skip_max = std::min<int64_t>(static_cast<int64_t>(num_windows) * 4 * 5,
                                               file_len - skip_start);
    if (skip_max <= 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: skip table past end of file");
    }
    std::vector<uint8_t> skip(static_cast<size_t>(skip_max));
    prx_store->read_at(skip_start, skip.data(), skip.size());
    ByteStream scur(skip.data(), skip.size());

    _windows.resize(static_cast<size_t>(num_windows));
    std::vector<int64_t> byte_offset(static_cast<size_t>(num_windows));
    int32_t doc_acc = 0;
    for (int32_t w = 0; w < num_windows; ++w) {
        PrxWinEntry& e = _windows[static_cast<size_t>(w)];
        e.doc_count = scur.ReadVInt();
        byte_offset[static_cast<size_t>(w)] = static_cast<uint32_t>(scur.ReadVInt());
        e.min_docid = scur.ReadVInt();
        e.max_docid = e.min_docid + scur.ReadVInt();
        if (e.doc_count <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx windowed: non-positive window doc_count");
        }
        e.doc_index_start = doc_acc;
        doc_acc += e.doc_count;
    }
    if (doc_acc != frq_lazy.doc_freq()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: Sum(doc_count) != .frq doc_freq");
    }

    // --- (3) Payloads follow the skip table contiguously. Locate each window by
    //         its byte_offset (relative to the first payload); length = next
    //         offset - this; the last window self-frames by its header. ---
    const int64_t payload_base = skip_start + static_cast<int64_t>(scur.pos());
    if (payload_base > file_len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: payload base past end of file");
    }
    for (int32_t w = 0; w < num_windows; ++w) {
        PrxWinEntry& e = _windows[static_cast<size_t>(w)];
        const int64_t off = byte_offset[static_cast<size_t>(w)];
        e.payload_pos = payload_base + off;
        int64_t plen;
        if (w + 1 < num_windows) {
            plen = byte_offset[static_cast<size_t>(w + 1)] - off;
        } else {
            plen = FrameTupleLen(prx_store, e.payload_pos, file_len - e.payload_pos);
        }
        if (off < 0 || plen <= 0 || e.payload_pos < payload_base ||
            e.payload_pos + plen > file_len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx windowed: window payload span out of bounds");
        }
        e.payload_len = plen;
    }
    return true;
}

int32_t SpimiWindowedTermPositions::PrxWindowIndexForDoc(int32_t p) const {
    // upper_bound on doc_index_start, then step back one (mirror the .frq reader).
    int32_t lo = 0;
    int32_t hi = static_cast<int32_t>(_windows.size());
    while (lo < hi) {
        const int32_t mid = lo + (hi - lo) / 2;
        if (_windows[static_cast<size_t>(mid)].doc_index_start <= p) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    const int32_t w = lo - 1;
    if (w < 0 || static_cast<size_t>(w) >= _windows.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: no window covers doc index");
    }
    return w;
}

void SpimiWindowedTermPositions::DecodeWindow(int32_t w, SpimiWindowedTermDocs& frq_lazy) {
    if (w == _cache_win) {
        return; // already cached
    }
    if (w < 0 || static_cast<size_t>(w) >= _windows.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: window index out of range");
    }
    const PrxWinEntry& e = _windows[static_cast<size_t>(w)];

    // Gather this `.prx` window's per-doc freqs over its global doc RANGE
    // [doc_index_start, doc_index_start+doc_count). Because `.prx` framing is
    // decoupled, this range may span several `.frq` windows. WindowDocsForDoc(p)
    // returns the covering `.frq` window's (docid,freq) pairs and caches that
    // window, so each spanned `.frq` window decodes at most once. Exact per-doc
    // (no doc is split — both streams cut on whole 256-doc units).
    std::vector<int32_t> freqs(static_cast<size_t>(e.doc_count));
    for (int32_t i = 0; i < e.doc_count; ++i) {
        const int32_t p = e.doc_index_start + i;
        const auto& fdocs = frq_lazy.WindowDocsForDoc(p);
        const int32_t flocal = p - frq_lazy.window_doc_index_start(frq_lazy.current_window_index());
        if (flocal < 0 || static_cast<size_t>(flocal) >= fdocs.size()) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .prx windowed: doc not in covering .frq window");
        }
        freqs[static_cast<size_t>(i)] = fdocs[static_cast<size_t>(flocal)].second;
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
        const int32_t freq = freqs[static_cast<size_t>(i)];
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
    // Covering window via the `.prx`-OWN doc_index_start table (decoupled from
    // .frq). frq_lazy is consumed only as the freq oracle inside DecodeWindow.
    const int32_t w = PrxWindowIndexForDoc(p);
    DecodeWindow(w, frq_lazy);
    const PrxWinEntry& e = _windows[static_cast<size_t>(w)];
    const int32_t local = p - e.doc_index_start;
    if (local < 0 || local >= e.doc_count) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .prx windowed: doc index outside covering window");
    }
    return _cache_positions[static_cast<size_t>(local)];
}

} // namespace doris::segment_v2::inverted_index::spimi
