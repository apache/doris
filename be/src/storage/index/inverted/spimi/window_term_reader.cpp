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

#include "storage/index/inverted/spimi/window_term_reader.h"

#include <algorithm>
#include <limits>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/frq_window_decode_internal.h"

namespace doris::segment_v2::inverted_index::spimi {

using frq_internal::ByteStream;
using frq_internal::DecodePforRun;
using frq_internal::ReadWindowPayload;

namespace {
// Header + skip-table prefix probe. The header is
//   [outer mode][inner mode][VInt W][VInt num_windows]
// followed by `num_windows` entries of 4 VInts each. We do NOT know
// `num_windows` until we read it, so we fetch a bounded prefix, parse the
// header, and — if the probe is too short to hold the whole table — re-read
// sized to the exact worst case. A VInt is at most 5 bytes.
constexpr size_t kPrefixProbe = 4096;
constexpr size_t kMaxVIntBytes = 5;
constexpr size_t kMaxSkipEntryBytes = 4 * kMaxVIntBytes; // 4 VInts per entry
} // namespace

bool SpimiWindowedTermDocs::Open(PostingStore* store, int64_t term_base, int32_t doc_freq,
                                 bool has_prox) {
    // Reset all state so Open is re-callable (a fresh seek of a new term).
    _windows.clear();
    _cur_docs.clear();
    _cur_win = -1;
    _pos = -1;
    _windows_decoded = 0;
    _store = store;
    _term_base = term_base;
    _doc_freq = doc_freq;
    _has_prox = has_prox;

    if (store == nullptr || term_base < 0 || term_base > store->length()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed Open: bad store / term_base");
    }
    const int64_t avail = store->length() - term_base; // bytes from term_base to file end
    if (doc_freq <= 0 || avail == 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed Open: bad doc_freq / available length");
    }

    // --- (1) Fetch a bounded header+skip-table prefix at term_base. ---
    auto fetch_prefix = [&](size_t want) -> std::vector<uint8_t> {
        const size_t n = std::min<int64_t>(static_cast<int64_t>(want), avail);
        std::vector<uint8_t> buf(n);
        store->read_at(term_base, buf.data(), n);
        return buf;
    };
    std::vector<uint8_t> prefix = fetch_prefix(kPrefixProbe);

    ByteStream cur(prefix.data(), prefix.size());
    const uint8_t mode = cur.ReadByte();
    // Only the bare windowed outer mode is handled lazily. A whole-term ZSTD
    // envelope (kCodeModeZstd) or any legacy mode has no per-window skip table
    // we can address without fully inflating, so the caller falls back to the
    // eager whole-term ReadTerm. NOTE: the writer never wraps a windowed block
    // in whole-term ZSTD (FinishTermWindowed writes straight to the real .frq,
    // bypassing FlushFrqBlock), so this is the complete set of windowed bytes.
    if (mode != FreqProxEncoder::kCodeModeSpimiWindowed) {
        return false;
    }

    _inner_mode = cur.ReadByte();
    if (_inner_mode != FreqProxEncoder::kCodeModeSpimiPfor &&
        _inner_mode != FreqProxEncoder::kCodeModeDefault) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: unknown inner_mode");
    }
    (void)cur.ReadVInt(); // W (not needed once the skip table gives us doc ranges)
    const int32_t num_windows = cur.ReadVInt();
    if (num_windows <= 0 || num_windows > doc_freq) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: num_windows out of range");
    }

    // If the probe might not cover the whole skip table, re-read sized to the
    // exact worst case (header bytes consumed so far + num_windows * 20). All
    // parses below are bounded by `prefix.size()` via ByteStream, so a short
    // probe never OOB-reads — it just triggers this exact-size re-read.
    {
        const size_t header_consumed = cur.pos();
        const size_t worst_case = header_consumed +
                                  static_cast<size_t>(num_windows) * kMaxSkipEntryBytes;
        if (worst_case > prefix.size() && static_cast<int64_t>(prefix.size()) < avail) {
            prefix = fetch_prefix(worst_case);
            cur = ByteStream(prefix.data(), prefix.size());
            // Re-skip the header so the cursor sits at the skip table again.
            (void)cur.ReadByte();                              // outer mode
            (void)cur.ReadByte();                              // inner mode
            (void)cur.ReadVInt();                              // W
            const int32_t nw2 = cur.ReadVInt();                // num_windows (must match)
            if (nw2 != num_windows) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq windowed: num_windows changed on re-read");
            }
        }
    }

    _windows.resize(static_cast<size_t>(num_windows));
    int64_t total = 0;
    int32_t prev_doc_index = 0;
    int32_t prev_max_docid = 0; // last window's max docid (== running last_doc into next window)
    std::vector<int64_t> rel_offsets(static_cast<size_t>(num_windows));
    for (int32_t w = 0; w < num_windows; ++w) {
        WinEntry& e = _windows[static_cast<size_t>(w)];
        e.doc_count = cur.ReadVInt();
        const int32_t byte_offset = cur.ReadVInt();
        e.min_docid = cur.ReadVInt();
        const int32_t max_docid_delta = cur.ReadVInt();

        if (e.doc_count <= 0 || e.doc_count > doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: win_doc_count out of range");
        }
        if (byte_offset < 0 || max_docid_delta < 0 || e.min_docid < 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: negative skip-table field");
        }
        e.max_docid = e.min_docid + max_docid_delta;
        if (e.max_docid < e.min_docid) [[unlikely]] {
            // int32 overflow on a crafted (min_docid + delta).
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: max_docid overflow");
        }
        // Windows must be contiguous, ascending and non-overlapping so that
        //   (a) binary search on max_docid is sound, and
        //   (b) prev_max_docid is the exact running last_doc into window w,
        // which is what lets each window decode standalone.
        if (w == 0) {
            // First window: min_docid is the term's absolute first doc id. doc 0
            // is legal (real segments are 0-based) — decode seeds prev_last_doc=0
            // and reconstructs last_doc = 0 + first_delta = min_docid, which is
            // correct for min_docid==0. Only a negative value is corrupt (already
            // rejected above by `e.min_docid < 0`), so no extra guard is needed
            // here. (Previously this rejected min_docid==0 on the mistaken premise
            // that docs are always >= 1, which broke V4 windowed terms whose first
            // doc is 0.)
        } else {
            if (e.min_docid <= prev_max_docid) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq windowed: windows not strictly ascending");
            }
            // byte_offset must be strictly ascending too, so the per-window
            // length (offset[w] - offset[w-1]) is positive and bounds the span.
            if (byte_offset <= rel_offsets[static_cast<size_t>(w - 1)]) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq windowed: win_byte_offset not ascending");
            }
        }

        e.doc_index_start = prev_doc_index;
        e.prev_last_doc = prev_max_docid;
        rel_offsets[static_cast<size_t>(w)] = byte_offset;

        prev_doc_index += e.doc_count;
        prev_max_docid = e.max_docid;
        total += e.doc_count;
    }
    if (total != doc_freq) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: window doc counts disagree with .tis");
    }

    // The byte right after the whole skip table is the first payload tuple; all
    // recorded win_byte_offset values are relative to it. The payload base is
    // (term_base + bytes consumed by header+skip-table). Resolve each window's
    // ABSOLUTE PostingStore offset and exact payload length, bounds-checking
    // against the file end so a crafted offset/length cannot drive an OOB read
    // at DecodeWindow time. Window w's length is offset[w+1]-offset[w] for
    // w<last (the byte_offset delta exactly frames the tuple); the LAST window
    // runs to the file end (term blocks are written contiguously to .frq with
    // the next term's freq_pointer == this term's end, and for the final term
    // the file end). All recorded byte offsets are relative to payload_base.
    const int64_t payload_base = term_base + static_cast<int64_t>(cur.pos());
    const int64_t file_len = store->length();
    if (payload_base > file_len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: payload base past end of file");
    }
    const int64_t payload_span = file_len - payload_base; // upper bound on the whole table
    // win_byte_offset[0] must be 0 (first payload tuple sits exactly at base).
    if (rel_offsets.front() != 0) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: first win_byte_offset must be 0");
    }
    for (int32_t w = 0; w < num_windows; ++w) {
        WinEntry& e = _windows[static_cast<size_t>(w)];
        const int64_t rel = rel_offsets[static_cast<size_t>(w)];
        if (rel > payload_span) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: win_byte_offset past end of file");
        }
        e.payload_pos = payload_base + rel;
        const int64_t next_rel =
                (w + 1 < num_windows) ? rel_offsets[static_cast<size_t>(w + 1)] : payload_span;
        e.payload_len = next_rel - rel;
        if (e.payload_len <= 0 || e.payload_pos + e.payload_len > file_len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: window payload length out of bounds");
        }
    }
    return true;
}

bool SpimiWindowedTermDocs::Open(const uint8_t* frq_data, size_t frq_length, int32_t doc_freq,
                                 bool has_prox) {
    // Wrap the resident term block (whose offset 0 is the term's freq_pointer)
    // in an internally-owned MemPostingStore and delegate. The owned store
    // outlives this reader (member); the borrowed `frq_data` must outlive it
    // too (the MemPostingStore borrows it, not copies).
    _owned_store = std::make_unique<MemPostingStore>(frq_data, frq_length);
    return Open(_owned_store.get(), /*term_base=*/0, doc_freq, has_prox);
}

int64_t SpimiWindowedTermDocs::WindowFrameLen(int64_t payload_pos, int64_t max_len) const {
    // Self-frame a window payload tuple by reading only its header:
    //   [win_mode byte][VInt uncomp]            (raw, win_mode==0)
    //   [win_mode byte][VInt uncomp][VInt comp] (zstd, win_mode==1)
    // The exact tuple length = header bytes + (raw ? uncomp : comp). We read a
    // small bounded probe (header is at most 1 + 2*5 = 11 bytes), parse it, and
    // return the exact length, clamped to `max_len` (the upper bound from the
    // file end). A header probe never over-reads: ByteStream bounds every read.
    constexpr size_t kHeaderProbe = 1 + 2 * kMaxVIntBytes; // win_mode + 2 VInts
    const size_t probe_n = static_cast<size_t>(std::min<int64_t>(kHeaderProbe, max_len));
    std::vector<uint8_t> hdr(probe_n);
    _store->read_at(payload_pos, hdr.data(), hdr.size());
    ByteStream cur(hdr.data(), hdr.size());
    const uint8_t win_mode = cur.ReadByte();
    int64_t body = 0;
    if (win_mode == 0 /*raw*/) {
        body = static_cast<uint32_t>(cur.ReadVInt());
    } else if (win_mode == 1 /*zstd*/) {
        (void)cur.ReadVInt(); // uncomp
        body = static_cast<uint32_t>(cur.ReadVInt()); // comp
    } else {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: unknown win_mode in self-frame");
    }
    const int64_t exact = static_cast<int64_t>(cur.pos()) + body;
    if (exact <= 0 || exact > max_len) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: self-framed window length out of bounds");
    }
    return exact;
}

void SpimiWindowedTermDocs::DecodeWindow(int32_t w) {
    if (w == _cur_win) {
        return; // already cached
    }
    if (w < 0 || static_cast<size_t>(w) >= _windows.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: window index out of range");
    }
    const WinEntry& e = _windows[static_cast<size_t>(w)];
    // Positioned-read EXACTLY this window's framed byte range into a local
    // buffer, then run the SAME ReadWindowPayload/DecodePforRun primitives over
    // it as the eager path (byte-identical decode).
    //
    // For an INNER window the length is the exact byte_offset delta. For the
    // LAST window we do NOT know the next term's freq_pointer, so `payload_len`
    // runs to the file end — which over-covers (potentially by a lot) when this
    // is not the final term. To avoid fetching the rest of the file on the real
    // IO path, self-frame the last window: read a small header probe, decode
    // [win_mode][VInt uncomp]([VInt comp]) to compute the EXACT tuple length,
    // then fetch exactly that.
    const bool is_last = (static_cast<size_t>(w) + 1 == _windows.size());
    int64_t fetch_len = e.payload_len;
    if (is_last) {
        fetch_len = WindowFrameLen(e.payload_pos, e.payload_len);
    }
    std::vector<uint8_t> win_bytes(static_cast<size_t>(fetch_len));
    _store->read_at(e.payload_pos, win_bytes.data(), win_bytes.size());
    ByteStream cur(win_bytes.data(), win_bytes.size());
    const std::vector<uint8_t> inner = ReadWindowPayload(cur);

    _cur_docs.clear();
    _cur_docs.reserve(static_cast<size_t>(e.doc_count));
    ByteStream wcur(inner.data(), inner.size());
    int32_t last_doc = e.prev_last_doc;
    const int32_t wc = e.doc_count;
    if (_inner_mode == FreqProxEncoder::kCodeModeSpimiPfor) {
        // PART-WISE: all wc doc-deltas first (one PFOR run), then all wc freqs.
        const auto dd = DecodePforRun(wcur, wc);
        std::vector<uint32_t> fq;
        if (_has_prox) {
            fq = DecodePforRun(wcur, wc);
        }
        for (int32_t i = 0; i < wc; ++i) {
            last_doc += static_cast<int32_t>(dd[static_cast<size_t>(i)]);
            const int32_t f = _has_prox ? static_cast<int32_t>(fq[static_cast<size_t>(i)]) : 1;
            _cur_docs.emplace_back(last_doc, f);
        }
    } else { // kCodeModeDefault (VInt parts)
        for (int32_t i = 0; i < wc; ++i) {
            if (_has_prox) {
                const auto code = static_cast<uint32_t>(wcur.ReadVInt());
                last_doc += static_cast<int32_t>(code >> 1U);
                const int32_t freq = ((code & 1U) != 0) ? 1 : wcur.ReadVInt();
                _cur_docs.emplace_back(last_doc, freq);
            } else {
                last_doc += static_cast<int32_t>(wcur.ReadVInt());
                _cur_docs.emplace_back(last_doc, 1);
            }
        }
    }
    // Defence-in-depth: the window must decode to exactly its declared count,
    // and its last doc must equal the skip table's recorded max_docid. A
    // mismatch means the skip table and payload disagree (crafted segment);
    // since lazy threading relies on max_docid == last-doc-of-window, surface
    // it rather than silently corrupt a downstream window's last_doc.
    if (static_cast<int32_t>(_cur_docs.size()) != wc) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: decoded window count mismatch");
    }
    if (_cur_docs.back().first != e.max_docid) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: window last doc disagrees with skip table");
    }
    _cur_win = w;
    ++_windows_decoded;
}

int32_t SpimiWindowedTermDocs::WindowIndexForDoc(int32_t p) const {
    // Same partition lookup as EnsureWindowForPos, but pure (no decode). The
    // skip-table invariants (contiguous, ascending, partition [0,doc_freq))
    // guarantee exactly one covering window for 0 <= p < _doc_freq.
    if (p < 0 || p >= _doc_freq) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: WindowIndexForDoc out of range");
    }
    const auto it = std::upper_bound(
            _windows.begin(), _windows.end(), p,
            [](int32_t pos, const WinEntry& e) { return pos < e.doc_index_start; });
    return static_cast<int32_t>((it - _windows.begin()) - 1);
}

const std::vector<std::pair<int32_t, int32_t>>& SpimiWindowedTermDocs::WindowDocsForDoc(int32_t p) {
    const int32_t w = WindowIndexForDoc(p);
    DecodeWindow(w); // no-op if already cached
    return _cur_docs;
}

void SpimiWindowedTermDocs::EnsureWindowForPos() {
    // Find the window whose [doc_index_start, +doc_count) contains _pos.
    // upper_bound on doc_index_start, then step back one — O(log num_windows).
    // The skip-table invariants (contiguous, ascending, partition [0,doc_freq))
    // guarantee exactly one such window for any 0 <= _pos < _doc_freq.
    const auto it = std::upper_bound(
            _windows.begin(), _windows.end(), _pos,
            [](int32_t pos, const WinEntry& e) { return pos < e.doc_index_start; });
    const auto w = static_cast<int32_t>((it - _windows.begin()) - 1);
    DecodeWindow(w);
}

bool SpimiWindowedTermDocs::next() {
    if (_pos + 1 >= _doc_freq) {
        _pos = _doc_freq; // park one-past-end
        return false;
    }
    ++_pos;
    EnsureWindowForPos();
    return true;
}

bool SpimiWindowedTermDocs::skipTo(int32_t target) {
    // If already positioned on a doc >= target, stay put (CLucene contract:
    // skipTo is "advance to first doc >= target"; a no-op when already there).
    if (_pos >= 0 && _pos < _doc_freq) {
        EnsureWindowForPos();
        if (_cur_docs[static_cast<size_t>(_pos -
                                          _windows[static_cast<size_t>(_cur_win)].doc_index_start)]
                    .first >= target) {
            return true;
        }
    }

    // Binary-search the skip table for the FIRST window with max_docid >= target.
    // That window is the only one that can contain a doc >= target whose
    // predecessors are all < target; all earlier windows are entirely < target.
    const auto it = std::lower_bound(_windows.begin(), _windows.end(), target,
                                     [](const WinEntry& e, int32_t t) { return e.max_docid < t; });
    if (it == _windows.end()) {
        // target exceeds every window's max docid → exhausted.
        _pos = _doc_freq;
        return false;
    }
    const auto w = static_cast<int32_t>(it - _windows.begin());
    const WinEntry& e = _windows[static_cast<size_t>(w)];

    if (target <= e.min_docid) {
        // Whole window is >= target; answer is its first doc. Still must not
        // move BACKWARDS: if the cursor is already past this window's start we
        // keep it (handled by the early no-op check above for the common case;
        // for a fresh seek _pos == -1 so this is the first doc of window w).
        DecodeWindow(w);
        _pos = e.doc_index_start;
        return true;
    }

    // target is strictly within (min_docid, max_docid]; decode ONLY this window
    // (skipping all earlier windows' inflation — the selective-decode win) and
    // linear-scan for the first doc >= target. The lower_bound guarantees
    // max_docid >= target, so the scan always finds one → termination.
    DecodeWindow(w);
    for (int32_t i = 0; i < e.doc_count; ++i) {
        if (_cur_docs[static_cast<size_t>(i)].first >= target) {
            _pos = e.doc_index_start + i;
            return true;
        }
    }
    // Unreachable given the lower_bound invariant; hard-fail rather than
    // silently park, since reaching here means the skip table's max_docid
    // lied about the window's contents (crafted segment).
    SPIMI_THROW_CORRUPT("SPIMI .frq windowed: skipTo covering window contains no doc >= target");
}

int32_t SpimiWindowedTermDocs::doc() const {
    if (_pos < 0) {
        return -1;
    }
    if (_pos >= _doc_freq) {
        return std::numeric_limits<int32_t>::max();
    }
    // _cur_win is guaranteed to cover _pos: every successful advance calls
    // EnsureWindowForPos()/DecodeWindow() before returning.
    const WinEntry& e = _windows[static_cast<size_t>(_cur_win)];
    return _cur_docs[static_cast<size_t>(_pos - e.doc_index_start)].first;
}

int32_t SpimiWindowedTermDocs::freq() const {
    DCHECK_GE(_pos, 0) << "freq() called before next()";
    DCHECK_LT(_pos, _doc_freq);
    const WinEntry& e = _windows[static_cast<size_t>(_cur_win)];
    return _cur_docs[static_cast<size_t>(_pos - e.doc_index_start)].second;
}

} // namespace doris::segment_v2::inverted_index::spimi
