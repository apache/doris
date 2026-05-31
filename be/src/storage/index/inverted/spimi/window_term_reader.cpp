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

bool SpimiWindowedTermDocs::Open(const uint8_t* frq_data, size_t frq_length, int32_t doc_freq,
                                 bool has_prox) {
    // Reset all state so Open is re-callable (a fresh seek of a new term).
    _windows.clear();
    _cur_docs.clear();
    _cur_win = -1;
    _pos = -1;
    _windows_decoded = 0;
    _frq_data = frq_data;
    _frq_length = frq_length;
    _doc_freq = doc_freq;
    _has_prox = has_prox;

    if (doc_freq <= 0 || frq_length == 0U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed Open: bad doc_freq / buffer length");
    }

    ByteStream cur(frq_data, frq_length);
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

    _windows.resize(static_cast<size_t>(num_windows));
    int64_t total = 0;
    int32_t prev_doc_index = 0;
    int32_t prev_max_docid = 0; // last window's max docid (== running last_doc into next window)
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
            if (e.min_docid <= 0) [[unlikely]] {
                // First absolute doc id is >= 1 (doc 0 with delta 0 is impossible
                // because the first doc-delta == first_docid and docs are >= 1 in
                // practice; but be defensive — min_docid must be the real first doc).
                // A min_docid of 0 would mean a zero first doc-delta, impossible for
                // a non-empty term whose first delta is its absolute first doc id.
                SPIMI_THROW_CORRUPT("SPIMI .frq windowed: first window min_docid out of range");
            }
        } else {
            if (e.min_docid <= prev_max_docid) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq windowed: windows not strictly ascending");
            }
        }

        e.doc_index_start = prev_doc_index;
        e.prev_last_doc = prev_max_docid;
        // payload_pos resolved below once payload_base is known.
        e.payload_pos = static_cast<size_t>(byte_offset); // temporarily holds the RELATIVE offset

        prev_doc_index += e.doc_count;
        prev_max_docid = e.max_docid;
        total += e.doc_count;
    }
    if (total != doc_freq) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: window doc counts disagree with .tis");
    }

    // The byte right after the whole skip table is the first payload tuple; all
    // recorded win_byte_offset values are relative to it. Resolve each window's
    // ABSOLUTE payload position and bounds-check it against the buffer so a
    // crafted offset cannot drive an OOB read at DecodeWindow time.
    const size_t payload_base = cur.pos();
    for (int32_t w = 0; w < num_windows; ++w) {
        WinEntry& e = _windows[static_cast<size_t>(w)];
        const size_t rel = e.payload_pos;
        if (rel > _frq_length - payload_base) [[unlikely]] {
            // rel + payload_base > _frq_length (computed without overflow).
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: win_byte_offset past end of buffer");
        }
        e.payload_pos = payload_base + rel;
    }
    // win_byte_offset[0] must be 0 (first payload tuple sits exactly at base).
    if (_windows.front().payload_pos != payload_base) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: first win_byte_offset must be 0");
    }
    return true;
}

void SpimiWindowedTermDocs::DecodeWindow(int32_t w) {
    if (w == _cur_win) {
        return; // already cached
    }
    if (w < 0 || static_cast<size_t>(w) >= _windows.size()) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq windowed: window index out of range");
    }
    const WinEntry& e = _windows[static_cast<size_t>(w)];
    ByteStream cur(_frq_data, _frq_length);
    cur.SeekTo(e.payload_pos); // bounds-checked
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
