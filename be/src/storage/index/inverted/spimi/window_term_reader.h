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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

// Window-addressed LAZY decoder for a V4 windowed `.frq` term block
// (outer mode `kCodeModeSpimiWindowed` = 0x06).
//
// The eager `SpimiTermDocsReader::ReadTerm` inflates+PFOR-decodes EVERY
// window of a term to materialize the whole posting list. For a
// selective query (a `skipTo(target)` landing on ~1% of a large term's
// docs) that is almost all wasted work. This reader instead:
//
//   - At `Open`, parses ONLY the fixed header + per-window skip table
//     (a handful of VInts per window, ZERO payload inflation). From the
//     skip table it precomputes, per window w: the absolute doc-index
//     range [doc_index_start[w], doc_index_start[w]+doc_count[w]); the
//     absolute [min_docid, max_docid] the window covers; and
//     `prev_last_doc[w]` = max_docid[w-1] (0 for w==0) = the running
//     `last_doc` threaded INTO window w. Because docs are strictly
//     ascending, max_docid[w-1] is exactly the last doc of window w-1,
//     so window w can be decoded STANDALONE (its first delta is relative
//     to prev_last_doc[w]) without decoding any predecessor.
//
//   - `next()` / `skipTo(target)` decode at most ONE new window each.
//     `skipTo` binary-searches the skip table on `max_docid` to find the
//     covering window, then decodes only that window — skipping all
//     earlier windows' inflation entirely.
//
// CORRECTNESS: the (doc_id, freq) at global doc-index p produced here is
// byte-identical to `SpimiTermDocsReader::ReadTerm(whole term)[p]` for
// every p, because both decode the SAME window bytes with the SAME
// primitives (shared `frq_window_decode_internal.h`) and thread the SAME
// running last_doc — the only difference is WHEN each window is decoded.
//
// IO: bytes stay resident (the caller already holds the whole `.frq` in
// memory). The win this increment is DECODE WORK avoided, exposed via
// `windows_total()` / `windows_decoded()`. The format carries per-window
// byte extents, so a follow-up can range-GET a single window's bytes.
//
// Positions (`.prx`) are NOT handled here; phrase queries keep the eager
// path. This reader is doc/freq-only.
class SpimiWindowedTermDocs {
public:
    SpimiWindowedTermDocs() = default;

    SpimiWindowedTermDocs(const SpimiWindowedTermDocs&) = delete;
    SpimiWindowedTermDocs& operator=(const SpimiWindowedTermDocs&) = delete;

    // Parses the windowed `.frq` header + skip table at `frq_data` (which
    // must point at the term's `freq_pointer`). Returns true on success.
    // Returns FALSE (without throwing) when the outer mode byte is NOT
    // kCodeModeSpimiWindowed, so the caller can fall back to the eager
    // `ReadTerm` path for legacy / ZSTD-wrapped blocks. Throws
    // `doris::Exception` (INVERTED_INDEX_FILE_CORRUPTED) on a structurally
    // invalid windowed block (untrusted-byte hard-fail). `frq_data` is
    // borrowed and must outlive this reader.
    bool Open(const uint8_t* frq_data, size_t frq_length, int32_t doc_freq, bool has_prox);

    // CLucene TermDocs iteration semantics, mirroring SpimiQueryTermDocs:
    //   - Before the first advance, the cursor is "pre-start"
    //     (doc_index() == -1; doc() returns -1 at the caller).
    //   - next() advances by one doc; false past the last (then parked
    //     one-past-end so doc() returns INT32_MAX at the caller).
    //   - skipTo(target) advances to the first doc >= target; false (and
    //     parked) if none.
    bool next();
    bool skipTo(int32_t target);

    // Current doc id / freq. Precondition: a successful next()/skipTo()
    // has positioned the cursor on a valid doc (0 <= doc_index < doc_freq).
    int32_t doc() const;
    int32_t freq() const;

    // Global doc index in [-1, doc_freq]: -1 pre-start, doc_freq when
    // exhausted, else the 0-based index of the current doc within the
    // materialized term. Lets the caller mirror SpimiQueryTermDocs's
    // terminal-state semantics exactly.
    int32_t doc_index() const { return _pos; }
    int32_t doc_freq() const { return _doc_freq; }

    // Decode-work telemetry (see class comment). `windows_decoded` counts
    // distinct DecodeWindow() inflations performed so far.
    int32_t windows_total() const { return static_cast<int32_t>(_windows.size()); }
    int32_t windows_decoded() const { return _windows_decoded; }

private:
    // One skip-table entry, fully resolved at Open (no payload inflation).
    struct WinEntry {
        int32_t doc_count = 0;       // docs in this window
        size_t payload_pos = 0;      // ABSOLUTE byte offset of this window's payload tuple
        int32_t min_docid = 0;       // first absolute docid in the window
        int32_t max_docid = 0;       // last absolute docid in the window (== min+delta)
        int32_t doc_index_start = 0; // prefix sum of doc_count: first global index of window
        int32_t prev_last_doc = 0;   // running last_doc threaded INTO this window (max_docid[w-1])
    };

    // Inflates + PFOR/VInt-decodes window `w` into `_cur_docs`, threading
    // `prev_last_doc[w]`. The ONLY place a window's payload is touched.
    void DecodeWindow(int32_t w);

    // Ensures the window covering global doc-index `_pos` is the cached
    // one; decodes it if not. Precondition: 0 <= _pos < _doc_freq.
    void EnsureWindowForPos();

    // Borrowed source buffer (the whole term .frq, starting at freq_pointer).
    const uint8_t* _frq_data = nullptr;
    size_t _frq_length = 0;

    int32_t _doc_freq = 0;
    bool _has_prox = false;
    uint8_t _inner_mode = 0; // kInnerPfor (0x05) or kInnerVInt (0x00)

    std::vector<WinEntry> _windows;

    // Single-window decode cache (enough for monotonic next/skipTo).
    int32_t _cur_win = -1;                              // window currently in _cur_docs (-1 none)
    std::vector<std::pair<int32_t, int32_t>> _cur_docs; // (doc_id, freq) for _cur_win

    int32_t _pos = -1; // global doc index; -1 pre-start, _doc_freq exhausted
    int32_t _windows_decoded = 0;
};

} // namespace doris::segment_v2::inverted_index::spimi
