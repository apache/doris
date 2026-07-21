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
#include <memory>
#include <utility>
#include <vector>

#include "common/logging.h" // DCHECK
#include "storage/index/inverted/spimi/posting_store.h"

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
// IO: bytes are pulled through a `PostingStore` (positioned reads). At
// `Open` only the header + skip-table prefix is fetched; each
// `DecodeWindow` fetches exactly that window's self-framed byte range.
// On the real path the `PostingStore` is backed by a CLucene `IndexInput`
// (Doris `read_at` + FILE_BLOCK_CACHE = S3 range-GET), so a selective
// `skipTo` transfers ~one window not the whole term. The decode-work
// telemetry (`windows_total()` / `windows_decoded()`) is unchanged.
//
// Positions (`.prx`) are NOT handled here; phrase queries keep the eager
// path. This reader is doc/freq-only.
class SpimiWindowedTermDocs {
public:
    SpimiWindowedTermDocs() = default;

    SpimiWindowedTermDocs(const SpimiWindowedTermDocs&) = delete;
    SpimiWindowedTermDocs& operator=(const SpimiWindowedTermDocs&) = delete;

    // Positioned-read entry point: parses the windowed `.frq` header + skip
    // table by pulling a bounded prefix from `store` starting at absolute
    // `term_base` (the term's `freq_pointer`). `store` is BORROWED and must
    // outlive this reader (the caller — `SpimiQueryTermDocs` — owns it).
    // Only the header+skip-table prefix is fetched here; each window's
    // payload is fetched lazily at `DecodeWindow`. Returns / throws exactly
    // like the resident overload below.
    bool Open(PostingStore* store, int64_t term_base, int32_t doc_freq, bool has_prox);

    // Resident-buffer convenience overload: wraps `[frq_data, frq_length)` in
    // an internally-owned `MemPostingStore` (whose logical offset 0 == the
    // term's `freq_pointer`) and delegates to the positioned-read `Open`.
    // Used by unit tests and any caller that already holds the term block
    // resident. Returns true on success; FALSE (without throwing) when the
    // outer mode byte is NOT kCodeModeSpimiWindowed, so the caller can fall
    // back to the eager `ReadTerm` path for legacy / ZSTD-wrapped blocks.
    // Throws `doris::Exception` (INVERTED_INDEX_FILE_CORRUPTED) on a
    // structurally invalid windowed block. `frq_data` is borrowed and must
    // outlive this reader.
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

    // --- Per-window doc partition accessors (used by the lazy `.prx`
    //     reader `SpimiWindowedTermPositions`, which aligns `.prx` window w
    //     1:1 with `.frq` window w). These read ONLY the skip table resolved
    //     at Open — no payload inflation. `w` must be in [0, windows_total()).
    int32_t window_doc_count(int32_t w) const {
        DCHECK(w >= 0 && static_cast<size_t>(w) < _windows.size());
        return _windows[static_cast<size_t>(w)].doc_count;
    }
    int32_t window_doc_index_start(int32_t w) const {
        DCHECK(w >= 0 && static_cast<size_t>(w) < _windows.size());
        return _windows[static_cast<size_t>(w)].doc_index_start;
    }

    // Index of the window covering global doc index `p` (upper_bound on
    // doc_index_start, then step back one). Precondition 0 <= p < doc_freq.
    // Pure metadata lookup — does NOT decode the window.
    int32_t WindowIndexForDoc(int32_t p) const;

    // Decodes (if not already cached) the window covering global doc index
    // `p` and returns its per-doc (doc_id, freq) vector. The i-th element is
    // the i-th doc of that window (i in [0, window_doc_count(w))). The `.prx`
    // reader pulls the per-doc freqs from `second`. Precondition
    // 0 <= p < doc_freq. After return, current_window_index() == the covering
    // window and current_window_docs() is this same vector.
    const std::vector<std::pair<int32_t, int32_t>>& WindowDocsForDoc(int32_t p);

    // The window currently held in the single-window decode cache (-1 none),
    // and its decoded (doc_id, freq) vector. Valid after any successful
    // next()/skipTo()/WindowDocsForDoc().
    int32_t current_window_index() const { return _cur_win; }
    const std::vector<std::pair<int32_t, int32_t>>& current_window_docs() const {
        return _cur_docs;
    }

private:
    // One skip-table entry, fully resolved at Open (no payload inflation).
    struct WinEntry {
        int32_t doc_count = 0;       // docs in this window
        int64_t payload_pos = 0;     // ABSOLUTE PostingStore offset of this window's payload tuple
        int64_t payload_len = 0;     // exact byte length of this window's payload tuple
        int32_t min_docid = 0;       // first absolute docid in the window
        int32_t max_docid = 0;       // last absolute docid in the window (== min+delta)
        int32_t doc_index_start = 0; // prefix sum of doc_count: first global index of window
        int32_t prev_last_doc = 0;   // running last_doc threaded INTO this window (max_docid[w-1])
    };

    // Inflates + PFOR/VInt-decodes window `w` into `_cur_docs`, threading
    // `prev_last_doc[w]`. The ONLY place a window's payload is fetched+touched.
    void DecodeWindow(int32_t w);

    // Self-frames a window payload tuple at absolute `payload_pos` by reading
    // only its header, returning the EXACT tuple byte length (clamped to
    // `max_len`). Used for the LAST window, whose end is the file end (an
    // over-estimate) — self-framing avoids fetching the rest of the file.
    int64_t WindowFrameLen(int64_t payload_pos, int64_t max_len) const;

    // Ensures the window covering global doc-index `_pos` is the cached
    // one; decodes it if not. Precondition: 0 <= _pos < _doc_freq.
    void EnsureWindowForPos();

    // Positioned-read byte source. `_store` is borrowed (owned by the
    // caller) for the positioned-read Open; for the resident overload it is
    // the internally-owned `_owned_store` below. `_term_base` is the absolute
    // PostingStore offset of the term's `freq_pointer` (0 for the resident
    // overload, which wraps exactly the term block).
    PostingStore* _store = nullptr;
    int64_t _term_base = 0;
    std::unique_ptr<MemPostingStore> _owned_store; // backs the resident Open overload

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
