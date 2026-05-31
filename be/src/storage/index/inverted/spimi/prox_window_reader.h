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

#include "storage/index/inverted/spimi/posting_store.h"
#include "storage/index/inverted/spimi/window_term_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

// Window-addressed LAZY decoder for a V4 windowed `.prx` term block
// (outer mode `kProxWindowed` = 0x02).
//
// The eager `SpimiProxReader::ReadPositions` inflates EVERY `.prx` window
// of a term and decodes the whole position stream up front. For a phrase /
// proximity query that only touches the positions of a few candidate docs
// (the `.frq` lazy reader yields candidates window-by-window), that is
// almost all wasted IO + decode. This reader instead:
//
//   - At `Open`, reads ONLY the `.prx` header (mode + W + num_windows) and
//     then self-frames each window's payload tuple by reading just its
//     header (win_mode + uncomp[+comp]) — ZERO payload inflation — to build
//     a per-window absolute byte-offset table. The doc partition for each
//     window (doc_count / doc_index_start) is COPIED from the aligned `.frq`
//     lazy reader's window table, not re-derived: `.prx` window w covers
//     exactly the docs of `.frq` window w (EmitFrq / EmitPrx iterate the
//     SAME chosen_windows), so `.prx` carries no per-window skip table.
//
//   - `PositionsForDoc(p, frq_lazy)` finds the window w covering global doc
//     index p (same upper_bound the `.frq` reader uses), inflates ONLY that
//     window's payload (range-read via the `PostingStore`), and slices the
//     window's contiguous VInt position-delta stream into per-doc position
//     vectors using that window's per-doc FREQS (obtained from the `.frq`
//     lazy reader's decoded covering window). The result is CACHED keyed by
//     w, so repeated nextPosition() within a doc and next() within the same
//     window need no re-inflation.
//
// CORRECTNESS: the position vector for any doc D is byte-identical to
// `SpimiProxReader::ReadPositions(whole-term .prx, freqs)[doc_index(D)]`,
// because (a) concatenating all windows' inflated payloads reproduces the
// whole-term VInt stream (same encoder), (b) the per-doc freqs used to
// slice are the SAME freqs the encoder used as pos_counts_per_doc, and (c)
// the per-doc delta accumulator resets to 0 at each doc in both paths.
// Defence-in-depth: Open asserts `.prx` num_windows == `.frq`
// windows_total(); after slicing a window, the whole window payload must be
// consumed exactly (sum of the window's freqs VInts), else SPIMI_THROW_CORRUPT.
//
// IO: bytes are pulled through a `PostingStore` (positioned reads). At Open
// only the header + per-window payload-header probes are fetched (O(num_windows)
// small reads). Each covered window fetches exactly its self-framed byte span.
// On the real path the store is backed by a CLucene `IndexInput` (Doris
// read_at + FILE_BLOCK_CACHE = S3 range-GET). Thread-safety: one store per
// reader (cloned per query thread), never shared.
class SpimiWindowedTermPositions {
public:
    SpimiWindowedTermPositions() = default;

    SpimiWindowedTermPositions(const SpimiWindowedTermPositions&) = delete;
    SpimiWindowedTermPositions& operator=(const SpimiWindowedTermPositions&) = delete;

    // Parses the windowed `.prx` header + self-framed per-window byte-offset
    // table by pulling bounded prefixes from `prx_store` starting at absolute
    // `prox_pointer` (the term's `prox_pointer`). The doc partition is copied
    // from `frq_lazy` (which must already be Open()'d for the SAME term).
    // `prx_store` is BORROWED and must outlive this reader (owned by the
    // caller). Returns:
    //   - true  : windowed `.prx` recognised and framed; PositionsForDoc usable.
    //   - false : the `.prx` block's first byte is NOT kProxWindowed (legacy
    //             raw / whole-term ZSTD) — the caller falls back to the eager
    //             SpimiProxReader path. No state is left usable.
    // Throws `doris::Exception` (INVERTED_INDEX_FILE_CORRUPTED) on a
    // structurally invalid windowed block (incl. num_windows mismatch with
    // the `.frq` reader).
    bool Open(PostingStore* prx_store, int64_t prox_pointer, const SpimiWindowedTermDocs& frq_lazy);

    // Returns the positions for global doc index `p` (0-based within the
    // term). `frq_lazy` MUST be the same reader passed to Open and must be
    // able to decode the covering window's freqs (it decodes on demand). The
    // returned reference is valid until the next PositionsForDoc call that
    // crosses into a different window (the cache holds one window).
    const std::vector<int32_t>& PositionsForDoc(int32_t p, SpimiWindowedTermDocs& frq_lazy);

    int32_t windows_total() const { return static_cast<int32_t>(_windows.size()); }
    // Distinct window payload inflations performed so far (test telemetry).
    int32_t windows_inflated() const { return _windows_inflated; }

private:
    struct PrxWinEntry {
        int64_t payload_pos = 0;     // ABSOLUTE PostingStore offset of the payload tuple
        int64_t payload_len = 0;     // exact byte length of the payload tuple (self-framed)
        int32_t doc_count = 0;       // docs in this window (copied from .frq)
        int32_t doc_index_start = 0; // first global doc index of this window (copied from .frq)
    };

    // Inflates window `w`'s payload and slices it into per-doc position
    // vectors using that window's freqs from `frq_lazy`. Caches the result
    // in `_cache_*`. The ONLY place a `.prx` window's payload is fetched.
    void DecodeWindow(int32_t w, SpimiWindowedTermDocs& frq_lazy);

    PostingStore* _prx_store = nullptr;
    int32_t _W = 0;

    std::vector<PrxWinEntry> _windows;

    // Single-window decode cache: the window currently inflated + its per-doc
    // sliced position vectors (indexed by local doc index within the window).
    int32_t _cache_win = -1;
    std::vector<std::vector<int32_t>> _cache_positions;

    int32_t _windows_inflated = 0;
};

} // namespace doris::segment_v2::inverted_index::spimi
