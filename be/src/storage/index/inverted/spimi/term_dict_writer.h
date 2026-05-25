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

#include <cstdint>
#include <string>
#include <string_view>

#include "storage/index/inverted/spimi/lucene_output.h"

namespace doris::segment_v2::inverted_index::spimi {

// Per-term posting metadata persisted into the term dictionary. Matches
// CLucene's `TermInfo`:
//   doc_freq      : number of documents containing this term
//   freq_pointer  : byte offset of the term's posting list in the .frq file
//   prox_pointer  : byte offset of the term's position list in the .prx file
//   skip_offset   : offset of the skip-list section relative to the term's
//                   freq_pointer; only written when doc_freq >= skip_interval
struct TermInfo {
    int32_t doc_freq = 0;
    int64_t freq_pointer = 0;
    int64_t prox_pointer = 0;
    int32_t skip_offset = 0;
};

// Reimplements CLucene's `STermInfosWriter<char>` against Doris-owned outputs.
// Produces a byte-identical .tis / .tii pair (Lucene format FORMAT = -4) that
// the existing CLucene reader can consume without modification.
//
// On-disk format (matches CLucene):
//
//   .tis (term dictionary, one entry per term)
//     header  : int32  FORMAT            (-4)
//               int64  -1                (legacy "size" placeholder)
//               int32  indexInterval     (default 128)
//               int32  skipInterval      (default 16)
//               int32  maxSkipLevels     (10)
//     entries : repeat for each term in (field_number, term) order
//               vint   prefix_len        (wide-char positions shared with prev term)
//               vint   suffix_len        (wide-char positions appended)
//               schars suffix            (modified UTF-8, see LuceneOutput)
//               vint   field_number
//               vint   doc_freq
//               vlong  freq_pointer_delta
//               vlong  prox_pointer_delta
//               vint   skip_offset       (only if doc_freq >= skipInterval)
//     footer  : int64  size              (number of entries written)
//
//   .tii (sparse index, every indexInterval-th entry from .tis)
//     header  : same as .tis
//     entries : same as .tis, with one extra trailing field after the
//               (optional) skip_offset:
//               vlong  tis_pointer_delta (delta to .tis byte offset of the
//                                         indexed entry's start)
//     footer  : int64  size              (.tii entries)
//               int64  tisSize           (.tis entries)
//
// The class is single-threaded; callers serialise their terms in
// (field, term) order and invoke Add() exactly once per term.
class TermDictWriter {
public:
    static constexpr int32_t kFormat = -4;
    static constexpr int32_t kDefaultIndexInterval = 128;
    // CLucene's TermInfosWriter sets skipInterval to PFOR_BLOCK_SIZE = 512
    // unconditionally in the .tii / .tis header, so we match that default
    // for byte-identity with the existing reader-and-writer pair. Callers
    // can still pass a smaller skip_interval for tighter skips at the cost
    // of differing from CLucene byte-for-byte.
    static constexpr int32_t kDefaultSkipInterval = 512;
    static constexpr int32_t kMaxSkipLevels = 10;

    // Both outputs must be empty (no header written yet). The TermDictWriter
    // does not own the outputs; the caller is responsible for their lifetime
    // and for flushing/closing the underlying files after Close() returns.
    TermDictWriter(LuceneOutput* tis_out, LuceneOutput* tii_out,
                   int32_t index_interval = kDefaultIndexInterval,
                   int32_t skip_interval = kDefaultSkipInterval);

    TermDictWriter(const TermDictWriter&) = delete;
    TermDictWriter& operator=(const TermDictWriter&) = delete;

    // Appends one term entry. Terms must be supplied in ascending order of
    // (field_number, term_utf8) — the writer asserts ordering against the
    // previous term and the previous freq/prox pointer monotonicity.
    void Add(int32_t field_number, std::string_view term_utf8, const TermInfo& info);

    // Writes the footers for .tii and .tis. After Close() the underlying
    // outputs may be flushed or rewound for inspection; no further Add() is
    // permitted.
    void Close();

    int64_t TisSize() const { return _tis_size; }
    int64_t TiiSize() const { return _tii_size; }

private:
    enum class Stream { Tis, Tii };

    void WriteHeader(LuceneOutput* out) const;

    // Writes a single entry to `out`. Updates `_last_<stream>_*` state in
    // place. For .tis entries, also adds an index entry to .tii when the
    // running .tis size is a multiple of indexInterval.
    void WriteEntry(Stream stream, int32_t field_number, const std::wstring& term_wide,
                    const TermInfo& info);

    // Writes the front-coded term portion (prefix_vint, suffix_vint,
    // suffix_schars, field_vint).
    void WriteTerm(LuceneOutput* out, const std::wstring& term_wide,
                   const std::wstring& last_term_wide, int32_t field_number);

    LuceneOutput* _tis_out;
    LuceneOutput* _tii_out;
    int32_t _index_interval;
    int32_t _skip_interval;

    int64_t _tis_size = 0;
    int64_t _tii_size = 0;
    int64_t _last_index_pointer = 0;
    bool _closed = false;

    std::wstring _last_tis_term;
    int32_t _last_tis_field = -1;
    TermInfo _last_tis_info {};

    std::wstring _last_tii_term;
    int32_t _last_tii_field = -1;
    TermInfo _last_tii_info {};
};

} // namespace doris::segment_v2::inverted_index::spimi
