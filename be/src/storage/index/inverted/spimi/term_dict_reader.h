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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// Reader for the `.tis` / `.tii` byte pair produced by `TermDictWriter`.
// Supports `LookupTerm(field, term_utf8)` returning the term's
// `TermInfo{doc_freq, freq_pointer, prox_pointer, skip_offset}` or
// `std::nullopt`.
//
// Construction parses the `.tii` sparse index entirely into memory
// (every `indexInterval`-th .tis entry, default 128 — so a million-
// term segment yields ~8K .tii entries, ~200 KiB). The `.tis` bytes
// are held by reference and scanned on demand at lookup time.
//
// This is the read-side counterpart to `TermDictWriter` and is the
// foundational layer the upcoming `SpimiSegmentReader` (P37b) uses
// to translate `(field_name, term_text)` queries into the byte
// offsets that `SpimiTermDocsReader::ReadTerm` needs.
//
// All lookups are O(log K) on the .tii index size plus O(I) linear
// scan of at most `indexInterval` .tis entries — same complexity
// class as CLucene's term-dictionary lookup.
class TermDictReader {
public:
    // Constructs a reader from `.tis` and `.tii` byte buffers as
    // produced by `TermDictWriter`. Both buffers must include the
    // footer the writer emits at `Close()` (.tis: 8-byte size,
    // .tii: 16-byte size + tisSize). The `tis_bytes` reference is
    // borrowed and must outlive the reader; the `.tii` index is
    // copied into the reader's own storage.
    TermDictReader(const std::vector<uint8_t>& tis_bytes, const std::vector<uint8_t>& tii_bytes);

    TermDictReader(const TermDictReader&) = delete;
    TermDictReader& operator=(const TermDictReader&) = delete;

    // Looks up `(field_number, term_utf8)`. Returns `nullopt` when no
    // such term exists. The input is converted to wide-char form
    // using the same `Utf8ToWide` rule the writer used, so any
    // round-trip of UTF-8 input to the dictionary survives a
    // matching read.
    std::optional<TermInfo> LookupTerm(int32_t field_number, std::string_view term_utf8) const;

    int32_t IndexInterval() const { return _index_interval; }
    int32_t SkipInterval() const { return _skip_interval; }
    int64_t TisSize() const { return _tis_size; }

private:
    // Materialised .tii entry. Term and pointers are fully decoded
    // (not deltas) so binary search is a flat compare.
    struct TiiEntry {
        int32_t field_number;
        std::wstring term_wide;
        TermInfo info;
        int64_t tis_pointer; // absolute byte offset into .tis
    };

    // Header parsing extracts indexInterval / skipInterval and
    // returns the offset past the header (where the first entry
    // begins). Asserts FORMAT == kFormat.
    static size_t DecodeHeader(const std::vector<uint8_t>& bytes, int32_t* index_interval,
                               int32_t* skip_interval);

    // Binary search `_tii_entries` for the largest entry e with
    // (e.field, e.term) <= (target_field, target_term_wide). Returns
    // the entry's index. Caller relies on the invariant that the
    // first entry is the sentinel (field=-1, term="") so the search
    // always has a starting anchor and never returns "before-begin".
    size_t LowerBoundTiiEntry(int32_t target_field, const std::wstring& target_term_wide) const;

    const std::vector<uint8_t>& _tis_bytes;
    int32_t _index_interval = 0;
    int32_t _skip_interval = 0;
    int64_t _tis_size = 0;
    size_t _tis_data_start = 0; // byte offset past .tis header
    std::vector<TiiEntry> _tii_entries;
};

} // namespace doris::segment_v2::inverted_index::spimi
