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
#include <vector>

#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// One term entry recovered from a sequential scan of a `.tis` file.
// `term_utf8` is the fully-reconstructed term text (after prefix
// decompression), converted back from the on-disk wide-char modified
// UTF-8 to standard UTF-8.  `info` carries absolute (non-delta)
// pointers into the companion `.frq` / `.prx` streams.
struct TermEntry {
    int32_t field_number = 0;
    std::string term_utf8;
    TermInfo info {};
};

// Forward-only sequential iterator over the `.tis` entries produced
// by `TermDictWriter`.  Parses the header, then each call to `Next()`
// decodes one entry until the footer is reached or `Done()` returns
// true.
//
// This is the sequential-scan counterpart to `TermDictReader` (which
// does point queries via the `.tii` sparse index).  TermEnum is used
// by the segment merger to walk every term in sorted order.
//
// The `.tis` bytes are borrowed by reference and must outlive the
// enum.
class TermEnum {
public:
    // `tis_bytes` must include the 8-byte footer emitted by
    // `TermDictWriter::Close()`.
    explicit TermEnum(const std::vector<uint8_t>& tis_bytes);

    TermEnum(const TermEnum&) = delete;
    TermEnum& operator=(const TermEnum&) = delete;

    // Advances to the next term.  Returns true if an entry was
    // decoded; returns false when all entries have been consumed
    // (the footer's declared count is reached).
    bool Next();

    // The most recently decoded entry.  Only valid after `Next()`
    // returned true.
    const TermEntry& Current() const { return _current; }

    // True once all entries have been consumed.
    bool Done() const { return _done; }

    // Number of entries declared in the `.tis` footer.
    int64_t TotalEntries() const { return _total_entries; }

    int32_t IndexInterval() const { return _index_interval; }
    int32_t SkipInterval() const { return _skip_interval; }

private:
    const std::vector<uint8_t>& _tis_bytes;
    size_t _pos = 0;            // current byte offset past header
    size_t _data_end = 0;       // byte offset of the footer (== size - 8)
    int32_t _index_interval = 0;
    int32_t _skip_interval = 0;
    int64_t _total_entries = 0; // from footer
    int64_t _consumed = 0;      // entries decoded so far
    bool _done = false;

    // Running prefix-decode state (inherited across entries).
    std::wstring _last_term;
    int64_t _last_freq_pointer = 0;
    int64_t _last_prox_pointer = 0;

    TermEntry _current;
};

} // namespace doris::segment_v2::inverted_index::spimi
