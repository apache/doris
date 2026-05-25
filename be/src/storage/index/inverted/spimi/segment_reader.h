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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/term_dict_reader.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

// Reader-side counterpart to `FieldInfosWriter`. Parses the bytes of
// a `.fnm` file into a list of `FieldInfoEntry` records, in field-
// number order. Field number = position in the list (CLucene's
// convention).
class FieldInfosReader {
public:
    // Returns the parsed entries. `fnm_bytes` must be the complete
    // file (no surrounding container). Crashes via DCHECK on
    // malformed bytes.
    static std::vector<FieldInfoEntry> Read(const std::vector<uint8_t>& fnm_bytes);
};

// Top-level read facade for a SPIMI segment. Composes:
//   - `FieldInfosReader` over `.fnm` bytes  (field name → number, has_prox)
//   - `TermDictReader`   over `.tis`/`.tii` (term  → freq_pointer, doc_freq)
//   - `SpimiTermDocsReader::ReadTerm` over `.frq` bytes at freq_pointer
//
// Construction takes byte buffers as input rather than file paths so
// the class is testable with `MemoryByteOutput` without touching
// the filesystem. The `SpimiFulltextIndexReader` adapter (P37c) will
// wrap a Doris `lucene::store::Directory*` and load the buffers
// using `openInput()`.
//
// The reader is read-only and thread-safe after construction (no
// mutable state). All searches go through `Search()` which is
// const.
class SpimiSegmentReader {
public:
    using DocFreq = SpimiTermDocsReader::DocFreq;

    // Constructs the reader from the four required SPIMI segment
    // file payloads. Byte buffers are moved into the reader; the
    // reader retains ownership for the duration of its lifetime
    // because `TermDictReader` and the on-demand `.frq` slicing
    // hold raw pointers into the buffers.
    //
    // `prx_bytes` is currently unused at the API level (the
    // positions stream is decoded lazily on phrase matches, which
    // P38 will wire in). It is taken here so the reader holds all
    // segment bytes from one place and tests don't have to construct
    // it incrementally.
    SpimiSegmentReader(std::vector<uint8_t> tis_bytes, std::vector<uint8_t> tii_bytes,
                       std::vector<uint8_t> frq_bytes, std::vector<uint8_t> prx_bytes,
                       std::vector<uint8_t> fnm_bytes);

    SpimiSegmentReader(const SpimiSegmentReader&) = delete;
    SpimiSegmentReader& operator=(const SpimiSegmentReader&) = delete;

    // Returns the (doc_id, freq) pairs for all documents containing
    // `term_utf8` in `field_name`. Empty vector if the field is
    // absent or the term has no postings.
    std::vector<DocFreq> Search(std::string_view field_name, std::string_view term_utf8) const;

    const std::vector<FieldInfoEntry>& FieldInfos() const { return _field_infos; }
    int32_t IndexInterval() const { return _term_dict->IndexInterval(); }
    int32_t SkipInterval() const { return _term_dict->SkipInterval(); }

private:
    // Looks up `field_name` in `_field_infos` and returns its field
    // number (= index in the list), or -1 if absent. CLucene uses
    // the field's position in the .fnm payload as its number, which
    // is exactly what the writer side relies on.
    int32_t FindFieldNumber(std::string_view field_name) const;

    std::vector<uint8_t> _tis_bytes;
    std::vector<uint8_t> _tii_bytes;
    std::vector<uint8_t> _frq_bytes;
    std::vector<uint8_t> _prx_bytes;
    std::vector<uint8_t> _fnm_bytes;
    std::vector<FieldInfoEntry> _field_infos;
    std::unique_ptr<TermDictReader> _term_dict;
};

} // namespace doris::segment_v2::inverted_index::spimi
