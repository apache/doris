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

#include "storage/index/inverted/spimi/query_term_positions.h"

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/spimi/prox_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

SpimiQueryTermPositions::SpimiQueryTermPositions(const TermDictReader* term_dict,
                                                 const uint8_t* frq_data, size_t frq_length,
                                                 const uint8_t* prx_data, size_t prx_length,
                                                 const std::vector<FieldInfoEntry>* field_infos,
                                                 const std::vector<std::wstring>* field_names_wide)
        : SpimiQueryTermDocs(term_dict, frq_data, frq_length, field_infos, field_names_wide),
          _prx_data(prx_data),
          _prx_length(prx_length) {
    DCHECK(_prx_data != nullptr || _prx_length == 0);
}

SpimiQueryTermPositions::~SpimiQueryTermPositions() = default;

void SpimiQueryTermPositions::LoadPositionsForCurrentTerm() {
    _positions.clear();
    _pos_index = -1;

    const int32_t field_number = current_field_number();
    if (field_number < 0) {
        return;
    }
    const auto& fi = (*field_infos())[static_cast<size_t>(field_number)];
    if (!fi.has_prox) {
        // omit_tfap mode — no positions to load. nextPosition() is
        // still a valid call per the CLucene contract; returns 0
        // matching SegmentTermPositions behaviour at line 67-68.
        return;
    }
    const TermInfo* ti = current_term_info();
    if (ti == nullptr || ti->doc_freq <= 0) {
        return;
    }

    // Build the per-doc freq budget for `SpimiProxReader::ReadPositions`.
    // Each doc's freq tells the reader how many VInts to consume
    // before resetting the position delta accumulator.
    std::vector<int32_t> freqs_per_doc;
    // Cap pre-reserve against the same DoS-bounding limit used in
    // `term_docs_reader.cpp` to defeat corrupt-doc_freq attacks.
    constexpr size_t kSafeReserveCap = 1U << 24;
    freqs_per_doc.reserve(std::min(static_cast<size_t>(ti->doc_freq), kSafeReserveCap));
    for (int32_t i = 0; i < ti->doc_freq; ++i) {
        const int32_t f = freq_at(i);
        DCHECK_GT(f, 0);
        freqs_per_doc.push_back(f);
    }

    // Same untrusted-byte bounds check as for freq_pointer in the
    // TermDocs base. `ti->prox_pointer` is a VLong sum from `.tis`;
    // unchecked pointer arithmetic on a corrupt segment is UB.
    if (ti->prox_pointer < 0 || static_cast<uint64_t>(ti->prox_pointer) > _prx_length)
            [[unlikely]] {
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis prox_pointer out of .prx bounds");
    }
    const auto fp = static_cast<size_t>(ti->prox_pointer);
    // Translate pure-SPIMI exceptions to CLuceneError at the
    // CLucene boundary: ReadPositions throws `doris::Exception`
    // (INVERTED_INDEX_FILE_CORRUPTED) on malformed `.prx` bytes;
    // the CLucene query engine only catches CLuceneError.
    try {
        _positions =
                SpimiProxReader::ReadPositions(_prx_data + fp, _prx_length - fp, freqs_per_doc);
    } catch (const ::doris::Exception& e) {
        _CLTHROWA(CL_ERR_IO, e.what());
    }
}

void SpimiQueryTermPositions::seek(lucene::index::Term* term) {
    SpimiQueryTermDocs::seek(term);
    LoadPositionsForCurrentTerm();
}

void SpimiQueryTermPositions::seek(lucene::index::TermEnum* term_enum) {
    SpimiQueryTermDocs::seek(term_enum);
    LoadPositionsForCurrentTerm();
}

bool SpimiQueryTermPositions::next() {
    if (!SpimiQueryTermDocs::next()) {
        _pos_index = -1;
        return false;
    }
    _pos_index = 0;
    return true;
}

bool SpimiQueryTermPositions::skipTo(int32_t target) {
    // CLucene contract: `SegmentTermPositions::skipTo` (CLucene
    // `_SegmentHeader.h:286` impl) advances the doc cursor AND
    // re-positions the prox stream to the start of that doc's
    // positions — so the next `nextPosition()` returns the first
    // position of the new doc. Without this override SPIMI inherited
    // the base `SpimiQueryTermDocs::skipTo`, which moved `_index`
    // but left `_pos_index` pointing inside the previous doc's
    // positions (or at -1 if no `next()` had run). `PhraseQuery::
    // do_next` reaches matches via `_lead2.advance(doc)` without a
    // preceding `next()`, so the position cursor was never reset
    // and every multi-doc phrase silently returned no hits.
    if (!SpimiQueryTermDocs::skipTo(target)) {
        _pos_index = -1;
        return false;
    }
    _pos_index = 0;
    return true;
}

int32_t SpimiQueryTermPositions::nextPosition() {
    const int32_t field_number = current_field_number();
    if (field_number < 0) {
        return 0;
    }
    const auto& fi = (*field_infos())[static_cast<size_t>(field_number)];
    if (!fi.has_prox) {
        // The field was written with omit_tfap=true — positions
        // were not emitted. `nextPosition()` on such a field is
        // semantically illegal: a phrase/regex/prefix query reading
        // a stream of all-zero positions would silently match
        // wrong rows (e.g. every multi-token doc reports "all
        // tokens at position 0"). Refuse with `_CLTHROWA` so the
        // upper-layer query routing surfaces a clear error rather
        // than producing wrong results.
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "SpimiQueryTermPositions::nextPosition called on omit_tfap field "
                  "(no positions written); phrase queries require support_phrase=true");
    }

    const int32_t doc_index = current_doc_index();
    DCHECK_GE(doc_index, 0) << "nextPosition() before first next()";
    DCHECK_LT(static_cast<size_t>(doc_index), _positions.size());
    const auto& doc_positions = _positions[static_cast<size_t>(doc_index)];
    DCHECK_GE(_pos_index, 0);
    DCHECK_LT(_pos_index, static_cast<int32_t>(doc_positions.size()))
            << "nextPosition() called more than freq() times for current doc";
    return doc_positions[static_cast<size_t>(_pos_index++)];
}

int32_t SpimiQueryTermPositions::read(int32_t* /*docs*/, int32_t* /*freqs*/, int32_t /*length*/) {
    // Match `SegmentTermPositions::read` semantics: throw rather
    // than silently return 0. Doris's current query paths use
    // `readRange` / `next` / `nextPosition`; a future composition
    // that batches positions via this API will get a loud
    // CLuceneError instead of a silent empty result.
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "SpimiQueryTermPositions::read is unsupported; use next() + nextPosition()");
}

int32_t SpimiQueryTermPositions::read(int32_t* /*docs*/, int32_t* /*freqs*/, int32_t* /*norms*/,
                                      int32_t /*length*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              "SpimiQueryTermPositions::read is unsupported; use next() + nextPosition()");
}

} // namespace doris::segment_v2::inverted_index::spimi
