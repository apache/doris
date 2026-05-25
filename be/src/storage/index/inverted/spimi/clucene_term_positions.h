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

// clang-format off
// Terms.h before Term.h — see clucene_term_docs.h for the
// `CL_NS(util)::Compare` dependency that breaks alphabetical sort.
#include <CLucene/StdHeader.h>
#include <CLucene/index/Terms.h>
#include <CLucene/index/Term.h>
// clang-format on

#include <cstdint>
#include <vector>

#include "storage/index/inverted/spimi/clucene_term_docs.h"

namespace doris::segment_v2::inverted_index::spimi {

// `lucene::index::TermPositions` subclass exposing positions
// (`nextPosition()`) on top of the (doc, freq) stream from
// `SpimiCLuceneTermDocs`. The CLucene query engine uses
// TermPositions for PhraseQuery, MATCH_PHRASE_PREFIX, MATCH_REGEXP,
// MATCH_PHRASE_EDGE — without this class those query types would
// be unreachable on SPIMI segments.
//
// Diamond inheritance: `TermPositions` virtually inherits from
// `TermDocs`, and `SpimiCLuceneTermDocs` is declared
// `public virtual TermDocs`. Multiply inheriting from both
// `SpimiCLuceneTermDocs` and `TermPositions` shares the same
// `TermDocs` subobject — the standard CLucene pattern (see
// `SegmentTermPositions : public SegmentTermDocs, public
// TermPositions` in `_SegmentHeader.h:286`).
//
// Implementation: at seek time, the term's full position list is
// decoded into `_positions[doc_index][pos_index]` via
// `SpimiProxReader::ReadPositions`. `nextPosition()` walks within
// the current doc's vector; `next()` advances the parent doc cursor
// and resets the per-doc position index. Memory is O(total
// positions) for the active seek — same simplicity / memory
// trade-off as the TermDocs side.
//
// Payloads: not supported. SPIMI does not write payloads, and the
// production fulltext path does not consume them. The payload-API
// virtuals return zero / nullptr / false.
class SpimiCLuceneTermPositions final : public SpimiCLuceneTermDocs,
                                        public lucene::index::TermPositions {
public:
    SpimiCLuceneTermPositions(const TermDictReader* term_dict, const uint8_t* frq_data,
                              size_t frq_length, const uint8_t* prx_data, size_t prx_length,
                              const std::vector<FieldInfoEntry>* field_infos,
                              const std::vector<std::wstring>* field_names_wide);

    ~SpimiCLuceneTermPositions() override;

    SpimiCLuceneTermPositions(const SpimiCLuceneTermPositions&) = delete;
    SpimiCLuceneTermPositions& operator=(const SpimiCLuceneTermPositions&) = delete;

    // TermDocs overrides — forward to the SpimiCLuceneTermDocs base
    // and refresh per-term position state.
    void seek(lucene::index::Term* term) override;
    void seek(lucene::index::TermEnum* term_enum) override;
    bool next() override;
    bool skipTo(int32_t target) override;

    // TermPositions overrides.
    int32_t nextPosition() override;
    int32_t getPayloadLength() const override { return 0; }
    uint8_t* getPayload(uint8_t* /*data*/) override { return nullptr; }
    bool isPayloadAvailable() const override { return false; }

    // Diamond-resolution helpers (`__asTermDocs` / `__asTermPositions`).
    lucene::index::TermDocs* __asTermDocs() override { return this; }
    lucene::index::TermPositions* __asTermPositions() override { return this; }

    // TermPositions::read() is intentionally not a batch API for
    // (doc, freq) — CLucene's `SegmentTermPositions::read` throws
    // UnsupportedOperationException. We follow the same convention:
    // the read overloads inherited from `SpimiCLuceneTermDocs` work
    // for (doc, freq) batch but ignore positions.
    int32_t read(int32_t* docs, int32_t* freqs, int32_t length) override;
    int32_t read(int32_t* docs, int32_t* freqs, int32_t* norms, int32_t length) override;

private:
    // Re-decodes positions for the currently seeked term out of
    // `_prx_data` using `SpimiProxReader::ReadPositions`. Called
    // from `seek()` after the parent has populated the docs vector.
    void LoadPositionsForCurrentTerm();

    const uint8_t* _prx_data;
    size_t _prx_length;

    // [doc_index][pos_index] — positions for each doc in the term's
    // postings list, in the order the docs appear in
    // `SpimiCLuceneTermDocs::_docs`.
    std::vector<std::vector<int32_t>> _positions;

    // Index of the next position to consume within the current doc.
    int32_t _pos_index = -1;
};

} // namespace doris::segment_v2::inverted_index::spimi
