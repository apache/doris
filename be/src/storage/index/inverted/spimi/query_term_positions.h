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
#include <memory>
#include <vector>

#include "storage/index/inverted/spimi/posting_store.h"
#include "storage/index/inverted/spimi/prox_window_reader.h"
#include "storage/index/inverted/spimi/query_term_docs.h"

namespace doris::segment_v2::inverted_index::spimi {

// `lucene::index::TermPositions` subclass exposing positions
// (`nextPosition()`) on top of the (doc, freq) stream from
// `SpimiQueryTermDocs`. The CLucene query engine uses
// TermPositions for PhraseQuery, MATCH_PHRASE_PREFIX, MATCH_REGEXP,
// MATCH_PHRASE_EDGE — without this class those query types would
// be unreachable on SPIMI segments.
//
// Diamond inheritance: `TermPositions` virtually inherits from
// `TermDocs`, and `SpimiQueryTermDocs` is declared
// `public virtual TermDocs`. Multiply inheriting from both
// `SpimiQueryTermDocs` and `TermPositions` shares the same
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
class SpimiQueryTermPositions final : public SpimiQueryTermDocs,
                                      public lucene::index::TermPositions {
public:
    // Resident-bytes constructor (unit tests; `.frq` AND `.prx` each wrapped in
    // a MemPostingStore so the lazy windowed paths are exercised identically to
    // the real positioned-read path). `prx_data` is borrowed and must outlive
    // this object. A null `prx_data` (with `prx_length == 0`) is allowed for
    // omit_tfap-only fields.
    SpimiQueryTermPositions(const TermDictReader* term_dict, const uint8_t* frq_data,
                            size_t frq_length, const uint8_t* prx_data, size_t prx_length,
                            const std::vector<FieldInfoEntry>* field_infos,
                            const std::vector<std::wstring>* field_names_wide);

    // Positioned-read constructor: takes ownership of a `.frq` PostingStore AND
    // a `.prx` PostingStore (each cloned per query thread by the reader). The
    // lazy windowed `.prx` reader range-reads only the covering window(s). A
    // null `prx_store` is allowed for segments whose only fields are omit_tfap
    // (empty `.prx`); in that case positions are never requested. Used by
    // `SpimiQueryIndexReader::termPositions`.
    SpimiQueryTermPositions(const TermDictReader* term_dict,
                            std::unique_ptr<PostingStore> frq_store,
                            std::unique_ptr<PostingStore> prx_store,
                            const std::vector<FieldInfoEntry>* field_infos,
                            const std::vector<std::wstring>* field_names_wide);

    ~SpimiQueryTermPositions() override;

    SpimiQueryTermPositions(const SpimiQueryTermPositions&) = delete;
    SpimiQueryTermPositions& operator=(const SpimiQueryTermPositions&) = delete;

    // TermDocs overrides — forward to the SpimiQueryTermDocs base
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
    // the read overloads inherited from `SpimiQueryTermDocs` work
    // for (doc, freq) batch but ignore positions.
    int32_t read(int32_t* docs, int32_t* freqs, int32_t length) override;
    int32_t read(int32_t* docs, int32_t* freqs, int32_t* norms, int32_t length) override;

protected:
    // Allow the window-addressed LAZY `.frq` path. The doc cursor + per-window
    // freqs from the base's `SpimiWindowedTermDocs` feed the lazy `.prx`
    // reader, which decodes ONLY the window covering each touched doc. When the
    // `.frq` block is NOT windowed (legacy/ZSTD), the base falls back to the
    // eager `_docs` vector and so does the `.prx` side (`_prx_lazy_active=false`).
    bool may_use_lazy_windowed() const override { return true; }

private:
    // Prepares per-term position state after the base seek. If the base used
    // the lazy `.frq` path AND the `.prx` block at `prox_pointer` is
    // kProxWindowed, opens the lazy `.prx` reader (no inflation) and sets
    // `_prx_lazy_active`. Otherwise builds the eager `_positions` vector via
    // `SpimiProxReader::ReadPositions` (byte-identical fallback for legacy /
    // raw / ZSTD non-windowed `.prx`).
    void LoadPositionsForCurrentTerm();

    // The current doc's positions, lazily resolved through the windowed `.prx`
    // reader (only used when `_prx_lazy_active`).
    const std::vector<int32_t>& LazyPositionsForCurrentDoc();

    // `.prx` byte source. Owned: a per-thread clone (real path) or a
    // MemPostingStore wrapping resident bytes (tests). Null only for
    // omit_tfap-only segments where positions are never requested.
    std::unique_ptr<PostingStore> _prx_store;

    // Window-addressed lazy `.prx` reader; active for windowed `.prx` blocks.
    SpimiWindowedTermPositions _prx_lazy;
    bool _prx_lazy_active = false;

    // For an INLINE term, the term's `.prx` bytes live in the resident `.tis`
    // entry (referenced by `current_term_info()->inline_prx`). We wrap them in
    // a transient MemPostingStore (offset 0 == prox_pointer) so the lazy /
    // eager `.prx` decoders read them WITHOUT touching the external `_prx_store`
    // (zero extra GET). Re-created per term; null for non-inline terms.
    std::unique_ptr<MemPostingStore> _inline_prx_store;

    // Eager fallback: [doc_index][pos_index] for legacy / non-windowed `.prx`.
    // Empty when `_prx_lazy_active`.
    std::vector<std::vector<int32_t>> _positions;

    // Index of the next position to consume within the current doc.
    int32_t _pos_index = -1;
};

} // namespace doris::segment_v2::inverted_index::spimi
