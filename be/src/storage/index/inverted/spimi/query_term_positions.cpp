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

#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/spimi/prox_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

SpimiQueryTermPositions::SpimiQueryTermPositions(const TermDictReader* term_dict,
                                                 const uint8_t* frq_data, size_t frq_length,
                                                 const uint8_t* prx_data, size_t prx_length,
                                                 const std::vector<FieldInfoEntry>* field_infos,
                                                 const std::vector<std::wstring>* field_names_wide)
        : SpimiQueryTermDocs(term_dict, frq_data, frq_length, field_infos, field_names_wide) {
    DCHECK(prx_data != nullptr || prx_length == 0);
    // Wrap resident `.prx` bytes in a MemPostingStore so the lazy windowed
    // path is exercised identically to the real positioned-read path. A null /
    // empty `.prx` (omit_tfap-only) leaves `_prx_store` null.
    if (prx_data != nullptr && prx_length > 0) {
        _prx_store = std::make_unique<MemPostingStore>(prx_data, prx_length);
    }
}

SpimiQueryTermPositions::SpimiQueryTermPositions(const TermDictReader* term_dict,
                                                 std::unique_ptr<PostingStore> frq_store,
                                                 std::unique_ptr<PostingStore> prx_store,
                                                 const std::vector<FieldInfoEntry>* field_infos,
                                                 const std::vector<std::wstring>* field_names_wide)
        : SpimiQueryTermDocs(term_dict, std::move(frq_store), field_infos, field_names_wide),
          _prx_store(std::move(prx_store)) {}

SpimiQueryTermPositions::~SpimiQueryTermPositions() = default;

void SpimiQueryTermPositions::LoadPositionsForCurrentTerm() {
    _positions.clear();
    _prx_lazy_active = false;
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
    if (_prx_store == nullptr) [[unlikely]] {
        // has_prox field but no `.prx` source — corrupt manifest / wiring.
        _CLTHROWA(CL_ERR_IO, "SPIMI: has_prox term but no .prx store");
    }

    // Same untrusted-byte bounds check as for freq_pointer in the TermDocs
    // base. `ti->prox_pointer` is a VLong sum from `.tis`; unchecked pointer
    // arithmetic on a corrupt segment is UB.
    if (ti->prox_pointer < 0 || ti->prox_pointer > _prx_store->length()) [[unlikely]] {
        _CLTHROWA(CL_ERR_IO, "SPIMI .tis prox_pointer out of .prx bounds");
    }

    // --- Fast path: windowed `.frq` (base on lazy path) + windowed `.prx`.
    //     Open the lazy `.prx` reader (header + per-window self-framing only,
    //     no inflation). Positions for each touched doc are decoded on demand,
    //     one covering window at a time. ---
    if (lazy_active()) {
        try {
            if (_prx_lazy.Open(_prx_store.get(), ti->prox_pointer, lazy_reader())) {
                _prx_lazy_active = true;
                return;
            }
        } catch (const ::doris::Exception& e) {
            _CLTHROWA(CL_ERR_IO, e.what());
        }
        // Open returned false: `.prx` is not windowed even though `.frq` is.
        // The encoder writes both windowed-or-neither, so this cannot happen
        // for a well-formed segment — surface it rather than silently produce
        // wrong positions.
        _CLTHROWA(CL_ERR_IO, "SPIMI: windowed .frq but non-windowed .prx (desynced segment)");
    }

    // --- Eager fallback: legacy / raw / ZSTD non-windowed `.prx`. The base is
    //     on the eager path here (`.frq` non-windowed), so the whole `_docs`
    //     vector is materialized and `freq_at(i)` is valid for every doc. Read
    //     the term's `.prx` slice resident (from prox_pointer to the file end —
    //     ReadPositions consumes only the bytes the freqs require) and decode
    //     it whole, byte-identically to the original eager path. ---
    std::vector<int32_t> freqs_per_doc;
    constexpr size_t kSafeReserveCap = 1U << 24;
    freqs_per_doc.reserve(std::min(static_cast<size_t>(ti->doc_freq), kSafeReserveCap));
    for (int32_t i = 0; i < ti->doc_freq; ++i) {
        const int32_t f = freq_at(i);
        DCHECK_GT(f, 0);
        freqs_per_doc.push_back(f);
    }

    const auto fp = static_cast<size_t>(ti->prox_pointer);
    const auto file_len = static_cast<size_t>(_prx_store->length());
    const size_t slice_len = file_len - fp;
    std::vector<uint8_t> prx_slice(slice_len);
    try {
        if (slice_len > 0) {
            _prx_store->read_at(static_cast<int64_t>(fp), prx_slice.data(), slice_len);
        }
        _positions =
                SpimiProxReader::ReadPositions(prx_slice.data(), prx_slice.size(), freqs_per_doc);
    } catch (const ::doris::Exception& e) {
        _CLTHROWA(CL_ERR_IO, e.what());
    }
}

const std::vector<int32_t>& SpimiQueryTermPositions::LazyPositionsForCurrentDoc() {
    const int32_t doc_index = current_doc_index();
    DCHECK_GE(doc_index, 0) << "nextPosition() before first next()";
    try {
        return _prx_lazy.PositionsForDoc(doc_index, lazy_reader());
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

    // Lazy windowed `.prx`: resolve the current doc's positions through the
    // window-addressed reader (decodes only the covering window). Eager
    // fallback: index the pre-decoded `_positions[doc_index]` vector.
    const std::vector<int32_t>& doc_positions =
            _prx_lazy_active ? LazyPositionsForCurrentDoc()
                             : _positions[static_cast<size_t>(current_doc_index())];
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
