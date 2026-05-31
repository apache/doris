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
// Terms.h must precede Term.h — it transitively pulls
// CLucene/util/Equators.h which defines `CL_NS(util)::Compare`
// referenced by Term.h's `Term_UnorderedCompare`. clang-format would
// sort them alphabetically and break the build.
#include <CLucene/StdHeader.h>
#include <CLucene/index/Terms.h>
#include <CLucene/index/Term.h>
// clang-format on

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/exception.h" // Exception used by DORIS_CHECK
#include "common/status.h"    // DORIS_CHECK macro
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/posting_store.h"
#include "storage/index/inverted/spimi/query_term_enum.h"
#include "storage/index/inverted/spimi/term_dict_reader.h"
#include "storage/index/inverted/spimi/term_docs_reader.h"
#include "storage/index/inverted/spimi/window_term_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

// `lucene::index::TermDocs` subclass that streams `(doc_id, freq)`
// pairs for a single term out of a SPIMI segment. The CLucene query
// engine uses TermDocs to walk the postings list of a term during
// TermQuery / BooleanQuery / scoring; matching the contract here
// gives SPIMI those query types for free.
//
// CLucene contract (`contrib/clucene/src/core/CLucene/index/Terms.h:28`):
//   - `seek(Term*)` or `seek(TermEnum*)` — positions to a term; the
//     enumeration restarts; `next()` must be called before `doc()` /
//     `freq()` are valid.
//   - `next()` — advances to next doc; false past last.
//   - `read(docs[], freqs[], len)` — batch read up to `len` entries.
//   - `skipTo(target)` — advance to first doc >= target.
//   - `close()` — release resources.
//   - `__asTermPositions()` — returns this if also a TermPositions
//     subclass, else nullptr. (P37c-2d will override in
//     `SpimiQueryTermPositions`.)
//
// Implementation strategy: at seek time, the term's full
// `(doc_id, freq)` list is decoded into an internal vector via
// `SpimiTermDocsReader::ReadTerm`. `next()` / `skipTo()` then walk
// the vector. This trades memory (O(df) per term during the seek's
// lifetime) for simplicity — matches the CLucene SegmentTermDocs
// model where `docs[PFOR_BLOCK_SIZE]` is buffered up front, but
// extended to cover the full term. P38 may switch to streaming
// once the production path is wired and benchmarks identify hot
// terms.
class SpimiQueryTermDocs : public virtual lucene::index::TermDocs {
public:
    // Resident-bytes constructor: `frq_data`/`frq_length` are borrowed (the
    // caller owns them and outlives this TermDocs). The `.frq` is wrapped in
    // an internally-owned `MemPostingStore`, so the lazy windowed reader pulls
    // its bytes through the same positioned-read seam as the real path. Used
    // by unit tests and by `SpimiQueryTermPositions` (which keeps `.frq`
    // resident this increment). `_term_dict`, `field_infos`, `field_names_wide`
    // are borrowed.
    SpimiQueryTermDocs(const TermDictReader* term_dict, const uint8_t* frq_data, size_t frq_length,
                       const std::vector<FieldInfoEntry>* field_infos,
                       const std::vector<std::wstring>* field_names_wide);

    // Positioned-read constructor: takes ownership of a `PostingStore` over the
    // whole `.frq` file (e.g. an `IndexInputPostingStore` cloned per query
    // thread from the reader's template `.frq` IndexInput). The lazy windowed
    // path then range-reads ONLY the header+skip-table prefix and each covering
    // window's bytes — a selective query transfers a fraction of the term.
    SpimiQueryTermDocs(const TermDictReader* term_dict,
                       std::unique_ptr<PostingStore> frq_store,
                       const std::vector<FieldInfoEntry>* field_infos,
                       const std::vector<std::wstring>* field_names_wide);

    ~SpimiQueryTermDocs() override;

    SpimiQueryTermDocs(const SpimiQueryTermDocs&) = delete;
    SpimiQueryTermDocs& operator=(const SpimiQueryTermDocs&) = delete;

    void seek(lucene::index::Term* term) override;
    void seek(lucene::index::TermEnum* term_enum) override;

    int32_t doc() const override;
    int32_t freq() const override;
    int32_t norm() const override { return 0; } // SPIMI omit_norms today

    bool next() override;

    int32_t read(int32_t* docs, int32_t* freqs, int32_t length) override;
    int32_t read(int32_t* docs, int32_t* freqs, int32_t* norms, int32_t length) override;
    // `DocRange` lives in the global namespace (no CL_NS_DEF wrapper
    // in `CLucene/index/DocRange.h`), unlike most CLucene types.
    bool readRange(DocRange* doc_range) override;

    bool skipTo(int32_t target) override;

    void close() override;

    lucene::index::TermPositions* __asTermPositions() override { return nullptr; }

    int32_t docFreq() override { return _doc_freq; }

protected:
    // Reset internal state to "not seeked". Called by `close()` and
    // at the start of each `seek()`. The shared seek body is in
    // `SeekByFieldAndText()`.
    void Reset();

    // Performs the byte-level seek: looks up `(field_number, text)`
    // in the term dictionary, decodes the term's posting list from
    // .frq into `_docs`. Pulled into a protected helper so
    // `SpimiQueryTermPositions` can share the doc-side state
    // setup before adding its own positions setup.
    void SeekByFieldAndText(int32_t field_number, const wchar_t* text);

    // Whether this TermDocs may use the window-addressed LAZY decoder
    // for V4 windowed `.frq` blocks. The doc/freq-only base returns
    // true (selective queries decode only the covering window(s)). The
    // position-aware subclass `SpimiQueryTermPositions` overrides this
    // to FALSE: it needs the whole eager `_docs` vector to build the
    // per-doc freq budget for `SpimiProxReader` and to index
    // `_positions[doc_index]` by `_index`, so it stays on the eager
    // path (phrase correctness untouched this increment).
    virtual bool may_use_lazy_windowed() const { return true; }

    // Accessors for the position-aware subclass.
    const TermDictReader* term_dict() const { return _term_dict; }
    const std::vector<FieldInfoEntry>* field_infos() const { return _field_infos; }
    const std::vector<std::wstring>* field_names_wide() const { return _field_names_wide; }
    int32_t current_field_number() const { return _current_field_number; }
    int32_t current_doc_index() const { return _index; }
    int32_t current_freq() const {
        // DORIS_CHECK rather than silent 0-fallback: a "current_freq"
        // call past-end (or before first next()) is a programmer
        // error in the caller, not a runtime input. Silently returning
        // 0 would mask the bug and let downstream scoring produce
        // zero-relevance results. Per CLAUDE.md "assert correctness,
        // no defensive if".
        DORIS_CHECK(_index >= 0 && _index < _doc_freq);
        return _docs[static_cast<size_t>(_index)].second;
    }
    // Returns the freq of the i-th doc in the seeked term's posting
    // list. Used by `SpimiQueryTermPositions` at seek time to
    // build the per-doc freq budget for `SpimiProxReader`. Caller
    // must ensure `0 <= i < _doc_freq`; we DORIS_CHECK rather than
    // unchecked subscript so a desync between the .tis-declared
    // `doc_freq` and the actual `.frq`-decoded `_docs.size()` (e.g.
    // crafted/corrupt segments) surfaces as a clear error instead
    // of UB heap-OOB read in release.
    int32_t freq_at(int32_t i) const {
        DORIS_CHECK(i >= 0 && static_cast<size_t>(i) < _docs.size());
        return _docs[static_cast<size_t>(i)].second;
    }
    const TermInfo* current_term_info() const {
        return _current_term_info ? &(*_current_term_info) : nullptr;
    }

private:
    // Maps a `Term*`'s field() (wide-char interned string) to the
    // field number by linear search in `_field_names_wide`. Returns
    // -1 on miss. CLucene Term fields are interned, but interning
    // pointer-equality would require the caller to use the same
    // intern table — safer to just compare strings.
    int32_t FindFieldNumberByName(const wchar_t* field) const;

    // Scratch buffers used by `readRange` to expose chunked docs/freqs
    // to the CLucene query engine. Allocated lazily on first call.
    // The DocRange protocol returns pointers to vectors owned by
    // this TermDocs; the buffers stay alive until the next
    // readRange call (or destruction), matching what
    // `TermDocsBuffer::readRange` does inside CLucene.
    std::vector<uint32_t> _range_docs;
    std::vector<uint32_t> _range_freqs;

    // Shared per-seek setup for the doc-side posting list. Given the
    // resolved `(field_number, TermInfo, has_prox)`, either opens the
    // lazy windowed reader (`_lazy == true`) or materializes the whole
    // term into `_docs` (eager). Called from both `seek` overloads so
    // the byte-level path is identical. Throws CLuceneError on corrupt
    // bytes (translated from `doris::Exception` at the boundary).
    void LoadDocsForTerm(int32_t field_number, const TermInfo& info, bool has_prox);

    const TermDictReader* _term_dict;
    // Positioned-read byte source for `.frq`, owned by this TermDocs. The lazy
    // windowed reader borrows it; the eager fallback one-shot-reads the term
    // block through it. Built from resident bytes (MemPostingStore) or a cloned
    // IndexInput (IndexInputPostingStore), depending on which ctor was used.
    std::unique_ptr<PostingStore> _frq_store;
    const std::vector<FieldInfoEntry>* _field_infos;
    const std::vector<std::wstring>* _field_names_wide;

    int32_t _current_field_number = -1;
    std::optional<TermInfo> _current_term_info;
    std::vector<SpimiTermDocsReader::DocFreq> _docs;
    int32_t _doc_freq = 0;
    int32_t _index = -1; // -1 before first next() (EAGER path cursor)

    // Lazy window-addressed decoder, active when `_lazy` is true (V4
    // windowed block AND `may_use_lazy_windowed()`). When active, the
    // doc()/freq()/next()/skipTo()/readRange() iteration routes through
    // `_lazy_reader` instead of the `_docs`/`_index` vector; `_docs`
    // stays empty. The lazy reader owns its own cursor; `_index` is
    // unused on this path.
    SpimiWindowedTermDocs _lazy_reader;
    bool _lazy = false;
};

} // namespace doris::segment_v2::inverted_index::spimi
