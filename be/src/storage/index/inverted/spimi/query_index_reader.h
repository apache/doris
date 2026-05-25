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
// CLucene headers trigger -Wshadow-field on g++15 strict builds
// (parameter `length` in `ArrayString4_<T>` shadows the inherited
// `ArrayBase<T>::length` member). The CLucene library itself is
// built with looser flags; suppress the warning at the include
// boundary so Doris -Werror doesn't trip on third-party code.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow-field"
#include <CLucene/StdHeader.h>
#include <CLucene/index/Terms.h>
#include <CLucene/index/Term.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/_FieldInfos.h>
#pragma GCC diagnostic pop
// clang-format on

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/term_dict_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

// `lucene::index::IndexReader` subclass exposing a SPIMI segment to
// the CLucene query engine. Composes the four previously-built
// pieces:
//   - `TermDictReader` for `.tis`/`.tii`
//   - `SpimiQueryTermEnum` for `terms()` / `terms(Term*)`
//   - `SpimiQueryTermDocs` for `termDocs()`
//   - `SpimiQueryTermPositions` for `termPositions()`
//
// Once instantiated and wrapped in `lucene::search::IndexSearcher`,
// the standard CLucene query engine drives ALL 16 Doris query types
// (TermQuery, PhraseQuery, BooleanQuery, RegexpQuery, WildcardQuery,
// …) through these interfaces. SPIMI does not need to re-implement
// query semantics — that's the whole point of subclassing
// IndexReader rather than building a Doris-native query layer.
//
// Lifetime: the byte buffers are moved into this reader and owned
// for the reader's lifetime. The internal sub-readers (term_dict,
// the CLucene FieldInfos*, norms array) live as long as the reader.
//
// Thread-safety: a `SpimiQueryIndexReader` is logically read-only
// but the sub-class objects it returns (TermEnum / TermDocs /
// TermPositions) are not thread-safe — match CLucene's standard
// convention. Callers create a new sub-reader per query thread.
//
// Write-side virtuals (`doClose`, `doSetNorm`, `doDelete`, etc.)
// throw `CL_ERR_UnsupportedOperation` — SPIMI segments are
// immutable; any caller mutating them is a programmer error.
// Marked `final` now that the override set is verified by 174 BE
// unit tests + cloud regression. If a future CLucene upgrade
// adds a pure virtual, the compile-time "abstract class marked
// final" diagnostic surfaces the gap immediately instead of
// throwing `CL_ERR_UnsupportedOperation` at query time (which
// was the exact failure mode P40 caught for `getTermInfosRAMUsed`).
class SpimiQueryIndexReader final : public lucene::index::IndexReader {
public:
    SpimiQueryIndexReader(std::vector<uint8_t> tis_bytes, std::vector<uint8_t> tii_bytes,
                            std::vector<uint8_t> frq_bytes, std::vector<uint8_t> prx_bytes,
                            std::vector<uint8_t> fnm_bytes, int32_t max_doc);

    ~SpimiQueryIndexReader() override;

    SpimiQueryIndexReader(const SpimiQueryIndexReader&) = delete;
    SpimiQueryIndexReader& operator=(const SpimiQueryIndexReader&) = delete;

    // -- read-side critical (drive the query engine) --
    int32_t numDocs() override { return _max_doc; }
    int32_t maxDoc() const override { return _max_doc; }

    lucene::index::TermEnum* terms(const void* io_ctx = nullptr) override;
    lucene::index::TermEnum* terms(const lucene::index::Term* t,
                                   const void* io_ctx = nullptr) override;
    lucene::index::TermDocs* termDocs(bool load_stats = false,
                                      const void* io_ctx = nullptr) override;
    lucene::index::TermPositions* termPositions(bool load_stats = false,
                                                const void* io_ctx = nullptr) override;

    int32_t docFreq(const lucene::index::Term* t) override;
    int32_t docNorm(const wchar_t* field, int32_t doc) override { return 1; }

    // ConjunctionQuery's ctor reads `getIndexVersion()` to pick the
    // skip-list decode path; the base class default unconditionally
    // throws CL_ERR_UnsupportedOperation, which would surface as a
    // CLuceneError on every multi-term query against V4. Return kV1
    // — matches the codec our writer emits (Lucene 2.x format with
    // FORMAT = -4 in `.tis`, kDefault block in `.frq`).
    ::IndexVersion getIndexVersion() override { return ::IndexVersion::kV1; }

    // `SpimiSearcherBuilder::build` reads this for the searcher
    // cache's per-reader memory accounting. Base class default
    // throws CL_ERR_UnsupportedOperation, which broke the entire
    // V4 read path until the end-to-end integration test caught
    // it. Return the approximate dictionary RAM footprint —
    // .tis + .tii bytes held by `_term_dict`, the dominant
    // post-construction allocation.
    int64_t getTermInfosRAMUsed() const override {
        return static_cast<int64_t>(_tis_bytes.size() + _tii_bytes.size());
    }
    std::optional<uint64_t> sumTotalTermFreq(const wchar_t* /*field*/) override {
        return std::nullopt;
    }

    bool isDeleted(int32_t /*n*/) override { return false; }
    bool hasDeletions() const override { return false; }

    // norms: V4 fulltext fields are written with `omit_norms=true`
    // (see `fulltext_writer.cpp` `EmitSegment`'s comment). CLucene's
    // `IndexReader::norms()` short-circuits to NULL when
    // `getFieldInfos()->fieldInfo(field)->omitNorms == true`, so
    // these overrides should not be called in V4 production paths.
    // The synthesizer body remains as a safety net for shadow-mode
    // / debug paths and is now mutex-guarded to defeat the
    // append-then-realloc race surfaced by the multi-agent review
    // (two concurrent first-time `norms("x")` calls on a cached
    // shared reader were racing on `_norms_cache.emplace_back`).
    uint8_t* norms(const wchar_t* field) override;
    void norms(const wchar_t* field, uint8_t* bytes) override;

    lucene::index::FieldInfos* getFieldInfos() override { return _field_infos_clucene.get(); }
    void getFieldNames(FieldOption fld_option, StringArrayWithDeletor& retarray) override;

    // -- read-side stubs (return safe defaults for SPIMI) --
    bool document(int32_t /*n*/, lucene::document::Document& /*doc*/,
                  const lucene::document::FieldSelector* /*fs*/) override {
        return false;
    }
    lucene::util::ArrayBase<lucene::index::TermFreqVector*>* getTermFreqVectors(
            int32_t /*docNumber*/) override {
        return nullptr;
    }
    lucene::index::TermFreqVector* getTermFreqVector(int32_t /*docNumber*/,
                                                     const wchar_t* /*field*/) override {
        return nullptr;
    }
    void getTermFreqVector(int32_t /*docNumber*/, const wchar_t* /*field*/,
                           lucene::index::TermVectorMapper* /*mapper*/) override {}
    void getTermFreqVector(int32_t /*docNumber*/,
                           lucene::index::TermVectorMapper* /*mapper*/) override {}

    // -- write-side virtuals: SPIMI segments are immutable --
    void doClose() override {}
    void doSetNorm(int32_t /*doc*/, const wchar_t* /*field*/, uint8_t /*value*/) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "SpimiQueryIndexReader: setNorm not supported (immutable segment)");
    }
    void doUndeleteAll() override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "SpimiQueryIndexReader: undeleteAll not supported (immutable segment)");
    }
    void doDelete(int32_t /*docNum*/) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "SpimiQueryIndexReader: delete not supported (immutable segment)");
    }
    void doCommit() override {}

    // CLucene's `NamedObject` base requires a polymorphic name —
    // used for RTTI-free downcasts in the search engine.
    const char* getObjectName() const override { return "SpimiQueryIndexReader"; }

    // Accessor for tests that want to verify the parsed field table.
    const std::vector<FieldInfoEntry>& field_infos_entries() const { return _field_infos_entries; }

private:
    // Builds the CLucene `FieldInfos*` from `_field_infos_entries`.
    // Called once at construction; the object is owned via
    // `_field_infos_clucene` and exposed via `getFieldInfos()`.
    void BuildCLuceneFieldInfos();

    // Lazily allocates and returns the all-1 norms array for the
    // given field. Cached in `_norms_cache_bytes` so repeated calls
    // return the same buffer (CLucene callers may hold the pointer).
    uint8_t* GetOrAllocateNormsForField(const wchar_t* field);

    // Bytes — owned by this reader for its lifetime.
    std::vector<uint8_t> _tis_bytes;
    std::vector<uint8_t> _tii_bytes;
    std::vector<uint8_t> _frq_bytes;
    std::vector<uint8_t> _prx_bytes;
    std::vector<uint8_t> _fnm_bytes;
    int32_t _max_doc;

    // Parsed structures.
    std::vector<FieldInfoEntry> _field_infos_entries;
    std::vector<std::wstring> _field_names_wide;
    std::unique_ptr<TermDictReader> _term_dict;

    // CLucene-owned objects. `FieldInfos` is a CL_NEW heap object;
    // we delete it in the destructor via `_CLDELETE`. A `unique_ptr`
    // with a custom deleter wraps it for RAII.
    struct CLuceneFieldInfosDeleter {
        void operator()(lucene::index::FieldInfos* p) const {
            if (p != nullptr) {
                _CLDELETE(p);
            }
        }
    };
    std::unique_ptr<lucene::index::FieldInfos, CLuceneFieldInfosDeleter> _field_infos_clucene;

    // Cached norms array per field name. CLucene callers may keep
    // the pointer past a single `norms()` call, so we own the
    // buffer for the reader's lifetime.
    //
    // `_norms_cache_mu` guards both `_norms_cache.emplace_back` (the
    // realloc race that would invalidate prior `data()` pointers) and
    // the linear-scan lookup. SpimiQueryIndexReader instances are
    // shared across query threads via the searcher cache; concurrent
    // first-time `norms("field_X")` calls would race without this.
    mutable std::mutex _norms_cache_mu;
    std::vector<std::pair<std::wstring, std::vector<uint8_t>>> _norms_cache;
};

} // namespace doris::segment_v2::inverted_index::spimi
