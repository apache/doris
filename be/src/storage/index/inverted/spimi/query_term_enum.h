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
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// `lucene::index::TermEnum` subclass that iterates a SPIMI segment's
// `.tis` term dictionary in (field_number, term_text) order. Used by
// `SpimiQueryIndexReader` to satisfy the CLucene query engine's
// term-enumeration contract; the engine drives all 16 query types
// (TermQuery, PhraseQuery, BooleanQuery, RegexpQuery, …) through this
// interface, so faithfully matching the contract gives SPIMI full
// query coverage without re-implementing query semantics.
//
// CLucene contract (`contrib/clucene/src/core/CLucene/index/Terms.h:126`):
//   - `next()` advances the cursor; returns false past the last term.
//   - `term(pointer=true)` returns the current `Term*` with refcount
//     incremented (caller releases via `_CLDECDELETE`); `pointer=false`
//     returns without bumping (no ownership transfer).
//   - `docFreq() const` returns the current term's docFreq.
//   - `close()` releases the held Term and stream resources.
//   - `skipTo(target)` walks forward; base-class default implementation
//     using next() is acceptable for our O(K) term counts.
//
// Construction owns the .tis byte slice (borrowed) and the per-field
// wide-char name table (one `std::wstring` per field_number; passed in
// from `FieldInfosReader::Read`). `Term*` instances are constructed
// on demand using the field name from this table.
class SpimiQueryTermEnum final : public lucene::index::TermEnum {
public:
    SpimiQueryTermEnum(const uint8_t* tis_data, size_t tis_length, int32_t skip_interval,
                         std::vector<std::wstring> field_names_by_number);

    ~SpimiQueryTermEnum() override;

    SpimiQueryTermEnum(const SpimiQueryTermEnum&) = delete;
    SpimiQueryTermEnum& operator=(const SpimiQueryTermEnum&) = delete;

    bool next() override;
    lucene::index::Term* term(bool pointer = true) override;
    int32_t docFreq() const override;
    void close() override;

    // The current term's TermInfo (freq_pointer, prox_pointer,
    // skip_offset) — needed by `SpimiQueryTermDocs::seek(TermEnum*)`
    // so the TermDocs can position into `.frq` without re-binary-
    // searching the `.tis` index. CLucene's `SegmentTermEnum` exposes
    // a similar `getTermInfo()` for the same reason.
    const TermInfo& term_info() const { return _current_info; }
    int32_t current_field_number() const { return _current_field; }

    // NamedObject identification — keeps CLucene's RTTI-free polymorphism
    // hooks consistent with what `SegmentTermEnum` exposes.
    const char* getObjectName() const override { return "SpimiQueryTermEnum"; }

private:
    const uint8_t* _tis_data;
    // Length covers .tis bytes EXCLUDING the 8-byte tisSize footer.
    // Computed in the ctor body AFTER bounds-checking `tis_length`
    // against the header+footer minimum; do not initialise via
    // `tis_length - 8U` in the member-initialiser list because that
    // subtraction runs before any bounds check and would underflow
    // to a huge size_t on a truncated file.
    size_t _tis_length = 0;
    int32_t _skip_interval;
    std::vector<std::wstring> _field_names_by_number;

    // Walk state. `_pos` advances through `.tis`. `_current_*` holds
    // the most recently decoded entry; `_current_term_text` is the
    // wide-char form used to build `_current_term`.
    size_t _pos;
    size_t _data_start = 0; // byte offset past .tis header
    bool _exhausted = false;

    int32_t _current_field = -1;
    std::wstring _current_term_text;
    TermInfo _current_info {};

    lucene::index::Term* _current_term = nullptr;

    // Decodes the header at the front of `.tis`, sets `_pos` to the
    // first entry's byte offset. Idempotent — `Init()` is called once
    // by the constructor.
    void Init();

    // Decodes one .tis entry into `_current_*`, advances `_pos`.
    // Called from `next()`. Caller must ensure `_pos < _tis_length`.
    void DecodeOne();
};

} // namespace doris::segment_v2::inverted_index::spimi
