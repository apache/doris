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

#include "storage/index/inverted/spimi/query_term_docs.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

SpimiQueryTermDocs::SpimiQueryTermDocs(const TermDictReader* term_dict, const uint8_t* frq_data,
                                           size_t frq_length,
                                           const std::vector<FieldInfoEntry>* field_infos,
                                           const std::vector<std::wstring>* field_names_wide)
        : _term_dict(term_dict),
          _frq_data(frq_data),
          _frq_length(frq_length),
          _field_infos(field_infos),
          _field_names_wide(field_names_wide) {
    DCHECK(_term_dict != nullptr);
    DCHECK(_field_infos != nullptr);
    DCHECK(_field_names_wide != nullptr);
    DCHECK_EQ(_field_infos->size(), _field_names_wide->size());
}

SpimiQueryTermDocs::~SpimiQueryTermDocs() = default;

void SpimiQueryTermDocs::Reset() {
    _current_field_number = -1;
    _current_term_info.reset();
    _docs.clear();
    _doc_freq = 0;
    _index = -1;
}

int32_t SpimiQueryTermDocs::FindFieldNumberByName(const wchar_t* field) const {
    if (field == nullptr) {
        return -1;
    }
    const std::wstring needle(field);
    for (size_t i = 0; i < _field_names_wide->size(); ++i) {
        if ((*_field_names_wide)[i] == needle) {
            return static_cast<int32_t>(i);
        }
    }
    return -1;
}

void SpimiQueryTermDocs::SeekByFieldAndText(int32_t field_number, const wchar_t* text) {
    Reset();
    if (field_number < 0) {
        return;
    }

    // Convert wide text → UTF-8 for `TermDictReader::LookupTerm`.
    // We re-encode here rather than caching a UTF-8 form on Term*
    // because Term is a CLucene type we don't control.
    std::string text_utf8;
    if (text != nullptr) {
        for (const wchar_t* p = text; *p != L'\0'; ++p) {
            const auto code = static_cast<uint32_t>(*p);
            if (code <= 0x7FU) {
                text_utf8.push_back(static_cast<char>(code));
            } else if (code <= 0x7FFU) {
                text_utf8.push_back(static_cast<char>(0xC0U | (code >> 6)));
                text_utf8.push_back(static_cast<char>(0x80U | (code & 0x3FU)));
            } else if (code <= 0xFFFFU) {
                text_utf8.push_back(static_cast<char>(0xE0U | (code >> 12)));
                text_utf8.push_back(static_cast<char>(0x80U | ((code >> 6) & 0x3FU)));
                text_utf8.push_back(static_cast<char>(0x80U | (code & 0x3FU)));
            } else {
                text_utf8.push_back(static_cast<char>(0xF0U | (code >> 18)));
                text_utf8.push_back(static_cast<char>(0x80U | ((code >> 12) & 0x3FU)));
                text_utf8.push_back(static_cast<char>(0x80U | ((code >> 6) & 0x3FU)));
                text_utf8.push_back(static_cast<char>(0x80U | (code & 0x3FU)));
            }
        }
    }

    // Translate pure-SPIMI exceptions to CLuceneError at the
    // boundary. `TermDictReader::LookupTerm` and
    // `SpimiTermDocsReader::ReadTerm` throw `doris::Exception` (with
    // `INVERTED_INDEX_FILE_CORRUPTED`) on malformed segment bytes;
    // the CLucene query engine that ultimately drives this method
    // only catches `CLuceneError`, so we re-throw as such here.
    try {
        const auto info = _term_dict->LookupTerm(field_number, text_utf8);
        if (!info.has_value()) {
            return;
        }
        _current_field_number = field_number;
        _current_term_info = info;
        _doc_freq = info->doc_freq;

        const auto& fi = (*_field_infos)[static_cast<size_t>(field_number)];
        // Hard-bound `freq_pointer` BEFORE the pointer arithmetic.
        if (info->freq_pointer < 0 ||
            static_cast<uint64_t>(info->freq_pointer) > _frq_length) [[unlikely]] {
            _CLTHROWA(CL_ERR_IO, "SPIMI .tis freq_pointer out of .frq bounds");
        }
        const auto fp = static_cast<size_t>(info->freq_pointer);
        _docs = SpimiTermDocsReader::ReadTerm(_frq_data + fp, _frq_length - fp, info->doc_freq,
                                              fi.has_prox);
    } catch (const ::doris::Exception& e) {
        _CLTHROWA(CL_ERR_IO, e.what());
    }
}

void SpimiQueryTermDocs::seek(lucene::index::Term* term) {
    if (term == nullptr) {
        Reset();
        return;
    }
    const int32_t fn = FindFieldNumberByName(term->field());
    SeekByFieldAndText(fn, term->text());
}

void SpimiQueryTermDocs::seek(lucene::index::TermEnum* term_enum) {
    if (term_enum == nullptr) {
        Reset();
        return;
    }
    // Fast path: if it's our own SpimiQueryTermEnum, we can skip
    // the dictionary lookup and use the cached `term_info`.
    auto* spimi_enum = dynamic_cast<SpimiQueryTermEnum*>(term_enum);
    if (spimi_enum != nullptr) {
        Reset();
        const int32_t field_number = spimi_enum->current_field_number();
        if (field_number < 0) {
            return;
        }
        if (static_cast<size_t>(field_number) >= _field_infos->size()) {
            return;
        }
        const auto& info = spimi_enum->term_info();
        if (info.doc_freq <= 0) {
            return;
        }
        _current_field_number = field_number;
        _current_term_info = info;
        _doc_freq = info.doc_freq;

        const auto& fi = (*_field_infos)[static_cast<size_t>(field_number)];
        // Same untrusted-byte bounds check as in SeekByFieldAndText.
        if (info.freq_pointer < 0 ||
            static_cast<uint64_t>(info.freq_pointer) > _frq_length) [[unlikely]] {
            _CLTHROWA(CL_ERR_IO, "SPIMI .tis freq_pointer out of .frq bounds");
        }
        const auto fp = static_cast<size_t>(info.freq_pointer);
        try {
            _docs = SpimiTermDocsReader::ReadTerm(_frq_data + fp, _frq_length - fp, info.doc_freq,
                                                  fi.has_prox);
        } catch (const ::doris::Exception& e) {
            _CLTHROWA(CL_ERR_IO, e.what());
        }
        return;
    }
    // Generic path: extract the Term* from the enum and reseek.
    auto* term = term_enum->term(/*pointer=*/false);
    seek(term);
}

int32_t SpimiQueryTermDocs::doc() const {
    // CLucene contract distinguishes TWO terminal states (see
    // `SegmentTermDocs::doc:118` and `SegmentTermDocs::next:130`):
    //   - pre-start (no next()/skipTo() called yet): `_doc = -1`
    //     (initialized in `seek()` body at line 104). `doc()`
    //     returns -1 so callers comparing against valid doc ids
    //     (e.g. `PhraseQuery::do_next`'s `if (other.doc() < doc)`)
    //     see the iterator as "not yet positioned" and advance it.
    //   - exhausted (next/skipTo returned false): `_doc` set to
    //     `LUCENE_INT32_MAX_SHOULDBE` (= INT_MAX) so `doc()` after
    //     a failed advance equals the INT_MAX that advance itself
    //     returned (asserted by `PhraseQuery::do_next` line 186).
    // P41 conflated the two — returning INT_MAX for both broke the
    // `_others` advancement in 3+ token phrase queries, surfaced
    // by the cloud regression test.
    if (_index < 0) {
        return -1;
    }
    if (_index >= _doc_freq) {
        return std::numeric_limits<int32_t>::max();
    }
    return _docs[static_cast<size_t>(_index)].first;
}

int32_t SpimiQueryTermDocs::freq() const {
    DCHECK_GE(_index, 0) << "freq() called before next()";
    DCHECK_LT(_index, _doc_freq);
    return _docs[static_cast<size_t>(_index)].second;
}

bool SpimiQueryTermDocs::next() {
    if (_index + 1 >= _doc_freq) {
        // Park `_index` one-past-end so the next `doc()` call returns
        // the INT_MAX sentinel rather than the previous valid doc id.
        _index = _doc_freq;
        return false;
    }
    ++_index;
    return true;
}

int32_t SpimiQueryTermDocs::read(int32_t* docs, int32_t* freqs, int32_t length) {
    int32_t n = 0;
    while (n < length && next()) {
        docs[n] = doc();
        freqs[n] = freq();
        ++n;
    }
    return n;
}

int32_t SpimiQueryTermDocs::read(int32_t* docs, int32_t* freqs, int32_t* norms, int32_t length) {
    int32_t n = 0;
    while (n < length && next()) {
        docs[n] = doc();
        freqs[n] = freq();
        norms[n] = 0; // omit_norms
        ++n;
    }
    return n;
}

bool SpimiQueryTermDocs::readRange(DocRange* doc_range) {
    // CRITICAL: TermQuery / ConjunctionQuery / DisjunctionQuery in
    // Doris's query engine drive their result bitmap exclusively
    // through `read_range` (see `query/term_query.cpp` etc.).
    // Returning `false` here silently produced empty results for
    // every single-term / multi-term-bitmap query on V4. A correct
    // implementation streams the term's docs/freqs in chunks via
    // the DocRange "many" protocol, matching how
    // `SegmentTermDocs::readRange` wires it for the CLucene path.
    if (_index + 1 >= _doc_freq) {
        return false;
    }
    constexpr size_t kChunkSize = 512;
    _range_docs.clear();
    _range_freqs.clear();
    _range_docs.reserve(std::min(kChunkSize, static_cast<size_t>(_doc_freq - (_index + 1))));
    _range_freqs.reserve(_range_docs.capacity());
    while (_range_docs.size() < kChunkSize && next()) {
        _range_docs.push_back(static_cast<uint32_t>(doc()));
        _range_freqs.push_back(static_cast<uint32_t>(freq()));
    }
    if (_range_docs.empty()) {
        return false;
    }
    doc_range->type_ = DocRangeType::kMany;
    doc_range->doc_many = &_range_docs;
    doc_range->doc_many_size_ = static_cast<uint32_t>(_range_docs.size());
    doc_range->freq_many = &_range_freqs;
    doc_range->freq_many_size_ = static_cast<uint32_t>(_range_freqs.size());
    // Run-detect identical to `SegmentTermDocs::readRange:203` — if
    // the chunk is a contiguous doc-id run, flip type to kRange so
    // the bitmap consumer can take the fast path.
    const uint32_t start = _range_docs.front();
    const uint32_t end = _range_docs.back();
    if ((end - start) == _range_docs.size() - 1) {
        doc_range->doc_range.first = start;
        doc_range->doc_range.second = start + static_cast<uint32_t>(_range_docs.size());
        doc_range->type_ = DocRangeType::kRange;
    }
    return true;
}

bool SpimiQueryTermDocs::skipTo(int32_t target) {
    while (_index + 1 < _doc_freq) {
        ++_index;
        if (_docs[static_cast<size_t>(_index)].first >= target) {
            return true;
        }
    }
    // Exhausted — same one-past-end parking as `next()`. Without
    // this, `doc()` after a failed advance returns the last
    // doc id and PhraseQuery's `doc == _lead1.doc()` invariant
    // (with `doc = INT_MAX` from the advance returning INT_MAX)
    // fails.
    _index = _doc_freq;
    return false;
}

void SpimiQueryTermDocs::close() {
    Reset();
}

} // namespace doris::segment_v2::inverted_index::spimi
