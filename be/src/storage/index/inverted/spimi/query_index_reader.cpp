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

#include "storage/index/inverted/spimi/query_index_reader.h"

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/query_term_docs.h"
#include "storage/index/inverted/spimi/query_term_enum.h"
#include "storage/index/inverted/spimi/query_term_positions.h"
#include "storage/index/inverted/spimi/segment_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

SpimiQueryIndexReader::SpimiQueryIndexReader(std::vector<uint8_t> tis_bytes,
                                             std::vector<uint8_t> tii_bytes,
                                             std::vector<uint8_t> frq_bytes,
                                             std::vector<uint8_t> prx_bytes,
                                             std::vector<uint8_t> fnm_bytes, int32_t max_doc)
        : _tis_bytes(std::move(tis_bytes)),
          _tii_bytes(std::move(tii_bytes)),
          _frq_bytes(std::move(frq_bytes)),
          _prx_bytes(std::move(prx_bytes)),
          _fnm_bytes(std::move(fnm_bytes)),
          _max_doc(max_doc) {
    DCHECK_GE(_max_doc, 0);
    _field_infos_entries = FieldInfosReader::Read(_fnm_bytes);
    _field_names_wide.reserve(_field_infos_entries.size());
    for (const auto& fi : _field_infos_entries) {
        _field_names_wide.push_back(Utf8ToWide(fi.name));
    }
    _term_dict = std::make_unique<TermDictReader>(_tis_bytes, _tii_bytes);
    BuildCLuceneFieldInfos();
}

SpimiQueryIndexReader::~SpimiQueryIndexReader() = default;

void SpimiQueryIndexReader::BuildCLuceneFieldInfos() {
    // Take ownership of the FieldInfos* BEFORE the populating loop
    // so a CLuceneError or bad_alloc inside `fi->add()` still gets
    // cleaned up by the unique_ptr's destructor. Previously the
    // raw pointer leaked on any throw between `_CLNEW` and
    // `reset(fi)`, and the catch site in `SpimiSearcherBuilder::
    // build` could observe that leak on malformed `.fnm`.
    _field_infos_clucene.reset(_CLNEW lucene::index::FieldInfos());
    auto* fi = _field_infos_clucene.get();
    for (const auto& entry : _field_infos_entries) {
        const std::wstring name_wide = Utf8ToWide(entry.name);
        // `FieldInfos::add(name, isIndexed, storeTermVector,
        //                  storePositionWithTermVector,
        //                  storeOffsetWithTermVector, omitNorms,
        //                  hasProx, storePayloads, indexVersion,
        //                  flags)` — pull each from FieldInfoEntry.
        fi->add(name_wide.c_str(), entry.is_indexed, entry.store_term_vector,
                entry.store_position_with_term_vector, entry.store_offset_with_term_vector,
                entry.omit_norms, entry.has_prox, entry.store_payloads,
                static_cast<::IndexVersion>(entry.index_version),
                static_cast<uint32_t>(entry.flags));
    }
}

lucene::index::TermEnum* SpimiQueryIndexReader::terms(const void* /*io_ctx*/) {
    return new SpimiQueryTermEnum(_tis_bytes.data(), _tis_bytes.size(), _term_dict->SkipInterval(),
                                  _field_names_wide);
}

lucene::index::TermEnum* SpimiQueryIndexReader::terms(const lucene::index::Term* t,
                                                      const void* io_ctx) {
    auto* en = terms(io_ctx);
    if (t == nullptr) {
        return en;
    }
    // `skipTo(target)` — base class implementation walks via next()
    // until the current term >= target. Sufficient for SPIMI's
    // expected term-count scale.
    en->skipTo(const_cast<lucene::index::Term*>(t));
    return en;
}

lucene::index::TermDocs* SpimiQueryIndexReader::termDocs(bool /*load_stats*/,
                                                         const void* /*io_ctx*/) {
    return new SpimiQueryTermDocs(_term_dict.get(), _frq_bytes.data(), _frq_bytes.size(),
                                  &_field_infos_entries, &_field_names_wide);
}

lucene::index::TermPositions* SpimiQueryIndexReader::termPositions(bool /*load_stats*/,
                                                                   const void* /*io_ctx*/) {
    return new SpimiQueryTermPositions(_term_dict.get(), _frq_bytes.data(), _frq_bytes.size(),
                                       _prx_bytes.data(), _prx_bytes.size(), &_field_infos_entries,
                                       &_field_names_wide);
}

int32_t SpimiQueryIndexReader::docFreq(const lucene::index::Term* t) {
    if (t == nullptr) {
        return 0;
    }
    const wchar_t* field = t->field();
    int32_t field_number = -1;
    if (field != nullptr) {
        const std::wstring needle(field);
        for (size_t i = 0; i < _field_names_wide.size(); ++i) {
            if (_field_names_wide[i] == needle) {
                field_number = static_cast<int32_t>(i);
                break;
            }
        }
    }
    if (field_number < 0 || t->text() == nullptr) {
        return 0;
    }
    // wide → UTF-8 for TermDictReader.
    std::string text_utf8;
    for (const wchar_t* p = t->text(); *p != L'\0'; ++p) {
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
    const auto info = _term_dict->LookupTerm(field_number, text_utf8);
    return info.has_value() ? info->doc_freq : 0;
}

uint8_t* SpimiQueryIndexReader::GetOrAllocateNormsForField(const wchar_t* field) {
    if (field == nullptr) {
        return nullptr;
    }
    const std::wstring key(field);
    // The reader is shared across query threads via the searcher
    // cache; lock so two concurrent first-time `norms("X")` calls
    // can't race on `_norms_cache.emplace_back` (the realloc would
    // invalidate the pointer returned to the loser thread). The
    // lock is taken only on `norms()` calls, which in V4
    // production never happen (omit_norms=true short-circuits in
    // CLucene's IndexReader::norms), so this is uncontended in
    // practice.
    std::lock_guard<std::mutex> lk(_norms_cache_mu);
    for (auto& [k, v] : _norms_cache) {
        if (k == key) {
            return v.data();
        }
    }
    constexpr uint8_t kDefaultNorm = 124;
    std::vector<uint8_t> bytes(static_cast<size_t>(_max_doc), kDefaultNorm);
    _norms_cache.emplace_back(key, std::move(bytes));
    return _norms_cache.back().second.data();
}

uint8_t* SpimiQueryIndexReader::norms(const wchar_t* field) {
    return GetOrAllocateNormsForField(field);
}

void SpimiQueryIndexReader::norms(const wchar_t* field, uint8_t* bytes) {
    if (bytes == nullptr) {
        return;
    }
    const uint8_t* src = GetOrAllocateNormsForField(field);
    if (src == nullptr) {
        return;
    }
    std::memcpy(bytes, src, static_cast<size_t>(_max_doc));
}

void SpimiQueryIndexReader::getFieldNames(FieldOption fld_option,
                                          StringArrayWithDeletor& retarray) {
    for (const auto& entry : _field_infos_entries) {
        bool include = false;
        switch (fld_option) {
        case FieldOption::ALL:
            include = true;
            break;
        case FieldOption::INDEXED:
            include = entry.is_indexed;
            break;
        case FieldOption::UNINDEXED:
            include = !entry.is_indexed;
            break;
        case FieldOption::STORES_PAYLOADS:
            include = entry.store_payloads;
            break;
        default:
            // Term-vector options — SPIMI never stores term vectors,
            // so all TERMVECTOR_* options match no fields.
            include = false;
            break;
        }
        if (!include) {
            continue;
        }
        const std::wstring name_wide = Utf8ToWide(entry.name);
        const auto len = name_wide.size();
        auto* buf = new wchar_t[len + 1];
        std::wmemcpy(buf, name_wide.c_str(), len);
        buf[len] = L'\0';
        retarray.push_back(buf);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
