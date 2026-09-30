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

#include "storage/index/inverted/spi/clucene_index_source.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Woverloaded-virtual"
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
#pragma clang diagnostic ignored "-Wreorder-ctor"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif
#include <CLucene.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/MultiReader.h>
#include <CLucene/index/Term.h>
#include <CLucene/index/_MultiSegmentReader.h>
#ifdef __clang__
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <boost/locale/encoding_utf.hpp>

#include "storage/index/inverted/inverted_index_common_impl.h"
#include "storage/index/inverted/similarity/bm25_similarity.h"
#include "storage/index/inverted/spi/clucene_postings_cursor.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2 {

namespace {

// The sub-readers of a reader split by segment, or null.
const lucene::util::ArrayBase<lucene::index::IndexReader*>* sub_readers(
        lucene::index::IndexReader* reader) {
    if (auto* multi_segment = dynamic_cast<lucene::index::MultiSegmentReader*>(reader)) {
        return multi_segment->getSubReaders();
    }
    if (auto* multi = dynamic_cast<lucene::index::MultiReader*>(reader)) {
        return multi->getSubReaders();
    }
    return nullptr;
}

} // namespace

CluceneIndexSource::CluceneIndexSource(std::shared_ptr<lucene::index::IndexReader> owner,
                                       lucene::index::IndexReader* reader, std::wstring field,
                                       const io::IOContext* io_ctx)
        : _owner(std::move(owner)), _reader(reader), _field(std::move(field)), _io_ctx(io_ctx) {}

uint32_t CluceneIndexSource::doc_count() const {
    return static_cast<uint32_t>(_reader->maxDoc());
}

Status CluceneIndexSource::open_term(std::string_view term, bool positions, bool scoring,
                                     std::unique_ptr<index_query::PostingsCursor>* out) {
    out->reset();
    const std::wstring text =
            boost::locale::conv::utf_to_utf<wchar_t>(term.data(), term.data() + term.size());
    auto t = make_term_ptr(_field.c_str(), text.c_str());
    if (positions) {
        auto iter = make_term_positions_ptr(_reader, t.get(), scoring, _io_ctx);
        if (iter != nullptr) {
            *out = std::make_unique<ClucenePostingsCursor>(std::move(iter));
        }
        return Status::OK();
    }
    auto iter = make_term_doc_ptr(_reader, t.get(), scoring, _io_ctx);
    if (iter != nullptr) {
        *out = std::make_unique<ClucenePostingsCursor>(std::move(iter));
    }
    return Status::OK();
}

Status CluceneIndexSource::expand_terms(index_query::TermPattern& pattern, int32_t max_expansions,
                                        std::vector<std::string>* out) {
    out->clear();
    if (!pattern.can_match()) {
        return Status::OK();
    }
    const std::string& prefix = pattern.enumeration_prefix();
    const std::wstring start_text = inverted_index::StringHelper::to_wstring(prefix);
    // A term without the text every match holds is skipped before it is converted.
    const std::wstring required = inverted_index::StringHelper::to_wstring(pattern.required_text());
    lucene::index::Term start(_field.c_str(), start_text.c_str());
    lucene::index::TermEnum* enumerator = _reader->terms(&start, _io_ctx);
    try {
        do {
            // The enumerator keeps its current term until next(), so no reference is taken.
            const lucene::index::Term* term = enumerator->term(false);
            if (term == nullptr || _field != term->field()) {
                break;
            }
            const std::wstring_view chars(term->text(), term->textLength());
            if (!required.empty() && chars.find(required) == std::wstring_view::npos) {
                continue;
            }
            std::string text = boost::locale::conv::utf_to_utf<char>(chars.data(),
                                                                     chars.data() + chars.size());
            if (!text.starts_with(prefix)) {
                break;
            }
            if (pattern.matches(text)) {
                out->push_back(std::move(text));
                if (max_expansions > 0 && out->size() == static_cast<size_t>(max_expansions)) {
                    break;
                }
            }
        } while (enumerator->next());
    }
    _CLFINALLY({
        enumerator->close();
        _CLDELETE(enumerator);
    });
    return Status::OK();
}

std::span<const float> CluceneIndexSource::norm_lengths() const {
    return BM25Similarity::lucene_norm_lengths();
}

bool CluceneIndexSource::is_live(uint32_t doc) const {
    return !_reader->isDeleted(static_cast<int32_t>(doc));
}

std::vector<index_query::IndexSegment> CluceneIndexSource::segments() const {
    if (_segments.has_value()) {
        return *_segments;
    }
    std::vector<index_query::IndexSegment> segments;
    if (const auto* subs = sub_readers(_reader); subs != nullptr) {
        const int32_t* starts = nullptr;
        if (auto* multi_segment = dynamic_cast<lucene::index::MultiSegmentReader*>(_reader)) {
            starts = multi_segment->getStarts();
        }
        segments.reserve(subs->length);
        uint32_t base = 0;
        for (size_t i = 0; i < subs->length; ++i) {
            lucene::index::IndexReader* sub = (*subs)[i];
            segments.push_back(
                    {.source = std::make_shared<CluceneIndexSource>(_owner, sub, _field, _io_ctx),
                     .doc_base = starts != nullptr ? static_cast<uint32_t>(starts[i]) : base});
            base += static_cast<uint32_t>(sub->maxDoc());
        }
    }
    _segments = std::move(segments);
    return *_segments;
}

std::shared_ptr<CluceneIndexSource> clucene_index_source(
        std::shared_ptr<lucene::index::IndexReader> reader, std::wstring field,
        const io::IOContext* io_ctx) {
    lucene::index::IndexReader* raw = reader.get();
    return std::make_shared<CluceneIndexSource>(std::move(reader), raw, std::move(field), io_ctx);
}

} // namespace doris::segment_v2
