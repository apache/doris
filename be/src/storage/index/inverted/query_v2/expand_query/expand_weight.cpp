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

#include "storage/index/inverted/query_v2/expand_query/expand_weight.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Woverloaded-virtual"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/Term.h>
#ifdef __clang__
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <boost/locale/encoding_utf.hpp>
#include <roaring/roaring.hh>
#include <string_view>

#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/const_score_query/const_score_scorer.h"
#include "storage/index/inverted/query_v2/nullable_scorer.h"
#include "storage/index/inverted/query_v2/segment_postings.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/roaring_docid_sink.h"

CL_NS_USE(index)

namespace doris::segment_v2::inverted_index::query_v2 {

std::vector<std::string> expand_terms(lucene::index::IndexReader* reader, const std::wstring& field,
                                      index_query::TermPattern& pattern, int32_t max_expansions,
                                      const io::IOContext* io_ctx) {
    std::vector<std::string> terms;
    if (!pattern.can_match()) {
        return terms;
    }
    const std::string& prefix = pattern.enumeration_prefix();
    const std::wstring start_text = StringHelper::to_wstring(prefix);
    // A term without the text every match holds is skipped before it is converted.
    const std::wstring required = StringHelper::to_wstring(pattern.required_text());
    Term start(field.c_str(), start_text.c_str());
    TermEnum* enumerator = reader->terms(&start, io_ctx);
    try {
        do {
            // The enumerator keeps its current term until next(), so no reference is taken.
            const Term* term = enumerator->term(false);
            if (term == nullptr || field != term->field()) {
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
                terms.push_back(std::move(text));
                if (max_expansions > 0 && terms.size() == static_cast<size_t>(max_expansions)) {
                    break;
                }
            }
        } while (enumerator->next());
    }
    _CLFINALLY({
        enumerator->close();
        _CLDELETE(enumerator);
    });
    return terms;
}

ExpandWeight::ExpandWeight(IndexQueryContextPtr context, std::wstring field,
                           index_query::TermPatternKind kind, std::string pattern)
        : _context(std::move(context)),
          _field(std::move(field)),
          _kind(kind),
          _pattern(std::move(pattern)) {}

ScorerPtr ExpandWeight::scorer(const QueryExecutionContext& context,
                               const std::string& binding_key) {
    index_query::TermPattern pattern;
    THROW_IF_ERROR(index_query::TermPattern::create(_kind, _pattern, &pattern));
    ScorerPtr scorer = std::make_shared<EmptyScorer>();
    auto reader = lookup_reader(_field, context, binding_key);
    if (reader != nullptr) {
        const auto terms = expand_terms(
                reader.get(), _field, pattern,
                index_query::expansion_limit(_kind, index_query::max_expansions(*_context)),
                _context->io_ctx);
        if (!terms.empty()) {
            auto docs = std::make_shared<roaring::Roaring>();
            index_query::RoaringDocIdSink sink(*docs);
            for (const auto& term : terms) {
                auto postings = create_term_posting(reader.get(), _field, term, false, nullptr,
                                                    _context->io_ctx);
                THROW_IF_ERROR(index_query::collect_postings<false>(
                        postings->doc_set(), nullptr, sink, [](uint32_t, uint32_t, uint32_t) {}));
            }
            scorer = std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(
                    std::make_shared<BitSetScorer>(std::move(docs)));
        }
    }
    return make_nullable_scorer(scorer, logical_field_or_fallback(context, binding_key, _field),
                                context.null_resolver);
}

} // namespace doris::segment_v2::inverted_index::query_v2
