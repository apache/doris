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

#include <memory>
#include <roaring/roaring.hh>
#include <vector>

#include "common/check.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/const_score_query/const_score_scorer.h"
#include "storage/index/inverted/query_v2/nullable_scorer.h"
#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/roaring_docid_sink.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::segment_v2::inverted_index::query_v2 {

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
    auto source = lookup_source(_field, context, binding_key);
    if (source != nullptr) {
        std::vector<std::string> terms;
        THROW_IF_ERROR(source->expand_terms(
                pattern,
                index_query::expansion_limit(_kind, index_query::max_expansions(*_context)),
                &terms));
        if (!terms.empty()) {
            auto docs = std::make_shared<roaring::Roaring>();
            index_query::RoaringDocIdSink sink(*docs);
            for (const auto& term : terms) {
                std::unique_ptr<index_query::PostingsCursor> cursor;
                THROW_IF_ERROR(source->open_term(term, /*positions=*/false,
                                                 /*scoring=*/false, &cursor));
                // The dictionary just listed the term, so its postings open.
                DORIS_CHECK(cursor != nullptr);
                index_query::BlockDocSet postings(*cursor);
                THROW_IF_ERROR(index_query::collect_postings<false>(
                        postings, nullptr, sink, [](uint32_t, uint32_t, uint32_t) {}));
            }
            scorer = std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(
                    std::make_shared<BitSetScorer>(std::move(docs)));
        }
    }
    return make_nullable_scorer(scorer, logical_field_or_fallback(context, binding_key, _field),
                                context.null_resolver);
}

} // namespace doris::segment_v2::inverted_index::query_v2
