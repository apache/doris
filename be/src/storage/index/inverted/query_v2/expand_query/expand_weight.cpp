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
#include "storage/index/query/exec/term_waves.h"
#include "storage/index/query/roaring_docid_sink.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::segment_v2::inverted_index::query_v2 {

namespace {

// The rows holding any of the expanded terms, read a wave of terms at a time.
Status collect_expanded_rows(index_query::IndexSource& source,
                             const std::vector<std::string>& terms,
                             const roaring::Roaring* candidates, roaring::Roaring* rows) {
    index_query::RoaringDocIdSink sink(*rows);
    std::vector<uint32_t> selected;
    if (candidates != nullptr) {
        selected.resize(candidates->cardinality());
        candidates->toUint32Array(selected.data());
    }
    return index_query::visit_term_postings(
            source, terms, /*scoring=*/false,
            [&sink, candidates](size_t, index_query::PostingsCursor* cursor) -> Status {
                // The dictionary just listed the term, so its postings open.
                DORIS_CHECK(cursor != nullptr);
                index_query::BlockDocSet postings(*cursor);
                return index_query::collect_postings<false>(postings, candidates, sink,
                                                            [](uint32_t, uint32_t, uint32_t) {});
            },
            candidates == nullptr ? nullptr : &selected);
}

} // namespace

ExpandWeight::ExpandWeight(IndexQueryContextPtr context, std::wstring field,
                           index_query::TermPatternKind kind, std::string pattern)
        : _context(std::move(context)),
          _field(std::move(field)),
          _kind(kind),
          _pattern(std::move(pattern)) {}

std::shared_ptr<roaring::Roaring> ExpandWeight::_rows(const QueryExecutionContext& context,
                                                      const std::string& binding_key,
                                                      const roaring::Roaring* candidates) {
    auto docs = std::make_shared<roaring::Roaring>();
    if (candidates != nullptr && candidates->isEmpty()) {
        return docs;
    }
    index_query::TermPattern pattern;
    THROW_IF_ERROR(index_query::TermPattern::create(_kind, _pattern, &pattern));
    auto source = lookup_source(_field, context, binding_key);
    if (source != nullptr) {
        std::vector<std::string> terms;
        THROW_IF_ERROR(source->expand_terms(
                pattern,
                index_query::expansion_limit(_kind, index_query::max_expansions(*_context)),
                &terms));
        THROW_IF_ERROR(collect_expanded_rows(*source, terms, candidates, docs.get()));
    }
    return docs;
}

ScorerPtr ExpandWeight::scorer(const QueryExecutionContext& context,
                               const std::string& binding_key) {
    auto scorer = std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(
            std::make_shared<BitSetScorer>(_rows(context, binding_key, nullptr)));
    return make_nullable_scorer(scorer, logical_field_or_fallback(context, binding_key, _field),
                                context.null_resolver);
}

index_query::TruthSet ExpandWeight::listed_rows(const QueryExecutionContext& context,
                                                const std::string& binding_key,
                                                const roaring::Roaring* candidates) {
    index_query::TruthSet result;
    result.true_rows = std::move(*_rows(context, binding_key, candidates));
    auto nulls = FieldNullBitmapFetcher::fetch(
            context.null_resolver, logical_field_or_fallback(context, binding_key, _field));
    if (nulls != nullptr) {
        result.null_rows = candidates == nullptr ? *nulls : *nulls & *candidates;
    }
    return result;
}

} // namespace doris::segment_v2::inverted_index::query_v2
