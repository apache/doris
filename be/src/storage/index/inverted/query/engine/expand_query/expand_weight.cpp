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

#include "storage/index/inverted/query/engine/expand_query/expand_weight.h"

#include <cmath>
#include <memory>
#include <roaring/roaring.hh>
#include <vector>

#include "common/check.h"
#include "storage/index/inverted/query/engine/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query/engine/const_score_query/const_score_scorer.h"
#include "storage/index/inverted/query/engine/nullable_scorer.h"
#include "storage/index/inverted/query/engine/score_combiner.h"
#include "storage/index/inverted/query/engine/term_query/term_scorer.h"
#include "storage/index/inverted/query/engine/union/buffered_union.h"
#include "storage/index/inverted/query/exec/block_doc_set.h"
#include "storage/index/inverted/query/exec/collect_postings.h"
#include "storage/index/inverted/query/exec/roaring_docid_sink.h"
#include "storage/index/inverted/query/exec/term_waves.h"
#include "storage/index/inverted/query/scoring/bm25_similarity.h"
#include "storage/index/inverted/query/spi/postings_cursor.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The rows holding any of the expanded terms, read a wave of terms at a time.
Status collect_expanded_rows(index_query::IndexSource& source, index_query::TermPattern& pattern,
                             int32_t max_expansions, const roaring::Roaring* candidates,
                             roaring::Roaring* rows) {
    std::vector<std::string> terms;
    RETURN_IF_ERROR(source.expand_terms(pattern, max_expansions, &terms));
    index_query::RoaringDocIdSink sink(*rows);
    if (candidates == nullptr) {
        return source.collect_terms(terms, sink);
    }
    std::vector<uint32_t> selected(candidates->cardinality());
    candidates->toUint32Array(selected.data());
    return index_query::visit_term_postings(
            source, terms, /*scoring=*/false,
            [&sink, candidates](size_t, index_query::PostingsCursor* cursor) -> Status {
                // The dictionary just listed the term, so its postings open.
                DORIS_CHECK(cursor != nullptr);
                index_query::BlockDocSet postings(*cursor);
                return index_query::collect_postings<false>(postings, candidates, sink,
                                                            [](uint32_t, uint32_t, uint32_t) {});
            },
            &selected);
}

ExpandWeight::ExpandWeight(IndexQueryContextPtr context, std::wstring field,
                           index_query::TermPatternKind kind, std::string pattern,
                           bool enable_scoring)
        : _context(std::move(context)),
          _field(std::move(field)),
          _kind(kind),
          _pattern(std::move(pattern)),
          // Prefix and phrase-edge expansions retain their constant-score behavior.
          _enable_scoring(enable_scoring && (kind == index_query::TermPatternKind::kRegexp ||
                                             kind == index_query::TermPatternKind::kWildcard)) {}

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
        THROW_IF_ERROR(collect_expanded_rows(
                *source, pattern,
                index_query::expansion_limit(_kind, index_query::max_expansions(*_context)),
                candidates, docs.get()));
    }
    return docs;
}

ScorerPtr ExpandWeight::scorer(const QueryExecutionContext& context,
                               const std::string& binding_key) {
    // The listing fast path discards term frequencies, so scored patterns need term scorers.
    if (_enable_scoring) {
        return make_nullable_scorer(_scored_terms(context, binding_key),
                                    logical_field_or_fallback(context, binding_key, _field),
                                    context.null_resolver);
    }
    auto scorer = std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(
            std::make_shared<BitSetScorer>(_rows(context, binding_key, nullptr)));
    return make_nullable_scorer(scorer, logical_field_or_fallback(context, binding_key, _field),
                                context.null_resolver);
}

ScorerPtr ExpandWeight::_scored_terms(const QueryExecutionContext& context,
                                      const std::string& binding_key) {
    index_query::TermPattern pattern;
    THROW_IF_ERROR(index_query::TermPattern::create(_kind, _pattern, &pattern));
    auto source = lookup_source(_field, context, binding_key);
    // An unbound field has no postings and contributes no matches.
    if (source == nullptr) {
        return std::make_shared<EmptyScorer>();
    }
    std::vector<std::string> terms;
    THROW_IF_ERROR(source->expand_terms(
            pattern, index_query::expansion_limit(_kind, index_query::max_expansions(*_context)),
            &terms));
    // A valid pattern may match no dictionary terms.
    if (terms.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    // Expansion terms are absent from collection statistics; use the bound source's statistics.
    index_query::IndexScoringStats stats;
    THROW_IF_ERROR(source->scoring_stats(&stats));
    DORIS_CHECK_GT(stats.doc_count, 0);
    // Docs-only SNII still reports its physical token count. Only legacy sources without
    // that metadata report zero, so use unit average length only for those sources.
    const uint64_t token_count = stats.token_count != 0 ? stats.token_count : stats.doc_count;
    const float avgdl = static_cast<float>(token_count) / stats.doc_count;
    DORIS_CHECK_GT(avgdl, 0.0F);
    std::vector<ScorerPtr> scorers;
    scorers.reserve(terms.size());
    auto logical_field = logical_field_or_fallback(context, binding_key, _field);
    for (const auto& term : terms) {
        uint64_t doc_freq = 0;
        THROW_IF_ERROR(source->doc_freq(term, &doc_freq));
        // The immutable source just enumerated this term and must have its postings.
        DCHECK_GT(doc_freq, 0);
        const auto idf = static_cast<float>(std::log1p(
                (static_cast<double>(stats.doc_count) - doc_freq + 0.5) / (doc_freq + 0.5)));
        auto similarity = std::make_shared<BM25Similarity>(idf, avgdl);
        auto postings = open_postings(*source, term, /*positions=*/false, /*enable_scoring=*/true,
                                      similarity);
        DCHECK(postings != nullptr);
        scorers.emplace_back(std::make_shared<TermScorer>(std::move(postings),
                                                          std::move(similarity), logical_field));
    }
    // One expanded term needs no union; multiple terms add their per-document BM25 scores.
    if (scorers.size() == 1) {
        return std::move(scorers.front());
    }
    return make_buffered_union(scorers, std::make_shared<SumCombiner>());
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
