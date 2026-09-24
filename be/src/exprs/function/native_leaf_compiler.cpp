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

#include "exprs/function/native_leaf_compiler.h"

#include <memory>
#include <roaring/roaring.hh>
#include <utility>
#include <vector>

#include "storage/compaction/collection_similarity.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/query/query_info.h"
#include "storage/index/inverted/query_v2/scored_bit_set_query/scored_bit_set_query.h"

namespace doris {
namespace {

namespace logical = index_query::logical;
namespace query_v2 = segment_v2::inverted_index::query_v2;
using segment_v2::InvertedIndexQueryInfo;
using segment_v2::InvertedIndexQueryType;

// The reader query a leaf maps to. Expanded terms keep a constant score, as on the CLucene path.
struct NativeQuery {
    InvertedIndexQueryType query_type = InvertedIndexQueryType::UNKNOWN_QUERY;
    InvertedIndexQueryInfo query_info;
    bool scored = true;
};

InvertedIndexQueryType expand_query_type(logical::ExpandKind kind) {
    switch (kind) {
    case logical::ExpandKind::kPrefix:
        // The reader runs a one-term phrase prefix as a prefix of that term.
        return InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    case logical::ExpandKind::kRegexp:
        return InvertedIndexQueryType::MATCH_REGEXP_QUERY;
    case logical::ExpandKind::kWildcard:
    default:
        return InvertedIndexQueryType::WILDCARD_QUERY;
    }
}

InvertedIndexQueryInfo single_terms(const std::vector<std::string>& terms) {
    InvertedIndexQueryInfo info;
    info.term_infos.reserve(terms.size());
    for (const auto& term : terms) {
        info.term_infos.emplace_back(term);
    }
    return info;
}

InvertedIndexQueryInfo slots(std::vector<segment_v2::TermInfo> term_infos) {
    InvertedIndexQueryInfo info;
    info.term_infos = std::move(term_infos);
    return info;
}

Status plan_native_query(const logical::Node& leaf, NativeQuery* out) {
    if (const auto* term = leaf.as<logical::Term>()) {
        *out = {.query_type = InvertedIndexQueryType::EQUAL_QUERY,
                .query_info = single_terms({term->term})};
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        // The reader knows all-of and any-of; the SEARCH compile step counts a
        // threshold above it.
        DORIS_CHECK(set->min_should_match == 0);
        *out = {.query_type = set->require_all ? InvertedIndexQueryType::MATCH_ALL_QUERY
                                               : InvertedIndexQueryType::MATCH_ANY_QUERY,
                .query_info = single_terms(set->terms)};
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        *out = {.query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                .query_info = slots(phrase->slots)};
    } else if (const auto* expand = leaf.as<logical::Expand>()) {
        *out = {.query_type = expand_query_type(expand->kind),
                .query_info = single_terms({expand->pattern}),
                .scored = false};
    } else {
        return Status::InternalError("leaf kind {} cannot run on a native index reader",
                                     leaf.value.index());
    }
    return Status::OK();
}

} // namespace

NativeLeafCompiler::NativeLeafCompiler(segment_v2::InvertedIndexReaderPtr reader,
                                       std::string stored_field_name)
        : _reader(std::move(reader)), _stored_field_name(std::move(stored_field_name)) {}

Status NativeLeafCompiler::compile(const logical::Node& leaf, const SearchLeafContext& ctx,
                                   query_v2::QueryPtr* out) {
    if (leaf.as<logical::Empty>() != nullptr) {
        *out = std::make_shared<query_v2::BitSetQuery>(roaring::Roaring());
        return Status::OK();
    }
    auto rows = std::make_shared<roaring::Roaring>();
    std::shared_ptr<CollectionSimilarity> score_sink;
    if (leaf.as<logical::Exists>() != nullptr) {
        rows->addRange(0, ctx.num_rows);
    } else {
        NativeQuery query;
        RETURN_IF_ERROR(plan_native_query(leaf, &query));
        // The reader publishes BM25 values into the similarity the context carries and the
        // collector also collects the scorer's score, so give the reader a private sink and let
        // the scores reach the collector through the scored query built below. An unscored leaf
        // hides the similarity instead. Both happen only when the reader would score, which
        // spares the other clauses a context copy.
        const bool reader_would_score = ctx.context->collection_similarity != nullptr &&
                                        segment_v2::IndexReaderHelper::is_need_similarity_score(
                                                query.query_type, &_reader->get_index_meta());
        std::shared_ptr<segment_v2::IndexQueryContext> reader_context = ctx.context;
        if (reader_would_score || ctx.domain != nullptr) {
            reader_context = std::make_shared<segment_v2::IndexQueryContext>(*ctx.context);
        }
        if (reader_would_score) {
            score_sink = query.scored ? std::make_shared<CollectionSimilarity>() : nullptr;
            reader_context->collection_similarity = score_sink;
        }
        if (ctx.domain != nullptr) {
            // SEARCH runs without the scan's candidates, so the domain is the only restriction.
            DORIS_CHECK(ctx.context->candidate_rows == nullptr);
            reader_context->candidate_rows = ctx.domain;
        }
        RETURN_IF_ERROR(_reader->query_analyzed(reader_context, _stored_field_name,
                                                query.query_type, query.query_info, rows));
        // Reply-direction fields land on the copy the reader was given. The domain is internal
        // to SEARCH, so the scan never hears that it was consumed.
        if (reader_context != ctx.context) {
            if (ctx.domain != nullptr) {
                reader_context->candidate_rows_consumed = false;
            }
            ctx.context->merge_reader_outputs(*reader_context);
        }
    }

    auto nulls = std::make_shared<roaring::Roaring>();
    if (_reader->has_null()) {
        segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(_reader->read_null_bitmap(ctx.context, &null_bitmap_cache_handle));
        auto cached_null_bitmap = null_bitmap_cache_handle.get_bitmap();
        DORIS_CHECK(cached_null_bitmap != nullptr);
        nulls = std::move(cached_null_bitmap);
    }
    *rows -= *nulls;
    // Only a clause the reader scored gets a scored query; the rest keep the constant score
    // the CLucene path also gives its unscored leaves.
    auto scores = score_sink != nullptr ? score_sink->release_scores() : ScoreMap {};
    if (!scores.empty()) {
        *out = std::make_shared<query_v2::ScoredBitSetQuery>(
                std::move(rows), std::move(nulls),
                std::make_shared<const ScoreMap>(std::move(scores)));
        return Status::OK();
    }
    *out = std::make_shared<query_v2::BitSetQuery>(std::move(rows), std::move(nulls));
    return Status::OK();
}

} // namespace doris
