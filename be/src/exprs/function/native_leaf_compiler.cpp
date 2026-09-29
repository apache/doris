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

#include "storage/compaction/collection_similarity.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/query_v2/scored_bit_set_query/scored_bit_set_query.h"
#include "storage/index/query/logical/node.h"

namespace doris {
namespace {

namespace logical = index_query::logical;
namespace query_v2 = segment_v2::inverted_index::query_v2;

// Whether the reader scores the leaf: a term, a term set or a phrase; an expansion keeps the
// constant score of the lazy path.
bool reader_scores(const logical::Node& leaf) {
    return leaf.as<logical::Term>() != nullptr || leaf.as<logical::TermSet>() != nullptr ||
           leaf.as<logical::Phrase>() != nullptr;
}

} // namespace

NativeLeafCompiler::NativeLeafCompiler(segment_v2::InvertedIndexReaderPtr reader,
                                       std::string stored_field_name,
                                       std::shared_ptr<LazyLeafCompiler> lazy)
        : _reader(std::move(reader)),
          _stored_field_name(std::move(stored_field_name)),
          _lazy(std::move(lazy)) {}

Status NativeLeafCompiler::compile(const logical::Node& leaf, const SearchLeafContext& ctx,
                                   query_v2::QueryPtr* out) {
    const bool scored = ctx.scoring && ctx.context->collection_similarity != nullptr;
    if (!scored || !reader_scores(leaf)) {
        return _lazy->compile(leaf, ctx, out);
    }
    return _compile_scored(leaf, ctx, out);
}

Status NativeLeafCompiler::_compile_scored(const logical::Node& leaf, const SearchLeafContext& ctx,
                                           query_v2::QueryPtr* out) {
    // The reader publishes BM25 values into the similarity the context carries and the collector
    // also collects the scorer's score, so the reader gets a private sink and the scores reach the
    // collector through the scored query built below. Whether the leaf scores at all is the
    // reader's decision.
    auto reader_context = std::make_shared<segment_v2::IndexQueryContext>(*ctx.context);
    auto score_sink = std::make_shared<CollectionSimilarity>();
    reader_context->collection_similarity = score_sink;
    if (ctx.domain != nullptr) {
        // SEARCH runs without the scan's candidates, so the domain is the only restriction.
        DORIS_CHECK(ctx.context->candidate_rows == nullptr);
        reader_context->candidate_rows = ctx.domain;
    }
    auto rows = std::make_shared<roaring::Roaring>();
    RETURN_IF_ERROR(_reader->query_leaf(reader_context, _stored_field_name, leaf, rows));
    // Reply-direction fields land on the copy the reader was given. The domain is internal to
    // SEARCH, so the scan never hears that it was consumed.
    if (ctx.domain != nullptr) {
        reader_context->candidate_rows_consumed = false;
    }
    ctx.context->merge_reader_outputs(*reader_context);

    auto nulls = std::make_shared<roaring::Roaring>();
    if (_reader->has_null()) {
        segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(_reader->read_null_bitmap(ctx.context, &null_bitmap_cache_handle));
        auto cached_null_bitmap = null_bitmap_cache_handle.get_bitmap();
        DORIS_CHECK(cached_null_bitmap != nullptr);
        nulls = std::move(cached_null_bitmap);
    }
    *rows -= *nulls;
    // Only a leaf the reader scored gets a scored query; the rest keep the constant score the
    // lazy path also gives its unscored leaves.
    auto scores = score_sink->release_scores();
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
