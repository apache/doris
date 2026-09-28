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
        // The reader publishes BM25 values into the similarity the context carries and the
        // collector also collects the scorer's score, so a scored compile gives the reader a
        // private sink and lets the scores reach the collector through the scored query built
        // below. An unscored compile and an expanded term (a constant score, as on the CLucene
        // path) hide the similarity instead. Whether the leaf scores at all is the reader's
        // decision.
        const bool similarity = ctx.context->collection_similarity != nullptr;
        std::shared_ptr<segment_v2::IndexQueryContext> reader_context = ctx.context;
        if (similarity || ctx.domain != nullptr) {
            reader_context = std::make_shared<segment_v2::IndexQueryContext>(*ctx.context);
        }
        if (similarity) {
            score_sink = ctx.scoring && leaf.as<logical::Expand>() == nullptr
                                 ? std::make_shared<CollectionSimilarity>()
                                 : nullptr;
            reader_context->collection_similarity = score_sink;
        }
        if (ctx.domain != nullptr) {
            // SEARCH runs without the scan's candidates, so the domain is the only restriction.
            DORIS_CHECK(ctx.context->candidate_rows == nullptr);
            reader_context->candidate_rows = ctx.domain;
        }
        RETURN_IF_ERROR(_reader->query_leaf(reader_context, _stored_field_name, leaf, rows));
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
