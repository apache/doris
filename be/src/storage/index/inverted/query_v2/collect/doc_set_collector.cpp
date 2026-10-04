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

#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"

#include "common/exception.h"
#include "storage/index/inverted/query_v2/collect/multi_segment_util.h"
#include "storage/index/inverted/query_v2/complete_null_bitmap.h"

namespace doris::segment_v2::inverted_index::query_v2 {

namespace {

// Adds a segment's rows, in its own docid space, to the global ones.
void add_segment_rows(const roaring::Roaring& segment_rows, uint32_t doc_base,
                      roaring::Roaring* rows) {
    if (doc_base == 0) {
        *rows |= segment_rows;
        return;
    }
    auto* shifted = roaring::api::roaring_bitmap_add_offset(&segment_rows.roaring,
                                                            static_cast<int64_t>(doc_base));
    if (shifted == nullptr) {
        throw Exception(ErrorCode::MEM_ALLOC_FAILED, "Failed to rebase segment rows");
    }
    *rows |= roaring::Roaring(shifted);
}

} // namespace

void collect_multi_segment_doc_set(const WeightPtr& weight, const QueryExecutionContext& context,
                                   const std::string& binding_key,
                                   const std::shared_ptr<roaring::Roaring>& roaring,
                                   const CollectionSimilarityPtr& similarity, bool enable_scoring,
                                   roaring::Roaring* null_rows) {
    const bool publish_scores = enable_scoring && similarity != nullptr;
    for_each_index_segment(
            context, binding_key, [&](const QueryExecutionContext& seg_ctx, uint32_t doc_base) {
                auto scorer = weight->scorer(seg_ctx, binding_key);
                if (!scorer) {
                    return;
                }
                if (null_rows != nullptr && scorer->has_null_bitmap(seg_ctx.null_resolver)) {
                    const auto* nulls = scorer->get_null_bitmap(seg_ctx.null_resolver);
                    if (nulls != nullptr) {
                        add_segment_rows(*nulls, doc_base, null_rows);
                    }
                }
                if (publish_scores) {
                    add_segment_rows(collect_scored_rows(scorer, doc_base, *similarity), doc_base,
                                     roaring.get());
                } else if (doc_base == 0) {
                    collect_true_rows(scorer, roaring.get());
                } else {
                    roaring::Roaring rows;
                    collect_true_rows(scorer, &rows);
                    add_segment_rows(rows, doc_base, roaring.get());
                }
            });
}

} // namespace doris::segment_v2::inverted_index::query_v2
