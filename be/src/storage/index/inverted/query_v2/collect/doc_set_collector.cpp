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

#include "storage/index/inverted/query_v2/complete_null_bitmap.h"

namespace doris::segment_v2::inverted_index::query_v2 {

void collect_multi_segment_doc_set(const WeightPtr& weight, const QueryExecutionContext& context,
                                   const std::string& binding_key,
                                   const std::shared_ptr<roaring::Roaring>& roaring,
                                   const CollectionSimilarityPtr& similarity, bool enable_scoring,
                                   roaring::Roaring* null_rows) {
    if (context.segment_num_rows == 0) {
        return;
    }
    auto scorer = weight->scorer(context, binding_key);
    if (!scorer) {
        return;
    }
    if (null_rows != nullptr && scorer->has_null_bitmap(context.null_resolver)) {
        const auto* nulls = scorer->get_null_bitmap(context.null_resolver);
        if (nulls != nullptr) {
            *null_rows |= *nulls;
        }
    }
    if (enable_scoring && similarity != nullptr) {
        *roaring |= collect_scored_rows(scorer, 0, *similarity);
    } else {
        collect_true_rows(scorer, roaring.get());
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
