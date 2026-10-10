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

#include "storage/index/inverted/query_v2/collect/top_k_collector.h"

namespace doris::segment_v2::inverted_index::query_v2 {

void collect_multi_segment_top_k(const WeightPtr& weight, const QueryExecutionContext& context,
                                 const std::string& binding_key, size_t k,
                                 const std::shared_ptr<roaring::Roaring>& roaring,
                                 const CollectionSimilarityPtr& similarity, bool use_wand,
                                 const std::shared_ptr<const roaring::Roaring>& delete_bitmap) {
    if (context.segment_num_rows == 0) {
        return;
    }
    TopKCollector collector(k);
    const float initial_threshold = collector.threshold();
    float threshold = initial_threshold;
    auto callback = [&](uint32_t doc_id, float score) -> float {
        if (delete_bitmap != nullptr && delete_bitmap->contains(doc_id)) {
            return threshold;
        }
        threshold = collector.collect(doc_id, score);
        return threshold;
    };
    if (use_wand) {
        weight->for_each_pruning(context, binding_key, initial_threshold, callback);
    } else {
        auto scorer = weight->scorer(context, binding_key);
        if (scorer) {
            Weight::for_each_pruning_scorer(scorer, initial_threshold, callback);
        }
    }

    for (const auto& doc : collector.into_sorted_vec()) {
        roaring->add(doc.doc_id);
        if (similarity) {
            similarity->collect(doc.doc_id, doc.score);
        }
    }
}

} // namespace doris::segment_v2::inverted_index::query_v2
