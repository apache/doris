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

#include "olap/rowset/segment_v2/inverted_index/query_v2/collect/doc_set_collector.h"

#include "olap/rowset/segment_v2/inverted_index/query_v2/collect/multi_segment_util.h"

namespace doris::segment_v2::inverted_index::query_v2 {

void collect_multi_segment_doc_set(const WeightPtr& weight, const QueryExecutionContext& context,
                                   const std::string& binding_key,
                                   const std::shared_ptr<roaring::Roaring>& roaring,
                                   const CollectionSimilarityPtr& similarity, bool enable_scoring) {
    for_each_index_segment(context, binding_key,
                           [&](const QueryExecutionContext& seg_ctx, uint32_t doc_base) {
                               auto scorer = weight->scorer(seg_ctx, binding_key);
                               if (!scorer) {
                                   return;
                               }

                               uint32_t doc = scorer->doc();
                               while (doc != TERMINATED) {
                                   uint32_t global_doc = doc + doc_base;
                                   roaring->add(global_doc);
                                   if (enable_scoring && similarity) {
                                       similarity->collect(global_doc, scorer->score());
                                   }
                                   doc = scorer->advance();
                               }
                           });
}

} // namespace doris::segment_v2::inverted_index::query_v2
