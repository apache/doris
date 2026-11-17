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

#include "storage/index/inverted/query/engine/null_bitmap_fetcher.h"

namespace doris::segment_v2::inverted_index::query_v2 {

std::shared_ptr<roaring::Roaring> FieldNullBitmapFetcher::fetch(
        const QueryExecutionContext& context, const std::string& logical_field,
        const Scorer* scorer) {
    return fetch(context.null_resolver, logical_field, scorer);
}

std::shared_ptr<roaring::Roaring> FieldNullBitmapFetcher::fetch(const NullBitmapResolver* resolver,
                                                                const std::string& logical_field,
                                                                const Scorer* scorer) {
    if (resolver == nullptr || logical_field.empty()) {
        return nullptr;
    }

    EmptyScorer fallback_scorer;
    const Scorer* resolver_scorer = scorer != nullptr ? scorer : &fallback_scorer;

    return resolver->bitmap_for(*resolver_scorer, logical_field);
}

} // namespace doris::segment_v2::inverted_index::query_v2
