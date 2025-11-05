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

#pragma once

#include <glog/logging.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "olap/rowset/segment_v2/index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/weight.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// Small helper that centralizes "field NULL bitmap" lookups so weights/scorers
// don't have to duplicate resolver plumbing.
class FieldNullBitmapFetcher {
public:
    FieldNullBitmapFetcher() = delete;

    static std::shared_ptr<roaring::Roaring> fetch(const QueryExecutionContext& context,
                                                   const std::string& logical_field,
                                                   const Scorer* scorer = nullptr) {
        return fetch(context.null_resolver, logical_field, scorer);
    }

    static std::shared_ptr<roaring::Roaring> fetch(const NullBitmapResolver* resolver,
                                                   const std::string& logical_field,
                                                   const Scorer* scorer = nullptr) {
        if (resolver == nullptr || logical_field.empty()) {
            return nullptr;
        }

        EmptyScorer fallback_scorer;
        const Scorer* resolver_scorer = scorer != nullptr ? scorer : &fallback_scorer;

        auto iterator = resolver->iterator_for(*resolver_scorer, logical_field);
        if (iterator == nullptr) {
            return nullptr;
        }

        auto has_null = iterator->has_null();
        if (!has_null.has_value() || !has_null.value()) {
            return nullptr;
        }

        segment_v2::InvertedIndexQueryCacheHandle cache_handle;
        auto status = iterator->read_null_bitmap(&cache_handle);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to read null bitmap for field '" << logical_field
                         << "': " << status.to_string();
            return nullptr;
        }

        auto bitmap_ptr = cache_handle.get_bitmap();
        if (bitmap_ptr == nullptr) {
            return nullptr;
        }

        return std::make_shared<roaring::Roaring>(*bitmap_ptr);
    }
};

} // namespace doris::segment_v2::inverted_index::query_v2
