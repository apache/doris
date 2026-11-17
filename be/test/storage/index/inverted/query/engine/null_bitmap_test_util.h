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

#include <memory>
#include <roaring/roaring.hh>

#include "storage/index/index_iterator.h"
#include "storage/index/inverted/common/inverted_index_cache.h"

namespace doris::segment_v2::inverted_index::query_v2::test_support {

inline std::shared_ptr<roaring::Roaring> null_bitmap_from_iterator(IndexIterator* iterator) {
    if (iterator == nullptr) {
        return nullptr;
    }
    auto has_null = iterator->has_null();
    if (!has_null.has_value() || !has_null.value()) {
        return nullptr;
    }
    InvertedIndexQueryCacheHandle handle;
    if (!iterator->read_null_bitmap(&handle).ok()) {
        return nullptr;
    }
    auto bitmap = handle.get_bitmap();
    return bitmap == nullptr ? nullptr : std::make_shared<roaring::Roaring>(*bitmap);
}

} // namespace doris::segment_v2::inverted_index::query_v2::test_support
