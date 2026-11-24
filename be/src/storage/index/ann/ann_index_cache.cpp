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

#include "storage/index/ann/ann_index_cache.h"

#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_storage.h"

namespace doris::segment_v2 {
AnnIndexCacheValue::AnnIndexCacheValue() = default;
AnnIndexCacheValue::~AnnIndexCacheValue() = default;
AnnIndexCacheHandle::AnnIndexCacheHandle() = default;
AnnIndexCacheHandle::~AnnIndexCacheHandle() = default;
AnnIndexCacheHandle::AnnIndexCacheHandle(AnnIndexCacheHandle&&) noexcept = default;
AnnIndexCacheHandle& AnnIndexCacheHandle::operator=(AnnIndexCacheHandle&&) noexcept = default;

VectorIndex* AnnIndexCacheHandle::index() const {
    auto* value =
            _owned_value ? _owned_value.get() : static_cast<AnnIndexCacheValue*>(_handle.value());
    return value ? value->index.get() : nullptr;
}
} // namespace doris::segment_v2
