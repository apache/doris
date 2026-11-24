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

#include "storage/cache/page_cache.h"

namespace doris::segment_v2 {
class AnnIndexSource;
class VectorIndex;

// The index keeps an input opened from source. For example, V2 inputs borrow
// the compound file stream, so index must be destroyed before source.
struct AnnIndexCacheValue : public LRUCacheValueBase {
    AnnIndexCacheValue();
    ~AnnIndexCacheValue() override;

    std::unique_ptr<AnnIndexSource> source;
    std::unique_ptr<VectorIndex> index;
};

// Queries pin the whole loaded IVF index. No shared_ptr may outlive the handle:
// erasing a pinned entry must keep both its bytes and memory charge alive.
class AnnIndexCacheHandle {
public:
    AnnIndexCacheHandle();
    ~AnnIndexCacheHandle();
    AnnIndexCacheHandle(AnnIndexCacheHandle&&) noexcept;
    AnnIndexCacheHandle& operator=(AnnIndexCacheHandle&&) noexcept;

    VectorIndex* index() const;

private:
    friend class AnnIndexReader;
    PageCacheHandle _handle;
    // Standalone readers (without an ExecEnv page cache) own the value locally.
    std::unique_ptr<AnnIndexCacheValue> _owned_value;
};
} // namespace doris::segment_v2
