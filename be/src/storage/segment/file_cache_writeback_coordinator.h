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

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/segment/page_prefetcher.h"

namespace doris::segment_v2 {

/// Adds density-qualified complete cache blocks to an existing page read plan without performing IO.
class FileCacheWritebackCoordinator {
public:
    FileCacheWritebackCoordinator() = default;
    explicit FileCacheWritebackCoordinator(PagePrefetchIOService* io_service)
            : _io_service(io_service) {}

    Status plan_block_completion(const std::vector<PageCandidate>& pages, uint64_t file_size,
                                 const PagePrefetchOptions& options, PageFetchPlan* plan) const;
    void mark_page_consumed(const std::shared_ptr<PrefetchRange>& range, uint32_t page_index) const;
    void invalidate_page(const std::shared_ptr<PrefetchRange>& range, uint32_t page_index) const;

private:
    PagePrefetchIOService* _io_service = nullptr;
};

} // namespace doris::segment_v2
