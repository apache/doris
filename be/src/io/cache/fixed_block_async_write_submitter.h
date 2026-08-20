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

#include <cstddef>
#include <cstdint>

#include "io/cache/async_cache_write_manager.h"
#include "io/cache/file_cache_common.h"
#include "util/slice.h"

namespace doris::io {

class BlockFileCache;

enum class FixedBlockSubmitResult : uint8_t {
    SUBMITTED,
    STALE_EPOCH,
    ALLOC_FAILED,
    EXISTING_INFLIGHT,
    ALREADY_DOWNLOADED,
    CACHE_DOWNLOADING,
    BACKPRESSURE,
};

struct FixedBlockSubmitRequest {
    BlockFileCache* cache = nullptr;
    UInt128Wrapper cache_hash;
    uint64_t block_offset = 0;
    size_t valid_size = 0;
    size_t file_size = 0;
    Slice complete_payload;
    CacheAdmissionContext admission_ctx;
    AsyncCacheWriteEpoch write_epoch;
};

/// Produces one fixed-capacity Phase 1 task from an already complete cache-block payload.
class FixedBlockAsyncWriteSubmitter {
public:
    static FixedBlockSubmitResult try_submit(const FixedBlockSubmitRequest& request);
};

} // namespace doris::io
