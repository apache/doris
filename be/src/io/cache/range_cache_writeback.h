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
#include <optional>

#include "io/cache/async_cache_write_manager.h"
#include "io/cache/range_writeback_dispatcher.h"
#include "io/fs/file_range_read_scheduler.h"
#include "io/fs/file_reader.h"

namespace doris::io {

class InflightWriteBufferIndex;
class PartialBlockWritebackManager;

struct RangeCacheWritebackOptions {
    /// Non-owning asynchronous destinations; both must outlive this RangeCacheWriteback.
    AsyncCacheWriteManager* write_manager {nullptr};
    PartialBlockWritebackManager* partial_block_manager {nullptr};
    /// Optional block-level deduplication index owned by the cached reader.
    InflightWriteBufferIndex* inflight_index {nullptr};
    /// Retained for background reads that complete partial blocks.
    FileReaderSPtr source_reader;
    UInt128Wrapper cache_hash;
    size_t file_size {0};
    size_t block_size {0};
    /// Immutable admission and asynchronous IO state captured from the foreground query.
    CacheAdmissionContext admission_ctx;
    FileRangeReadIOContext io_context;
};

/// Routes an already consumed variable-length range into the two File Cache writeback paths.
/// Complete blocks enter the asynchronous cache writer directly; partial fragments enter the
/// BE-level hole-fill queue.
class RangeCacheWriteback {
public:
    explicit RangeCacheWriteback(RangeCacheWritebackOptions options);

    /// Capture the invalidation fence before the foreground range read starts. No epoch is retained
    /// while asynchronous cache writing is disabled or shutting down.
    std::optional<AsyncCacheWriteEpoch> capture_write_epoch() const;

    /// Copy each block fragment into its owning asynchronous path. The range buffer only needs to
    /// remain valid for this call.
    RangeWritebackDispatchResult submit_consumed_range(const FileRange& range, Slice data,
                                                       const AsyncCacheWriteEpoch& write_epoch);

private:
    /// Copy a complete fragment directly into the asynchronous cache writer.
    bool _submit_complete_block(const FileCacheBlockFragment& fragment,
                                const AsyncCacheWriteEpoch& write_epoch);
    /// Copy a partial fragment into the background hole-fill manager.
    bool _submit_partial_block(const FileCacheBlockFragment& fragment,
                               const AsyncCacheWriteEpoch& write_epoch);

    const RangeCacheWritebackOptions _options;
};

} // namespace doris::io
