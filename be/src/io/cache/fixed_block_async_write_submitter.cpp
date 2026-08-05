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

#include "io/cache/fixed_block_async_write_submitter.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "util/time.h"

namespace doris::io {

FixedBlockSubmitResult FixedBlockAsyncWriteSubmitter::try_submit(
        const FixedBlockSubmitRequest& request) {
    DORIS_CHECK(request.cache != nullptr);
    const size_t block_size = static_cast<size_t>(config::file_cache_each_block_size);
    DORIS_CHECK(block_size > 0);
    DORIS_CHECK(request.file_size > 0);
    DORIS_CHECK(request.block_offset % block_size == 0);
    DORIS_CHECK(request.block_offset < request.file_size);
    DORIS_CHECK(request.valid_size ==
                std::min(block_size, request.file_size - request.block_offset));
    DORIS_CHECK(request.complete_payload.data != nullptr);
    DORIS_CHECK(request.complete_payload.size == request.valid_size);

    auto* manager = request.cache->async_write_manager();
    DORIS_CHECK(manager != nullptr);
    if (!manager->check_write_epoch(request.write_epoch)) {
        return FixedBlockSubmitResult::STALE_EPOCH;
    }

    ReadStatistics read_stats;
    CacheContext cache_context = request.admission_ctx.to_cache_context(&read_stats);
    auto probe = request.cache->probe(request.cache_hash, request.block_offset, request.valid_size,
                                      cache_context);
    DORIS_CHECK(probe.file_blocks.size() == 1);
    const auto& existing_block = probe.file_blocks.front();
    if (existing_block != nullptr && !request.cache->is_block_deleting(existing_block)) {
        switch (existing_block->state()) {
        case FileBlock::State::DOWNLOADED:
            return FixedBlockSubmitResult::ALREADY_DOWNLOADED;
        case FileBlock::State::DOWNLOADING:
            return FixedBlockSubmitResult::CACHE_DOWNLOADING;
        case FileBlock::State::EMPTY:
        case FileBlock::State::SKIP_CACHE:
            break;
        }
    }

    AsyncCacheWriteBufferPtr tracked_buffer;
    if (!manager->allocate_tracked_buffer(block_size, &tracked_buffer).ok()) {
        return FixedBlockSubmitResult::ALLOC_FAILED;
    }
    DORIS_CHECK(tracked_buffer != nullptr);
    DORIS_CHECK(tracked_buffer->size() == block_size);
    memcpy(tracked_buffer->data(), request.complete_payload.data, request.valid_size);

    if (!manager->check_write_epoch(request.write_epoch)) {
        return FixedBlockSubmitResult::STALE_EPOCH;
    }

    AsyncCacheWriteTask task {
            .cache_hash = request.cache_hash,
            .file_offset = request.block_offset,
            .write_size = request.valid_size,
            .buffer = tracked_buffer,
            .admission_ctx = request.admission_ctx,
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = request.write_epoch,
            .on_finalized = nullptr,
    };
    std::shared_ptr<InflightWriteBufferEntry> entry;
    auto* inflight_index = request.cache->inflight_write_buffer_index();
    DORIS_CHECK(inflight_index != nullptr);
    if (config::enable_async_file_cache_write_inflight_write_buffer_index) {
        entry = std::make_shared<InflightWriteBufferEntry>(tracked_buffer, request.block_offset,
                                                           request.valid_size, task.submit_ts_us);
        TEST_SYNC_POINT_CALLBACK("FixedBlockAsyncWriteSubmitter::try_submit:before_inflight_insert",
                                 &task);
        auto existing =
                inflight_index->insert_if_absent(request.cache_hash, request.block_offset, entry);
        if (existing != nullptr) {
            return FixedBlockSubmitResult::EXISTING_INFLIGHT;
        }
        task.on_finalized = [cache_hash = request.cache_hash, offset = request.block_offset,
                             inflight_index, entry](const AsyncCacheWriteTask&) {
            inflight_index->remove_if(cache_hash, offset, entry);
        };
    }

    if (!manager->try_submit(std::move(task))) {
        if (entry != nullptr) {
            inflight_index->remove_if(request.cache_hash, request.block_offset, entry);
            inflight_index->record_backpressure_rollback();
        }
        return FixedBlockSubmitResult::BACKPRESSURE;
    }
    return FixedBlockSubmitResult::SUBMITTED;
}

} // namespace doris::io
