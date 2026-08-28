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

#include "io/cache/range_cache_writeback.h"

#include <utility>

#include "common/logging.h"
#include "io/cache/partial_block_writeback_manager.h"

namespace doris::io {

RangeCacheWriteback::RangeCacheWriteback(RangeCacheWritebackOptions options)
        : _options(std::move(options)) {
    DORIS_CHECK(_options.write_manager != nullptr);
    DORIS_CHECK(_options.partial_block_manager != nullptr);
    DORIS_CHECK(_options.source_reader != nullptr);
    DORIS_CHECK(_options.file_size > 0);
    DORIS_CHECK(_options.source_reader->size() == _options.file_size);
    DORIS_CHECK(_options.block_size > 0);
}

std::optional<AsyncCacheWriteEpoch> RangeCacheWriteback::capture_write_epoch() const {
    if (!_options.write_manager->accepting()) {
        return std::nullopt;
    }
    return _options.write_manager->current_write_epoch(_options.cache_hash);
}

RangeWritebackDispatchResult RangeCacheWriteback::submit_consumed_range(
        const FileRange& range, Slice data, const AsyncCacheWriteEpoch& write_epoch) {
    DORIS_CHECK(write_epoch.key_token != nullptr);
    if (!_options.write_manager->accepting()) {
        return {};
    }
    RangeWritebackDispatcher dispatcher(
            _options.file_size, _options.block_size,
            [&](const FileCacheBlockFragment& fragment) {
                return _submit_complete_block(fragment, write_epoch);
            },
            [&](const FileCacheBlockFragment& fragment) {
                return _submit_partial_block(fragment, write_epoch);
            });
    return dispatcher.dispatch(range, data);
}

bool RangeCacheWriteback::_submit_complete_block(const FileCacheBlockFragment& fragment,
                                                 const AsyncCacheWriteEpoch& write_epoch) {
    DORIS_CHECK(fragment.complete());
    const auto result = _options.write_manager->try_submit_block(AsyncCacheWriteBlockRequest {
            .cache_hash = _options.cache_hash,
            .file_offset = fragment.block_offset,
            .data = fragment.data,
            .buffer_size = _options.block_size,
            .admission_ctx = _options.admission_ctx,
            .write_epoch = write_epoch,
            .inflight_index = _options.inflight_index,
    });
    return result == AsyncCacheWriteBlockSubmitResult::SUBMITTED ||
           result == AsyncCacheWriteBlockSubmitResult::ALREADY_INFLIGHT;
}

bool RangeCacheWriteback::_submit_partial_block(const FileCacheBlockFragment& fragment,
                                                const AsyncCacheWriteEpoch& write_epoch) {
    DORIS_CHECK(!fragment.complete());
    const auto result = _options.partial_block_manager->try_submit(PartialBlockWritebackRequest {
            .write_manager = _options.write_manager,
            .inflight_index = _options.inflight_index,
            .source_reader = _options.source_reader,
            .cache_hash = _options.cache_hash,
            .block_offset = fragment.block_offset,
            .block_valid_size = fragment.block_valid_size,
            .fragment_offset = fragment.fragment_offset,
            .data = fragment.data,
            .admission_ctx = _options.admission_ctx,
            .write_epoch = write_epoch,
            .io_context = _options.io_context,
    });
    return result == PartialBlockSubmitResult::QUEUED ||
           result == PartialBlockSubmitResult::MERGED ||
           result == PartialBlockSubmitResult::ACTIVE_DEDUPLICATED ||
           result == PartialBlockSubmitResult::CACHE_WRITE_INFLIGHT;
}

} // namespace doris::io
