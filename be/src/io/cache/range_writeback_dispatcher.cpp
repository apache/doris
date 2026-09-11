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

#include "io/cache/range_writeback_dispatcher.h"

#include <algorithm>
#include <utility>

#include "common/logging.h"

namespace doris::io {

RangeWritebackDispatcher::RangeWritebackDispatcher(
        size_t file_size, size_t block_size, FileCacheBlockFragmentConsumer complete_block_consumer,
        FileCacheBlockFragmentConsumer partial_block_consumer)
        : _file_size(file_size),
          _block_size(block_size),
          _complete_block_consumer(std::move(complete_block_consumer)),
          _partial_block_consumer(std::move(partial_block_consumer)) {
    DORIS_CHECK(_file_size > 0);
    DORIS_CHECK(_block_size > 0);
    DORIS_CHECK(_complete_block_consumer);
    DORIS_CHECK(_partial_block_consumer);
}

RangeWritebackDispatchResult RangeWritebackDispatcher::dispatch(const FileRange& range,
                                                                Slice data) const {
    DORIS_CHECK(range.size > 0);
    DORIS_CHECK(range.offset < _file_size);
    DORIS_CHECK(range.size <= _file_size - range.offset);
    DORIS_CHECK(data.data != nullptr);
    DORIS_CHECK(data.size == range.size);

    RangeWritebackDispatchResult result;
    size_t file_offset = range.offset;
    while (file_offset < range.end()) {
        const size_t block_offset = file_offset / _block_size * _block_size;
        const size_t block_valid_size = std::min(_block_size, _file_size - block_offset);
        const size_t fragment_end = std::min(range.end(), block_offset + block_valid_size);
        const size_t fragment_size = fragment_end - file_offset;
        FileCacheBlockFragment fragment {
                .block_offset = block_offset,
                .block_valid_size = block_valid_size,
                .fragment_offset = file_offset - block_offset,
                .data = Slice(data.data + file_offset - range.offset, fragment_size),
        };
        if (fragment.complete()) {
            ++result.complete_block_count;
            result.complete_block_bytes += fragment.data.size;
            if (_complete_block_consumer(fragment)) {
                ++result.submitted_complete_block_count;
            }
        } else {
            ++result.partial_fragment_count;
            result.partial_fragment_bytes += fragment.data.size;
            if (_partial_block_consumer(fragment)) {
                ++result.submitted_partial_fragment_count;
            }
        }
        file_offset = fragment_end;
    }
    return result;
}

} // namespace doris::io
