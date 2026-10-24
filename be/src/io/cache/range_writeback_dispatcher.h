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
#include <functional>

#include "io/fs/file_range_coalescer.h"
#include "util/slice.h"

namespace doris::io {

/// One contiguous fragment of a File Cache block. `block_valid_size` is shorter than the fixed
/// block size only at physical EOF. The data slice remains valid only during the consumer call.
struct FileCacheBlockFragment {
    size_t block_offset {0};
    size_t block_valid_size {0};
    size_t fragment_offset {0};
    Slice data;

    bool complete() const { return fragment_offset == 0 && data.size == block_valid_size; }
};

using FileCacheBlockFragmentConsumer = std::function<bool(const FileCacheBlockFragment& fragment)>;

struct RangeWritebackDispatchResult {
    /// Fragments classified by whether they cover all valid bytes of their cache block.
    size_t complete_block_count {0};
    size_t partial_fragment_count {0};
    size_t complete_block_bytes {0};
    size_t partial_fragment_bytes {0};
    /// Fragments whose selected consumer returned true.
    size_t submitted_complete_block_count {0};
    size_t submitted_partial_fragment_count {0};
};

/// Splits an already consumed query range at File Cache block boundaries. Consumers run
/// synchronously and must copy or otherwise acquire ownership of the fragment before returning.
class RangeWritebackDispatcher {
public:
    /// Both consumers are required and run inline in dispatch().
    RangeWritebackDispatcher(size_t file_size, size_t block_size,
                             FileCacheBlockFragmentConsumer complete_block_consumer,
                             FileCacheBlockFragmentConsumer partial_block_consumer);

    /// `range` must be non-empty and contained by the file, while `data` must cover it exactly.
    /// The result counts every classified fragment and each consumer's accepted submissions.
    RangeWritebackDispatchResult dispatch(const FileRange& range, Slice data) const;

private:
    const size_t _file_size;
    const size_t _block_size;
    const FileCacheBlockFragmentConsumer _complete_block_consumer;
    const FileCacheBlockFragmentConsumer _partial_block_consumer;
};

} // namespace doris::io
