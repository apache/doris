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

#include "io/fs/read_ahead_range_buffer.h"

#include <cstring>
#include <utility>

#include "common/logging.h"
#include "io/fs/file_range_read_scheduler.h"
#include "io/fs/read_ahead_metrics.h"
#include "runtime/runtime_profile.h"

namespace doris::io {

ReadAheadRangeBuffer::ReadAheadRangeBuffer(std::shared_ptr<ReadAheadStatistics> statistics)
        : _statistics(std::move(statistics)) {
    DORIS_CHECK(_statistics != nullptr);
}

void ReadAheadRangeBuffer::register_slice(FileRange slice, std::shared_ptr<Range> range,
                                          size_t buffer_offset) {
    DORIS_CHECK(range != nullptr);
    const bool inserted =
            _slices.emplace(Key {slice.offset, slice.size}, Slot {std::move(range), buffer_offset})
                    .second;
    DORIS_CHECK(inserted);
}

bool ReadAheadRangeBuffer::contains(FileRange slice) const {
    return _slices.contains({slice.offset, slice.size});
}

void ReadAheadRangeBuffer::release(FileRange slice) {
    _slices.erase({slice.offset, slice.size});
}

bool ReadAheadRangeBuffer::try_read(size_t offset, Slice output, Status* status) {
    const auto slot = _slices.find({offset, output.size});
    if (slot == _slices.end()) {
        return false;
    }
    auto range = std::move(slot->second.range);
    const size_t buffer_offset = slot->second.buffer_offset;
    _slices.erase(slot);
    bool ready = false;
    {
        SCOPED_TIMER(&_statistics->wait_time);
        *status = range->read->wait(&ready);
    }
    if (!status->ok()) {
        return true;
    }
    const Slice source = range->read->slice(buffer_offset, output.size);
    std::memcpy(output.data, source.data, source.size);
    COUNTER_UPDATE(ready ? &_statistics->ready_page_hits : &_statistics->wait_page_hits, 1);
    COUNTER_UPDATE(&_statistics->consumed_page_bytes, static_cast<int64_t>(source.size));
    read_ahead_bvars().consumed_page_bytes << static_cast<int64_t>(source.size);
    // Several exact slices can share one physical GET. Publish the whole range once, when its
    // first slice reaches PageIO, preserving the existing pre-checksum consumption boundary.
    if (!range->consumed) {
        range->consumed = true;
        COUNTER_UPDATE(&_statistics->consumed_range_bytes,
                       static_cast<int64_t>(range->read->range().size));
        if (range->consumer) {
            auto consumer = std::move(range->consumer);
            consumer(*range->read);
        }
    }
    return true;
}

} // namespace doris::io
