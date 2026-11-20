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

#include <functional>
#include <map>
#include <memory>

#include "io/fs/file_range_coalescer.h"

namespace doris {
class Status;
struct Slice;
namespace io {
class FileRangeRead;
struct ReadAheadStatistics;

using ReadAheadRangeConsumer = std::function<void(const FileRangeRead&)>;
using ReadAheadRangeConsumerFactory = std::function<ReadAheadRangeConsumer()>;

/// Foreground-owned exact slices of scheduled ranges. Workers only access FileRangeRead; they
/// never access this registry or call its consumer. No column or scan state is retained here.
class ReadAheadRangeBuffer {
public:
    struct Range {
        std::shared_ptr<FileRangeRead> read;
        ReadAheadRangeConsumer consumer;
        bool consumed {false};
    };

    explicit ReadAheadRangeBuffer(std::shared_ptr<ReadAheadStatistics> statistics);

    void register_slice(FileRange slice, std::shared_ptr<Range> range, size_t buffer_offset);
    bool contains(FileRange slice) const;
    void release(FileRange slice);

    /// Returns false for an unregistered slice. A registered slice is retired on either outcome;
    /// a failed status lets the caller read the original source instead.
    bool try_read(size_t offset, Slice output, Status* status);

private:
    struct Key {
        size_t offset;
        size_t size;

        bool operator<(const Key& other) const {
            return offset < other.offset || (offset == other.offset && size < other.size);
        }
    };
    struct Slot {
        std::shared_ptr<Range> range;
        size_t buffer_offset;
    };

    const std::shared_ptr<ReadAheadStatistics> _statistics;
    std::map<Key, Slot> _slices;
};

} // namespace io
} // namespace doris
