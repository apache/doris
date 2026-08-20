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
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "storage/olap_common.h"

namespace doris {

struct RowsetWriterContext;

namespace segment_v2 {

struct SegmentIndexFileCacheRange {
    uint64_t offset = 0;
    uint64_t size = 0;

    bool empty() const { return size == 0; }

    bool is_valid_for(uint64_t file_size) const {
        return !empty() && offset < file_size && size <= file_size - offset;
    }
};

struct SegmentIndexFileCacheInfo {
    uint64_t segment_file_size = 0;
    std::vector<SegmentIndexFileCacheRange> index_ranges;

    void add_index_range(uint64_t offset, uint64_t size) {
        if (size == 0) {
            return;
        }
        index_ranges.push_back({offset, size});
    }

    bool empty() const { return index_ranges.empty(); }

    uint64_t cache_start_offset() const {
        return empty() ? segment_file_size : index_ranges.front().offset;
    }
};

struct SegmentIndexFileCachePreloadTask {
    uint32_t segment_id = 0;
    SegmentIndexFileCacheInfo info;
};

enum class SegmentIndexFileCacheLoadReason {
    LOAD,
    CUMULATIVE_COMPACTION,
    BASE_COMPACTION,
    SCHEMA_CHANGE,
};

struct SegmentIndexFileCacheLoadContext {
    io::FileSystemSPtr fs;
    std::string segment_path;
    RowsetId rowset_id;
    int64_t tablet_id = 0;
    uint32_t segment_id = 0;
    SegmentIndexFileCacheRange range;
    uint64_t segment_file_size = 0;
    SegmentIndexFileCacheLoadReason reason = SegmentIndexFileCacheLoadReason::LOAD;
};

class SegmentIndexFileCacheLoader {
public:
    static Status preload_segment_index_to_file_cache(const RowsetWriterContext& context,
                                                      uint32_t segment_id,
                                                      const std::string& segment_path,
                                                      const SegmentIndexFileCacheInfo& info);

    static Status preload_segment_indexes_to_file_cache(
            const RowsetWriterContext& context,
            const std::vector<SegmentIndexFileCachePreloadTask>& tasks);

    static Status load_segment_index_to_file_cache(const SegmentIndexFileCacheLoadContext& ctx);
};

} // namespace segment_v2
} // namespace doris
