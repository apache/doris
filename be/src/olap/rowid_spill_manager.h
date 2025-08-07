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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "util/uid_util.h"

namespace doris {

class RowIdSpillManager {
public:
    struct MetaInfo {
        uint32_t segment_count {0};
        uint64_t segment_info_offset {0};
        uint64_t data_offset {0};
    };

    struct SegmentInfo {
        uint32_t row_count; // src segment row count
        uint64_t offset;    // current segment offset relate to data_offset
        uint64_t size;      // actual element count spilled to file
    };

    static constexpr size_t ENTRY_BYTES =
            sizeof(uint32_t) * 3; // src_row_id, dst_segment_id, dst_row_id

    explicit RowIdSpillManager(const std::string& path) : _path(path) {}

    ~RowIdSpillManager() {
        if (_fd >= 0) {
            close(_fd);
            unlink(_path.c_str());
        }
    }

    Status init(const std::vector<uint32_t>& segment_row_counts);

    // Write segment data to spill file
    Status spill_segment_mapping(
            uint32_t segment_id,
            const std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>& mappings);

    // Read all mappings for a segment
    Status read_segment_mapping(
            uint32_t segment_id,
            std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>* mappings);
    Status read_segment_mapping_internal(
            uint32_t segment_id, const std::function<void(uint32_t, uint32_t, uint32_t)>& callback);

private:
    std::string _path;
    int _fd {-1};
    mutable std::mutex _mutex;

    MetaInfo _header;
    // segment_id -> SegmentInfo
    std::vector<SegmentInfo> _segment_infos;
};

} // namespace doris