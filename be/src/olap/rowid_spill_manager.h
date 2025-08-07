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
    // File layout:
    // | FileHeader | SegmentInfo[N] | SegmentData[N] |
    struct FileHeader {
        uint32_t magic {0x52494443}; // "RIDC"
        uint32_t version {1};
        uint32_t segment_count {0};
        uint64_t segment_info_offset {0};
        uint64_t data_offset {0};
    } __attribute__((packed));

    struct SegmentInfo {
        uint32_t segment_id;
        uint32_t row_count;
        uint64_t offset;
        uint64_t size;
    } __attribute__((packed));

    explicit RowIdSpillManager(const std::string& path) : _path(path) {}

    ~RowIdSpillManager() {
        if (_fd >= 0) {
            close(_fd);
            unlink(_path.c_str());
        }
    }

    // Initialize spill file with segment information
    Status init(const std::vector<uint32_t>& segment_row_counts);

    // Write segment data to spill file
    Status spill_segment(
            uint32_t segment_id,
            const std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>& mappings);

    // Read all mappings for a segment
    Status read_segment(
            uint32_t segment_id,
            std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>* mappings) const;

private:
    static uint32_t _calc_checksum(const void* data, size_t len) {
        uint32_t checksum = 0;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(data);
        for (size_t i = 0; i < len; i++) {
            checksum = ((checksum << 5) + checksum) + p[i];
        }
        return checksum;
    }

    std::string _path;
    int _fd {-1};
    mutable std::mutex _mutex;

    FileHeader _header;
    std::vector<SegmentInfo> _segment_infos;
};

} // namespace doris