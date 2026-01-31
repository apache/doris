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

#include <memory_resource>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/status.h"

namespace doris {
#include "common/compile_check_begin.h"

class TrackableResource : public std::pmr::memory_resource {
public:
    explicit TrackableResource(
            std::pmr::memory_resource* upstream = std::pmr::get_default_resource())
            : _upstream(upstream) {}

    std::size_t bytes_allocated() const { return _bytes_allocated; }
    void reset() { _bytes_allocated = 0; }

private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        _bytes_allocated += bytes;
        return _upstream->allocate(bytes, alignment);
    }

    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
        _bytes_allocated -= bytes;
        _upstream->deallocate(p, bytes, alignment);
    }

    bool do_is_equal(const memory_resource& other) const noexcept override {
        return this == &other;
    }

    std::pmr::memory_resource* _upstream;
    std::size_t _bytes_allocated {0};
};

using RowIdMappingType = std::pmr::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>;

class RowIdSpillManager {
public:
    struct MetaInfo {
        uint32_t segment_count {0};
        uint64_t segment_info_offset {0};
        uint64_t next_data_offset {0};
    };

    struct SegmentInfo {
        uint32_t row_count; // src segment row count
        uint64_t offset;    // current segment offset relate to data_offset
        uint64_t size;      // actual element count spilled to file
    };

    static constexpr std::size_t ENTRY_BYTES =
            sizeof(uint32_t) * 3; // src_row_id, dst_segment_id, dst_row_id

    explicit RowIdSpillManager(std::string path) : _path(std::move(path)) {}

    ~RowIdSpillManager() {
        if (_fd >= 0) {
            close(_fd);
            unlink(_path.c_str());
        }
    }

    Status init();
    Status init_new_segment(uint32_t internal_id, uint32_t row_count);

    // spill segment data to file
    Status spill_segment_mapping(uint32_t internal_id, const RowIdMappingType& mappings);

    // Read all mappings for a segment
    Status read_segment_mapping(uint32_t internal_id, RowIdMappingType* mappings);
    Status read_segment_mapping_internal(
            uint32_t internal_id,
            const std::function<void(uint32_t, uint32_t, uint32_t)>& callback);

    std::string dump_info() const;
    std::string dump_segment_info(uint32_t internal_id) const;

private:
    std::string _path;
    int _fd {-1};
    mutable std::mutex _mutex;

    MetaInfo _header;
    // internal_id -> SegmentInfo
    std::unordered_map<uint32_t, SegmentInfo> _segment_infos;
};

#include "common/compile_check_end.h"
} // namespace doris