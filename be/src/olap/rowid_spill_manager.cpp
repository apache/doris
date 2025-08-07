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

#include "olap/rowid_spill_manager.h"

#include <fmt/format.h>

namespace doris {

Status RowIdSpillManager::init(const std::vector<uint32_t>& segment_row_counts) {
    std::lock_guard<std::mutex> lock(_mutex);

    _fd = open(_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (_fd < 0) {
        return Status::IOError(fmt::format("Failed to create spill file: {}", strerror(errno)));
    }

    // Initialize header
    _header.segment_count = segment_row_counts.size();
    _header.segment_info_offset = sizeof(FileHeader);
    _header.data_offset =
            _header.segment_info_offset + segment_row_counts.size() * sizeof(SegmentInfo);

    // Write header placeholder
    if (write(_fd, &_header, sizeof(_header)) != sizeof(_header)) {
        return Status::IOError(fmt::format("Failed to write header: {}", strerror(errno)));
    }

    // Initialize segment infos
    uint64_t current_offset = 0;
    _segment_infos.reserve(segment_row_counts.size());

    for (size_t i = 0; i < segment_row_counts.size(); i++) {
        SegmentInfo info;
        info.segment_id = i;
        info.row_count = segment_row_counts[i];
        info.offset = current_offset;
        info.size = 0; // Will be updated when segment is spilled

        _segment_infos.push_back(info);
        current_offset += segment_row_counts[i] * sizeof(std::pair<uint32_t, uint32_t>);
    }

    // Write segment infos
    auto bytes_to_write = _segment_infos.size() * sizeof(SegmentInfo);
    if (write(_fd, _segment_infos.data(), bytes_to_write) != bytes_to_write) {
        return Status::IOError(fmt::format("Failed to write segment infos: {}", strerror(errno)));
    }

    // Pre-allocate data section
    if (fallocate(_fd, 0, _header.data_offset, current_offset) != 0) {
        return Status::IOError(fmt::format("Failed to allocate file space: {}", strerror(errno)));
    }

    return Status::OK();
}

Status RowIdSpillManager::spill_segment(
        uint32_t segment_id,
        const std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>& mappings) {
    std::lock_guard<std::mutex> lock(_mutex);

    if (segment_id >= _segment_infos.size()) {
        return Status::InvalidArgument(fmt::format("Invalid segment id: {}", segment_id));
    }

    auto& info = _segment_infos[segment_id];

    // Sort mappings by row_id for sequential write
    std::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t>>> sorted_entries(mappings.begin(),
                                                                                   mappings.end());
    std::sort(sorted_entries.begin(), sorted_entries.end());

    // Write mappings to file
    uint64_t file_offset = _header.data_offset + info.offset;
    for (const auto& [row_id, value] : sorted_entries) {
        if (pwrite(_fd, &value, sizeof(value), file_offset) != sizeof(value)) {
            return Status::IOError(fmt::format("Failed to write mapping: {}", strerror(errno)));
        }
        file_offset += sizeof(value);
    }

    // Update segment info
    info.size = sorted_entries.size() * sizeof(std::pair<uint32_t, uint32_t>);

    // Update segment info in file
    uint64_t info_offset = _header.segment_info_offset + segment_id * sizeof(SegmentInfo);
    if (pwrite(_fd, &info, sizeof(info), info_offset) != sizeof(info)) {
        return Status::IOError(fmt::format("Failed to update segment info: {}", strerror(errno)));
    }

    return Status::OK();
}

Status RowIdSpillManager::read_segment(
        uint32_t segment_id,
        std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>>* mappings) const {
    std::lock_guard<std::mutex> lock(_mutex);

    if (segment_id >= _segment_infos.size()) {
        return Status::InvalidArgument(fmt::format("Invalid segment id: {}", segment_id));
    }

    const auto& info = _segment_infos[segment_id];
    if (info.size == 0) {
        return Status::OK(); // Empty segment
    }

    // Read all mappings
    std::vector<std::pair<uint32_t, uint32_t>> values(info.row_count);
    uint64_t file_offset = _header.data_offset + info.offset;

    ssize_t bytes_read = pread(_fd, values.data(), info.size, file_offset);
    if (bytes_read != info.size) {
        return Status::IOError(fmt::format("Failed to read segment data: {}", strerror(errno)));
    }

    // Convert to map
    mappings->clear();
    for (uint32_t i = 0; i < info.row_count; i++) {
        if (values[i].first != UINT32_MAX || values[i].second != UINT32_MAX) {
            (*mappings)[i] = values[i];
        }
    }

    return Status::OK();
}

} // namespace doris