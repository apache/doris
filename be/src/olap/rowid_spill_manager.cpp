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

#include "util/coding.h"

namespace doris {

Status RowIdSpillManager::init() {
    std::lock_guard<std::mutex> lock(_mutex);

    _fd = open(_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (_fd < 0) {
        return Status::IOError(fmt::format("Failed to create spill file: {}", strerror(errno)));
    }

    _header.segment_count = 0;
    _header.next_data_offset = 0;

    return Status::OK();
}

Status RowIdSpillManager::init_new_segment(uint32_t internal_id, uint32_t row_count) {
    std::lock_guard<std::mutex> lock(_mutex);

    _segment_infos[internal_id] = SegmentInfo {
            .row_count = row_count,
            .offset = _header.next_data_offset,
            .size = 0,
    };
    _header.next_data_offset += row_count * ENTRY_BYTES;
    return Status::OK();
}

Status RowIdSpillManager::spill_segment_mapping(uint32_t internal_id,
                                                const RowIdMappingType& mappings) {
    std::lock_guard<std::mutex> lock(_mutex);

    if (internal_id >= _segment_infos.size()) {
        return Status::InvalidArgument(fmt::format("Invalid segment id: {}", internal_id));
    }

    auto& info = _segment_infos[internal_id];

    // spill current segment's mappings from memory to file
    uint64_t file_offset = info.offset + info.size * ENTRY_BYTES;
    std::string data_buffer;
    data_buffer.reserve(mappings.size() * ENTRY_BYTES);
    uint64_t count = 0;
    for (const auto& [row_id, value] : mappings) {
        if (value.first == UINT32_MAX && value.second == UINT32_MAX) {
            continue; // Skip empty mappings
        }
        put_fixed32_le(&data_buffer, row_id);
        put_fixed32_le(&data_buffer, value.first);
        put_fixed32_le(&data_buffer, value.second);
        ++count;
    }
    info.size += count;
    ssize_t bytes_written = ::pwrite(_fd, data_buffer.data(), data_buffer.size(), file_offset);
    if (bytes_written != static_cast<ssize_t>(data_buffer.size())) {
        return Status::IOError(fmt::format("Failed to write segment data: {}", strerror(errno)));
    }
    return Status::OK();
}

Status RowIdSpillManager::read_segment_mapping(uint32_t segment_id, RowIdMappingType* mappings) {
    return read_segment_mapping_internal(
            segment_id, [&](uint32_t src_row_id, uint32_t dst_segment_id, uint32_t dst_row_id) {
                mappings->emplace(src_row_id, std::make_pair(dst_segment_id, dst_row_id));
            });
}

Status RowIdSpillManager::read_segment_mapping_internal(
        uint32_t segment_id, const std::function<void(uint32_t, uint32_t, uint32_t)>& callback) {
    std::lock_guard<std::mutex> lock(_mutex);

    if (segment_id >= _segment_infos.size()) {
        return Status::InvalidArgument(fmt::format("Invalid segment id: {}", segment_id));
    }

    const auto& info = _segment_infos[segment_id];
    if (info.size == 0) {
        return Status::OK(); // Empty segment
    }

    uint64_t file_offset = info.offset;
    std::size_t bytes = info.size * ENTRY_BYTES;
    std::string buffer(bytes, '\0');
    ssize_t bytes_read = ::pread(_fd, buffer.data(), bytes, file_offset);
    if (bytes_read != static_cast<ssize_t>(bytes)) {
        return Status::IOError(fmt::format("Failed to read segment data: {}", strerror(errno)));
    }

    const char* ptr = buffer.data();
    for (std::size_t i = 0; i < info.size; i++) {
        uint32_t src_row_id = decode_fixed32_le(reinterpret_cast<const uint8_t*>(ptr));
        ptr += sizeof(uint32_t);
        uint32_t dst_segment_id = decode_fixed32_le(reinterpret_cast<const uint8_t*>(ptr));
        ptr += sizeof(uint32_t);
        uint32_t dst_row_id = decode_fixed32_le(reinterpret_cast<const uint8_t*>(ptr));
        ptr += sizeof(uint32_t);
        callback(src_row_id, dst_segment_id, dst_row_id);
    }
    return Status::OK();
}

} // namespace doris