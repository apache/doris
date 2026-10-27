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

#include "storage/iterator/row_sources_buffer.h"

#include <fcntl.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sstream>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache_factory.h"

namespace doris {

// --------------  row source  ---------------//
RowSource::RowSource(uint16_t source_num, bool agg_flag) {
    _data = (source_num & SOURCE_FLAG) | (source_num & AGG_FLAG);
    _data = agg_flag ? (_data | AGG_FLAG) : (_data & SOURCE_FLAG);
}

uint16_t RowSource::get_source_num() const {
    return _data & SOURCE_FLAG;
}

bool RowSource::agg_flag() const {
    return (_data & AGG_FLAG) != 0;
}

void RowSource::set_agg_flag(bool agg_flag) {
    _data = agg_flag ? (_data | AGG_FLAG) : (_data & SOURCE_FLAG);
}

uint16_t RowSource::data() const {
    return _data;
}

/*  --------------  row source buffer -------------   */

// current row_sources must save in memory so agg key can update agg flag
Status RowSourcesBuffer::append(const std::vector<RowSource>& row_sources) {
    if (_buffer.allocated_bytes() + row_sources.size() * sizeof(UInt16) >
        config::vertical_compaction_max_row_source_memory_mb * 1024 * 1024) {
        // Use capacity() - size() to get the truly available element slots.
        // Note: PODArrayBase::allocated_bytes() includes pad_left and pad_right,
        // which are NOT usable for storing elements. Using allocated_bytes() here
        // would over-estimate the available space and lead to a missed spill,
        // causing _buffer to grow beyond the configured memory limit when
        // push_back triggers reallocation below.
        size_t available_slots = _buffer.capacity() - _buffer.size();
        if (available_slots < row_sources.size()) {
            VLOG_NOTICE << "RowSourceBuffer is too large, serialize and reset buffer: "
                        << _buffer.allocated_bytes() << ", total size: " << _total_size;
            // serialize current buffer
            RETURN_IF_ERROR(_create_buffer_file());
            RETURN_IF_ERROR(_serialize());
            _reset_buffer();
        }
    }
    uint64_t source_position = _total_size;
    for (const auto& source : row_sources) {
        _buffer.push_back(source.data());
        auto source_num = source.get_source_num();
        if (source_num >= _last_source_positions.size()) {
            _last_source_positions.resize(source_num + 1);
        }
        _last_source_positions[source_num] = source_position++;
    }
    _total_size += row_sources.size();
    return Status::OK();
}

Status RowSourcesBuffer::seek_to_begin() {
    _buf_idx = 0;
    _read_index = 0;
    if (_fd > 0) {
        auto offset = lseek(_fd, 0, SEEK_SET);
        if (offset != 0) {
            LOG(WARNING) << "failed to seek to 0";
            return Status::InternalError("failed to seek to 0");
        }
        _reset_buffer();
    }
    return Status::OK();
}

Status RowSourcesBuffer::has_remaining() {
    if (_buf_idx < _buffer.size()) {
        return Status::OK();
    }
    DCHECK(_buf_idx == _buffer.size());
    if (_fd > 0) {
        _reset_buffer();
        auto st = _deserialize();
        if (!st.ok()) {
            return st;
        }
        return Status::OK();
    }
    return Status::EndOfFile("end of row source buffer");
}

void RowSourcesBuffer::set_agg_flag(uint64_t index, bool agg) {
    DCHECK(index < _buffer.size());
    RowSource ori(_buffer[index]);
    ori.set_agg_flag(agg);
    _buffer[index] = ori.data();
}

bool RowSourcesBuffer::get_agg_flag(uint64_t index) {
    DCHECK(index < _buffer.size());
    RowSource ori(_buffer[index]);
    return ori.agg_flag();
}

size_t RowSourcesBuffer::continuous_agg_count(uint64_t index) {
    size_t result = 1;
    int64_t start = index + 1;
    int64_t end = _buffer.size();
    while (start < end) {
        RowSource next(_buffer[start++]);
        if (next.agg_flag()) {
            ++result;
        } else {
            break;
        }
    }
    return result;
}

size_t RowSourcesBuffer::same_source_count(uint16_t source, size_t limit) {
    int result = 1;
    int64_t start = _buf_idx + 1;
    int64_t end = _buffer.size();
    while (result < limit && start < end) {
        RowSource next(_buffer[start++]);
        if (source != next.get_source_num()) {
            break;
        }
        ++result;
    }
    return result;
}

size_t RowSourcesBuffer::same_source_continuous_non_agg_count(uint16_t source, size_t limit) {
    size_t result = 0;
    int64_t start = _buf_idx;
    int64_t end = _buffer.size();
    while (result < limit && start < end) {
        RowSource rs(_buffer[start]);
        // Stop if different source or agg_flag=true
        if (rs.get_source_num() != source || rs.agg_flag()) {
            break;
        }
        ++result;
        ++start;
    }
    return result;
}

Status RowSourcesBuffer::_create_buffer_file() {
    if (_fd != -1) {
        return Status::OK();
    }
    std::stringstream file_path_ss;
    if (config::is_cloud_mode()) {
        std::vector<std::string> paths = io::FileCacheFactory::instance()->get_base_paths();
        if (paths.empty()) {
            return Status::InternalError("fail to create write buffer due to missing cache path");
        }
        std::size_t hash_val = std::hash<int64_t> {}(_tablet_id);
        int64_t idx = hash_val % paths.size();
        file_path_ss << paths[idx] << "/compaction_row_source_" << _tablet_id;
    } else {
        file_path_ss << _tablet_path << "/compaction_row_source_" << _tablet_id;
    }

    if (_reader_type == ReaderType::READER_BASE_COMPACTION) {
        file_path_ss << "_base";
    } else if (_reader_type == ReaderType::READER_CUMULATIVE_COMPACTION ||
               _reader_type == ReaderType::READER_SEGMENT_COMPACTION) {
        file_path_ss << "_cumu";
    } else if (_reader_type == ReaderType::READER_FULL_COMPACTION) {
        file_path_ss << "_full";
    } else if (_reader_type == ReaderType::READER_COLD_DATA_COMPACTION) {
        file_path_ss << "_cold";
    } else {
        DCHECK(false);
        return Status::InternalError("unknown reader type");
    }
    file_path_ss << ".XXXXXX";
    std::string file_path = file_path_ss.str();
    LOG(INFO) << "Vertical compaction row sources buffer path: " << file_path;
    _fd = mkstemp(file_path.data());
    if (_fd < 0) {
        LOG(WARNING) << "failed to create tmp file, file_path=" << file_path
                     << ", err: " << strerror(errno);
        return Status::InternalError("failed to create tmp file ");
    }
    // file will be released after fd is close
    unlink(file_path.data());
    return Status::OK();
}

Status RowSourcesBuffer::flush() {
    if (_fd > 0 && !_buffer.empty()) {
        RETURN_IF_ERROR(_serialize());
        _reset_buffer();
    }
    return Status::OK();
}

Status RowSourcesBuffer::_serialize() {
    size_t rows = _buffer.size();
    if (rows == 0) {
        return Status::OK();
    }
    // write size
    ssize_t bytes_written = ::write(_fd, &rows, sizeof(rows));
    if (bytes_written != sizeof(size_t)) {
        LOG(WARNING) << "failed to write buffer size to file, bytes_written=" << bytes_written;
        return Status::InternalError("fail to write buffer size to file");
    }
    // write data
    bytes_written = ::write(_fd, _buffer.data(), _buffer.size() * sizeof(UInt16));
    if (bytes_written != _buffer.size() * sizeof(UInt16)) {
        LOG(WARNING) << "failed to write buffer data to file, bytes_written=" << bytes_written
                     << " buffer size=" << _buffer.size() * sizeof(UInt16);
        return Status::InternalError("fail to write buffer size to file");
    }
    return Status::OK();
}

Status RowSourcesBuffer::_deserialize() {
    size_t rows = 0;
    ssize_t bytes_read = ::read(_fd, &rows, sizeof(rows));
    if (bytes_read == 0) {
        LOG(WARNING) << "end of row source buffer file";
        return Status::EndOfFile("end of row source buffer file");
    } else if (bytes_read != sizeof(size_t)) {
        LOG(WARNING) << "failed to read buffer size from file, bytes_read=" << bytes_read;
        return Status::InternalError("failed to read buffer size from file");
    }
    _buffer.resize(rows);
    auto& internal_data = _buffer;
    bytes_read = ::read(_fd, internal_data.data(), rows * sizeof(UInt16));
    if (bytes_read != rows * sizeof(UInt16)) {
        LOG(WARNING) << "failed to read buffer data from file, bytes_read=" << bytes_read
                     << ", expect bytes=" << rows * sizeof(UInt16);
        return Status::InternalError("failed to read buffer data from file");
    }
    return Status::OK();
}

} // namespace doris
