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

#include "olap/stream_index_writer.h"

#include <vector>

namespace doris {

PositionEntryWriter::PositionEntryWriter() : _positions_count(0), _statistics_size(0) {
    memset(_statistics_buffer, 0, sizeof(_statistics_buffer));
}

int64_t PositionEntryWriter::positions(size_t index) const {
    if (index < _positions_count) {
        return static_cast<int64_t>(_positions[index]);
    }

    return -1;
}

Status PositionEntryWriter::set_statistic(ColumnStatistics* statistic) {
    _statistics_size = statistic->size();
    return statistic->write_to_buffer(_statistics_buffer, MAX_STATISTIC_LENGTH);
}

bool PositionEntryWriter::has_statistic() const {
    return _statistics_size != 0;
}

int32_t PositionEntryWriter::positions_count() const {
    return _positions_count;
}

int32_t PositionEntryWriter::output_size() const {
    return _positions_count * sizeof(uint32_t) + _statistics_size;
}

Status PositionEntryWriter::add_position(uint32_t position) {
    if (_positions_count < MAX_POSITION_SIZE) {
        _positions[_positions_count] = position;
        _positions_count++;
        return Status::OK();
    }

    return Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW);
}

void PositionEntryWriter::reset_write_offset() {
    _positions_count = 0;
}

Status PositionEntryWriter::remove_written_position(uint32_t from, size_t count) {
    if (from + count > _positions_count) {
        return Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND);
    }

    for (size_t i = from; i < _positions_count - count; ++i) {
        _positions[i] = _positions[i + count];
    }

    _positions_count -= count;
    return Status::OK();
}

void PositionEntryWriter::write_to_buffer(char* out_buffer) {
    // 实际上这样比较快
    //for(uint32_t i = 0; i < _positions_count; ++i) {
    //    buffer[i] = _positions[i];
    //}
    size_t position_output_size = _positions_count * sizeof(uint32_t);
    memcpy(out_buffer, _positions, position_output_size);
    memcpy(out_buffer + position_output_size, _statistics_buffer, _statistics_size);
}

/////////////////////////////////////////////////////////////////////////////

StreamIndexWriter::StreamIndexWriter(FieldType field_type) : _field_type(field_type) {}

StreamIndexWriter::~StreamIndexWriter() {}

Status StreamIndexWriter::add_index_entry(const PositionEntryWriter& entry) {
    try {
        _index_to_write.push_back(entry);
    } catch (...) {
        OLAP_LOG_WARNING("add entry to index vector fail");
        return Status::OLAPInternalError(OLAP_ERR_STL_ERROR);
    }

    return Status::OK();
}

PositionEntryWriter* StreamIndexWriter::mutable_entry(uint32_t index) {
    if (index < _index_to_write.size()) {
        return &_index_to_write[index];
    }

    return nullptr;
}

size_t StreamIndexWriter::entry_size() {
    return _index_to_write.size();
}

Status StreamIndexWriter::reset() {
    try {
        _index_to_write.clear();
    } catch (...) {
        OLAP_LOG_WARNING("add entry to index vector fail");
        return Status::OLAPInternalError(OLAP_ERR_STL_ERROR);
    }

    return Status::OK();
}

size_t StreamIndexWriter::output_size() {
    if (_index_to_write.size() == 0) {
        return sizeof(_header);
    } else {
        return _index_to_write.size() * _index_to_write[0].output_size() + sizeof(_header);
    }
}

Status StreamIndexWriter::write_to_buffer(char* buffer, size_t buffer_size) {
    if (nullptr == buffer) {
        OLAP_LOG_WARNING("given buffer is null");
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    if (output_size() > buffer_size) {
        OLAP_LOG_WARNING("need more buffer, size=%lu, given=%lu", output_size(), buffer_size);
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    // write header
    int32_t entry_size = 0;

    if (_index_to_write.size() != 0) {
        // entry size 包含了position和统计信息的长度
        // string列的统计信息可能是0,因此实际上写不进去
        entry_size = _index_to_write[0].output_size();
        _header.position_format = _index_to_write[0].positions_count();

        if (_index_to_write[0].has_statistic()) {
            _header.statistic_format = _field_type;
        }
    }

    _header.block_count = _index_to_write.size();
    VLOG_TRACE << "header info. pos: " << _header.position_format
               << ", stat:" << _header.statistic_format << ", entry_size:" << entry_size;
    memcpy(buffer, reinterpret_cast<char*>(&_header), sizeof(_header));
    // set offset, write data
    char* write_offset = buffer + sizeof(_header);

    for (size_t i = 0; i < _index_to_write.size(); ++i) {
        _index_to_write[i].write_to_buffer(write_offset);
        write_offset += entry_size;
    }

    return Status::OK();
}

} // namespace doris
