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

#include "olap/stream_index_reader.h"

namespace doris {

PositionEntryReader::PositionEntryReader()
        : _positions(nullptr), _positions_count(0), _statistics_offset(0) {}

OLAPStatus PositionEntryReader::init(StreamIndexHeader* header, FieldType type,
                                     bool null_supported) {
    if (nullptr == header) {
        return OLAP_ERR_INIT_FAILED;
    }

    set_positions_count(header->position_format);

    if (OLAP_SUCCESS != _statistics.init(type, null_supported)) {
        return OLAP_ERR_INIT_FAILED;
    }

    return OLAP_SUCCESS;
}

void PositionEntryReader::attach(char* buffer) {
    _positions = reinterpret_cast<const uint32_t*>(buffer);
    _statistics.attach(buffer + _statistics_offset);
}

int64_t PositionEntryReader::positions(size_t index) const {
    if (index < _positions_count) {
        return static_cast<int64_t>(_positions[index]);
    }

    return -1;
}

size_t PositionEntryReader::entry_size() const {
    return sizeof(uint32_t) * _positions_count + _statistics.size();
}

const ColumnStatistics& PositionEntryReader::column_statistic() const {
    return _statistics;
}

void PositionEntryReader::set_positions_count(size_t count) {
    _positions_count = count;
    _statistics_offset = count * sizeof(uint32_t);
}

int32_t PositionEntryReader::positions_count() const {
    return _positions_count;
}

StreamIndexReader::StreamIndexReader()
        : _buffer(nullptr),
          _buffer_size(0),
          _start_offset(0),
          _step_size(0),
          _is_using_cache(false),
          _entry() {}

/////////////////////////////////////////////////////////////////////////////

StreamIndexReader::~StreamIndexReader() {
    if (!_is_using_cache) {
        SAFE_DELETE_ARRAY(_buffer);
    }
}

OLAPStatus StreamIndexReader::init(char* buffer, size_t buffer_size, FieldType type,
                                   bool is_using_cache, bool null_supported) {
    if (nullptr == buffer) {
        OLAP_LOG_WARNING("buffer given is invalid.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    _buffer = buffer;
    _buffer_size = buffer_size;
    _is_using_cache = is_using_cache;
    _null_supported = null_supported;
    OLAPStatus res = _parse_header(type);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to parse header");
        return res;
    }

    return OLAP_SUCCESS;
}

const PositionEntryReader& StreamIndexReader::entry(uint64_t entry_id) {
    _entry.attach(_buffer + _start_offset + _step_size * entry_id);
    return _entry;
}

size_t StreamIndexReader::entry_count() {
    return _entry_count;
}

OLAPStatus StreamIndexReader::_parse_header(FieldType type) {
    if (_buffer_size < sizeof(StreamIndexHeader)) {
        OLAP_LOG_WARNING("invalid length");
        return OLAP_ERR_OUT_OF_BOUND;
    }

    StreamIndexHeader* header = reinterpret_cast<StreamIndexHeader*>(_buffer);
    OLAPStatus res = OLAP_SUCCESS;

    res = _entry.init(header, type, _null_supported);

    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to init statistic reader");
        return OLAP_ERR_INIT_FAILED;
    }

    _start_offset = sizeof(StreamIndexHeader);
    _step_size = _entry.entry_size();
    _entry_count = header->block_count;

    if (_entry_count * _step_size + _start_offset > _buffer_size) {
        LOG(WARNING) << "invalid header length, entry_count=" << _entry_count
                     << ", step_size=" << _step_size << ", start_offset=" << _start_offset
                     << ", buffer_size=" << _buffer_size;
        return OLAP_ERR_FILE_FORMAT_ERROR;
    }

    return res;
}

} // namespace doris
