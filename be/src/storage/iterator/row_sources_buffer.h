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

#include <glog/logging.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/pod_array.h"
#include "core/types.h"
#include "io/io_common.h"

namespace doris {

// Row source represent row location in multi-segments
// use a uint16_t to store info
// the lower 15 bits means segment_id in segment pool, and the higher 1 bits means agg flag.
// In unique-key, agg flags means this key should be deleted, this comes from two way: old version
// key or delete_sign.
class RowSource {
public:
    RowSource(uint16_t data) : _data(data) {}
    RowSource(uint16_t source_num, bool agg_flag);

    uint16_t get_source_num() const;
    bool agg_flag() const;
    void set_agg_flag(bool agg_flag);
    uint16_t data() const;

private:
    uint16_t _data;
    static const uint16_t SOURCE_FLAG = 0x7FFF;
    static const uint16_t AGG_FLAG = 0x8000;
};

/* rows source buffer
this buffer should have a memory limit, once reach memory limit, write
buffer data to tmp file.
usage:
    RowSourcesBuffer buffer(tablet_id, tablet_storage_path, reader_type);
    buffer.append()
    buffer.append()
    buffer.flush()
    buffer.seek_to_begin()
    while (buffer.has_remaining().ok()) {
        auto cur = buffer.current().get_source_num();
        auto same = buffer.same_source_count(cur, limit);
        // do copy block data
        buffer.advance(same);
    }
*/
class RowSourcesBuffer {
public:
    RowSourcesBuffer(int64_t tablet_id, const std::string& tablet_path, ReaderType reader_type)
            : _tablet_id(tablet_id), _tablet_path(tablet_path), _reader_type(reader_type) {}

    ~RowSourcesBuffer() {
        _reset_buffer();
        if (_fd > 0) {
            ::close(_fd);
        }
    }

    // write batch row source
    Status append(const std::vector<RowSource>& row_sources);
    Status flush();

    RowSource current() {
        DCHECK(_buf_idx < _buffer.size());
        return RowSource(_buffer[_buf_idx]);
    }
    void advance(int64_t step = 1) {
        DCHECK(_buf_idx + step <= _buffer.size());
        _buf_idx += step;
        _read_index += step;
    }

    uint64_t buf_idx() const { return _buf_idx; }
    uint64_t total_size() const { return _total_size; }
    uint64_t buffered_size() { return _buffer.size(); }
    bool is_source_exhausted(uint16_t source) const {
        DCHECK(source < _last_source_positions.size());
        return source < _last_source_positions.size() &&
               _read_index > _last_source_positions[source];
    }
    void set_agg_flag(uint64_t index, bool agg);
    bool get_agg_flag(uint64_t index);

    Status has_remaining();

    Status seek_to_begin();

    size_t same_source_count(uint16_t source, size_t limit);

    // return continuous agg_flag=true count from index
    size_t continuous_agg_count(uint64_t index);

    // Count continuous rows from current position that:
    // 1. Have the same source number
    // 2. Have agg_flag=false (non-aggregated rows)
    // Stops at first agg_flag=true row or different source
    // Used for batch optimization in unique_key_next_batch
    size_t same_source_continuous_non_agg_count(uint16_t source, size_t limit);

private:
    Status _create_buffer_file();
    Status _serialize();
    Status _deserialize();
    void _reset_buffer() {
        _buffer.clear();
        _buf_idx = 0;
    }

    int64_t _tablet_id;
    std::string _tablet_path;
    ReaderType _reader_type = ReaderType::UNKNOWN;
    uint64_t _buf_idx = 0;
    int _fd = -1;
    PaddedPODArray<UInt16> _buffer;
    uint64_t _total_size = 0;
    uint64_t _read_index = 0;
    std::vector<uint64_t> _last_source_positions;
};

} // namespace doris
