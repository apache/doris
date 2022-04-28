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

#include "olap/olap_define.h"
#include "olap/stream_index_common.h"

namespace doris {

class PositionEntryReader {
public:
    PositionEntryReader();
    ~PositionEntryReader() {}
    // 使用前需要初始化，需要header来计算每一组position/stat的偏移量
    Status init(StreamIndexHeader* header, FieldType type, bool null_supported);
    // attach到一块内存上读取position和stat
    void attach(char* buffer);
    // 返回指定下标的position
    int64_t positions(size_t index) const;
    // 一个entry的内存大小
    size_t entry_size() const;
    // 统计信息
    const ColumnStatistics& column_statistic() const;
    // 设置position的个数，并据此计算出偏移
    void set_positions_count(size_t count);
    // 一共有几个position
    int32_t positions_count() const;
    bool all_null() const {
        return _statistics.minimum()->is_null() && _statistics.maximum()->is_null();
    }

private:
    const uint32_t* _positions;
    size_t _positions_count;
    ColumnStatistics _statistics;
    size_t _statistics_offset;
};

class PositionProvider {
public:
    PositionProvider() : _entry(nullptr), _index(0) {}
    explicit PositionProvider(const PositionEntryReader* entry) : _entry(entry), _index(0) {}
    ~PositionProvider() {}
    int64_t get_next() { return _entry->positions(_index++); }
    bool all_null() const { return _entry->all_null(); }

private:
    const PositionEntryReader* _entry;
    int _index;
};

class StreamIndexReader {
public:
    StreamIndexReader();
    ~StreamIndexReader();

    Status init(char* buffer, size_t buffer_size, FieldType type, bool is_using_cache,
                bool null_supported);
    const PositionEntryReader& entry(uint64_t entry_id);
    size_t entry_count();

protected:
    Status _parse_header(FieldType type);

private:
    char* _buffer;
    size_t _buffer_size;
    size_t _start_offset;
    size_t _step_size;
    size_t _entry_count;
    bool _is_using_cache;
    bool _null_supported;
    PositionEntryReader _entry;
};

} // namespace doris
