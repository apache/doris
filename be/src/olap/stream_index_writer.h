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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_WRITER_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_WRITER_H

#include <vector>

#include "olap/file_stream.h"
#include "olap/stream_index_common.h"

namespace doris {

// 目前写入和读取分离了，主要是因为写入用了一个定长的buffer
// 不过实际上可以合并
class PositionEntryWriter {
public:
    PositionEntryWriter();
    // 获取指定位置的position信息
    int64_t positions(size_t index) const;
    // 获取position的个数
    int32_t positions_count() const;
    // 设置统计信息
    OLAPStatus set_statistic(ColumnStatistics* statistic);
    // 判断当前entry是否写入了统计信息
    bool has_statistic() const;
    // 输出到buffer的大小，用来预先分配内存用
    int32_t output_size() const;
    // 增加一个position, 目前给出的上限是16
    // ，目前不会有超出这个数量的位置信息
    OLAPStatus add_position(uint32_t position);
    // 实际上是重置position，便于重写时写入正确的位置
    void reset_write_offset();
    // 移除从from起，count个已经写入的位置，用于去除present流
    OLAPStatus remove_written_position(uint32_t from, size_t count);
    // 将内容输出到outbuffer中。
    void write_to_buffer(char* out_buffer);

private:
    uint32_t _positions[MAX_POSITION_SIZE];
    size_t _positions_count;
    char _statistics_buffer[MAX_STATISTIC_LENGTH];
    size_t _statistics_size;
};

class StreamIndexWriter {
public:
    StreamIndexWriter(FieldType field_type = OLAP_FIELD_TYPE_NONE);
    ~StreamIndexWriter();

    OLAPStatus add_index_entry(const PositionEntryWriter& entry);
    PositionEntryWriter* mutable_entry(uint32_t index);
    size_t entry_size();
    OLAPStatus reset();

    size_t output_size();
    OLAPStatus write_to_buffer(char* buffer, size_t buffer_size);

protected:
    std::vector<PositionEntryWriter> _index_to_write;
    StreamIndexHeader _header;
    FieldType _field_type;
};

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_WRITER_H
