// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_SEGMENT_WRITER_H
#define BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_SEGMENT_WRITER_H

#include "olap/olap_define.h"
#include "olap/writer.h"

namespace palo {
namespace column_file {

class ColumnWriter;
class OutStreamFactory;
class ColumnDataHeaderMessage;

// 列文件格式的Writer,接口参考IWriter中的定义
class SegmentWriter {
public:
    explicit SegmentWriter(const std::string& file_name,
            SmartOLAPTable table,
            uint32_t stream_buffer_size);
    ~SegmentWriter();
    OLAPStatus init(uint32_t write_mbytes_per_sec);
    // 写入一行数据, 使用row_cursor读取每个列
    OLAPStatus write(RowCursor* row_cursor);
    // 记录index信息
    OLAPStatus create_row_index_entry();
    // 通过对缓存的使用,预估最终segment的大小
    uint64_t estimate_segment_size();
    // 生成文件并写入缓存的数据
    OLAPStatus finalize(uint32_t* segment_file_size);

    bool is_row_block_full() {
        return (_row_in_block >= _table->num_rows_per_row_block()) ? true : false;
    }
private:
    // Helper: 生成最终的PB文件头
    OLAPStatus _make_file_header(ColumnDataHeaderMessage* file_header);

    std::string _file_name;
    SmartOLAPTable _table;
    uint32_t _stream_buffer_size; // 输出缓冲区大小
    std::vector<ColumnWriter*> _root_writers;
    OutStreamFactory* _stream_factory;
    uint64_t _row_count;    // 已经写入的行总数
    uint64_t _row_in_block; // 当前block中的数据
    uint64_t _block_count;  // 已经写入的block个数

    // write limit
    uint32_t _write_mbytes_per_sec;

    DISALLOW_COPY_AND_ASSIGN(SegmentWriter);
};

}  // namespace column_file
}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_SEGMENT_WRITER_H
