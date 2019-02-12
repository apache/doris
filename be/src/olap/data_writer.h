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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_DATA_WRITER_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_DATA_WRITER_H

#include "olap/olap_table.h"
#include "olap/row_block.h"
#include "olap/schema.h"
#include "olap/wrapper_field.h"

namespace doris {
class RowBlock;
class SegmentWriter;

class ColumnDataWriter {
public:
    // Factory function
    // 调用者获得新建的对象, 并负责delete释放
    static ColumnDataWriter* create(OLAPTablePtr table, SegmentGroup* segment_group, bool is_push_write);
    ColumnDataWriter(OLAPTablePtr table, SegmentGroup* segment_group, bool is_push_write);
    ~ColumnDataWriter();
    OLAPStatus init();
    OLAPStatus attached_by(RowCursor* row_cursor);
    OLAPStatus write(const char* row);
    void next(const RowCursor& row_cursor);
    void next(const char* row, const Schema* schema);
    OLAPStatus finalize();
    uint64_t written_bytes();
    MemPool* mem_pool();
private:
    OLAPStatus _add_segment();
    OLAPStatus _flush_segment_with_verfication();
    OLAPStatus _finalize_segment();
    OLAPStatus _flush_row_block(bool finalize);
    OLAPStatus _init_segment();

    bool _is_push_write;
    OLAPTablePtr _table;
    // first is min, second is max
    std::vector<std::pair<WrapperField*, WrapperField*>> _column_statistics;
    uint32_t _row_index;

    SegmentGroup* _segment_group;
    RowBlock* _row_block;      // 使用RowBlcok缓存要写入的数据
    RowCursor _cursor;
    SegmentWriter* _segment_writer;
    int64_t _num_rows;
    uint32_t _block_id;        // 当前Segment内的block编号
    uint32_t _max_segment_size;
    uint32_t _segment;
    int64_t _all_num_rows;
    bool _new_segment_created;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_DATA_WRITER_H
