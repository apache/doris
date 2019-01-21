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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_WRITER_H

#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_group.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/field_info.h"

#include <vector>

namespace doris {

class AlphaRowsetWriter : public RowsetWriter {
public:
    AlphaRowsetWriter();

    virtual OLAPStatus init(const RowsetWriterContext& rowset_writer_context);

    // add a row block to rowset
    virtual OLAPStatus add_row(RowCursor* row);

    virtual OLAPStatus add_row(const char* row, Schema* schema);

    virtual OLAPStatus add_row_block(RowBlock* row_block);

    // add rowset by create hard link
    virtual OLAPStatus add_rowset(RowsetSharedPtr rowset);

    virtual OLAPStatus flush();

    // get a rowset
    virtual RowsetSharedPtr build();

    // release a rowset
    virtual OLAPStatus release();

    virtual MemPool* mem_pool();

    virtual Version version();

    virtual int32_t num_rows();

private:
    void _init();

private:
    int32_t _segment_group_id;
    SegmentGroup* _cur_segment_group;
    ColumnDataWriter* _column_data_writer;
    std::shared_ptr<RowsetMeta> _current_rowset_meta;
    bool is_pending_rowset;
    int _num_rows_written;
    RowsetWriterContext _rowset_writer_context;
    std::vector<SegmentGroup*> _segment_groups;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_WRITER_H