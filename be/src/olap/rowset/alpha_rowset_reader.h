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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H

#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_group.h"
#include "olap/rowset/column_data.h"
#include "olap/rowset/alpha_rowset_meta.h"

#include <vector>

namespace doris {

class AlphaRowsetReader : public RowsetReader {
public:
    AlphaRowsetReader(int num_key_fields, int num_short_key_fields,
        int num_rows_per_row_block, const std::string rowset_path,
        RowsetMeta* rowset_meta, std::vector<std::shared_ptr<SegmentGroup>> segment_groups);

    // reader init
    virtual OLAPStatus init(ReaderContext* read_context);

    // check whether rowset has more data
    virtual bool has_next();

    // read next block data
    virtual OLAPStatus next(RowCursor* row);

    // close reader
    virtual void close();

private:
    OLAPStatus _init_segment_groups(ReaderContext* read_context);

    OLAPStatus _init_column_datas(ReaderContext* read_context);

    OLAPStatus _get_next_row_for_singleton_rowset(RowCursor* row);

    OLAPStatus _get_next_row_for_cumulative_rowset(RowCursor* row);

    OLAPStatus _get_next_block(ColumnData* column_data, RowBlock* row_block);

    OLAPStatus _refresh_next_block(ColumnData* column_datam, RowBlock* row_block);

private:
    int _num_key_fields;
    int _num_short_key_fields;
    int _num_rows_per_row_block;
    std::string _rowset_path;
    AlphaRowsetMeta* _alpha_rowset_meta;
    std::vector<std::shared_ptr<SegmentGroup>> _segment_groups;
    std::vector<std::unique_ptr<ColumnData>> _column_datas;
    std::vector<RowBlock*> _row_blocks;
    int _key_range_size;
    int _key_range_index;
    bool _is_cumulative_rowset;
    ReaderContext* _current_read_context;
};

}

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H
