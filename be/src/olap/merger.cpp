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

#include "olap/merger.h"

#include <memory>
#include <vector>

#include "olap/column_data.h"
#include "olap/olap_define.h"
#include "olap/segment_group.h"
#include "olap/olap_table.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/data_writer.h"

using std::list;
using std::string;
using std::unique_ptr;
using std::vector;

namespace doris {

Merger::Merger(OLAPTablePtr table, SegmentGroup* segment_group, ReaderType type) : 
        _table(table),
        _segment_group(segment_group),
        _reader_type(type),
        _row_count(0) {}

OLAPStatus Merger::merge(const vector<ColumnData*>& olap_data_arr,
                         uint64_t* merged_rows, uint64_t* filted_rows) {
    // Create and initiate reader for scanning and multi-merging specified
    // OLAPDatas.
    Reader reader;
    ReaderParams reader_params;
    reader_params.olap_table = _table;
    reader_params.reader_type = _reader_type;
    reader_params.olap_data_arr = olap_data_arr;

    if (_reader_type == READER_BASE_COMPACTION) {
        reader_params.version = _segment_group->version();
    }

    if (OLAP_SUCCESS != reader.init(reader_params)) {
        OLAP_LOG_WARNING("fail to initiate reader. [table='%s']",
                _table->full_name().c_str());
        return OLAP_ERR_INIT_FAILED;
    }

    // create and initiate writer for generating new index and data files.
    unique_ptr<ColumnDataWriter> writer(ColumnDataWriter::create(_table, _segment_group, false));

    if (NULL == writer) {
        OLAP_LOG_WARNING("fail to allocate writer.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    bool has_error = false;
    RowCursor row_cursor;

    if (OLAP_SUCCESS != row_cursor.init(_table->tablet_schema())) {
        OLAP_LOG_WARNING("fail to init row cursor.");
        has_error = true;
    }

    bool eof = false;
    // The following procedure would last for long time, half of one day, etc.
    while (!has_error) {
        // Attach row cursor to the memory position of the row block being
        // written in writer.
        if (OLAP_SUCCESS != writer->attached_by(&row_cursor)) {
            OLAP_LOG_WARNING("attach row failed. [table='%s']",
                    _table->full_name().c_str());
            has_error = true;
            break;
        }
        row_cursor.allocate_memory_for_string_type(_table->tablet_schema(), writer->mem_pool());

        // Read one row into row_cursor
        OLAPStatus res = reader.next_row_with_aggregation(&row_cursor, &eof);
        if (OLAP_SUCCESS == res && eof) {
            VLOG(3) << "reader read to the end.";
            break;
        } else if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("reader read failed.");
            has_error = true;
            break;
        }

        // Goto next row position in the row block being written
        writer->next(row_cursor);
        ++_row_count;
    }

    if (has_error) {
        LOG(WARNING) << "compaction failed.";
        return OLAP_ERR_OTHER_ERROR;
    }

    if (OLAP_SUCCESS != writer->finalize()) {
        OLAP_LOG_WARNING("fail to finalize writer. [table='%s']",
                _table->full_name().c_str());
        has_error = true;
    }

    if (!has_error) {
        *merged_rows = reader.merged_rows();
        *filted_rows = reader.filted_rows();
    }

    return has_error ? OLAP_ERR_OTHER_ERROR : OLAP_SUCCESS;
}

}  // namespace doris
