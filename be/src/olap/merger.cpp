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

#include "olap/olap_define.h"
#include "olap/rowset/segment_group.h"
#include "olap/tablet.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"

using std::list;
using std::string;
using std::unique_ptr;
using std::vector;

namespace doris {

Merger::Merger(TabletSharedPtr tablet, RowsetWriterSharedPtr writer, ReaderType type) :
        _tablet(tablet),
        _output_rs_writer(writer),
        _reader_type(type),
        _row_count(0) {}

Merger::Merger(TabletSharedPtr tablet, ReaderType type, RowsetWriterSharedPtr writer,
               const std::vector<RowsetReaderSharedPtr>& rs_readers) :
        _tablet(tablet),
        _output_rs_writer(writer),
        _input_rs_readers(rs_readers),
        _reader_type(type),
        _row_count(0) {}

OLAPStatus Merger::merge(const vector<RowsetReaderSharedPtr>& rs_readers,
                         int64_t* merged_rows, int64_t* filted_rows) {
    _input_rs_readers = rs_readers;
    OLAPStatus res = merge();
    if (res == OLAP_SUCCESS) {
        *merged_rows= _merged_rows;
        *filted_rows = _filted_rows;
    }
    return res;
}

OLAPStatus Merger::merge() {
    // Create and initiate reader for scanning and multi-merging specified
    // OLAPDatas.
    Reader reader;
    ReaderParams reader_params;
    reader_params.tablet = _tablet;
    reader_params.reader_type = _reader_type;
    reader_params.rs_readers = _input_rs_readers;
    reader_params.version = _output_rs_writer->version();

    if (OLAP_SUCCESS != reader.init(reader_params)) {
        LOG(WARNING) << "fail to initiate reader. tablet=" << _tablet->full_name();
        return OLAP_ERR_INIT_FAILED;
    }

    bool has_error = false;
    RowCursor row_cursor;

    if (OLAP_SUCCESS != row_cursor.init(_tablet->tablet_schema())) {
        LOG(WARNING) << "fail to init row cursor.";
        has_error = true;
    }

    std::unique_ptr<Arena> arena(new Arena());
    bool eof = false;
    row_cursor.allocate_memory_for_string_type(_tablet->tablet_schema());
    // The following procedure would last for long time, half of one day, etc.
    while (!has_error) {
        // Read one row into row_cursor
        OLAPStatus res = reader.next_row_with_aggregation(&row_cursor, arena.get(), &eof);
        if (OLAP_SUCCESS == res && eof) {
            VLOG(3) << "reader read to the end.";
            break;
        } else if (OLAP_SUCCESS != res) {
            LOG(WARNING) << "reader read failed.";
            has_error = true;
            break;
        }

        if (OLAP_SUCCESS != _output_rs_writer->add_row(row_cursor)) {
            LOG(WARNING) << "add row to builder failed. tablet=" << _tablet->full_name();
            has_error = true;
            break;
        }

        // the memory allocate by arena has been copied,
        // so we should release these memory immediately
        arena.reset(new Arena());

        // Goto next row position in the row block being written
        ++_row_count;
    }

    if (_output_rs_writer->flush() != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to finalize writer. "
                     << "tablet=" << _tablet->full_name();
        has_error = true;
    }

    if (!has_error) {
        _merged_rows = reader.merged_rows();
        _filted_rows = reader.filtered_rows();
    }

    return has_error ? OLAP_ERR_OTHER_ERROR : OLAP_SUCCESS;
}
}  // namespace doris
