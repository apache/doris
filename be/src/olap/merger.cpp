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
#include "olap/tablet.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "util/trace.h"

namespace doris {

OLAPStatus Merger::merge_rowsets(TabletSharedPtr tablet,
                                 ReaderType reader_type,
                                 const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                                 RowsetWriter* dst_rowset_writer,
                                 Merger::Statistics* stats_output) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");

    Reader reader;
    ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = reader_type;
    reader_params.rs_readers = src_rowset_readers;
    reader_params.version = dst_rowset_writer->version();
    RETURN_NOT_OK(reader.init(reader_params));

    RowCursor row_cursor;
    RETURN_NOT_OK_LOG(row_cursor.init(tablet->tablet_schema()),
                 "failed to init row cursor when merging rowsets of tablet " + tablet->full_name());
    row_cursor.allocate_memory_for_string_type(tablet->tablet_schema());

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));

    // The following procedure would last for long time, half of one day, etc.
    int64_t output_rows = 0;
    while (true) {
        ObjectPool objectPool;
        bool eof = false;
        // Read one row into row_cursor
        RETURN_NOT_OK_LOG(reader.next_row_with_aggregation(&row_cursor, mem_pool.get(), &objectPool, &eof),
                          "failed to read next row when merging rowsets of tablet " + tablet->full_name());
        if (eof) {
            break;
        }
        RETURN_NOT_OK_LOG(dst_rowset_writer->add_row(row_cursor),
                          "failed to write row when merging rowsets of tablet " + tablet->full_name());
        output_rows++;
        LOG_IF(INFO, config::row_step_for_compaction_merge_log != 0 && output_rows % config::row_step_for_compaction_merge_log == 0)
              << "Merge rowsets stay alive. "
              << "tablet=" << tablet->full_name()
              << ", merged rows=" << output_rows;
        // the memory allocate by mem pool has been copied,
        // so we should release memory immediately
        mem_pool->clear();
    }

    if (stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.filtered_rows();
    }

    RETURN_NOT_OK_LOG(dst_rowset_writer->flush(),
                 "failed to flush rowset when merging rowsets of tablet " + tablet->full_name());
    return OLAP_SUCCESS;
}

}  // namespace doris
