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

#include <stdint.h>

#include <vector>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"

namespace doris {
class KeyBoundsPB;
class RowIdConversion;
class RowsetWriter;

namespace segment_v2 {
class SegmentWriter;
} // namespace segment_v2

namespace vectorized {
class RowSourcesBuffer;
class VerticalBlockReader;
}; // namespace vectorized

class Merger {
public:
    struct Statistics {
        // number of rows written to the destination rowset after merge
        int64_t output_rows = 0;
        int64_t merged_rows = 0;
        int64_t filtered_rows = 0;
        RowIdConversion* rowid_conversion = nullptr;
    };

    // merge rows from `src_rowset_readers` and write into `dst_rowset_writer`.
    // return OK and set statistics into `*stats_output`.
    // return others on error

    static Status vmerge_rowsets(TabletSharedPtr tablet, ReaderType reader_type,
                                 TabletSchemaSPtr cur_tablet_schema,
                                 const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                                 RowsetWriter* dst_rowset_writer, Statistics* stats_output);
    static Status vertical_merge_rowsets(
            TabletSharedPtr tablet, ReaderType reader_type, TabletSchemaSPtr tablet_schema,
            const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
            RowsetWriter* dst_rowset_writer, int64_t max_rows_per_segment, Statistics* stats_output,
            std::shared_ptr<MemTracker> process_block_mem_tracker = nullptr);

public:
    // for vertical compaction
    static void vertical_split_columns(TabletSchemaSPtr tablet_schema,
                                       std::vector<std::vector<uint32_t>>* column_groups);
    static Status vertical_compact_one_group(
            TabletSharedPtr tablet, ReaderType reader_type, TabletSchemaSPtr tablet_schema,
            bool is_key, const std::vector<uint32_t>& column_group,
            vectorized::RowSourcesBuffer* row_source_buf,
            const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
            RowsetWriter* dst_rowset_writer, int64_t max_rows_per_segment, Statistics* stats_output,
            std::shared_ptr<MemTracker> process_block_mem_tracker);

    // for segcompaction
    static Status vertical_compact_one_group(TabletSharedPtr tablet, ReaderType reader_type,
                                             TabletSchemaSPtr tablet_schema, bool is_key,
                                             const std::vector<uint32_t>& column_group,
                                             vectorized::RowSourcesBuffer* row_source_buf,
                                             vectorized::VerticalBlockReader& src_block_reader,
                                             segment_v2::SegmentWriter& dst_segment_writer,
                                             int64_t max_rows_per_segment, Statistics* stats_output,
                                             uint64_t* index_size, KeyBoundsPB& key_bounds);
};

} // namespace doris
