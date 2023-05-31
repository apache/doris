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

#include <butil/macros.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/compaction.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"

namespace doris {
class CompactionDupnokeyOpt {
public:
    CompactionDupnokeyOpt(Compaction& parent) : _parent(parent) {};
    ~CompactionDupnokeyOpt() {};
    Status handle_duplicate_nokey_compaction();

private:
    Status _do_compact_segments(const std::map<RowsetSharedPtr, std::vector<size_t>>& to_compacts,
                                Merger::Statistics* output_stats);

    bool _duplicate_nokey_check_and_split_segments(
            std::map<RowsetSharedPtr, std::vector<size_t>>& to_compacts,
            std::map<RowsetSharedPtr, std::vector<size_t>>& to_links, size_t& compact_segments,
            size_t& compact_segment_file_size, size_t& link_segments,
            size_t& link_segment_file_size);
    Status _do_compact_duplicate_nokey_over_size(
            std::map<RowsetSharedPtr, std::vector<size_t>>& to_links,
            size_t link_segment_file_size);
    Status _get_segcompaction_reader(
            const std::map<RowsetSharedPtr, std::vector<size_t>>& to_compacts,
            TabletSharedPtr tablet, std::shared_ptr<Schema> schema, OlapReaderStatistics* stat,
            vectorized::RowSourcesBuffer& row_sources_buf, bool is_key,
            std::vector<uint32_t>& return_columns,
            std::unique_ptr<vectorized::VerticalBlockReader>* reader);

    Status vertical_compact_one_group(TabletSharedPtr tablet, ReaderType reader_type,
                                      TabletSchemaSPtr tablet_schema, bool is_key,
                                      const std::vector<uint32_t>& column_group,
                                      vectorized::VerticalBlockReader& src_block_reader,
                                      RowsetWriter* dst_rowset_writer, int64_t max_rows_per_segment,
                                      Merger::Statistics* stats_output);

private:
    Compaction& _parent;
    RowsetWriterContext _ctx;
};
} // namespace doris