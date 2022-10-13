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
#include "olap/row_cursor.h"
#include "olap/tablet.h"
#include "olap/tuple_reader.h"
#include "util/trace.h"
#include "vec/olap/block_reader.h"

namespace doris {

Status Merger::merge_rowsets(TabletSharedPtr tablet, ReaderType reader_type,
                             TabletSchemaSPtr cur_tablet_schema,
                             const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                             RowsetWriter* dst_rowset_writer, Merger::Statistics* stats_output) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");

    TupleReader reader;
    TabletReader::ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = reader_type;
    reader_params.rs_readers = src_rowset_readers;
    reader_params.version = dst_rowset_writer->version();
    {
        std::shared_lock rdlock(tablet->get_header_lock());
        auto delete_preds = tablet->delete_predicates();
        std::copy(delete_preds.cbegin(), delete_preds.cend(),
                  std::inserter(reader_params.delete_predicates,
                                reader_params.delete_predicates.begin()));
    }
    TabletSchemaSPtr merge_tablet_schema = std::make_shared<TabletSchema>();
    merge_tablet_schema->copy_from(*cur_tablet_schema);
    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred_rs : reader_params.delete_predicates) {
        merge_tablet_schema->merge_dropped_columns(tablet->tablet_schema(del_pred_rs->version()));
    }
    reader_params.tablet_schema = merge_tablet_schema;
    RETURN_NOT_OK(reader.init(reader_params));

    RowCursor row_cursor;
    RETURN_NOT_OK_LOG(
            row_cursor.init(cur_tablet_schema),
            "failed to init row cursor when merging rowsets of tablet " + tablet->full_name());
    row_cursor.allocate_memory_for_string_type(cur_tablet_schema);

    std::unique_ptr<MemPool> mem_pool(new MemPool());

    // The following procedure would last for long time, half of one day, etc.
    int64_t output_rows = 0;
    while (true) {
        ObjectPool objectPool;
        bool eof = false;
        // Read one row into row_cursor
        RETURN_NOT_OK_LOG(
                reader.next_row_with_aggregation(&row_cursor, mem_pool.get(), &objectPool, &eof),
                "failed to read next row when merging rowsets of tablet " + tablet->full_name());
        if (eof) {
            break;
        }
        RETURN_NOT_OK_LOG(
                dst_rowset_writer->add_row(row_cursor),
                "failed to write row when merging rowsets of tablet " + tablet->full_name());
        output_rows++;
        LOG_IF(INFO, config::row_step_for_compaction_merge_log != 0 &&
                             output_rows % config::row_step_for_compaction_merge_log == 0)
                << "Merge rowsets stay alive. "
                << "tablet=" << tablet->full_name() << ", merged rows=" << output_rows;
        // the memory allocate by mem pool has been copied,
        // so we should release memory immediately
        mem_pool->clear();
    }

    if (stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.filtered_rows();
    }

    RETURN_NOT_OK_LOG(
            dst_rowset_writer->flush(),
            "failed to flush rowset when merging rowsets of tablet " + tablet->full_name());
    return Status::OK();
}

Status Merger::vmerge_rowsets(TabletSharedPtr tablet, ReaderType reader_type,
                              TabletSchemaSPtr cur_tablet_schema,
                              const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                              RowsetWriter* dst_rowset_writer, Statistics* stats_output) {
    TRACE_COUNTER_SCOPE_LATENCY_US("merge_rowsets_latency_us");

    vectorized::BlockReader reader;
    TabletReader::ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = reader_type;
    reader_params.rs_readers = src_rowset_readers;
    reader_params.version = dst_rowset_writer->version();
    {
        std::shared_lock rdlock(tablet->get_header_lock());
        auto delete_preds = tablet->delete_predicates();
        std::copy(delete_preds.cbegin(), delete_preds.cend(),
                  std::inserter(reader_params.delete_predicates,
                                reader_params.delete_predicates.begin()));
    }
    TabletSchemaSPtr merge_tablet_schema = std::make_shared<TabletSchema>();
    merge_tablet_schema->copy_from(*cur_tablet_schema);
    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred_rs : reader_params.delete_predicates) {
        merge_tablet_schema->merge_dropped_columns(tablet->tablet_schema(del_pred_rs->version()));
    }
    reader_params.tablet_schema = merge_tablet_schema;
    if (tablet->enable_unique_key_merge_on_write()) {
        reader_params.delete_bitmap = &tablet->tablet_meta()->delete_bitmap();
    }

    if (stats_output && stats_output->rowid_conversion) {
        reader_params.record_rowids = true;
    }

    reader_params.return_columns.resize(cur_tablet_schema->num_columns());
    std::iota(reader_params.return_columns.begin(), reader_params.return_columns.end(), 0);
    reader_params.origin_return_columns = &reader_params.return_columns;
    RETURN_NOT_OK(reader.init(reader_params));

    if (reader_params.record_rowids) {
        stats_output->rowid_conversion->set_dst_rowset_id(dst_rowset_writer->rowset_id());
        // init segment rowid map for rowid conversion
        std::vector<uint32_t> segment_num_rows;
        for (auto& rs_reader : reader_params.rs_readers) {
            RETURN_NOT_OK(rs_reader->get_segment_num_rows(&segment_num_rows));
            stats_output->rowid_conversion->init_segment_map(rs_reader->rowset()->rowset_id(),
                                                             segment_num_rows);
        }
    }

    vectorized::Block block = cur_tablet_schema->create_block(reader_params.return_columns);
    size_t output_rows = 0;
    bool eof = false;
    while (!eof) {
        // Read one block from block reader
        RETURN_NOT_OK_LOG(
                reader.next_block_with_aggregation(&block, nullptr, nullptr, &eof),
                "failed to read next block when merging rowsets of tablet " + tablet->full_name());
        RETURN_NOT_OK_LOG(
                dst_rowset_writer->add_block(&block),
                "failed to write block when merging rowsets of tablet " + tablet->full_name());

        if (reader_params.record_rowids && block.rows() > 0) {
            std::vector<uint32_t> segment_num_rows;
            RETURN_IF_ERROR(dst_rowset_writer->get_segment_num_rows(&segment_num_rows));
            stats_output->rowid_conversion->add(reader.current_block_row_locations(),
                                                segment_num_rows);
        }

        output_rows += block.rows();
        block.clear_column_data();
    }

    if (stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.filtered_rows();
    }

    RETURN_NOT_OK_LOG(
            dst_rowset_writer->flush(),
            "failed to flush rowset when merging rowsets of tablet " + tablet->full_name());

    return Status::OK();
}

} // namespace doris
