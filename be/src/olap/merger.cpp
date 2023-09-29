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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <numeric>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/reader.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/utils.h"
#include "util/slice.h"
#include "vec/core/block.h"
#include "vec/olap/block_reader.h"
#include "vec/olap/vertical_block_reader.h"
#include "vec/olap/vertical_merge_iterator.h"

namespace doris {

Status Merger::vmerge_rowsets(TabletSharedPtr tablet, ReaderType reader_type,
                              TabletSchemaSPtr cur_tablet_schema,
                              const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                              RowsetWriter* dst_rowset_writer, Statistics* stats_output) {
    vectorized::BlockReader reader;
    TabletReader::ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = reader_type;

    TabletReader::ReadSource read_source;
    read_source.rs_splits.reserve(src_rowset_readers.size());
    for (const RowsetReaderSharedPtr& rs_reader : src_rowset_readers) {
        read_source.rs_splits.emplace_back(RowSetSplits(rs_reader));
    }
    read_source.fill_delete_predicates();
    reader_params.set_read_source(std::move(read_source));

    reader_params.version = dst_rowset_writer->version();

    TabletSchemaSPtr merge_tablet_schema = std::make_shared<TabletSchema>();
    merge_tablet_schema->copy_from(*cur_tablet_schema);

    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred_rs : reader_params.delete_predicates) {
        merge_tablet_schema->merge_dropped_columns(*del_pred_rs->tablet_schema());
    }
    reader_params.tablet_schema = merge_tablet_schema;

    if (stats_output && stats_output->rowid_conversion) {
        reader_params.record_rowids = true;
    }

    reader_params.return_columns.resize(cur_tablet_schema->num_columns());
    std::iota(reader_params.return_columns.begin(), reader_params.return_columns.end(), 0);
    reader_params.origin_return_columns = &reader_params.return_columns;
    RETURN_IF_ERROR(reader.init(reader_params));

    if (reader_params.record_rowids) {
        stats_output->rowid_conversion->set_dst_rowset_id(dst_rowset_writer->rowset_id());
        // init segment rowid map for rowid conversion
        std::vector<uint32_t> segment_num_rows;
        for (auto& rs_split : reader_params.rs_splits) {
            RETURN_IF_ERROR(rs_split.rs_reader->get_segment_num_rows(&segment_num_rows));
            stats_output->rowid_conversion->init_segment_map(
                    rs_split.rs_reader->rowset()->rowset_id(), segment_num_rows);
        }
    }

    vectorized::Block block = cur_tablet_schema->create_block(reader_params.return_columns);
    size_t output_rows = 0;
    bool eof = false;
    while (!eof && !StorageEngine::instance()->stopped()) {
        // Read one block from block reader
        RETURN_NOT_OK_STATUS_WITH_WARN(
                reader.next_block_with_aggregation(&block, &eof),
                "failed to read next block when merging rowsets of tablet " + tablet->full_name());
        RETURN_NOT_OK_STATUS_WITH_WARN(
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
    if (StorageEngine::instance()->stopped()) {
        return Status::Error<INTERNAL_ERROR>("tablet {} failed to do compaction, engine stopped",
                                             tablet->full_name());
    }

    if (stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.filtered_rows();
    }

    RETURN_NOT_OK_STATUS_WITH_WARN(
            dst_rowset_writer->flush(),
            "failed to flush rowset when merging rowsets of tablet " + tablet->full_name());

    return Status::OK();
}

// split columns into several groups, make sure all keys in one group
// unique_key should consider sequence&delete column
void Merger::vertical_split_columns(TabletSchemaSPtr tablet_schema,
                                    std::vector<std::vector<uint32_t>>* column_groups) {
    uint32_t num_key_cols = tablet_schema->num_key_columns();
    uint32_t total_cols = tablet_schema->num_columns();
    std::vector<uint32_t> key_columns;
    for (auto i = 0; i < num_key_cols; ++i) {
        key_columns.emplace_back(i);
    }
    // in unique key, sequence & delete sign column should merge with key columns
    int32_t sequence_col_idx = -1;
    int32_t delete_sign_idx = -1;
    // in key column compaction, seq_col real index is _num_key_columns
    // and delete_sign column is _block->columns() - 1
    if (tablet_schema->keys_type() == KeysType::UNIQUE_KEYS) {
        if (tablet_schema->has_sequence_col()) {
            sequence_col_idx = tablet_schema->sequence_col_idx();
            key_columns.emplace_back(sequence_col_idx);
        }
        delete_sign_idx = tablet_schema->field_index(DELETE_SIGN);
        if (delete_sign_idx != -1) {
            key_columns.emplace_back(delete_sign_idx);
        }
    }
    VLOG_NOTICE << "sequence_col_idx=" << sequence_col_idx
                << ", delete_sign_idx=" << delete_sign_idx;
    // for duplicate no keys
    if (!key_columns.empty()) {
        column_groups->emplace_back(std::move(key_columns));
    }
    std::vector<uint32_t> value_columns;
    for (auto i = num_key_cols; i < total_cols; ++i) {
        if (i == sequence_col_idx || i == delete_sign_idx) {
            continue;
        }
        if ((i - num_key_cols) % config::vertical_compaction_num_columns_per_group == 0) {
            column_groups->emplace_back();
        }
        column_groups->back().emplace_back(i);
    }
}

Status Merger::vertical_compact_one_group(
        TabletSharedPtr tablet, ReaderType reader_type, TabletSchemaSPtr tablet_schema, bool is_key,
        const std::vector<uint32_t>& column_group, vectorized::RowSourcesBuffer* row_source_buf,
        const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
        RowsetWriter* dst_rowset_writer, int64_t max_rows_per_segment, Statistics* stats_output) {
    // build tablet reader
    VLOG_NOTICE << "vertical compact one group, max_rows_per_segment=" << max_rows_per_segment;
    vectorized::VerticalBlockReader reader(row_source_buf);
    TabletReader::ReaderParams reader_params;
    reader_params.is_key_column_group = is_key;
    reader_params.tablet = tablet;
    reader_params.reader_type = reader_type;

    TabletReader::ReadSource read_source;
    read_source.rs_splits.reserve(src_rowset_readers.size());
    for (const RowsetReaderSharedPtr& rs_reader : src_rowset_readers) {
        read_source.rs_splits.emplace_back(RowSetSplits(rs_reader));
    }
    read_source.fill_delete_predicates();
    reader_params.set_read_source(std::move(read_source));

    reader_params.version = dst_rowset_writer->version();

    TabletSchemaSPtr merge_tablet_schema = std::make_shared<TabletSchema>();
    merge_tablet_schema->copy_from(*tablet_schema);

    for (auto& del_pred_rs : reader_params.delete_predicates) {
        merge_tablet_schema->merge_dropped_columns(*del_pred_rs->tablet_schema());
    }

    reader_params.tablet_schema = merge_tablet_schema;

    if (is_key && stats_output && stats_output->rowid_conversion) {
        reader_params.record_rowids = true;
    }

    reader_params.return_columns = column_group;
    reader_params.origin_return_columns = &reader_params.return_columns;
    RETURN_IF_ERROR(reader.init(reader_params));

    if (reader_params.record_rowids) {
        stats_output->rowid_conversion->set_dst_rowset_id(dst_rowset_writer->rowset_id());
        // init segment rowid map for rowid conversion
        std::vector<uint32_t> segment_num_rows;
        for (auto& rs_split : reader_params.rs_splits) {
            RETURN_IF_ERROR(rs_split.rs_reader->get_segment_num_rows(&segment_num_rows));
            stats_output->rowid_conversion->init_segment_map(
                    rs_split.rs_reader->rowset()->rowset_id(), segment_num_rows);
        }
    }

    vectorized::Block block = tablet_schema->create_block(reader_params.return_columns);
    size_t output_rows = 0;
    bool eof = false;
    while (!eof && !StorageEngine::instance()->stopped()) {
        // Read one block from block reader
        RETURN_NOT_OK_STATUS_WITH_WARN(
                reader.next_block_with_aggregation(&block, &eof),
                "failed to read next block when merging rowsets of tablet " + tablet->full_name());
        RETURN_NOT_OK_STATUS_WITH_WARN(
                dst_rowset_writer->add_columns(&block, column_group, is_key, max_rows_per_segment),
                "failed to write block when merging rowsets of tablet " + tablet->full_name());

        if (is_key && reader_params.record_rowids && block.rows() > 0) {
            std::vector<uint32_t> segment_num_rows;
            RETURN_IF_ERROR(dst_rowset_writer->get_segment_num_rows(&segment_num_rows));
            stats_output->rowid_conversion->add(reader.current_block_row_locations(),
                                                segment_num_rows);
        }
        output_rows += block.rows();
        block.clear_column_data();
    }
    if (StorageEngine::instance()->stopped()) {
        return Status::Error<INTERNAL_ERROR>("tablet {} failed to do compaction, engine stopped",
                                             tablet->full_name());
    }

    if (is_key && stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = reader.merged_rows();
        stats_output->filtered_rows = reader.filtered_rows();
    }
    RETURN_IF_ERROR(dst_rowset_writer->flush_columns(is_key));

    return Status::OK();
}

// for segcompaction
Status Merger::vertical_compact_one_group(TabletSharedPtr tablet, ReaderType reader_type,
                                          TabletSchemaSPtr tablet_schema, bool is_key,
                                          const std::vector<uint32_t>& column_group,
                                          vectorized::RowSourcesBuffer* row_source_buf,
                                          vectorized::VerticalBlockReader& src_block_reader,
                                          segment_v2::SegmentWriter& dst_segment_writer,
                                          int64_t max_rows_per_segment, Statistics* stats_output,
                                          uint64_t* index_size, KeyBoundsPB& key_bounds) {
    // build tablet reader
    VLOG_NOTICE << "vertical compact one group, max_rows_per_segment=" << max_rows_per_segment;
    // TODO: record_rowids
    vectorized::Block block = tablet_schema->create_block(column_group);
    size_t output_rows = 0;
    bool eof = false;
    while (!eof && !StorageEngine::instance()->stopped()) {
        // Read one block from block reader
        RETURN_NOT_OK_STATUS_WITH_WARN(
                src_block_reader.next_block_with_aggregation(&block, &eof),
                "failed to read next block when merging rowsets of tablet " + tablet->full_name());
        if (!block.rows()) {
            break;
        }
        RETURN_NOT_OK_STATUS_WITH_WARN(
                dst_segment_writer.append_block(&block, 0, block.rows()),
                "failed to write block when merging rowsets of tablet " + tablet->full_name());

        output_rows += block.rows();
        block.clear_column_data();
    }
    if (StorageEngine::instance()->stopped()) {
        return Status::Error<INTERNAL_ERROR>("tablet {} failed to do compaction, engine stopped",
                                             tablet->full_name());
    }

    if (is_key && stats_output != nullptr) {
        stats_output->output_rows = output_rows;
        stats_output->merged_rows = src_block_reader.merged_rows();
        stats_output->filtered_rows = src_block_reader.filtered_rows();
    }

    // segcompaction produce only one segment at once
    RETURN_IF_ERROR(dst_segment_writer.finalize_columns_data());
    RETURN_IF_ERROR(dst_segment_writer.finalize_columns_index(index_size));

    if (is_key) {
        Slice min_key = dst_segment_writer.min_encoded_key();
        Slice max_key = dst_segment_writer.max_encoded_key();
        DCHECK_LE(min_key.compare(max_key), 0);
        key_bounds.set_min_key(min_key.to_string());
        key_bounds.set_max_key(max_key.to_string());
    }

    return Status::OK();
}

// steps to do vertical merge:
// 1. split columns into column groups
// 2. compact groups one by one, generate a row_source_buf when compact key group
// and use this row_source_buf to compact value column groups
// 3. build output rowset
Status Merger::vertical_merge_rowsets(TabletSharedPtr tablet, ReaderType reader_type,
                                      TabletSchemaSPtr tablet_schema,
                                      const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                                      RowsetWriter* dst_rowset_writer, int64_t max_rows_per_segment,
                                      Statistics* stats_output) {
    LOG(INFO) << "Start to do vertical compaction, tablet_id: " << tablet->tablet_id();
    std::vector<std::vector<uint32_t>> column_groups;
    vertical_split_columns(tablet_schema, &column_groups);

    vectorized::RowSourcesBuffer row_sources_buf(tablet->tablet_id(), tablet->tablet_path(),
                                                 reader_type);
    // compact group one by one
    for (auto i = 0; i < column_groups.size(); ++i) {
        VLOG_NOTICE << "row source size: " << row_sources_buf.total_size();
        bool is_key = (i == 0);
        RETURN_IF_ERROR(vertical_compact_one_group(
                tablet, reader_type, tablet_schema, is_key, column_groups[i], &row_sources_buf,
                src_rowset_readers, dst_rowset_writer, max_rows_per_segment, stats_output));
        if (is_key) {
            static_cast<void>(row_sources_buf.flush());
        }
        static_cast<void>(row_sources_buf.seek_to_begin());
    }

    // finish compact, build output rowset
    VLOG_NOTICE << "finish compact groups";
    RETURN_IF_ERROR(dst_rowset_writer->final_flush());

    return Status::OK();
}

} // namespace doris
