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

#include "storage/iterator/vertical_block_reader.h"

#include <assert.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <iterator>
#include <ostream>

#include "cloud/config.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "runtime/runtime_state.h"
#include "storage/compaction/compaction.h"
#include "storage/iterator/agg_key_block_iterator.h"
#include "storage/iterator/mask_merge_iterator.h"
#include "storage/iterator/output_block_limits.h"
#include "storage/iterator/row_sources_buffer.h"
#include "storage/iterator/union_iterator.h"
#include "storage/iterator/vertical_key_merge_iterator.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"
#include "util/simd/bits.h"

namespace doris {
using namespace ErrorCode;

uint64_t VerticalBlockReader::nextId = 1;

VerticalBlockReader::VerticalBlockReader(RowSourcesBuffer* row_sources_buffer,
                                         VerticalCompactionContextStats* context_stats)
        : _row_sources_buffer(row_sources_buffer), _context_stats(context_stats) {
    _id = nextId++;
}

VerticalBlockReader::~VerticalBlockReader() = default;

std::vector<size_t> VerticalBlockReader::_segment_group_sizes(
        const RowsetMeta& rowset_meta, std::pair<int64_t, int64_t> segment_offsets,
        size_t num_segment_iters) {
    auto [segment_start, segment_end] = segment_offsets;
    if (segment_start == segment_end) {
        segment_start = 0;
        segment_end = rowset_meta.num_segments();
    }

    DORIS_CHECK_GE(segment_start, 0);
    DORIS_CHECK_LE(segment_start, segment_end);
    DORIS_CHECK_LE(segment_end, rowset_meta.num_segments());
    DORIS_CHECK_EQ(num_segment_iters, static_cast<size_t>(segment_end - segment_start));
    DORIS_CHECK_GT(rowset_meta.segment_group_sizes().size(), 1);

    std::vector<size_t> group_sizes;
    int64_t group_start = 0;
    for (const auto group_size : rowset_meta.segment_group_sizes()) {
        DORIS_CHECK_GT(group_size, 0);
        const int64_t group_end = group_start + group_size;
        const int64_t selected_start = std::max(group_start, segment_start);
        const int64_t selected_end = std::min(group_end, segment_end);
        if (selected_start < selected_end) {
            group_sizes.push_back(static_cast<size_t>(selected_end - selected_start));
        }
        group_start = group_end;
    }
    DORIS_CHECK_EQ(group_start, rowset_meta.num_segments());
    return group_sizes;
}

Status VerticalBlockReader::next_block_with_aggregation(Block* block, bool* eof) {
    auto res = (this->*_next_block_func)(block, eof);
    if (!config::is_cloud_mode()) {
        if (!res.ok()) [[unlikely]] {
            static_cast<Tablet*>(_tablet.get())->report_error(res);
        }
    }
    return res;
}

Status VerticalBlockReader::_get_sources(const ReaderParams& read_params,
                                         std::vector<RowwiseIteratorUPtr>* sources) {
    auto res = _capture_rs_readers(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return res;
    }
    RuntimeState* runtime_state = read_params.runtime_state;
    for (const auto& rs_split : read_params.rs_splits) {
        if (runtime_state != nullptr) {
            RETURN_IF_CANCELLED(runtime_state);
        }
        RETURN_IF_ERROR(rs_split.rs_reader->init(&_reader_context, rs_split));
        const auto rowset = rs_split.rs_reader->rowset();
        // segment iterator will be inited here
        // In vertical compaction, every group will load segment so we should cache
        // segment to avoid tot many s3 head request
        bool use_cache = !rowset->is_local();
        std::vector<RowwiseIteratorUPtr> segment_iters;
        RETURN_IF_ERROR(rs_split.rs_reader->get_segment_iterators(&_reader_context, &segment_iters,
                                                                  use_cache));
        // The segments of a rowset that overlap on keys are merged, each its own source. The
        // segments that do not are read one after another as one source, so that the merge
        // holds one of them at a time: all of them when the rowset does not overlap, those of
        // each group when its segments are ordered within groups that overlap.
        if (!rowset->is_segments_overlapping()) {
            _append_source(std::move(segment_iters), sources);
        } else if (rowset->rowset_meta()->segments_overlap() == NONOVERLAPPING_WITHIN_GROUP) {
            auto group_begin = segment_iters.begin();
            for (const size_t group_size : _segment_group_sizes(
                         *rowset->rowset_meta(), rs_split.segment_offsets, segment_iters.size())) {
                const auto group_end = group_begin + static_cast<ptrdiff_t>(group_size);
                _append_source(
                        {std::make_move_iterator(group_begin), std::make_move_iterator(group_end)},
                        sources);
                group_begin = group_end;
            }
        } else {
            std::ranges::move(segment_iters, std::back_inserter(*sources));
        }
        rs_split.rs_reader->reset_read_options();
    }
    return Status::OK();
}

void VerticalBlockReader::_append_source(std::vector<RowwiseIteratorUPtr> segment_iters,
                                         std::vector<RowwiseIteratorUPtr>* sources) const {
    if (segment_iters.size() == 1) {
        sources->push_back(std::move(segment_iters.front()));
    } else if (segment_iters.size() > 1) {
        sources->push_back(new_union_iterator(std::move(segment_iters), _read_schema));
    }
}

// The key group is merged by the key, or by the cluster key of a merge-on-write table that has
// one, a newer row of a key first. A UNIQUE_KEYS table without a cluster key and an AGG_KEYS
// table keep the first row of every key: the row sources mark the others, which the value groups
// skip or fold.
MergeDecision VerticalBlockReader::_key_group_merge(const ReaderParams& read_params) const {
    MergeDecision decision = decide_merge(
            *_tablet_schema, *_read_schema, read_params.key_group_cluster_key_idxes,
            /*key_ordered=*/true, /*reverse=*/false, _reader_context.merge.older_first);
    if (_tablet_schema->keys_type() == KeysType::AGG_KEYS) {
        decision.drop_older_versions = true;
    }
    return decision;
}

Status VerticalBlockReader::_init_collect_iter(const ReaderParams& read_params,
                                               CompactionSampleInfo* sample_info) {
    std::vector<RowwiseIteratorUPtr> sources;
    if (read_params.segment_iters_ptr == nullptr) {
        RETURN_IF_ERROR(_get_sources(read_params, &sources));
    } else {
        // Segcompaction hands over the iterators of the segments it compacts, which overlap;
        // the context only names the read.
        sources = std::move(*read_params.segment_iters_ptr);
        _reader_context.reader_type = read_params.reader_type;
    }
    if (_tablet_schema->num_key_columns() == 0 && sources.size() > 1) {
        // Rows without a key have no order to merge by: the sources are read one after
        // another, the oldest first, as one source.
        std::vector<RowwiseIteratorUPtr> all_sources;
        all_sources.push_back(new_union_iterator(std::move(sources), _read_schema));
        sources = std::move(all_sources);
    }
    _is_key_column_group = read_params.is_key_column_group;

    if (_is_key_column_group) {
        _vcollect_iter =
                new_vertical_key_merge_iterator(std::move(sources), _read_schema,
                                                _key_group_merge(read_params), _row_sources_buffer);
    } else {
        _vcollect_iter = new_vertical_mask_merge_iterator(std::move(sources), _row_sources_buffer,
                                                          _context_stats);
    }
    // init collect iterator
    StorageReadOptions opts(_stats);
    if (read_params.batch_size > 0) {
        opts.block_row_max = cast_set<int>(read_params.batch_size);
    }
    opts.runtime_state = read_params.runtime_state;
    opts.tablet_id = _tablet->tablet_id();
    RETURN_IF_ERROR(_vcollect_iter->init(opts, sample_info));

    // The value groups of an AGGREGATE KEY table fold the rows the agg flags join to a key.
    if (!_is_key_column_group && _tablet_schema->keys_type() == KeysType::AGG_KEYS) {
        _agg_block_iter = std::make_unique<AggKeyBlockIterator>(
                _vcollect_iter.get(),
                OutputBlockLimits {.max_rows = cast_set<size_t>(_reader_context.batch_size)},
                _read_schema, read_params.get_be_exec_version(), &_folded_rows);
        RETURN_IF_ERROR(_agg_block_iter->init());
    }
    return Status::OK();
}

Status VerticalBlockReader::init(const ReaderParams& read_params) {
    return init(read_params, nullptr);
}

Status VerticalBlockReader::init(const ReaderParams& read_params,
                                 CompactionSampleInfo* sample_info) {
    StorageReadOptions opts(_stats);
    if (read_params.batch_size > 0) {
        _reader_context.batch_size = cast_set<int>(read_params.batch_size);
    } else {
        _reader_context.batch_size = opts.block_row_max;
    }
    RETURN_IF_ERROR(TabletReader::init(read_params));

    RETURN_IF_ERROR(_read_schema->init_from_tablet_schema(*_tablet_schema,
                                                          /*merge_by_sequence_mapping=*/false,
                                                          /*map_changelog_columns=*/false));

    auto status = _init_collect_iter(read_params, sample_info);
    if (!status.ok()) [[unlikely]] {
        if (!config::is_cloud_mode()) {
            static_cast<Tablet*>(_tablet.get())->report_error(status);
        }
        return status;
    }

    switch (_tablet_schema->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &VerticalBlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        if (_tablet_schema->cluster_key_uids().empty()) {
            _next_block_func = &VerticalBlockReader::_unique_key_next_block;
            if (_filter_delete) {
                _delete_filter_column = ColumnUInt8::create();
            }
        } else {
            _next_block_func = &VerticalBlockReader::_direct_next_block;
        }
        break;
    case KeysType::AGG_KEYS:
        _next_block_func = &VerticalBlockReader::_agg_key_next_block;
        break;
    default:
        DCHECK(false) << "No next row function for type:" << _tablet_schema->keys_type();
        break;
    }

    // Use sparse optimization flag from ReaderParams (calculated in merger.cpp based on avg_row_bytes threshold)
    _enable_sparse_optimization = read_params.enable_sparse_optimization;

    // Save sample_info pointer for null count tracking
    _sample_info = sample_info;

    return Status::OK();
}

Status VerticalBlockReader::_direct_next_block(Block* block, bool* eof) {
    auto res = _vcollect_iter->next_batch(block);
    if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
        return res;
    }
    *eof = (res.is<END_OF_FILE>());
    _eof = *eof;
    return Status::OK();
}

Status VerticalBlockReader::_agg_key_next_block(Block* block, bool* eof) {
    if (_is_key_column_group) {
        // collect_iter will filter agg keys
        auto res = _vcollect_iter->next_batch(block);
        if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
            return res;
        }
        *eof = (res.is<END_OF_FILE>());
        _eof = *eof;
        return Status::OK();
    }
    RETURN_IF_ERROR(_agg_block_iter->next_block(block, eof));
    _eof = *eof;
    return Status::OK();
}

Status VerticalBlockReader::_unique_key_next_block(Block* block, bool* eof) {
    if (_is_key_column_group) {
        // Record row_source_buffer current size for key column agg flag
        // _vcollect_iter->next_batch(block) will fill row_source_buffer but delete sign is ignored
        // we calc delete sign column if it's base compaction and update row_sourece_buffer's agg flag
        // after we get current block
        VLOG_NOTICE << "reader id: " << _id
                    << ", buffer size: " << _row_sources_buffer->buffered_size();
        uint64_t row_source_idx = _row_sources_buffer->buffered_size();
        uint64_t row_buffer_size_start = row_source_idx;
        uint64_t merged_rows_start = _vcollect_iter->merged_rows();
        uint64_t filtered_rows_start = _stats.rows_del_filtered;

        auto res = _vcollect_iter->next_batch(block);
        if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
            return res;
        }

        if (_row_sources_buffer->buffered_size() < row_buffer_size_start) {
            row_buffer_size_start = 0;
            row_source_idx = 0;
        }

        size_t merged_rows_in_rs_buffer = 0;
        for (uint64_t i = row_buffer_size_start; i < _row_sources_buffer->buffered_size(); i++) {
            if (_row_sources_buffer->get_agg_flag(i)) {
                merged_rows_in_rs_buffer++;
            }
        }

        size_t block_rows = block->rows();
        if (_delete_sign_available && block_rows > 0) {
            int delete_sign_idx = _read_schema->delete_sign_ordinal();
            if (delete_sign_idx < 0) {
                *eof = (res.is<END_OF_FILE>());
                _eof = *eof;
                return Status::OK();
            }
            DCHECK(delete_sign_idx > 0);
            auto target_columns_guard = block->mutate_columns_scoped();
            auto& target_columns = target_columns_guard.mutable_columns();
            auto delete_filter_column = IColumn::mutate(std::move(_delete_filter_column));
            auto* delete_filter_data_column =
                    reinterpret_cast<ColumnUInt8*>(delete_filter_column.get());
            delete_filter_data_column->resize(block_rows);

            auto* __restrict filter_data = delete_filter_data_column->get_data().data();
            auto* __restrict delete_data =
                    reinterpret_cast<ColumnInt8*>(target_columns[delete_sign_idx].get())
                            ->get_data()
                            .data();

            int cur_row = 0;
            while (cur_row < block_rows) {
                if (_row_sources_buffer->get_agg_flag(row_source_idx)) {
                    row_source_idx++;
                    continue;
                }
                bool sign = (delete_data[cur_row] == 0);
                filter_data[cur_row] = sign;
                if (UNLIKELY(!sign)) {
                    _row_sources_buffer->set_agg_flag(row_source_idx, true);
                }
                cur_row++;
                row_source_idx++;
            }
            while (row_source_idx < _row_sources_buffer->buffered_size()) {
                row_source_idx++;
            }

            const auto column_to_keep = target_columns.size();
            target_columns_guard.restore();
            _delete_filter_column = std::move(delete_filter_column);
            ColumnWithTypeAndName column_with_type_and_name {_delete_filter_column,
                                                             std::make_shared<DataTypeUInt8>(),
                                                             "__DORIS_COMPACTION_FILTER__"};
            block->insert(column_with_type_and_name);
            RETURN_IF_ERROR(Block::filter_block(block, column_to_keep, column_to_keep));
            _stats.rows_del_filtered += block_rows - block->rows();
        }

        size_t filtered_rows_in_rs_buffer = 0;
        for (auto i = row_buffer_size_start; i < _row_sources_buffer->buffered_size(); i++) {
            if (_row_sources_buffer->get_agg_flag(i)) {
                filtered_rows_in_rs_buffer++;
            }
        }
        filtered_rows_in_rs_buffer -= merged_rows_in_rs_buffer;

        auto merged_rows_cur_batch = _vcollect_iter->merged_rows() - merged_rows_start;
        auto filtered_rows_cur_batch = _stats.rows_del_filtered - filtered_rows_start;

        DCHECK_EQ(merged_rows_in_rs_buffer, merged_rows_cur_batch);
        DCHECK_EQ(filtered_rows_in_rs_buffer, filtered_rows_cur_batch);
        *eof = (res.is<END_OF_FILE>());
        _eof = *eof;
        return Status::OK();
    }

    // Value column processing - use batch optimization
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    const size_t column_count = block->columns();

    // Try to use batch optimization for value column compaction
    // Only use batch optimization when sparse optimization is enabled
    {
        auto* mask_iter = dynamic_cast<VerticalMaskMergeIterator*>(_vcollect_iter.get());
        if (mask_iter != nullptr && _enable_sparse_optimization) {
            // Step 1: Batch fetch row information
            std::vector<RowBatch> batches;
            size_t actual_rows = 0;
            RETURN_IF_ERROR(mask_iter->unique_key_next_batch(&batches, _reader_context.batch_size,
                                                             &actual_rows));
            if (actual_rows == 0) {
                *eof = true;
                _eof = true;
                return Status::OK();
            }

            // Step 2: Prepare columns - pre-fill NULL for fixed-width, reserve for others
            std::vector<ColumnNullable*> nullable_dst_cols;
            std::vector<bool> supports_replace;
            const size_t old_rows = target_columns.empty() ? 0 : target_columns.front()->size();
            _prepare_sparse_columns(target_columns, actual_rows, nullable_dst_cols,
                                    supports_replace);

            // Step 3: Process each batch
            size_t dst_offset = old_rows;
            for (const auto& batch : batches) {
                Block* src_block = batch.block.get();
                DCHECK(src_block != nullptr);
                DCHECK(src_block->columns() == column_count);

                for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
                    const auto& src_col = src_block->get_by_position(col_idx).column;
                    _process_sparse_column(nullable_dst_cols[col_idx], supports_replace[col_idx],
                                           target_columns[col_idx], src_col, batch, dst_offset);
                }
                dst_offset += batch.count;
            }

            return Status::OK();
        }
    }

    // Fallback: original row-by-row processing
    int target_block_row = 0;
    do {
        Status res = _vcollect_iter->unique_key_next_row(&_next_row);
        if (UNLIKELY(!res.ok())) {
            if (UNLIKELY(res.is<END_OF_FILE>())) {
                *eof = true;
                _eof = true;
                break;
            }
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        const auto& src_block = _next_row.block;
        assert(src_block->columns() == column_count);
        RETURN_IF_CATCH_EXCEPTION({
            for (size_t i = 0; i < column_count; ++i) {
                target_columns[i]->insert_from(*(src_block->get_by_position(i).column),
                                               _next_row.row_pos);
            }
        });
        ++target_block_row;
    } while (target_block_row < _reader_context.batch_size);
    return Status::OK();
}

// Prepare columns for sparse optimization: pre-fill NULL for fixed-width types, reserve for others
void VerticalBlockReader::_prepare_sparse_columns(MutableColumns& columns, size_t actual_rows,
                                                  std::vector<ColumnNullable*>& nullable_dst_cols,
                                                  std::vector<bool>& supports_replace) {
    size_t column_count = columns.size();
    nullable_dst_cols.resize(column_count, nullptr);
    supports_replace.resize(column_count, false);

    for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
        auto& col = columns[col_idx];
        if (is_column_nullable(*col)) {
            auto* nullable_col =
                    assert_cast<ColumnNullable*, TypeCheckOnRelease::DISABLE>(col.get());
            nullable_dst_cols[col_idx] = nullable_col;
            supports_replace[col_idx] = nullable_col->support_replace_column_data_range();

            if (supports_replace[col_idx]) {
                // Fixed-width types: pre-fill with NULL for sparse optimization
                size_t new_size = nullable_col->size() + actual_rows;
                nullable_col->get_null_map_column().get_data().resize_fill(new_size, 1);
                nullable_col->get_nested_column().resize(new_size);
            } else {
                // Variable-length types: just reserve space
                col->reserve(col->size() + actual_rows);
            }
        } else {
            // Non-Nullable column, reserve space
            col->reserve(col->size() + actual_rows);
        }
    }
}

// Copy non-NULL runs from source to destination using run-length encoding
void VerticalBlockReader::_copy_non_null_runs(ColumnNullable* nullable_dst,
                                              const ColumnNullable* nullable_src,
                                              const uint8_t* null_map, const RowBatch& batch,
                                              size_t dst_offset) {
    size_t i = 0;
    while (i < batch.count) {
        // Skip NULL values (null_map == 1)
        while (i < batch.count && null_map[batch.start_row + i] != 0) {
            i++;
        }
        if (i >= batch.count) break;

        // Found start of non-NULL run
        size_t run_start = i;
        while (i < batch.count && null_map[batch.start_row + i] == 0) {
            i++;
        }
        size_t run_length = i - run_start;

        // Batch copy this non-NULL run
        nullable_dst->replace_column_data_range(*nullable_src, batch.start_row + run_start,
                                                run_length, dst_offset + run_start);
    }
}

// Process a single column for one batch in sparse optimization
void VerticalBlockReader::_process_sparse_column(ColumnNullable* nullable_dst,
                                                 bool supports_replace,
                                                 MutableColumnPtr& target_col,
                                                 const ColumnPtr& src_col, const RowBatch& batch,
                                                 size_t dst_offset) {
    if (nullable_dst != nullptr && supports_replace) {
        // Sparse optimization path for fixed-width types
        const auto* nullable_src = static_cast<const ColumnNullable*>(src_col.get());
        const auto& null_map = nullable_src->get_null_map_data();

        size_t non_null_count = simd::count_zero_num(
                reinterpret_cast<const int8_t*>(null_map.data() + batch.start_row),
                static_cast<size_t>(batch.count));

        if (_sample_info != nullptr) {
            _sample_info->null_count += (batch.count - non_null_count);
        }

        if (non_null_count == 0) {
            // All NULL, skip (already pre-filled)
        } else if (non_null_count == batch.count) {
            // All non-NULL, use batch copy
            nullable_dst->replace_column_data_range(*nullable_src, batch.start_row, batch.count,
                                                    dst_offset);
        } else {
            // Mixed case: copy non-NULL runs
            _copy_non_null_runs(nullable_dst, nullable_src, null_map.data(), batch, dst_offset);
        }
    } else if (nullable_dst != nullptr) {
        // Variable-length nullable types
        if (_sample_info != nullptr) {
            const auto* nullable_src = static_cast<const ColumnNullable*>(src_col.get());
            const auto& null_map = nullable_src->get_null_map_data();
            size_t non_null_count = simd::count_zero_num(
                    reinterpret_cast<const int8_t*>(null_map.data() + batch.start_row),
                    static_cast<size_t>(batch.count));
            _sample_info->null_count += (batch.count - non_null_count);
        }
        target_col->insert_range_from(*src_col, batch.start_row, batch.count);
    } else {
        // Non-Nullable column
        target_col->insert_range_from(*src_col, batch.start_row, batch.count);
    }
}

} // namespace doris
