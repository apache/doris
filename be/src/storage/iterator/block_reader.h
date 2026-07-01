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

#include <parallel_hashmap/phmap.h>
#include <stddef.h>
#include <sys/types.h>

#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "exprs/aggregate/aggregate_function.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/utils.h"

namespace doris {
class ColumnPredicate;
class FunctionFilter;
class RuntimeProfile;

class BlockReader final : public TabletReader {
public:
    ~BlockReader() override;

    // Initialize BlockReader with tablet, data version and fetch range.
    Status init(const ReaderParams& read_params) override;

    Status next_block_with_aggregation(Block* block, bool* eof) override;

    std::vector<RowLocation> current_block_row_locations() { return _block_row_locations; }

    void update_profile(RuntimeProfile* profile) override {
        return _vcollect_iter.update_profile(profile);
    }

    // Returns the configured preferred output block byte budget; 0 when adaptive is disabled.
    size_t preferred_block_size_bytes() const override {
        return config::enable_adaptive_batch_size ? _reader_context.preferred_block_size_bytes : 0;
    }

private:
    // Directly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    Status _direct_next_block(Block* block, bool* eof);
    // Just same as _direct_next_block, but this is only for AGGREGATE KEY tables.
    // And this is an optimization for AGGR tables.
    // When there is only one rowset and is not overlapping, we can read it directly without aggregation.
    Status _direct_agg_key_next_block(Block* block, bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    Status _agg_key_next_block(Block* block, bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_block is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    Status _unique_key_next_block(Block* block, bool* eof);

    Status _min_delta_next_block(Block* block, bool* eof);

    Status _detail_change_next_block(Block* block, bool* eof);

    Status _ensure_binlog_column_pos(const Block& src_block);

    int64_t _read_binlog_op(const IColumn& col, size_t row) const;

    Status _write_binlog_op(IColumn& col, int64_t op) const;

    bool _is_binlog_meta_column(int idx) const;

    int _resolve_source_column_index(int idx, bool use_before) const;

    void _init_pending_row_columns(const Block& block);

    bool _emit_pending_row(MutableColumns& target_columns, size_t& output_row_count);

    Status _replace_key_next_block(Block* block, bool* eof);

    Status _init_collect_iter(const ReaderParams& read_params);

    Status _init_agg_state(const ReaderParams& read_params);

    Status _insert_data_normal(MutableColumns& columns);

    // for partial update table
    void _update_last_mutil_seq(int seq_idx);
    void _compare_sequence_map_and_replace(MutableColumns& columns);

    // Check if the accumulated output columns have reached the preferred byte budget,
    // used to limit the output block size for adaptive batch sizing.
    bool _reached_byte_budget(const MutableColumns& columns) const;

    void _append_agg_data(MutableColumns& columns);

    void _update_agg_data(MutableColumns& columns);

    size_t _copy_agg_data();

    void _update_agg_value(MutableColumns& columns, int begin, int end, bool is_close = true);

    Status _append_change_row(MutableColumns& target_columns, const Block& src_block,
                              size_t row_pos, int64_t output_op, bool use_before);

    // return false if keys of rowsets are mono ascending and disjoint
    bool _rowsets_not_mono_asc_disjoint(const ReaderParams& read_params);

    bool _is_row_store_only_derived_output_column(uint32_t output_column_idx) const;

    VCollectIterator _vcollect_iter;
    IteratorRowRef _next_row {{}, -1, false};

    std::vector<AggregateFunctionPtr> _agg_functions;
    std::vector<AggregateDataPtr> _agg_places;

    std::vector<int> _normal_columns_idx; // key column on agg mode, all column on uniq mode
    std::vector<int> _agg_columns_idx;
    std::vector<int> _return_columns_loc;
    std::vector<uint32_t> _output_column_cids;

    std::vector<int> _agg_data_counters;
    int _last_agg_data_counter = 0;

    // Buffer of consecutive rows that share the same primary key, used by
    // _min_delta_next_block to fold INSERT/UPDATE/DELETE into a single net change.
    // Rows are appended as the merge iterator advances and cleared after each key group.
    MutableColumns _stored_data_columns;
    std::vector<IteratorRowRef> _stored_row_ref;

    std::vector<bool> _stored_has_null_tag;
    std::vector<bool> _stored_has_variable_length_tag;

    // One-row carry-over buffer holding the AFTER row of an UPDATE pair when the BEFORE row
    // was already emitted on the boundary of batch_max_rows(). Flushed by _emit_pending_row()
    // at the start of the next call to *_next_block.
    MutableColumns _pending_row_columns;
    bool _has_pending_row = false;

    phmap::flat_hash_map<const Block*, std::vector<std::pair<int, int>>> _temp_ref_map;

    bool _eof = false;

    Status (BlockReader::*_next_block_func)(Block* block, bool* eof) = nullptr;

    std::vector<RowLocation> _block_row_locations;

    ColumnPtr _delete_filter_column;

    bool _is_rowsets_overlapping = true;

    int _binlog_op_pos = -1;
    int _binlog_lsn_pos = -1;
    int _binlog_timestamp_pos = -1;
    bool _binlog_column_pos_inited = false;

    bool _has_seq_map = false;
    // for check multi seq
    std::unordered_map<uint32_t, MutableColumnPtr> _seq_columns;
    // MutableColumns _seq_columns;
    // seq in return_columns, val pos in _normal_columns_idx
    std::unordered_map<uint32_t, std::vector<uint32_t>> _seq_map_in_origin_block;
    std::unordered_map<uint32_t, std::vector<uint32_t>> _seq_map_not_in_origin_block;
    // For each src column index in the binlog block, the index of its companion __BEFORE__
    // column (or itself if no BEFORE mirror exists). Built lazily by _ensure_binlog_column_pos
    // and consulted via _resolve_source_column_index when emitting BEFORE rows.
    std::vector<int> _before_column_idx;
    Arena _arena;
};

} // namespace doris
