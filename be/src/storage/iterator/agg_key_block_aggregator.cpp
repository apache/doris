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

#include "storage/iterator/agg_key_block_aggregator.h"

#include <glog/logging.h>

#include <ranges>
#include <utility>

#include "common/cast_set.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/simd/bits.h"

namespace doris {

AggKeyBlockAggregator::AggKeyBlockAggregator(ReadSchemaSPtr read_schema, uint64_t* merged_rows)
        : _read_schema(std::move(read_schema)),
          _num_key_columns(_read_schema->num_key_columns()),
          _merged_rows(merged_rows) {}

AggKeyBlockAggregator::~AggKeyBlockAggregator() {
    for (auto& aggregation : _aggregations) {
        aggregation.function->destroy(aggregation.place);
    }
}

Status AggKeyBlockAggregator::init(int be_exec_version) {
    DCHECK(_aggregations.empty());
    const auto num_block_columns = static_cast<uint32_t>(_read_schema->num_block_columns());
    _aggregations.reserve(num_block_columns - _num_key_columns);
    for (auto ordinal = static_cast<uint32_t>(_num_key_columns); ordinal < num_block_columns;
         ++ordinal) {
        const auto& tablet_column = *_read_schema->column(ordinal);
        const auto& data_type = _read_schema->data_type(ordinal);
        AggregateFunctionPtr function =
                tablet_column.get_aggregate_function(AGG_READER_SUFFIX, be_exec_version, data_type);
        if (!function) {
            return Status::InternalError(
                    "Failed to init the aggregate function of column {} (ordinal {})",
                    tablet_column.name(), ordinal);
        }
        const auto column = data_type->create_column();
        const IColumn* columns[] = {column.get()};
        function->check_input_columns_type(columns);
        function->check_result_column_type(*column);

        AggregateDataPtr place =
                _state_arena.aligned_alloc(function->size_of_data(), function->align_of_data());
        // A throwing create() leaves the memory to the arena; the places created before are in
        // _aggregations, which is reserved, and the destructor destroys them.
        function->create(place);
        _aggregations.push_back(
                {.ordinal = ordinal, .function = std::move(function), .place = place});
    }
    _batch_columns = _read_schema->create_read_block().mutate_columns();
    return Status::OK();
}

void AggKeyBlockAggregator::_mark_same_keys(const Block& source) {
    const size_t rows = source.rows();
    _same_key.assign(rows, 1);
    // From the last key column, which changes most often: once no row has the key of the row
    // before, the other key columns need no comparison.
    for (size_t ordinal : std::views::iota(size_t {0}, _num_key_columns) | std::views::reverse) {
        source.get_by_position(ordinal).column->update_equal_flags(_same_key);
        if (!simd::contain_one(_same_key.data() + 1, rows - 1)) {
            break;
        }
    }
    // The first row goes on with the batch's last key when it has that key.
    bool same_key = _batch_rows > 0;
    for (size_t ordinal : std::views::iota(size_t {0}, _num_key_columns) | std::views::reverse) {
        if (!same_key) {
            break;
        }
        const IColumn& batch = *_batch_columns[ordinal];
        same_key = batch.compare_at(batch.size() - 1, 0, *source.get_by_position(ordinal).column,
                                    -1) == 0;
    }
    _same_key[0] = same_key;
}

void AggKeyBlockAggregator::aggregate(const Block& source) {
    const size_t rows = source.rows();
    DCHECK_GT(rows, 0);
    _mark_same_keys(source);
    // The source starts with a new key: the batch's last key is complete.
    const bool previous_neq = _batch_rows > 0 && !_same_key[0];
    // 1. The first row of every new key, and the row count of every key. When the source goes on
    // with the batch's last key, its rows are the first count, which has no first row.
    _selective_index.clear();
    _aggregate_loops.clear();
    if (_same_key[0]) {
        _aggregate_loops.push_back(0);
    }
    for (size_t pos = 0; pos < rows; ++pos) {
        if (!_same_key[pos]) {
            _selective_index.push_back(cast_set<uint32_t>(pos));
            _aggregate_loops.push_back(0);
        }
        ++_aggregate_loops.back();
    }
    _batch_rows += _selective_index.size();
    // Every row but the first of a key is folded into it.
    *_merged_rows += rows - _selective_index.size();
    // 2. The key columns of the new keys, as one range when each of them has one row.
    if (!_selective_index.empty()) {
        const bool every_key_has_one_row =
                _selective_index.size() == rows - _selective_index.front();
        for (size_t ordinal = 0; ordinal < _num_key_columns; ++ordinal) {
            const IColumn& key = *source.get_by_position(ordinal).column;
            if (every_key_has_one_row) {
                _batch_columns[ordinal]->insert_range_from(key, _selective_index.front(),
                                                           _selective_index.size());
                continue;
            }
            _batch_columns[ordinal]->insert_indices_from(
                    key, _selective_index.data(),
                    _selective_index.data() + _selective_index.size());
        }
    }
    // 3. The value columns, column by column: every key but the last is complete and appended.
    // The last one stays in the states, as the next source may go on with it.
    const size_t last_key_start = rows - _aggregate_loops.back();
    const size_t complete_keys = _aggregate_loops.size() - 1;
    if (previous_neq || complete_keys > 0) {
        for (ValueAggregation& aggregation : _aggregations) {
            if (previous_neq) {
                _output_value_to_batch(aggregation);
            }
            const IColumn& values = *source.get_by_position(aggregation.ordinal).column;
            // A key's own rows are checked for NULLs only when the rows of all keys hold one.
            const bool has_null = values.has_null(0, last_key_start);
            size_t begin = 0;
            for (size_t key = 0; key < complete_keys; ++key) {
                const size_t end = begin + _aggregate_loops[key];
                _aggregate_range(aggregation, values, begin, end,
                                 has_null && values.has_null(begin, end));
                _output_value_to_batch(aggregation);
                begin = end;
            }
        }
        // No state holds a key now: release what the appended keys allocated.
        _arena.clear();
    }
    for (ValueAggregation& aggregation : _aggregations) {
        const IColumn& values = *source.get_by_position(aggregation.ordinal).column;
        _aggregate_range(aggregation, values, last_key_start, rows,
                         values.has_null(last_key_start, rows));
    }
}

void AggKeyBlockAggregator::output_finished_keys(Block* block) {
    DCHECK_EQ(block->rows(), 0);
    DCHECK_GT(_batch_rows, 1);
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    _swap_batch(columns);
    // The open last key starts the next batch: its key row moves there, its values stay in the
    // states and `_arena`.
    const size_t last_key = columns[0]->size() - 1;
    for (size_t ordinal = 0; ordinal < _num_key_columns; ++ordinal) {
        _batch_columns[ordinal]->insert_from(*columns[ordinal], last_key);
        columns[ordinal]->pop_back(1);
    }
    _batch_rows = 1;
}

void AggKeyBlockAggregator::output_all_keys(Block* block) {
    DCHECK_EQ(block->rows(), 0);
    DCHECK_GT(_batch_rows, 0);
    // The batch's last key is complete.
    for (ValueAggregation& aggregation : _aggregations) {
        _output_value_to_batch(aggregation);
    }
    _arena.clear();
    auto columns_guard = block->mutate_columns_scoped();
    _swap_batch(columns_guard.mutable_columns());
    _batch_rows = 0;
}

void AggKeyBlockAggregator::_aggregate_range(ValueAggregation& aggregation, const IColumn& source,
                                             size_t begin, size_t end, bool has_null) {
    const IColumn* columns = &source;
    // add_batch_range() takes the last row, not the end.
    aggregation.function->add_batch_range(begin, end - 1, aggregation.place, &columns, _arena,
                                          has_null);
}

void AggKeyBlockAggregator::_output_value_to_batch(ValueAggregation& aggregation) {
    aggregation.function->insert_result_into(aggregation.place,
                                             *_batch_columns[aggregation.ordinal]);
    aggregation.function->reset(aggregation.place);
}

void AggKeyBlockAggregator::_swap_batch(MutableColumns& columns) {
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        std::swap(columns[ordinal], _batch_columns[ordinal]);
    }
}

} // namespace doris
