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

#include "storage/iterator/agg_key_block_iterator.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "common/exception.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "storage/iterators.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// Whether the `_reader` function of `aggregation` returns the single input of a one-row group
// unchanged: the sum, extremum or replacement of one value is that value, and the union of one
// HLL or bitmap holds the same values.
bool reader_function_returns_single_input(FieldAggregationMethod aggregation) {
    switch (aggregation) {
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM:
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN:
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX:
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE:
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_HLL_UNION:
    case FieldAggregationMethod::OLAP_FIELD_AGGREGATION_BITMAP_UNION:
        return true;
    default:
        return false;
    }
}

} // namespace

AggValueAggregator::~AggValueAggregator() {
    for (auto& column : _columns) {
        column.function->destroy(column.place);
    }
}

Status AggValueAggregator::init(const ReadSchema& read_schema, std::vector<uint32_t> agg_ordinals,
                                int be_exec_version) {
    DCHECK(_columns.empty());
    _agg_ordinals = std::move(agg_ordinals);
    _columns.reserve(_agg_ordinals.size());
    for (auto ordinal : _agg_ordinals) {
        const auto& tablet_column = *read_schema.column(ordinal);
        const auto& data_type = read_schema.data_type(ordinal);
        AggregateFunctionPtr function =
                tablet_column.get_aggregate_function(AGG_READER_SUFFIX, be_exec_version, data_type);
        // Every folded value column must resolve its reader aggregate function.
        if (!function) {
            return Status::InternalError(
                    "Failed to init the reader aggregate function of column {} (ordinal {})",
                    tablet_column.name(), ordinal);
        }
        const auto column = data_type->create_column();
        const IColumn* columns[] = {column.get()};
        function->check_input_columns_type(columns);
        function->check_result_column_type(*column);
        _returns_single_row_unchanged =
                _returns_single_row_unchanged &&
                reader_function_returns_single_input(tablet_column.aggregation());

        AggregateDataPtr place =
                _state_arena.aligned_alloc(function->size_of_data(), function->align_of_data());
        // A throwing create() leaves the memory to the arena; the places created before are in
        // _columns, which is reserved, and the destructor destroys them.
        function->create(place);
        _columns.push_back({.ordinal = ordinal,
                            .function = std::move(function),
                            .place = place,
                            .block_has_null = false});
    }
    return Status::OK();
}

void AggValueAggregator::start_block(const Block& block) {
    const size_t rows = block.rows();
    for (auto& column : _columns) {
        column.block_has_null = block.get_by_position(column.ordinal).column->has_null(0, rows);
    }
}

void AggValueAggregator::add_rows(const Block& block, size_t begin, size_t end) {
    DCHECK_LT(begin, end);
    for (auto& column : _columns) {
        const IColumn* source = block.get_by_position(column.ordinal).column.get();
        column.function->add_batch_range(begin, end - 1, column.place, &source, _arena,
                                         column.block_has_null);
    }
}

void AggValueAggregator::close_group(MutableColumns& out) {
    for (auto& column : _columns) {
        column.function->insert_result_into(column.place, *out[column.ordinal]);
        column.function->reset(column.place);
    }
    _arena.clear();
}

AggKeyBlockIterator::AggKeyBlockIterator(RowwiseIterator* rows, OutputBlockLimits limits,
                                         ReadSchemaSPtr read_schema, int be_exec_version,
                                         uint64_t* merged_rows)
        : _rows(rows),
          _limits(limits),
          _read_schema(std::move(read_schema)),
          _be_exec_version(be_exec_version),
          _merged_rows(merged_rows) {}

Status AggKeyBlockIterator::init() {
    // Every merged and caller Block has the exact read-schema layout: the key columns and the
    // REPLACE columns are copied from a group's first row, every other column is folded.
    std::vector<uint32_t> agg_ordinals;
    const auto num_block_columns = static_cast<uint32_t>(_read_schema->num_block_columns());
    for (uint32_t ordinal = 0; ordinal < num_block_columns; ++ordinal) {
        const auto& column = *_read_schema->column(ordinal);
        if (column.is_key() ||
            column.aggregation() == FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE) {
            _head_ordinals.push_back(ordinal);
        } else {
            agg_ordinals.push_back(ordinal);
        }
    }
    RETURN_IF_ERROR(_aggregator.init(*_read_schema, std::move(agg_ordinals), _be_exec_version));
    _copy_one_row_groups = _aggregator.returns_single_row_unchanged();
    _merged = _read_schema->create_read_block();
    return Status::OK();
}

Status AggKeyBlockIterator::_read_merged_block() {
    _merged.clear_column_data();
    _pos = 0;
    Status st = _rows->next_batch(&_merged, &_same_key);
    if (st.is<ErrorCode::END_OF_FILE>()) {
        _rows_eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(st);
    DCHECK_EQ(_same_key.size(), _merged.rows());
    _aggregator.start_block(_merged);
    return Status::OK();
}

size_t AggKeyBlockIterator::_group_end(size_t begin) const {
    size_t end = begin + 1;
    while (end < _same_key.size() && _same_key[end]) {
        ++end;
    }
    return end;
}

Status AggKeyBlockIterator::_copy_heads(MutableColumns& columns) {
    if (_heads.empty()) {
        return Status::OK();
    }
    RETURN_IF_CATCH_EXCEPTION({
        for (auto ordinal : _head_ordinals) {
            columns[ordinal]->insert_indices_from(*_merged.get_by_position(ordinal).column,
                                                  _heads.data(), _heads.data() + _heads.size());
        }
    });
    _heads.clear();
    return Status::OK();
}

Status AggKeyBlockIterator::_copy_single_rows(MutableColumns& columns, size_t begin, size_t end) {
    for (size_t row = begin; row < end; ++row) {
        _heads.push_back(static_cast<uint32_t>(row));
    }
    RETURN_IF_CATCH_EXCEPTION({
        for (auto ordinal : _aggregator.agg_ordinals()) {
            columns[ordinal]->insert_range_from(*_merged.get_by_position(ordinal).column, begin,
                                                end - begin);
        }
    });
    return Status::OK();
}

Status AggKeyBlockIterator::_advance_merged_block(MutableColumns& columns,
                                                  OutputProgress* progress) {
    // The collected heads point into `_merged`: copy them before it is refilled.
    RETURN_IF_ERROR(_copy_heads(columns));
    RETURN_IF_ERROR(_read_merged_block());
    if (_rows_eof) {
        if (_group_open) {
            _aggregator.close_group(columns);
            _group_open = false;
            ++progress->rows;
        }
        _done = true;
    }
    return Status::OK();
}

Status AggKeyBlockIterator::_output_full(MutableColumns& columns, OutputProgress* progress,
                                         bool* full) {
    *full = progress->rows == _limits.max_rows;
    if (!*full && _limits.has_byte_budget() &&
        progress->rows - progress->rows_at_last_check >=
                OutputBlockLimits::BYTE_BUDGET_CHECK_INTERVAL_ROWS) {
        progress->rows_at_last_check = progress->rows;
        // The budget measures the copied columns.
        RETURN_IF_ERROR(_copy_heads(columns));
        *full = _limits.reached_byte_budget(columns);
    }
    return Status::OK();
}

size_t AggKeyBlockIterator::_one_row_groups_end(size_t room) const {
    // Row r is a one-row group when the row after it starts another group of this merged
    // Block; the last row of the Block may go on into the next one.
    const size_t limit = std::min(_merged.rows() - 1, _pos + room);
    size_t end = _pos;
    while (end < limit && !_same_key[end + 1]) {
        ++end;
    }
    return end;
}

Status AggKeyBlockIterator::_fold_merged_rows(MutableColumns& columns, OutputProgress* progress,
                                              bool* full) {
    while (_pos < _merged.rows()) {
        if (_same_key[_pos]) {
            // The open group goes on from the previous merged Block.
            DCHECK(_group_open && _pos == 0);
            const size_t end = _group_end(_pos);
            _aggregator.add_rows(_merged, _pos, end);
            progress->merged_rows += end - _pos;
            _pos = end;
            continue;
        }
        // Row `_pos` starts a group; stopping in front of it is safe since it is unconsumed.
        if (_group_open) {
            _aggregator.close_group(columns);
            _group_open = false;
            ++progress->rows;
        }
        RETURN_IF_ERROR(_output_full(columns, progress, full));
        if (*full) {
            return Status::OK();
        }
        if (_copy_one_row_groups) {
            const size_t end = _one_row_groups_end(_limits.max_rows - progress->rows);
            if (end > _pos) {
                RETURN_IF_ERROR(_copy_single_rows(columns, _pos, end));
                progress->rows += end - _pos;
                _pos = end;
                continue;
            }
        }
        _heads.push_back(static_cast<uint32_t>(_pos));
        const size_t end = _group_end(_pos);
        _aggregator.add_rows(_merged, _pos, end);
        progress->merged_rows += end - _pos - 1;
        _group_open = true;
        _pos = end;
    }
    return Status::OK();
}

Status AggKeyBlockIterator::next_block(Block* block, bool* eof) {
    if (_done) {
        *eof = true;
        return Status::OK();
    }
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& columns = target_columns_guard.mutable_columns();
    OutputProgress progress;
    bool full = false;
    while (!full && !_done) {
        if (_pos == _merged.rows()) {
            RETURN_IF_ERROR(_advance_merged_block(columns, &progress));
        } else {
            RETURN_IF_ERROR(_fold_merged_rows(columns, &progress, &full));
        }
    }
    RETURN_IF_ERROR(_copy_heads(columns));
    *_merged_rows += progress.merged_rows;
    *eof = progress.rows == 0;
    return Status::OK();
}

} // namespace doris
