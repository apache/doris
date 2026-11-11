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

#include "common/check.h"
#include "common/config.h"
#include "common/exception.h"
#include "core/column/column_equal_flags.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "storage/iterators.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// Whether the storage function of `aggregation` returns the single input of a one-row group
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

AggKeyBlockIterator::ValueAggregator::~ValueAggregator() {
    for (auto& column : _columns) {
        column.function->destroy(column.place);
    }
}

Status AggKeyBlockIterator::ValueAggregator::init(const ReadSchema& read_schema,
                                                  std::vector<uint32_t> agg_ordinals,
                                                  int be_exec_version) {
    DCHECK(_columns.empty());
    _agg_ordinals = std::move(agg_ordinals);
    _columns.reserve(_agg_ordinals.size());
    for (auto ordinal : _agg_ordinals) {
        const auto& tablet_column = *read_schema.column(ordinal);
        const auto& data_type = read_schema.data_type(ordinal);
        // The rows of a key come in the order they were written, so REPLACE_IF_NOT_NULL takes the
        // last non-NULL value, as its `_load` function does. That function keeps a pointer to the
        // row instead of a copy, which fold_group() allows: it folds a group within one call.
        const bool takes_last_value =
                tablet_column.aggregation() ==
                FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL;
        const char* suffix = takes_last_value ? AGG_LOAD_SUFFIX : AGG_READER_SUFFIX;
        AggregateFunctionPtr function =
                tablet_column.get_aggregate_function(suffix, be_exec_version, data_type);
        // Every folded value column must resolve its aggregate function.
        if (!function) {
            return Status::InternalError(
                    "Failed to init the aggregate function of column {} (ordinal {})",
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

void AggKeyBlockIterator::ValueAggregator::start_block(const Block& block) {
    const size_t rows = block.rows();
    for (auto& column : _columns) {
        column.block_has_null = block.get_by_position(column.ordinal).column->has_null(0, rows);
    }
}

void AggKeyBlockIterator::ValueAggregator::fold_group(const Block& block, size_t begin, size_t end,
                                                      MutableColumns& out) {
    DCHECK_LT(begin, end);
    for (auto& column : _columns) {
        const IColumn* source = block.get_by_position(column.ordinal).column.get();
        column.function->add_batch_range(begin, end - 1, column.place, &source, _arena,
                                         column.block_has_null);
        column.function->insert_result_into(column.place, *out[column.ordinal]);
        column.function->reset(column.place);
    }
    _arena.clear();
}

AggKeyBlockIterator::AggKeyBlockIterator(RowwiseIteratorUPtr rows, bool merged, size_t max_rows,
                                         size_t preferred_block_size_bytes,
                                         ReadSchemaSPtr read_schema, int be_exec_version,
                                         uint64_t* merged_rows)
        : _rows(std::move(rows)),
          _merged_input(merged),
          _max_rows(max_rows),
          _preferred_block_size_bytes(preferred_block_size_bytes),
          _read_schema(std::move(read_schema)),
          _be_exec_version(be_exec_version),
          _merged_rows(merged_rows) {}

Status AggKeyBlockIterator::init() {
    // Every merged and caller Block has the exact read-schema layout: the key columns and the
    // REPLACE columns are copied from a group's last row, every other column is folded.
    std::vector<uint32_t> agg_ordinals;
    const auto num_block_columns = static_cast<uint32_t>(_read_schema->num_block_columns());
    for (uint32_t ordinal = 0; ordinal < num_block_columns; ++ordinal) {
        const auto& column = *_read_schema->column(ordinal);
        if (column.is_key()) {
            _key_ordinals.push_back(ordinal);
        }
        if (column.is_key() ||
            column.aggregation() == FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE) {
            _copied_ordinals.push_back(ordinal);
        } else {
            agg_ordinals.push_back(ordinal);
        }
    }
    RETURN_IF_ERROR(_aggregator.init(*_read_schema, std::move(agg_ordinals), _be_exec_version));
    _copy_one_row_groups = _aggregator.returns_single_row_unchanged();
    _merged = _read_schema->create_read_block();
    return Status::OK();
}

Status AggKeyBlockIterator::_read_merged_block(const MutableColumns& columns) {
    _merged.clear_column_data();
    _pos = 0;
    Status st = _rows->next_batch(&_merged);
    if (st.is<ErrorCode::END_OF_FILE>()) {
        _done = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(st);
    _mark_same_keys(columns);
    _aggregator.start_block(_merged);
    return Status::OK();
}

void AggKeyBlockIterator::_mark_same_keys(const MutableColumns& columns) {
    const size_t rows = _merged.rows();
    DCHECK_GT(rows, 0);
    if (!_merged_input) {
        _same_key.assign(rows, 0);
        return;
    }
    _same_key.assign(rows, 1);
    for (uint32_t ordinal : _key_ordinals) {
        const ColumnWithTypeAndName& key = _merged.get_by_position(ordinal);
        build_equal_flags_for_column(*key.column, remove_nullable(key.type)->get_primitive_type(),
                                     &_same_key);
    }
    // A merged Block never splits the rows of one key, so its first row starts a group. Folding
    // it on would emit a key twice: check it against the last group of the Block before, which
    // ends the output unless the output is empty.
    _same_key[0] = 0;
    DORIS_CHECK(columns[_key_ordinals.front()]->empty() || !_continues_last_output_key(columns))
            << "a merged block goes on with the key of the merged block before it";
}

bool AggKeyBlockIterator::_continues_last_output_key(const MutableColumns& columns) const {
    return std::ranges::all_of(_key_ordinals, [&](uint32_t ordinal) {
        const IColumn& output = *columns[ordinal];
        const IColumn& merged = *_merged.get_by_position(ordinal).column;
        return output.compare_at(output.size() - 1, 0, merged, -1) == 0;
    });
}

size_t AggKeyBlockIterator::_group_end(size_t begin) const {
    size_t end = begin + 1;
    while (end < _same_key.size() && _same_key[end]) {
        ++end;
    }
    return end;
}

Status AggKeyBlockIterator::_copy_last_rows(MutableColumns& columns) {
    if (_last_rows.empty()) {
        return Status::OK();
    }
    RETURN_IF_CATCH_EXCEPTION({
        for (auto ordinal : _copied_ordinals) {
            columns[ordinal]->insert_indices_from(*_merged.get_by_position(ordinal).column,
                                                  _last_rows.data(),
                                                  _last_rows.data() + _last_rows.size());
        }
    });
    _last_rows.clear();
    return Status::OK();
}

Status AggKeyBlockIterator::_copy_single_rows(MutableColumns& columns, size_t begin, size_t end) {
    for (size_t row = begin; row < end; ++row) {
        _last_rows.push_back(static_cast<uint32_t>(row));
    }
    RETURN_IF_CATCH_EXCEPTION({
        for (auto ordinal : _aggregator.agg_ordinals()) {
            columns[ordinal]->insert_range_from(*_merged.get_by_position(ordinal).column, begin,
                                                end - begin);
        }
    });
    return Status::OK();
}

Status AggKeyBlockIterator::_advance_merged_block(MutableColumns& columns) {
    // The collected last rows point into `_merged`: copy them before it is refilled.
    RETURN_IF_ERROR(_copy_last_rows(columns));
    return _read_merged_block(columns);
}

Status AggKeyBlockIterator::_output_full(MutableColumns& columns, OutputProgress* progress,
                                         bool* full) {
    *full = progress->rows == _max_rows;
    if (!*full && _has_byte_budget() &&
        progress->rows - progress->rows_at_last_check >= BYTE_BUDGET_CHECK_INTERVAL_ROWS) {
        progress->rows_at_last_check = progress->rows;
        // The budget measures the copied columns.
        RETURN_IF_ERROR(_copy_last_rows(columns));
        *full = Block::columns_byte_size(columns) >= _preferred_block_size_bytes;
    }
    return Status::OK();
}

bool AggKeyBlockIterator::_has_byte_budget() const {
    return config::enable_adaptive_batch_size && _preferred_block_size_bytes > 0;
}

size_t AggKeyBlockIterator::_one_row_groups_end(size_t room) const {
    // Row r is a one-row group when the row after it starts another group or r is the last row,
    // since a merged Block holds the whole group of its last row.
    const size_t rows = _merged.rows();
    const size_t limit = std::min(rows, _pos + room);
    size_t end = _pos;
    while (end < limit && (end + 1 == rows || !_same_key[end + 1])) {
        ++end;
    }
    return end;
}

Status AggKeyBlockIterator::_fold_merged_rows(MutableColumns& columns, OutputProgress* progress,
                                              bool* full) {
    while (_pos < _merged.rows()) {
        // Row `_pos` starts a group; stopping in front of it is safe since it is unconsumed.
        DCHECK(!_same_key[_pos]);
        RETURN_IF_ERROR(_output_full(columns, progress, full));
        if (*full) {
            return Status::OK();
        }
        if (_copy_one_row_groups) {
            const size_t end = _one_row_groups_end(_max_rows - progress->rows);
            if (end > _pos) {
                RETURN_IF_ERROR(_copy_single_rows(columns, _pos, end));
                progress->rows += end - _pos;
                _pos = end;
                continue;
            }
        }
        const size_t end = _group_end(_pos);
        _last_rows.push_back(static_cast<uint32_t>(end - 1));
        _aggregator.fold_group(_merged, _pos, end, columns);
        ++progress->rows;
        progress->merged_rows += end - _pos - 1;
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
            RETURN_IF_ERROR(_advance_merged_block(columns));
        } else {
            RETURN_IF_ERROR(_fold_merged_rows(columns, &progress, &full));
        }
    }
    RETURN_IF_ERROR(_copy_last_rows(columns));
    *_merged_rows += progress.merged_rows;
    *eof = progress.rows == 0;
    return Status::OK();
}

} // namespace doris
