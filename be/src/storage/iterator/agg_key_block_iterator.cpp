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
#include "common/exception.h"
#include "core/column/column_equal_flags.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "storage/iterators.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

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

        AggregateDataPtr place =
                _state_arena.aligned_alloc(function->size_of_data(), function->align_of_data());
        // A throwing create() leaves the memory to the arena; the places created before are in
        // _columns, which is reserved, and the destructor destroys them.
        function->create(place);
        _columns.push_back({.ordinal = ordinal, .function = std::move(function), .place = place});
    }
    return Status::OK();
}

void AggKeyBlockIterator::ValueAggregator::fold_group(const Block& block, size_t begin, size_t end,
                                                      MutableColumns& out) {
    DCHECK_LT(begin, end);
    for (auto& column : _columns) {
        const IColumn* source = block.get_by_position(column.ordinal).column.get();
        // A nullable function adds the rows as one batch when none of them is NULL.
        column.function->add_batch_range(begin, end - 1, column.place, &source, _arena,
                                         source->has_null(begin, end));
        column.function->insert_result_into(column.place, *out[column.ordinal]);
        column.function->reset(column.place);
    }
    _arena.clear();
}

AggKeyBlockIterator::AggKeyBlockIterator(RowwiseIteratorUPtr input, size_t max_rows,
                                         size_t preferred_block_size_bytes,
                                         ReadSchemaSPtr read_schema, int be_exec_version,
                                         uint64_t* merged_rows)
        : _input(std::move(input)),
          _max_rows(max_rows),
          _preferred_block_size_bytes(preferred_block_size_bytes),
          _read_schema(std::move(read_schema)),
          _be_exec_version(be_exec_version),
          _merged_rows(merged_rows) {}

Status AggKeyBlockIterator::init() {
    // Every input and caller Block has the exact read-schema layout: the key columns and the
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
    _input_block = _read_schema->create_read_block();
    return _read_input_block();
}

Status AggKeyBlockIterator::next_block(Block* block, bool* eof) {
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    size_t rows = 0;
    // `_input_block` holds the next rows to fold; it always fits into an empty output Block.
    while (!_done && (rows == 0 || _input_fits(columns, rows))) {
        RETURN_IF_ERROR(_fold_input_block(columns));
        rows += _num_groups;
        RETURN_IF_ERROR(_read_input_block());
        // An input Block never splits the rows of one key, so its first row must not have the key
        // of the last output row.
        DORIS_CHECK(_done || !_continues_last_output_key(columns))
                << "an input block goes on with the key of the input block before it";
    }
    *eof = rows == 0;
    return Status::OK();
}

Status AggKeyBlockIterator::_read_input_block() {
    _input_block.clear_column_data();
    Status st = _input->next_batch(&_input_block);
    if (st.is<ErrorCode::END_OF_FILE>()) {
        _done = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(st);
    const size_t rows = _input_block.rows();
    DCHECK_GT(rows, 0);
    _same_key.assign(rows, 1);
    for (uint32_t ordinal : _key_ordinals) {
        const ColumnWithTypeAndName& key = _input_block.get_by_position(ordinal);
        build_equal_flags_for_column(*key.column, remove_nullable(key.type)->get_primitive_type(),
                                     &_same_key);
    }
    // An input Block never splits the rows of one key, so its first row starts a group.
    _same_key[0] = 0;
    _num_groups = static_cast<size_t>(std::ranges::count(_same_key, 0));
    // The heap merge ends a Block at the row limit, but for the rows of its last key.
    DCHECK_LE(_num_groups, _max_rows);
    return Status::OK();
}

bool AggKeyBlockIterator::_input_fits(const MutableColumns& columns, size_t rows) const {
    if (rows + _num_groups > _max_rows) {
        return false;
    }
    if (_preferred_block_size_bytes == 0) {
        return true;
    }
    // The input Block folds into `_num_groups` rows of about its average row size.
    const size_t folded_bytes = _input_block.bytes() * _num_groups / _input_block.rows();
    return Block::columns_byte_size(columns) + folded_bytes <= _preferred_block_size_bytes;
}

Status AggKeyBlockIterator::_fold_input_block(MutableColumns& columns) {
    const size_t rows = _input_block.rows();
    // Whether row `row`, which starts a group, is the whole group: the next row starts another.
    auto one_row_group = [&](size_t row) { return row + 1 == rows || !_same_key[row + 1]; };
    // The last row of every group, which the key and REPLACE columns are copied from.
    std::vector<uint32_t> last_rows;
    last_rows.reserve(_num_groups);
    uint64_t merged_rows = 0;
    for (size_t begin = 0; begin < rows;) {
        size_t end = begin + 1;
        if (one_row_group(begin)) {
            // A stretch of one-row groups is copied as it is stored, as a read that needs no merge
            // returns it.
            while (end < rows && one_row_group(end)) {
                ++end;
            }
            RETURN_IF_CATCH_EXCEPTION({
                for (auto ordinal : _aggregator.agg_ordinals()) {
                    columns[ordinal]->insert_range_from(
                            *_input_block.get_by_position(ordinal).column, begin, end - begin);
                }
            });
            for (size_t row = begin; row < end; ++row) {
                last_rows.push_back(static_cast<uint32_t>(row));
            }
        } else {
            // The group goes on while the rows have its key.
            while (end < rows && _same_key[end]) {
                ++end;
            }
            _aggregator.fold_group(_input_block, begin, end, columns);
            last_rows.push_back(static_cast<uint32_t>(end - 1));
            merged_rows += end - begin - 1;
        }
        begin = end;
    }
    RETURN_IF_CATCH_EXCEPTION({
        for (auto ordinal : _copied_ordinals) {
            columns[ordinal]->insert_indices_from(*_input_block.get_by_position(ordinal).column,
                                                  last_rows.data(),
                                                  last_rows.data() + last_rows.size());
        }
    });
    *_merged_rows += merged_rows;
    return Status::OK();
}

bool AggKeyBlockIterator::_continues_last_output_key(const MutableColumns& columns) const {
    return std::ranges::all_of(_key_ordinals, [&](uint32_t ordinal) {
        const IColumn& output = *columns[ordinal];
        const IColumn& input = *_input_block.get_by_position(ordinal).column;
        return output.compare_at(output.size() - 1, 0, input, -1) == 0;
    });
}

} // namespace doris
