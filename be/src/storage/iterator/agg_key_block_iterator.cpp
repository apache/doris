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

#include "common/cast_set.h"
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
        AggregateFunctionPtr function =
                tablet_column.get_aggregate_function(AGG_READER_SUFFIX, be_exec_version, data_type);
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

void AggKeyBlockIterator::ValueAggregator::fold_groups(const Block& block, size_t begin,
                                                       std::span<const uint32_t> group_rows,
                                                       MutableColumns& out) {
    for (auto& column : _columns) {
        const IColumn& source = *block.get_by_position(column.ordinal).column;
        IColumn& result = *out[column.ordinal];
        // The groups of one row from `copy_from` on are copied as one range, in front of the next
        // group that is folded.
        size_t copy_from = begin;
        size_t row = begin;
        auto copy_one_row_groups = [&] {
            if (row > copy_from) {
                result.insert_range_from(source, copy_from, row - copy_from);
            }
        };
        for (uint32_t rows : group_rows) {
            if (rows > 1) {
                copy_one_row_groups();
                _fold(column, source, row, row + rows, result);
                copy_from = row + rows;
            }
            row += rows;
        }
        copy_one_row_groups();
    }
}

void AggKeyBlockIterator::ValueAggregator::add_to_open_group(const Block& block, size_t begin,
                                                             size_t end) {
    for (auto& column : _columns) {
        const IColumn* source = block.get_by_position(column.ordinal).column.get();
        // A nullable function adds the rows as one batch when none of them is NULL.
        column.function->add_batch_range(begin, end - 1, column.place, &source, _arena,
                                         source->has_null(begin, end));
    }
}

void AggKeyBlockIterator::ValueAggregator::close_open_group(MutableColumns& out) {
    for (auto& column : _columns) {
        column.function->insert_result_into(column.place, *out[column.ordinal]);
        column.function->reset(column.place);
    }
    _arena.clear();
}

void AggKeyBlockIterator::ValueAggregator::_fold(AggColumn& column, const IColumn& source,
                                                 size_t begin, size_t end, IColumn& result) {
    const IColumn* columns = &source;
    column.function->add_batch_range(begin, end - 1, column.place, &columns, _arena,
                                     source.has_null(begin, end));
    column.function->insert_result_into(column.place, result);
    column.function->reset(column.place);
    _arena.clear();
}

AggKeyBlockIterator::AggKeyBlockIterator(RowwiseIteratorUPtr input,
                                         std::vector<uint32_t> key_ordinals, size_t max_rows,
                                         size_t preferred_block_size_bytes,
                                         ReadSchemaSPtr read_schema, int be_exec_version,
                                         uint64_t* merged_rows)
        : _input(std::move(input)),
          _key_ordinals(std::move(key_ordinals)),
          _max_rows(max_rows),
          _preferred_block_size_bytes(preferred_block_size_bytes),
          _read_schema(std::move(read_schema)),
          _be_exec_version(be_exec_version),
          _merged_rows(merged_rows) {}

Status AggKeyBlockIterator::init() {
    // The key columns and the REPLACE columns are copied from a group's first row, every other
    // column is folded.
    std::vector<uint32_t> agg_ordinals;
    const auto num_block_columns = static_cast<uint32_t>(_read_schema->num_block_columns());
    for (uint32_t ordinal = 0; ordinal < num_block_columns; ++ordinal) {
        if (std::ranges::find(_key_ordinals, ordinal) != _key_ordinals.end() ||
            _read_schema->column(ordinal)->aggregation() ==
                    FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE) {
            _copied_ordinals.push_back(ordinal);
        } else {
            agg_ordinals.push_back(ordinal);
        }
    }
    RETURN_IF_ERROR(_aggregator.init(*_read_schema, std::move(agg_ordinals), _be_exec_version));
    _input_block = _read_schema->create_read_block();
    _open_row = _read_schema->create_read_block();
    return _read_input_block();
}

Status AggKeyBlockIterator::next_block(Block* block, bool* eof) {
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    const size_t max_rows = _output_max_rows();
    size_t rows = 0;
    while (!_done) {
        if (_input_row == _input_block.rows()) {
            RETURN_IF_ERROR(_read_input_block());
            // The merge may split the rows of a key between its Blocks: the first row goes on with
            // the open group when it has the key of the last output row.
            if (!_done && _open_rows > 0) {
                _same_key[0] = _continues_last_output_key(columns);
            }
            continue;
        }
        bool full = false;
        RETURN_IF_CATCH_EXCEPTION(full = _fold_input_block(columns, max_rows, &rows));
        if (full) {
            break;
        }
    }
    if (_open_rows > 0) {
        RETURN_IF_CATCH_EXCEPTION(_close_open_group(columns));
    }
    *eof = rows == 0;
    return Status::OK();
}

Status AggKeyBlockIterator::_read_input_block() {
    _input_block.clear_column_data();
    _input_row = 0;
    Status st = _input->next_batch(&_input_block);
    if (st.is<ErrorCode::END_OF_FILE>()) {
        _done = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(st);
    DCHECK_GT(_input_block.rows(), 0);
    _same_key.assign(_input_block.rows(), 1);
    for (uint32_t ordinal : _key_ordinals) {
        const ColumnWithTypeAndName& key = _input_block.get_by_position(ordinal);
        build_equal_flags_for_column(*key.column, remove_nullable(key.type)->get_primitive_type(),
                                     &_same_key);
    }
    // The first row starts a group, unless the caller finds that it goes on with the open group.
    _same_key[0] = 0;
    return Status::OK();
}

size_t AggKeyBlockIterator::_output_max_rows() const {
    const size_t input_rows = _input_block.rows();
    if (_preferred_block_size_bytes == 0 || input_rows == 0) {
        return _max_rows;
    }
    const size_t row_bytes = std::max<size_t>(1, _input_block.bytes() / input_rows);
    return std::clamp<size_t>(_preferred_block_size_bytes / row_bytes, 1, _max_rows);
}

bool AggKeyBlockIterator::_fold_input_block(MutableColumns& columns, size_t max_rows,
                                            size_t* rows) {
    const size_t input_rows = _input_block.rows();
    size_t row = _input_row;
    // The rows that go on with the open group.
    while (row < input_rows && _same_key[row]) {
        ++row;
    }
    if (row > _input_row) {
        _add_to_open_group(_input_row, row);
    }
    if (row == input_rows) {
        _input_row = row;
        return false;
    }
    // A new key: the open group is complete.
    if (_open_rows > 0) {
        _close_open_group(columns);
    }
    // 1. The groups while the output has room: the first row of each and its row count.
    _first_rows.clear();
    _group_rows.clear();
    for (; row < input_rows; ++row) {
        if (!_same_key[row]) {
            if (*rows == max_rows) {
                break;
            }
            ++*rows;
            _first_rows.push_back(cast_set<uint32_t>(row));
            _group_rows.push_back(0);
        }
        ++_group_rows.back();
    }
    _input_row = row;
    if (_first_rows.empty()) {
        return true;
    }
    // 2. The key and REPLACE columns: the first row of every group, the newest one.
    for (auto ordinal : _copied_ordinals) {
        columns[ordinal]->insert_indices_from(*_input_block.get_by_position(ordinal).column,
                                              _first_rows.data(),
                                              _first_rows.data() + _first_rows.size());
    }
    // 3. The other value columns. The group that ends the input Block stays open.
    const bool ends_block = row == input_rows;
    const std::span<const uint32_t> complete(_group_rows.data(),
                                             _group_rows.size() - (ends_block ? 1 : 0));
    _aggregator.fold_groups(_input_block, _first_rows.front(), complete, columns);
    for (uint32_t group_rows : complete) {
        *_merged_rows += group_rows - 1;
    }
    if (ends_block) {
        _add_to_open_group(_first_rows.back(), input_rows);
    }
    return !ends_block;
}

void AggKeyBlockIterator::_add_to_open_group(size_t begin, size_t end) {
    if (_open_rows == 0 && end - begin == 1) {
        _open_row.clear_column_data();
        auto open_guard = _open_row.mutate_columns_scoped();
        auto& open_columns = open_guard.mutable_columns();
        for (auto ordinal : _aggregator.agg_ordinals()) {
            open_columns[ordinal]->insert_from(*_input_block.get_by_position(ordinal).column,
                                               begin);
        }
    } else {
        if (_open_rows == 1) {
            // A second row: the group is folded, from its first row on.
            _aggregator.add_to_open_group(_open_row, 0, 1);
        }
        _aggregator.add_to_open_group(_input_block, begin, end);
    }
    _open_rows += end - begin;
}

void AggKeyBlockIterator::_close_open_group(MutableColumns& columns) {
    if (_open_rows == 1) {
        // A key of one row is copied as it is stored.
        for (auto ordinal : _aggregator.agg_ordinals()) {
            columns[ordinal]->insert_from(*_open_row.get_by_position(ordinal).column, 0);
        }
    } else {
        _aggregator.close_open_group(columns);
        *_merged_rows += _open_rows - 1;
    }
    _open_rows = 0;
}

bool AggKeyBlockIterator::_continues_last_output_key(const MutableColumns& columns) const {
    return std::ranges::all_of(_key_ordinals, [&](uint32_t ordinal) {
        const IColumn& output = *columns[ordinal];
        const IColumn& input = *_input_block.get_by_position(ordinal).column;
        return output.compare_at(output.size() - 1, 0, input, -1) == 0;
    });
}

} // namespace doris
