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

#include <algorithm>
#include <utility>

#include "common/cast_set.h"
#include "core/column/column_equal_flags.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

AggKeyBlockAggregator::ValueAggregator::~ValueAggregator() {
    for (auto& column : _columns) {
        column.function->destroy(column.place);
    }
}

Status AggKeyBlockAggregator::ValueAggregator::init(const ReadSchema& read_schema,
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

void AggKeyBlockAggregator::ValueAggregator::fold_groups(const Block& block, size_t begin,
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

void AggKeyBlockAggregator::ValueAggregator::add_to_open_group(const Block& block, size_t begin,
                                                               size_t end) {
    for (auto& column : _columns) {
        const IColumn* source = block.get_by_position(column.ordinal).column.get();
        // A nullable function adds the rows as one batch when none of them is NULL.
        column.function->add_batch_range(begin, end - 1, column.place, &source, _arena,
                                         source->has_null(begin, end));
    }
}

void AggKeyBlockAggregator::ValueAggregator::close_open_group(MutableColumns& out) {
    for (auto& column : _columns) {
        column.function->insert_result_into(column.place, *out[column.ordinal]);
        column.function->reset(column.place);
    }
    _arena.clear();
}

void AggKeyBlockAggregator::ValueAggregator::_fold(AggColumn& column, const IColumn& source,
                                                   size_t begin, size_t end, IColumn& result) {
    const IColumn* columns = &source;
    column.function->add_batch_range(begin, end - 1, column.place, &columns, _arena,
                                     source.has_null(begin, end));
    column.function->insert_result_into(column.place, result);
    column.function->reset(column.place);
    _arena.clear();
}

AggKeyBlockAggregator::AggKeyBlockAggregator(ReadSchemaSPtr read_schema,
                                             std::vector<uint32_t> key_ordinals, size_t max_rows,
                                             size_t preferred_block_size_bytes,
                                             uint64_t* merged_rows)
        : _read_schema(std::move(read_schema)),
          _key_ordinals(std::move(key_ordinals)),
          _max_rows(max_rows),
          _preferred_block_size_bytes(preferred_block_size_bytes),
          _merged_rows(merged_rows) {}

Status AggKeyBlockAggregator::init(int be_exec_version) {
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
    RETURN_IF_ERROR(_values.init(*_read_schema, std::move(agg_ordinals), be_exec_version));
    _aggregate_columns = _read_schema->create_read_block().mutate_columns();
    _open_group_single_row = _read_schema->create_read_block();
    return Status::OK();
}

void AggKeyBlockAggregator::update_source(const Block& source) {
    DCHECK(source_exhausted());
    _source = &source;
    _source_rows = source.rows();
    DCHECK_GT(_source_rows, 0);
    _source_row = 0;
    _same_key.assign(_source_rows, 1);
    for (uint32_t ordinal : _key_ordinals) {
        const ColumnWithTypeAndName& key = source.get_by_position(ordinal);
        build_equal_flags_for_column(*key.column, remove_nullable(key.type)->get_primitive_type(),
                                     &_same_key);
    }
    // The first row goes on with the open group when it has the last key of the batch.
    _same_key[0] =
            _open_group_row_count > 0 && std::ranges::all_of(_key_ordinals, [&](uint32_t ordinal) {
                const IColumn& batch = *_aggregate_columns[ordinal];
                return batch.compare_at(batch.size() - 1, 0,
                                        *source.get_by_position(ordinal).column, -1) == 0;
            });
}

size_t AggKeyBlockAggregator::_row_limit() const {
    if (_preferred_block_size_bytes == 0) {
        return _max_rows;
    }
    const size_t row_bytes = std::max<size_t>(1, _source->bytes() / _source_rows);
    return std::clamp<size_t>(_preferred_block_size_bytes / row_bytes, 1, _max_rows);
}

void AggKeyBlockAggregator::aggregate() {
    if (source_exhausted()) {
        return;
    }
    if (_aggregate_rows == 0) {
        _max_aggregate_rows = _row_limit();
    }
    size_t row = _source_row;
    // The rows that go on with the open group.
    while (row < _source_rows && _same_key[row]) {
        ++row;
    }
    if (row > _source_row) {
        _add_to_open_group(_source_row, row);
    }
    if (row == _source_rows) {
        _source_row = row;
        return;
    }
    // A new key: the open group is complete.
    if (_open_group_row_count > 0) {
        _close_open_group();
    }
    // 1. The groups while the batch has room: the first row of each and its row count.
    _first_rows.clear();
    _group_rows.clear();
    for (; row < _source_rows; ++row) {
        if (!_same_key[row]) {
            if (_aggregate_rows == _max_aggregate_rows) {
                break;
            }
            ++_aggregate_rows;
            _first_rows.push_back(cast_set<uint32_t>(row));
            _group_rows.push_back(0);
        }
        ++_group_rows.back();
    }
    _source_row = row;
    if (_first_rows.empty()) {
        return;
    }
    // 2. The key and REPLACE columns: the first row of every group, the newest one.
    for (auto ordinal : _copied_ordinals) {
        _aggregate_columns[ordinal]->insert_indices_from(*_source->get_by_position(ordinal).column,
                                                         _first_rows.data(),
                                                         _first_rows.data() + _first_rows.size());
    }
    // 3. The other value columns. The group that ends the source stays open.
    const bool ends_source = row == _source_rows;
    const std::span<const uint32_t> complete(_group_rows.data(),
                                             _group_rows.size() - (ends_source ? 1 : 0));
    _values.fold_groups(*_source, _first_rows.front(), complete, _aggregate_columns);
    for (uint32_t group_rows : complete) {
        *_merged_rows += group_rows - 1;
    }
    if (ends_source) {
        _add_to_open_group(_first_rows.back(), _source_rows);
    }
}

void AggKeyBlockAggregator::aggregate_result(Block* block) {
    DCHECK_EQ(block->rows(), 0);
    if (_open_group_row_count > 0) {
        _close_open_group();
    }
    // The batch takes the place of the empty columns of `block`, which the next batch fills.
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        std::swap(columns[ordinal], _aggregate_columns[ordinal]);
    }
    _aggregate_rows = 0;
}

void AggKeyBlockAggregator::_add_to_open_group(size_t begin, size_t end) {
    if (_open_group_row_count == 0 && end - begin == 1) {
        _open_group_single_row.clear_column_data();
        auto row_guard = _open_group_single_row.mutate_columns_scoped();
        auto& row_columns = row_guard.mutable_columns();
        for (auto ordinal : _values.agg_ordinals()) {
            row_columns[ordinal]->insert_from(*_source->get_by_position(ordinal).column, begin);
        }
    } else {
        if (_open_group_row_count == 1) {
            // A second row: the group is folded, from its first row on.
            _values.add_to_open_group(_open_group_single_row, 0, 1);
        }
        _values.add_to_open_group(*_source, begin, end);
    }
    _open_group_row_count += end - begin;
}

void AggKeyBlockAggregator::_close_open_group() {
    if (_open_group_row_count == 1) {
        // A key of one row is copied as it is stored.
        for (auto ordinal : _values.agg_ordinals()) {
            _aggregate_columns[ordinal]->insert_from(
                    *_open_group_single_row.get_by_position(ordinal).column, 0);
        }
    } else {
        _values.close_open_group(_aggregate_columns);
        *_merged_rows += _open_group_row_count - 1;
    }
    _open_group_row_count = 0;
}

} // namespace doris
