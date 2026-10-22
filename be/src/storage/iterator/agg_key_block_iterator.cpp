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

#include "common/exception.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

AggValueAggregator::~AggValueAggregator() {
    for (auto& column : _columns) {
        column.function->destroy(column.place);
        delete[] column.place;
    }
}

Status AggValueAggregator::init(const ReadSchema& read_schema, std::vector<uint32_t> agg_ordinals,
                                int be_exec_version, size_t max_buffered_rows) {
    DCHECK(_columns.empty());
    _agg_ordinals = std::move(agg_ordinals);
    _max_buffered_rows = max_buffered_rows;
    _columns.reserve(_agg_ordinals.size());
    for (auto ordinal : _agg_ordinals) {
        const auto& tablet_column = *read_schema.column(ordinal);
        const auto& data_type = read_schema.data_type(ordinal);
        AggregateFunctionPtr function =
                tablet_column.get_aggregate_function(AGG_READER_SUFFIX, be_exec_version, data_type);
        // Every aggregate value column must resolve its reader aggregate function.
        if (!function) {
            return Status::InternalError(
                    "Failed to init the reader aggregate function of column {} (ordinal {})",
                    tablet_column.name(), ordinal);
        }
        AggColumn column;
        column.ordinal = ordinal;
        column.function = function;
        // Fixed-length columns are written by position (replace_column_data), so the buffer
        // is pre-filled with `max_buffered_rows` default rows.
        column.buffer = data_type->create_column();
        column.buffer->insert_many_defaults(_max_buffered_rows);
        column.variable_length = column.buffer->is_variable_length();
        const IColumn* columns[] = {column.buffer.get()};
        function->check_input_columns_type(columns);
        function->check_result_column_type(*column.buffer);

        column.place = new char[function->size_of_data()];
        SAFE_CREATE(function->create(column.place), { delete[] column.place; });
        _columns.push_back(std::move(column));
    }
    return Status::OK();
}

void AggValueAggregator::add_row(const IteratorRowRef& row, MutableColumns& out) {
    _buffered_rows.push_back(row);
    _open_group_rows++;

    // The merge iterator refreshes a Block once its last row is consumed, which would leave
    // the buffered references dangling; so the references are materialized here at the
    // latest. The buffer sized `_max_buffered_rows` bounds the references as well.
    const bool is_last_row_of_block = row.block->rows() == row.row_pos + 1;
    if (is_last_row_of_block || _buffered_rows.size() == _max_buffered_rows) {
        flush(out);
    }
}

void AggValueAggregator::close_group() {
    _closed_group_rows.push_back(_open_group_rows);
    _open_group_rows = 0;
}

void AggValueAggregator::flush(MutableColumns& out) {
    const size_t rows = _materialize_buffered_rows();
    for (auto& column : _columns) {
        column.buffer_has_null = column.buffer->has_null(0, rows);
    }

    size_t begin = 0;
    for (size_t group_rows : _closed_group_rows) {
        _aggregate_rows(begin, group_rows, /*close=*/true, out);
        begin += group_rows;
    }
    _closed_group_rows.clear();

    // The open group continues in the rows still to come: keep its state in the places.
    const size_t open_group_rows = _open_group_rows;
    _open_group_rows = 0;
    if (open_group_rows > 0) {
        _aggregate_rows(begin, open_group_rows, /*close=*/false, out);
    }
}

size_t AggValueAggregator::_materialize_buffered_rows() {
    const size_t rows = _buffered_rows.size();
    for (size_t i = 0; i < rows; ++i) {
        const auto& row = _buffered_rows[i];
        _rows_by_block[row.block.get()].emplace_back(row.row_pos, static_cast<int>(i));
    }

    for (auto& column : _columns) {
        auto& buffer = *column.buffer;
        if (column.variable_length) {
            // Variable-length values cannot be replaced in place: rebuild the buffer in order.
            buffer.clear();
            for (const auto& row : _buffered_rows) {
                buffer.insert_from(*row.block->get_by_position(column.ordinal).column, row.row_pos);
            }
        } else {
            for (const auto& [block, positions] : _rows_by_block) {
                if (positions.empty()) {
                    continue;
                }
                const auto& src_column = *block->get_by_position(column.ordinal).column;
                for (const auto& [src_pos, dst_pos] : positions) {
                    buffer.replace_column_data(src_column, src_pos, dst_pos);
                }
            }
        }
    }

    for (auto& [_, positions] : _rows_by_block) {
        positions.clear();
    }
    _buffered_rows.clear();
    return rows;
}

void AggValueAggregator::_aggregate_rows(size_t begin, size_t rows, bool close,
                                         MutableColumns& out) {
    for (auto& column : _columns) {
        // A closed group whose rows were all materialized by an earlier flush has no rows left
        // here (rows == 0); its result is still in the place.
        if (rows > 0) {
            const IColumn* buffer = column.buffer.get();
            column.function->add_batch_range(begin, begin + rows - 1, column.place, &buffer, _arena,
                                             column.buffer_has_null);
        }
        if (close) {
            column.function->insert_result_into(column.place, *out[column.ordinal]);
            column.function->reset(column.place);
        }
    }
    if (close) {
        _arena.clear();
    }
}

AggKeyBlockIterator::AggKeyBlockIterator(VCollectIterator* collect_iter, OutputBlockLimits limits,
                                         ReadSchemaSPtr read_schema, int be_exec_version,
                                         uint64_t* merged_rows)
        : _cursor(collect_iter),
          _limits(limits),
          _read_schema(std::move(read_schema)),
          _be_exec_version(be_exec_version),
          _merged_rows(merged_rows) {}

Status AggKeyBlockIterator::init() {
    // Every merge and caller Block has the exact read-schema layout: the key columns are
    // copied, every other column is an aggregate value column.
    std::vector<uint32_t> agg_ordinals;
    const auto num_block_columns = static_cast<uint32_t>(_read_schema->num_block_columns());
    for (uint32_t ordinal = 0; ordinal < num_block_columns; ++ordinal) {
        if (_read_schema->column(ordinal)->is_key()) {
            _key_ordinals.push_back(ordinal);
        } else {
            agg_ordinals.push_back(ordinal);
        }
    }
    RETURN_IF_ERROR(_aggregator.init(*_read_schema, std::move(agg_ordinals), _be_exec_version,
                                     _limits.max_rows));
    return _cursor.init();
}

Status AggKeyBlockIterator::_append_key_columns(MutableColumns& columns) {
    const IteratorRowRef& row = _cursor.row();
    RETURN_IF_CATCH_EXCEPTION({
        for (auto ordinal : _key_ordinals) {
            columns[ordinal]->insert_from(*row.block->get_by_position(ordinal).column, row.row_pos);
        }
    });
    return Status::OK();
}

Status AggKeyBlockIterator::next_block(Block* block, bool* eof) {
    if (UNLIKELY(_cursor.eof())) {
        *eof = true;
        return Status::OK();
    }

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();

    // The cursor's row always starts a key group: the previous call stopped right before it.
    RETURN_IF_ERROR(_append_key_columns(target_columns));
    size_t rows = 1;
    size_t merged_rows = 0;
    _aggregator.add_row(_cursor.row(), target_columns);

    while (true) {
        RETURN_IF_ERROR(_cursor.advance());
        if (_cursor.eof()) {
            *eof = true;
            break;
        }

        if (!_cursor.row().is_same) {
            // The row starts a new group and is still unconsumed, so stopping here is safe.
            if (rows == _limits.max_rows || _limits.reached_byte_budget_at(rows, target_columns)) {
                break;
            }
            _aggregator.close_group();
            RETURN_IF_ERROR(_append_key_columns(target_columns));
            rows++;
        } else {
            merged_rows++;
        }
        _aggregator.add_row(_cursor.row(), target_columns);
    }

    // The loop only stops at eof or on the first row of a new key, so the last group of this
    // Block is complete: close it and emit every group buffered so far.
    _aggregator.close_group();
    _aggregator.flush(target_columns);

    *_merged_rows += merged_rows;
    return Status::OK();
}

} // namespace doris
