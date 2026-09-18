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

#include "exprs/table_function/vstack.h"

#include <algorithm>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "exprs/vexpr.h"

namespace doris {

VStackTableFunction::VStackTableFunction() {
    _fn_name = "stack";
}

Status VStackTableFunction::process_init(Block* block, RuntimeState* /*state*/) {
    const auto& children = _expr_context->root()->children();
    DORIS_CHECK_GE(children.size(), 2);

    int column_index = -1;
    RETURN_IF_ERROR(children[0]->execute(_expr_context.get(), block, &column_index));
    const auto& num_rows_column = block->get_by_position(column_index).column;
    DORIS_CHECK(is_column_const(*num_rows_column));
    const auto num_rows = assert_cast<const ColumnConst&>(*num_rows_column).get_int(0);
    DORIS_CHECK_GT(num_rows, 0);
    _num_rows = static_cast<size_t>(num_rows);
    _num_fields = (children.size() - 2) / _num_rows + 1;

    _value_columns.clear();
    _value_columns.reserve(children.size() - 1);
    for (size_t i = 1; i < children.size(); ++i) {
        RETURN_IF_ERROR(children[i]->execute(_expr_context.get(), block, &column_index));
        const auto& value_column = block->get_by_position(column_index).column;
        const auto& [column, is_const] = unpack_if_const(value_column);
        _value_columns.emplace_back(ValueColumn {.column = column, .is_const = is_const});
    }
    return Status::OK();
}

void VStackTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);
    _row_idx = row_idx;
    _cur_size = static_cast<int64_t>(_num_rows);
}

void VStackTableFunction::process_close() {
    _value_columns.clear();
    _row_idx = 0;
    _num_rows = 0;
    _num_fields = 0;
}

void VStackTableFunction::_insert_value(IColumn& destination, const IColumn& source,
                                        size_t source_row) {
    auto* nullable_destination = check_and_get_column<ColumnNullable>(&destination);
    DORIS_CHECK(nullable_destination != nullptr);

    if (const auto* nullable_source = check_and_get_column<ColumnNullable>(&source)) {
        nullable_destination->get_nested_column().insert_from(nullable_source->get_nested_column(),
                                                              source_row);
        nullable_destination->get_null_map_data().push_back(
                nullable_source->get_null_map_data()[source_row]);
    } else {
        nullable_destination->get_nested_column().insert_from(source, source_row);
        nullable_destination->get_null_map_data().push_back(0);
    }
}

void VStackTableFunction::_insert_output_row(MutableColumnPtr& column, size_t output_row) const {
    IColumn* output = column.get();
    if (_num_fields == 1) {
        const size_t value_index = output_row;
        if (value_index < _value_columns.size()) {
            const auto& value_column = _value_columns[value_index];
            _insert_value(*output, *value_column.column, value_column.is_const ? 0 : _row_idx);
        } else {
            output->insert_default();
        }
        return;
    }

    if (_is_nullable) {
        auto& nullable_output = assert_cast<ColumnNullable&>(*output);
        nullable_output.get_null_map_data().push_back(0);
        output = &nullable_output.get_nested_column();
    }

    auto& struct_output = assert_cast<ColumnStruct&>(*output);
    for (size_t field_index = 0; field_index < _num_fields; ++field_index) {
        const size_t value_index = output_row * _num_fields + field_index;
        auto& field = struct_output.get_column(field_index);
        if (value_index < _value_columns.size()) {
            const auto& value_column = _value_columns[value_index];
            _insert_value(field, *value_column.column, value_column.is_const ? 0 : _row_idx);
        } else {
            field.insert_default();
        }
    }
}

void VStackTableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    for (int i = 0; i < length; ++i) {
        _insert_output_row(column, static_cast<size_t>(_cur_offset));
    }
}

int VStackTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, static_cast<int>(_cur_size - _cur_offset));
    for (int i = 0; i < max_step; ++i) {
        _insert_output_row(column, static_cast<size_t>(_cur_offset + i));
    }
    forward(max_step);
    return max_step;
}

} // namespace doris
