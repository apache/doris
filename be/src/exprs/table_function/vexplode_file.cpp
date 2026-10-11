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

#include "exprs/table_function/vexplode_file.h"

#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_file.h"
#include "exprs/vexpr.h"

namespace doris {

VExplodeFileTableFunction::VExplodeFileTableFunction() {
    _fn_name = "vexplode_file";
}

Status VExplodeFileTableFunction::process_init(Block* block, RuntimeState* state) {
    const auto& children = _expr_context->root()->children();
    DORIS_CHECK(children.size() == 1);
    ColumnPtr input;
    RETURN_IF_ERROR(
            children[0]->execute_column(_expr_context.get(), block, nullptr, block->rows(), input));
    _input = input->convert_to_full_column_if_const();
    const IColumn* nested = _input.get();
    _nulls = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*nested)) {
        _nulls = nullable->get_null_map_data().data();
        nested = &nullable->get_nested_column();
    }
    _file = check_and_get_column<ColumnFile>(nested);
    if (!_file) return Status::InvalidArgument("explode_file requires FILE input");
    return Status::OK();
}

void VExplodeFileTableFunction::process_row(size_t row_idx) {
    DCHECK_LT(row_idx, _input->size());
    TableFunction::process_row(row_idx);
    _row = row_idx;
    _cur_size = (!_nulls || !_nulls[row_idx]) ? 1 : 0;
}

void VExplodeFileTableFunction::process_close() {
    _input = nullptr;
    _file = nullptr;
    _nulls = nullptr;
    _row = 0;
}

void VExplodeFileTableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    if (current_empty()) {
        if (is_outer()) column->insert_many_defaults(length);
        return;
    }
    IColumn* nested = column.get();
    if (_is_nullable) {
        auto& nullable = assert_cast<ColumnNullable&>(*nested);
        nullable.get_null_map_column().insert_many_defaults(length);
        nested = &nullable.get_nested_column();
    }
    auto& structure = assert_cast<ColumnStruct&>(*nested);
    DCHECK_EQ(structure.tuple_size(), DataTypeFile::FIELD_COUNT);
    for (size_t i = 0; i < DataTypeFile::FIELD_COUNT; ++i) {
        structure.get_column(i).insert_many_from(_file->get_column(i), _row, length);
    }
}

int VExplodeFileTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    if (max_step <= 0 || eos()) return 0;
    if (current_empty() && !is_outer()) {
        forward();
        return 0;
    }
    get_same_many_values(column, 1);
    forward();
    return 1;
}

} // namespace doris
