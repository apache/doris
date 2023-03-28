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

#include "vec/exprs/table_function/vexplode_numbers.h"

#include "common/status.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VExplodeNumbersTableFunction::VExplodeNumbersTableFunction() {
    _fn_name = "vexplode_numbers";
}

Status VExplodeNumbersTableFunction::process_init(Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 1)
            << "VExplodeSplitTableFunction must be have 1 children but have "
            << _vexpr_context->root()->children().size();

    int value_column_idx = -1;
    RETURN_IF_ERROR(_vexpr_context->root()->children()[0]->execute(_vexpr_context, block,
                                                                   &value_column_idx));
    _value_column = block->get_by_position(value_column_idx).column;
    if (is_column_const(*_value_column)) {
        _cur_size = 0;
        auto& column_nested = assert_cast<const ColumnConst&>(*_value_column).get_data_column_ptr();
        if (column_nested->is_nullable()) {
            if (!column_nested->is_null_at(0)) {
                _cur_size = static_cast<const ColumnNullable*>(column_nested.get())
                                    ->get_nested_column()
                                    .get_int(0);
            }
        } else {
            _cur_size = column_nested->get_int(0);
        }

        if (_cur_size && _cur_size <= block->rows()) { // avoid elements_column too big or empty
            _is_const = true;                          // use const optimize
            for (int i = 0; i < _cur_size; i++) {
                ((ColumnInt32*)_elements_column.get())->insert_value(i);
            }
        }
    }
    return Status::OK();
}

Status VExplodeNumbersTableFunction::process_row(size_t row_idx) {
    RETURN_IF_ERROR(TableFunction::process_row(row_idx));
    if (_is_const) {
        return Status::OK();
    }

    StringRef value = _value_column->get_data_at(row_idx);
    if (value.data != nullptr) {
        _cur_size = std::max(0, *reinterpret_cast<const int*>(value.data));
    }
    return Status::OK();
}

Status VExplodeNumbersTableFunction::process_close() {
    _value_column = nullptr;
    return Status::OK();
}

void VExplodeNumbersTableFunction::get_value(MutableColumnPtr& column) {
    if (current_empty()) {
        column->insert_default();
    } else {
        if (_is_nullable) {
            static_cast<ColumnInt32*>(
                    static_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get())
                    ->insert_value(_cur_offset);
            static_cast<ColumnUInt8*>(
                    static_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                    ->insert_default();
        } else {
            static_cast<ColumnInt32*>(column.get())->insert_value(_cur_offset);
        }
    }
}

} // namespace doris::vectorized
