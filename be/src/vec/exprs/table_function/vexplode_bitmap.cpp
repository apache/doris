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

#include "vec/exprs/table_function/vexplode_bitmap.h"

#include "common/status.h"
#include "util/bitmap_value.h"
#include "vec/columns/columns_number.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

VExplodeBitmapTableFunction::VExplodeBitmapTableFunction() {
    _fn_name = "vexplode_bitmap";
}

Status VExplodeBitmapTableFunction::process_init(Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 1)
            << "VExplodeNumbersTableFunction must be have 1 children but have "
            << _vexpr_context->root()->children().size();

    int value_column_idx = -1;
    RETURN_IF_ERROR(_vexpr_context->root()->children()[0]->execute(_vexpr_context, block,
                                                                   &value_column_idx));
    _value_column = block->get_by_position(value_column_idx).column;

    return Status::OK();
}

Status VExplodeBitmapTableFunction::reset() {
    _eos = false;
    _cur_offset = 0;
    if (!current_empty()) {
        _cur_iter.reset(new BitmapValueIterator(*_cur_bitmap));
    }
    return Status::OK();
}

Status VExplodeBitmapTableFunction::forward(int step) {
    if (!current_empty()) {
        for (int i = 0; i < step; i++) {
            ++(*_cur_iter);
        }
    }
    return TableFunction::forward(step);
}

void VExplodeBitmapTableFunction::get_value(MutableColumnPtr& column) {
    if (current_empty()) {
        column->insert_default();
    } else {
        if (_is_nullable) {
            static_cast<ColumnInt64*>(
                    static_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get())
                    ->insert_value(**_cur_iter);
            static_cast<ColumnUInt8*>(
                    static_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                    ->insert_default();
        } else {
            static_cast<ColumnInt64*>(column.get())->insert_value(**_cur_iter);
        }
    }
}

Status VExplodeBitmapTableFunction::process_row(size_t row_idx) {
    RETURN_IF_ERROR(TableFunction::process_row(row_idx));

    StringRef value = _value_column->get_data_at(row_idx);

    if (value.data) {
        _cur_bitmap = reinterpret_cast<const BitmapValue*>(value.data);

        _cur_size = _cur_bitmap->cardinality();
        if (!current_empty()) {
            _cur_iter.reset(new BitmapValueIterator(*_cur_bitmap));
        }
    }

    return Status::OK();
}

Status VExplodeBitmapTableFunction::process_close() {
    _value_column = nullptr;
    return Status::OK();
}

} // namespace doris::vectorized
