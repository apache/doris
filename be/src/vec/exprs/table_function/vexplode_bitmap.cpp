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

#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <vector>

#include "common/status.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VExplodeBitmapTableFunction::VExplodeBitmapTableFunction() {
    _fn_name = "vexplode_bitmap";
}

Status VExplodeBitmapTableFunction::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 1)
            << "VExplodeNumbersTableFunction must be have 1 children but have "
            << _expr_context->root()->children().size();

    int value_column_idx = -1;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &value_column_idx));
    _value_column = block->get_by_position(value_column_idx).column;

    return Status::OK();
}

void VExplodeBitmapTableFunction::reset() {
    _eos = false;
    _cur_offset = 0;
    if (!current_empty()) {
        _cur_iter = std::make_unique<BitmapValueIterator>(*_cur_bitmap);
    }
}

void VExplodeBitmapTableFunction::forward(int step) {
    if (!current_empty()) {
        for (int i = 0; i < step; i++) {
            ++(*_cur_iter);
        }
    }
    TableFunction::forward(step);
}

void VExplodeBitmapTableFunction::get_value(MutableColumnPtr& column) {
    if (current_empty()) {
        column->insert_default();
    } else {
        if (_is_nullable) {
            assert_cast<ColumnInt64*>(
                    assert_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get())
                    ->insert_value(**_cur_iter);
            assert_cast<ColumnUInt8*>(
                    assert_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                    ->insert_default();
        } else {
            assert_cast<ColumnInt64*>(column.get())->insert_value(**_cur_iter);
        }
    }
}

void VExplodeBitmapTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    StringRef value = _value_column->get_data_at(row_idx);

    if (value.data) {
        _cur_bitmap = reinterpret_cast<const BitmapValue*>(value.data);

        _cur_size = _cur_bitmap->cardinality();
        if (!current_empty()) {
            _cur_iter = std::make_unique<BitmapValueIterator>(*_cur_bitmap);
        }
    }
}

void VExplodeBitmapTableFunction::process_close() {
    _value_column = nullptr;
}

} // namespace doris::vectorized
