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

#include "vec/exprs/table_function/vexplode.h"

#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VExplodeTableFunction::VExplodeTableFunction() {
    _fn_name = "vexplode";
}

Status VExplodeTableFunction::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 1)
            << "VExplodeTableFunction only support 1 child but has "
            << _expr_context->root()->children().size();

    int value_column_idx = -1;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &value_column_idx));

    _array_column =
            block->get_by_position(value_column_idx).column->convert_to_full_column_if_const();
    if (!extract_column_array_info(*_array_column, _detail)) {
        return Status::NotSupported("column type {} not supported now",
                                    block->get_by_position(value_column_idx).column->get_name());
    }

    return Status::OK();
}

void VExplodeTableFunction::process_row(size_t row_idx) {
    DCHECK(row_idx < _array_column->size());
    TableFunction::process_row(row_idx);

    if (!_detail.array_nullmap_data || !_detail.array_nullmap_data[row_idx]) {
        _array_offset = (*_detail.offsets_ptr)[row_idx - 1];
        _cur_size = (*_detail.offsets_ptr)[row_idx] - _array_offset;
    }
}

void VExplodeTableFunction::process_close() {
    _array_column = nullptr;
    _detail.reset();
    _array_offset = 0;
}

void VExplodeTableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    size_t pos = _array_offset + _cur_offset;
    if (current_empty() || (_detail.nested_nullmap_data && _detail.nested_nullmap_data[pos])) {
        column->insert_many_defaults(length);
    } else {
        if (_is_nullable) {
            assert_cast<ColumnNullable*>(column.get())
                    ->get_nested_column_ptr()
                    ->insert_many_from(*_detail.nested_col, pos, length);
            assert_cast<ColumnUInt8*>(
                    assert_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                    ->insert_many_defaults(length);
        } else {
            column->insert_many_from(*_detail.nested_col, pos, length);
        }
    }
}

int VExplodeTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    size_t pos = _array_offset + _cur_offset;
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            auto nested_column = nullable_column->get_nested_column_ptr();
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
            nested_column->insert_range_from(*_detail.nested_col, pos, max_step);
            size_t old_size = nullmap_column->size();
            nullmap_column->resize(old_size + max_step);
            memcpy(nullmap_column->get_data().data() + old_size,
                   _detail.nested_nullmap_data + pos * sizeof(UInt8), max_step * sizeof(UInt8));
        } else {
            column->insert_range_from(*_detail.nested_col, pos, max_step);
        }
    }
    forward(max_step);
    return max_step;
}
} // namespace doris::vectorized
