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

#include "vec/exprs/table_function/vposexplode.h"

#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VPosExplodeTableFunction::VPosExplodeTableFunction() {
    _fn_name = "posexplode";
}

Status VPosExplodeTableFunction::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 1)
            << "VPosExplodeTableFunction only support 1 child but has "
            << _expr_context->root()->children().size();

    int value_column_idx = -1;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &value_column_idx));

    _collection_column =
            block->get_by_position(value_column_idx).column->convert_to_full_column_if_const();

    if (!extract_column_array_info(*_collection_column, _array_detail)) {
        return Status::NotSupported("column type {} not supported now, only support array",
                                    block->get_by_position(value_column_idx).column->get_name());
    }
    if (is_column_nullable(*_collection_column)) {
        _array_data_column =
                assert_cast<const ColumnArray&>(
                        assert_cast<const ColumnNullable&>(*_collection_column).get_nested_column())
                        .get_data_ptr();
    } else {
        _array_data_column = assert_cast<const ColumnArray&>(*_collection_column).get_data_ptr();
    }
    return Status::OK();
}

void VPosExplodeTableFunction::process_row(size_t row_idx) {
    DCHECK(row_idx < _collection_column->size());
    TableFunction::process_row(row_idx);

    if (!_array_detail.array_nullmap_data || !_array_detail.array_nullmap_data[row_idx]) {
        _collection_offset = (*_array_detail.offsets_ptr)[row_idx - 1];
        _cur_size = (*_array_detail.offsets_ptr)[row_idx] - _collection_offset;
    }
}

void VPosExplodeTableFunction::process_close() {
    _collection_column = nullptr;
    _array_data_column = nullptr;
    _array_detail.reset();
    _collection_offset = 0;
}

void VPosExplodeTableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    // now we only support array column explode to struct column
    size_t pos = _collection_offset + _cur_offset;
    // if current is empty array row, also append a default value
    if (current_empty()) {
        column->insert_many_defaults(length);
        return;
    }
    ColumnStruct* ret = nullptr;
    // this _is_nullable is whole output column's nullable
    if (_is_nullable) {
        ret = assert_cast<ColumnStruct*>(
                assert_cast<ColumnNullable*>(column.get())->get_nested_column_ptr().get());
        assert_cast<ColumnUInt8*>(
                assert_cast<ColumnNullable*>(column.get())->get_null_map_column_ptr().get())
                ->insert_many_defaults(length);
    } else if (column->is_column_struct()) {
        ret = assert_cast<ColumnStruct*>(column.get());
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "only support array column explode to struct column");
    }
    if (!ret || ret->tuple_size() != 2) {
        throw Exception(
                ErrorCode::INTERNAL_ERROR,
                "only support array column explode to two column, but given:  ", ret->tuple_size());
    }
    auto& pose_column_nullable = assert_cast<ColumnNullable&>(ret->get_column(0));
    pose_column_nullable.get_null_map_column().insert_many_defaults(length);
    assert_cast<ColumnInt32&>(pose_column_nullable.get_nested_column())
            .insert_many_vals(_cur_offset, length);
    ret->get_column(1).insert_many_from(*_array_data_column, pos, length);
}

int VPosExplodeTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    size_t pos = _collection_offset + _cur_offset;
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        ColumnStruct* struct_column = nullptr;
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            struct_column =
                    assert_cast<ColumnStruct*>(nullable_column->get_nested_column_ptr().get());
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
            // here nullmap_column insert max_step many defaults as if array[row_idx] is NULL
            // will be not update value, _cur_size = 0, means current_empty;
            // so here could insert directly
            nullmap_column->insert_many_defaults(max_step);
        } else {
            struct_column = assert_cast<ColumnStruct*>(column.get());
        }
        if (!struct_column || struct_column->tuple_size() != 2) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "only support array column explode to two column, but given:  ",
                            struct_column->tuple_size());
        }
        auto& pose_column_nullable = assert_cast<ColumnNullable&>(struct_column->get_column(0));
        pose_column_nullable.get_null_map_column().insert_many_defaults(max_step);
        assert_cast<ColumnInt32&>(pose_column_nullable.get_nested_column())
                .insert_range_of_integer(_cur_offset, _cur_offset + max_step);
        struct_column->get_column(1).insert_range_from(*_array_data_column, pos, max_step);
    }
    forward(max_step);
    return max_step;
}
} // namespace doris::vectorized
