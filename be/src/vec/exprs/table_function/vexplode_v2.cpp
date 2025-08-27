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

#include "vec/exprs/table_function/vexplode_v2.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <ostream>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_variant.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

#include "common/compile_check_begin.h"
#include "vec/columns/column_struct.h"

VExplodeV2TableFunction::VExplodeV2TableFunction() {
    _fn_name = "vexplode";
}

Status VExplodeV2TableFunction::_process_init_variant(Block* block, int value_column_idx,
                                                      int children_column_idx) {
    // explode variant array
    auto column_without_nullable = remove_nullable(block->get_by_position(value_column_idx).column);
    auto column = column_without_nullable->convert_to_full_column_if_const();
    auto& variant_column = assert_cast<ColumnVariant&>(*(column->assume_mutable()));
    variant_column.finalize();
    _multi_detail[children_column_idx].output_as_variant = true;
    if (!variant_column.is_null_root()) {
        _array_columns[children_column_idx] = variant_column.get_root();
        // We need to wrap the output nested column within a variant column.
        // Otherwise the type is missmatched
        const auto* array_type = check_and_get_data_type<DataTypeArray>(
                remove_nullable(variant_column.get_root_type()).get());
        if (array_type == nullptr) {
            return Status::NotSupported("explode not support none array type {}",
                                        variant_column.get_root_type()->get_name());
        }
        _multi_detail[children_column_idx].nested_type = array_type->get_nested_type();
    } else {
        // null root, use nothing type
        _array_columns[children_column_idx] = ColumnNullable::create(
                ColumnArray::create(ColumnNothing::create(0)), ColumnUInt8::create(0));
        _array_columns[children_column_idx]->assume_mutable()->insert_many_defaults(
                variant_column.size());
        _multi_detail[children_column_idx].nested_type = std::make_shared<DataTypeNothing>();
    }
    return Status::OK();
}

Status VExplodeV2TableFunction::process_init(Block* block, RuntimeState* state) {
    auto expr_size = _expr_context->root()->children().size();
    CHECK(expr_size >= 1) << "VExplodeV2TableFunction support one or more child but has "
                          << expr_size;

    int value_column_idx = -1;
    _multi_detail.resize(expr_size);
    _array_offsets.resize(expr_size);
    _array_columns.resize(expr_size);

    for (int i = 0; i < expr_size; i++) {
        RETURN_IF_ERROR(_expr_context->root()->children()[i]->execute(_expr_context.get(), block,
                                                                      &value_column_idx));
        if (block->get_by_position(value_column_idx).type->get_primitive_type() == TYPE_VARIANT) {
            RETURN_IF_ERROR(_process_init_variant(block, value_column_idx, i));
        } else {
            _array_columns[i] = block->get_by_position(value_column_idx)
                                        .column->convert_to_full_column_if_const();
        }
        if (!extract_column_array_info(*_array_columns[i], _multi_detail[i])) {
            return Status::NotSupported(
                    "column type {} not supported now",
                    block->get_by_position(value_column_idx).column->get_name());
        }
    }

    return Status::OK();
}

void VExplodeV2TableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    for (int i = 0; i < _multi_detail.size(); i++) {
        auto& detail = _multi_detail[i];
        if (!detail.array_nullmap_data || !detail.array_nullmap_data[row_idx]) {
            _array_offsets[i] = (*detail.offsets_ptr)[row_idx - 1];
            // find max size in array
            auto cur_size = (*detail.offsets_ptr)[row_idx] - _array_offsets[i];
            _cur_size = std::max<unsigned long>(_cur_size, cur_size);
        }
    }
    _row_idx = row_idx;
}

void VExplodeV2TableFunction::process_close() {
    _multi_detail.clear();
    _array_offsets.clear();
    _array_columns.clear();
    _row_idx = 0;
}

void VExplodeV2TableFunction::get_same_many_values(MutableColumnPtr& column, int length) {
    if (current_empty()) {
        column->insert_many_defaults(length);
        return;
    }
    ColumnStruct* struct_column = nullptr;
    if (_is_nullable) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        struct_column = assert_cast<ColumnStruct*>(nullable_column->get_nested_column_ptr().get());
        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        nullmap_column->insert_many_defaults(length);
    } else {
        struct_column = assert_cast<ColumnStruct*>(column.get());
    }
    if (!struct_column) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "Only multiple columns can be returned within a struct.");
    }

    if (_generate_row_index) {
        auto& pos_column = assert_cast<ColumnInt32&>(struct_column->get_column(0));
        pos_column.insert_many_vals(static_cast<int32_t>(_cur_offset), length);
    }

    for (int i = 0; i < _multi_detail.size(); i++) {
        auto& detail = _multi_detail[i];
        size_t pos = _array_offsets[i] + _cur_offset;
        size_t element_size = _multi_detail[i].array_col->size_at(_row_idx);
        auto& struct_field = struct_column->get_column(i + (_generate_row_index ? 1 : 0));
        if ((detail.array_nullmap_data && detail.array_nullmap_data[_row_idx])) {
            struct_field.insert_many_defaults(length);
        } else {
            auto* nullable_column = assert_cast<ColumnNullable*>(struct_field.get_ptr().get());
            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
            // only need to check if the value at position pos is null
            if (element_size < _cur_offset ||
                (detail.nested_nullmap_data && detail.nested_nullmap_data[pos])) {
                nullable_column->insert_many_defaults(length);
            } else {
                nullable_column->get_nested_column_ptr()->insert_many_from(*detail.nested_col, pos,
                                                                           length);
                nullmap_column->insert_many_defaults(length);
            }
        }
    }
}

int VExplodeV2TableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
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
            nullmap_column->insert_many_defaults(max_step);

        } else {
            struct_column = assert_cast<ColumnStruct*>(column.get());
        }
        if (!struct_column) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Only multiple columns can be returned within a struct.");
        }

        if (_generate_row_index) {
            auto& pos_column = assert_cast<ColumnInt32&>(struct_column->get_column(0));
            pos_column.insert_range_of_integer(static_cast<int32_t>(_cur_offset),
                                               static_cast<int32_t>(_cur_offset + max_step));
        }

        for (int i = 0; i < _multi_detail.size(); i++) {
            auto& detail = _multi_detail[i];
            size_t pos = _array_offsets[i] + _cur_offset;
            size_t element_size = _multi_detail[i].array_col->size_at(_row_idx);
            auto& struct_field = struct_column->get_column(i + (_generate_row_index ? 1 : 0));
            if (detail.array_nullmap_data && detail.array_nullmap_data[_row_idx]) {
                struct_field.insert_many_defaults(max_step);
            } else {
                auto* nullable_column = assert_cast<ColumnNullable*>(struct_field.get_ptr().get());
                auto* nullmap_column =
                        assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
                if (element_size >= _cur_offset + max_step) {
                    nullable_column->get_nested_column_ptr()->insert_range_from(*detail.nested_col,
                                                                                pos, max_step);
                    if (detail.nested_nullmap_data) {
                        size_t old_size = nullmap_column->size();
                        nullmap_column->resize(old_size + max_step);
                        memcpy(nullmap_column->get_data().data() + old_size,
                               detail.nested_nullmap_data + pos, max_step * sizeof(UInt8));
                    } else {
                        nullmap_column->insert_many_defaults(max_step);
                    }
                } else {
                    auto current_insert_num = element_size - _cur_offset;
                    nullable_column->get_nested_column_ptr()->insert_range_from(
                            *detail.nested_col, pos, current_insert_num);
                    if (detail.nested_nullmap_data) {
                        size_t old_size = nullmap_column->size();
                        nullmap_column->resize(old_size + current_insert_num);
                        memcpy(nullmap_column->get_data().data() + old_size,
                               detail.nested_nullmap_data + pos,
                               current_insert_num * sizeof(UInt8));
                    } else {
                        nullmap_column->insert_many_defaults(current_insert_num);
                    }
                    nullable_column->insert_many_defaults(max_step - current_insert_num);
                }
            }
        }
    }

    forward(max_step);
    return max_step;
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
