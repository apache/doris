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

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_object.h"
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

VExplodeTableFunction::VExplodeTableFunction() {
    _fn_name = "vexplode";
}

Status
VExplodeTableFunction::_process_init_variant(Block *block, int value_column_idx, ColumnArrayExecutionData &data,
                                             ColumnPtr& array_column) {
    // explode variant array
    auto column_without_nullable = remove_nullable(block->get_by_position(value_column_idx).column);
    auto column = column_without_nullable->convert_to_full_column_if_const();
    const auto &variant_column = assert_cast<const ColumnObject &>(*column);
    data.output_as_variant = true;
    if (!variant_column.is_null_root()) {
        array_column = variant_column.get_root();
        // We need to wrap the output nested column within a variant column.
        // Otherwise the type is missmatched
        const auto *array_type = check_and_get_data_type<DataTypeArray>(
                remove_nullable(variant_column.get_root_type()).get());
        if (array_type == nullptr) {
            return Status::NotSupported("explode not support none array type {}",
                                        variant_column.get_root_type()->get_name());
        }
        data.nested_type = array_type->get_nested_type();
    } else {
        // null root, use nothing type
        array_column = ColumnNullable::create(ColumnArray::create(ColumnNothing::create(0)),
                                              ColumnUInt8::create(0));
        array_column->assume_mutable()->insert_many_defaults(variant_column.size());
        data.nested_type = std::make_shared<DataTypeNothing>();
    }
    return Status::OK();
}

Status VExplodeTableFunction::process_init(Block *block, RuntimeState *state) {
    CHECK(_expr_context->root()->children().size() >= 1)
                    << "VExplodeTableFunction support 1 or more child but has "
                    << _expr_context->root()->children().size();

    int value_column_idx = -1;
    _multi_detail.resize(_expr_context->root()->children().size());
    _array_offsets.resize(_expr_context->root()->children().size());
    _array_columns.resize(_expr_context->root()->children().size());

    for (int i = 0; i < _expr_context->root()->children().size(); i++) {
        RETURN_IF_ERROR(_expr_context->root()->children()[i]->execute(_expr_context.get(), block,
                                                                      &value_column_idx));
        ColumnArrayExecutionData detail = ColumnArrayExecutionData();
        if (WhichDataType(remove_nullable(block->get_by_position(value_column_idx).type))
                .is_variant_type()) {
            RETURN_IF_ERROR(_process_init_variant(block, value_column_idx, detail, _array_columns[i]));
        } else {
            _array_columns[i] =
                    block->get_by_position(value_column_idx).column->convert_to_full_column_if_const();
        }
        if (!extract_column_array_info(*_array_columns[i], detail)) {
            return Status::NotSupported("column type {} not supported now",
                                        block->get_by_position(value_column_idx).column->get_name());
        }
        _multi_detail[i] = detail;
    }

    return Status::OK();
}

void VExplodeTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    for (int i = 0; i < _multi_detail.size(); i++) {
        auto &detail = _multi_detail[i];
        if (!detail.array_nullmap_data || !detail.array_nullmap_data[row_idx]) {
            _array_offsets[i] = (*detail.offsets_ptr)[row_idx - 1];
            // find max size in array
            auto cur_size = (*detail.offsets_ptr)[row_idx] - _array_offsets[i];
            if (_cur_size < cur_size) {
                _cur_size = cur_size;
            }
        }
    }
    _row_idx = row_idx;
}

void VExplodeTableFunction::process_close() {
    _multi_detail.clear();
    _array_offsets.clear();
    _array_columns.clear();
    _row_idx = 0;
}

void VExplodeTableFunction::get_same_many_values(MutableColumnPtr &column, int length) {
    if (current_empty()) {
        column->insert_many_defaults(length);
        return;
    }
    ColumnStruct *struct_column = nullptr;
    if (_is_nullable) {
        auto *nullable_column = assert_cast<ColumnNullable *>(column.get());
        struct_column =
                assert_cast<ColumnStruct *>(nullable_column->get_nested_column_ptr().get());
        auto *nullmap_column =
                assert_cast<ColumnUInt8 *>(nullable_column->get_null_map_column_ptr().get());
        nullmap_column->insert_many_defaults(length);
    } else {
        struct_column = assert_cast<ColumnStruct *>(column.get());
    }
    if (!struct_column) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "Only multiple columns can be returned within a struct.");
    }
    for (int i = 0; i < _multi_detail.size(); i++) {
        auto &detail = _multi_detail[i];
        size_t pos = _array_offsets[i] + _cur_offset;
        size_t element_size = _multi_detail[i].array_col->size_at(_row_idx);
        auto &struct_field = struct_column->get_column(i);
        if ((detail.array_nullmap_data && detail.array_nullmap_data[_row_idx])) {
            column->insert_many_defaults(length);
        } else {
            auto *nullable_column = assert_cast<ColumnNullable *>(struct_field.get_ptr().get());
            auto *nullmap_column =
                    assert_cast<ColumnUInt8 *>(nullable_column->get_null_map_column_ptr().get());
            if (element_size < _cur_offset || (detail.nested_nullmap_data && detail.nested_nullmap_data[pos])) {
                nullable_column->insert_many_defaults(length);
            } else {
                nullable_column->get_nested_column_ptr()->insert_many_from(*detail.nested_col, pos, length);
                nullmap_column->insert_many_defaults(length);
            }
        }
    }
}

int VExplodeTableFunction::get_value(MutableColumnPtr &column, int max_step) {
    max_step = std::min(max_step, (int) (_cur_size - _cur_offset));
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        ColumnStruct *struct_column = nullptr;
        if (_is_nullable) {
            auto *nullable_column = assert_cast<ColumnNullable *>(column.get());
            struct_column =
                    assert_cast<ColumnStruct *>(nullable_column->get_nested_column_ptr().get());
            auto *nullmap_column =
                    assert_cast<ColumnUInt8 *>(nullable_column->get_null_map_column_ptr().get());
            nullmap_column->insert_many_defaults(max_step);

        } else {
            struct_column = assert_cast<ColumnStruct *>(column.get());
        }
        if (!struct_column) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Only multiple columns can be returned within a struct.");
        }
        for (int i = 0; i < _multi_detail.size(); i++) {
            auto &detail = _multi_detail[i];
            size_t pos = _array_offsets[i] + _cur_offset;
            size_t element_size = _multi_detail[i].array_col->size_at(_row_idx);
            auto &struct_field = struct_column->get_column(i);
            if (detail.array_nullmap_data && detail.array_nullmap_data[_row_idx]) {
                struct_field.insert_many_defaults(max_step);
            } else {
                auto *nullable_column = assert_cast<ColumnNullable *>(struct_field.get_ptr().get());
                auto *nullmap_column =
                        assert_cast<ColumnUInt8 *>(nullable_column->get_null_map_column_ptr().get());
                if (element_size >= _cur_offset + max_step) {
                    nullable_column->get_nested_column_ptr()->insert_range_from(*detail.nested_col, pos, max_step);
                    if (detail.nested_nullmap_data) {
                        for (int j = 0; j < max_step; j++) {
                            if (detail.nested_nullmap_data[pos + j]) {
                                nullmap_column->insert_value(1);
                            } else {
                                nullmap_column->insert_value(0);
                            }
                        }
                    } else {
                        nullmap_column->insert_many_defaults(max_step);
                    }
                } else {
                    nullable_column->get_nested_column_ptr()->insert_range_from(*detail.nested_col, pos,
                                                                                element_size - _cur_offset);
                    if (detail.nested_nullmap_data) {
                        for (int j = 0; j < element_size - _cur_offset; j++) {
                            if (detail.nested_nullmap_data[pos + j]) {
                                nullmap_column->insert_value(1);
                            } else {
                                nullmap_column->insert_value(0);
                            }
                        }
                    } else {
                        nullmap_column->insert_many_defaults(element_size - _cur_offset);
                    }
                    nullable_column->insert_many_defaults(max_step - (element_size - _cur_offset));
                }
            }
        }
    }

    forward(max_step);
    return max_step;
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
