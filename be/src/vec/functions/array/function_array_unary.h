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
#pragma once

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

// define functions with one array type argument.
template <typename Impl, typename Name>
class FunctionArrayUnary : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayUnary>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        ColumnArrayExecutionData src;
        if (!extract_column_array_info(*src_column, src)) {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }

        bool is_nullable = src.nested_nullmap_data ? true : false;
        ColumnArrayMutableData dst = create_mutable_data(src.nested_col, is_nullable);
        dst.offsets_ptr->reserve(input_rows_count);

        DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
        auto nested_type = assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();

        auto res_val = _execute_by_type(*src.nested_col, *src.offsets_ptr, *dst.nested_col,
                                        *dst.offsets_ptr, src.nested_nullmap_data,
                                        dst.nested_nullmap_data, nested_type);
        if (!res_val) {
            return Status::RuntimeError(
                    fmt::format("execute failed or unsupported types for function {}({})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name()));
        }

        ColumnPtr res_column = assemble_column_array(dst);
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:
    bool _execute_by_type(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                          IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                          const UInt8* src_null_map, ColumnUInt8::Container* dest_null_map,
                          DataTypePtr& nested_type) {
        Impl impl;
        bool res = false;
        WhichDataType which(remove_nullable(nested_type));
        if (which.is_uint8()) {
            res = impl.template _execute_number<ColumnUInt8>(src_column, src_offsets, dest_column,
                                                             dest_offsets, src_null_map,
                                                             dest_null_map);
        } else if (which.is_int8()) {
            res = impl.template _execute_number<ColumnInt8>(src_column, src_offsets, dest_column,
                                                            dest_offsets, src_null_map,
                                                            dest_null_map);
        } else if (which.is_int16()) {
            res = impl.template _execute_number<ColumnInt16>(src_column, src_offsets, dest_column,
                                                             dest_offsets, src_null_map,
                                                             dest_null_map);
        } else if (which.is_int32()) {
            res = impl.template _execute_number<ColumnInt32>(src_column, src_offsets, dest_column,
                                                             dest_offsets, src_null_map,
                                                             dest_null_map);
        } else if (which.is_int64()) {
            res = impl.template _execute_number<ColumnInt64>(src_column, src_offsets, dest_column,
                                                             dest_offsets, src_null_map,
                                                             dest_null_map);
        } else if (which.is_int128()) {
            res = impl.template _execute_number<ColumnInt128>(src_column, src_offsets, dest_column,
                                                              dest_offsets, src_null_map,
                                                              dest_null_map);
        } else if (which.is_float32()) {
            res = impl.template _execute_number<ColumnFloat32>(src_column, src_offsets, dest_column,
                                                               dest_offsets, src_null_map,
                                                               dest_null_map);
        } else if (which.is_float64()) {
            res = impl.template _execute_number<ColumnFloat64>(src_column, src_offsets, dest_column,
                                                               dest_offsets, src_null_map,
                                                               dest_null_map);
        } else if (which.is_date()) {
            res = impl.template _execute_number<ColumnDate>(src_column, src_offsets, dest_column,
                                                            dest_offsets, src_null_map,
                                                            dest_null_map);
        } else if (which.is_date_time()) {
            res = impl.template _execute_number<ColumnDateTime>(src_column, src_offsets,
                                                                dest_column, dest_offsets,
                                                                src_null_map, dest_null_map);
        } else if (which.is_decimal128()) {
            res = impl.template _execute_number<ColumnDecimal128>(src_column, src_offsets,
                                                                  dest_column, dest_offsets,
                                                                  src_null_map, dest_null_map);
        } else if (which.is_string()) {
            res = impl._execute_string(src_column, src_offsets, dest_column, dest_offsets,
                                       src_null_map, dest_null_map);
        }
        return res;
    }
};

} // namespace doris::vectorized