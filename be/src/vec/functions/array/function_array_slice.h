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

#include <string_view>

#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

class FunctionArraySlice : public IFunction {
public:
    static constexpr auto name = "array_slice";
    static FunctionPtr create() { return std::make_shared<FunctionArraySlice>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "First argument for function: " << name
                << " should be DataTypeArray but it has type " << arguments[0]->get_name() << ".";
        DCHECK(is_integer(arguments[1]))
                << "Second argument for function: " << name << " should be Integer but it has type "
                << arguments[1]->get_name() << ".";
        if (arguments.size() > 2) {
            DCHECK(is_integer(arguments[2]))
                    << "Third argument for function: " << name
                    << " should be Integer but it has type " << arguments[2]->get_name() << ".";
        }
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto array_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto offset_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnPtr length_column = nullptr;
        if (arguments.size() > 2) {
            length_column =
                    block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        }
        // extract src array column
        ColumnArrayExecutionData src;
        if (!extract_column_array_info(*array_column, src)) {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported types for function {}({}, {})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }
        // prepare dst array column
        bool is_nullable = src.nested_nullmap_data ? true : false;
        ColumnArrayMutableData dst = create_mutable_data(src.nested_col, is_nullable);
        dst.offsets_ptr->reserve(input_rows_count);
        // execute
        _execute_internal(dst, src, *offset_column, length_column.get());
        ColumnPtr res_column = assemble_column_array(dst);
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:
    void _execute_internal(ColumnArrayMutableData& dst, ColumnArrayExecutionData& src,
                           const IColumn& offset_column, const IColumn* length_column) {
        size_t cur = 0;
        for (size_t row = 0; row < src.offsets_ptr->size(); ++row) {
            size_t off = (*src.offsets_ptr)[row - 1];
            size_t len = (*src.offsets_ptr)[row] - off;
            Int64 start = offset_column.get_int(row);
            if (len == 0 || start == 0) {
                dst.offsets_ptr->push_back(cur);
                continue;
            }
            if (start > 0 && start <= len) {
                start += off - 1;
            } else if (start < 0 && -start <= len) {
                start += off + len;
            } else {
                dst.offsets_ptr->push_back(cur);
                continue;
            }
            Int64 end;
            if (length_column) {
                Int64 size = length_column->get_int(row);
                end = std::max((Int64)off, std::min((Int64)(off + len), start + size));
            } else {
                end = off + len;
            }
            for (size_t pos = start; pos < end; ++pos) {
                if (src.nested_nullmap_data && src.nested_nullmap_data[pos]) {
                    dst.nested_col->insert_default();
                    dst.nested_nullmap_data->push_back(1);
                } else {
                    dst.nested_col->insert_from(*src.nested_col, pos);
                    if (dst.nested_nullmap_data) {
                        dst.nested_nullmap_data->push_back(0);
                    }
                }
            }
            if (start < end) {
                cur += end - start;
            }
            dst.offsets_ptr->push_back(cur);
        }
    }
};

} // namespace doris::vectorized
