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

#include <memory>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

class FunctionArrayCrossProduct : public IFunction {
public:
    using ColumnType = PrimitiveTypeTraits<TYPE_FLOAT>::ColumnType;

    static constexpr auto name = "array_cross_product";

    static FunctionPtr create() { return std::make_shared<FunctionArrayCrossProduct>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr result_type =
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>()));
        if (arguments[0]->is_nullable() || arguments[1]->is_nullable()) {
            return make_nullable(result_type);
        }
        return result_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        ColumnArrayExecutionDatas array_datas(2);
        if (!extract_column_array_info(*left_column, array_datas[0]) ||
            !extract_column_array_info(*right_column, array_datas[1])) [[unlikely]] {
            return Status::RuntimeError("execute failed, unsupported types for function {}({}, {})",
                                        get_name(),
                                        block.get_by_position(arguments[0]).type->get_name(),
                                        block.get_by_position(arguments[1]).type->get_name());
        }
        const auto& left_data = array_datas[0];
        const auto& right_data = array_datas[1];

        const auto& left_nested_data =
                assert_cast<const ColumnFloat32&>(*left_data.nested_col).get_data();
        const auto& right_nested_data =
                assert_cast<const ColumnFloat32&>(*right_data.nested_col).get_data();

        auto res_data = ColumnType::create();
        auto& res_values = res_data->get_data();
        res_values.reserve(input_rows_count * VECTOR_DIM);
        auto res_offsets = ColumnArray::ColumnOffsets::create();
        auto& offsets = res_offsets->get_data();
        offsets.resize(input_rows_count);
        auto result_nested_null_map = ColumnUInt8::create();
        auto& result_nested_null_map_data = result_nested_null_map->get_data();
        result_nested_null_map_data.reserve(input_rows_count * VECTOR_DIM);
        auto result_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& result_null_map_data = result_null_map->get_data();
        size_t result_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row) {
            const auto left_row = index_check_const(row, left_const);
            const auto right_row = index_check_const(row, right_const);
            const bool is_null =
                    is_top_null(left_data, left_row) || is_top_null(right_data, right_row);
            result_null_map_data[row] = is_null;
            if (is_null) {
                offsets[row] = result_offset;
                continue;
            }

            size_t left_begin = (*left_data.offsets_ptr)[left_row - 1];
            size_t right_begin = (*right_data.offsets_ptr)[right_row - 1];
            auto dim1 = (*left_data.offsets_ptr)[left_row] - left_begin;
            auto dim2 = (*right_data.offsets_ptr)[right_row] - right_begin;

            RETURN_IF_ERROR(check_vector_dims(dim1, dim2));
            if (has_nested_null(left_data, left_begin) ||
                has_nested_null(right_data, right_begin)) {
                return Status::InvalidArgument("function {} cannot have null", get_name());
            }

            compute_cross_product(left_nested_data, left_begin, right_nested_data, right_begin,
                                  res_values, result_nested_null_map_data);
            result_offset += VECTOR_DIM;
            offsets[row] = result_offset;
        }

        auto result_column = ColumnArray::create(
                ColumnNullable::create(std::move(res_data), std::move(result_nested_null_map)),
                std::move(res_offsets));
        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(result, ColumnNullable::create(std::move(result_column),
                                                                     std::move(result_null_map)));
        } else {
            block.replace_by_position(result, std::move(result_column));
        }
        return Status::OK();
    }

private:
    static constexpr size_t VECTOR_DIM = 3;

    static bool is_top_null(const ColumnArrayExecutionData& data, size_t row) {
        return data.array_nullmap_data && data.array_nullmap_data[row];
    }

    static bool has_nested_null(const ColumnArrayExecutionData& data, size_t begin) {
        return data.nested_nullmap_data &&
               (data.nested_nullmap_data[begin] || data.nested_nullmap_data[begin + 1] ||
                data.nested_nullmap_data[begin + 2]);
    }

    Status check_vector_dims(size_t dim1, size_t dim2) const {
        if (dim1 != VECTOR_DIM || dim2 != VECTOR_DIM) {
            return Status::InvalidArgument(
                    "function {} requires both input arrays to have exactly 3 elements, got {} "
                    "and {}",
                    get_name(), dim1, dim2);
        }
        return Status::OK();
    }

    static void compute_cross_product(const ColumnFloat32::Container& left_nested_data,
                                      size_t left_begin,
                                      const ColumnFloat32::Container& right_nested_data,
                                      size_t right_begin, ColumnFloat32::Container& res_values,
                                      ColumnUInt8::Container& result_nested_null_map_data) {
        float x0 = left_nested_data[left_begin];
        float x1 = left_nested_data[left_begin + 1];
        float x2 = left_nested_data[left_begin + 2];
        float y0 = right_nested_data[right_begin];
        float y1 = right_nested_data[right_begin + 1];
        float y2 = right_nested_data[right_begin + 2];
        res_values.push_back(x1 * y2 - x2 * y1);
        res_values.push_back(x2 * y0 - x0 * y2);
        res_values.push_back(x0 * y1 - x1 * y0);
        result_nested_null_map_data.push_back(0);
        result_nested_null_map_data.push_back(0);
        result_nested_null_map_data.push_back(0);
    }
};

void register_function_array_cross_product(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCrossProduct>();
    factory.register_alias("array_cross_product", "cross_product");
}

} // namespace doris
