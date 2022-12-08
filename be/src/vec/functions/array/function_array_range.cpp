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

#include "common/status.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/columns_number.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <typename Impl>
class FunctionArrayRange : public IFunction {
public:
    static constexpr auto name = "array_range";

    static FunctionPtr create() { return std::make_shared<FunctionArrayRange>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool use_default_implementation_for_constants() const override { return true; }

    ColumnNumbers get_arguments_that_are_always_constant() const override {
        return {get_number_of_arguments()};
    }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto nested_type = make_nullable(std::make_shared<DataTypeInt32>());
        auto res = std::make_shared<DataTypeArray>(nested_type);
        return make_nullable(res);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct RangeImplUtil {
    static Status range_execute(Block& block, const ColumnNumbers& arguments, size_t result,
                                size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        auto return_nested_type = make_nullable(std::make_shared<DataTypeInt32>());
        auto dest_array_column_ptr = ColumnArray::create(return_nested_type->create_column(),
                                                         ColumnArray::ColumnOffsets::create());
        IColumn* dest_nested_column = &dest_array_column_ptr->get_data();
        ColumnNullable* dest_nested_nullable_col =
                reinterpret_cast<ColumnNullable*>(dest_nested_column);
        dest_nested_column = dest_nested_nullable_col->get_nested_column_ptr();
        auto& dest_nested_null_map = dest_nested_nullable_col->get_null_map_column().get_data();

        auto args_null_map = ColumnUInt8::create(input_rows_count, 0);
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(args_null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }
        auto start_column = assert_cast<const ColumnVector<Int32>*>(argument_columns[0].get());
        auto end_column = assert_cast<const ColumnVector<Int32>*>(argument_columns[1].get());
        auto step_column = assert_cast<const ColumnVector<Int32>*>(argument_columns[2].get());

        DCHECK(dest_nested_column != nullptr);
        auto& dest_offsets = dest_array_column_ptr->get_offsets();
        auto nested_column = reinterpret_cast<ColumnVector<Int32>*>(dest_nested_column);
        dest_offsets.reserve(input_rows_count);
        dest_nested_column->reserve(input_rows_count);
        dest_nested_null_map.reserve(input_rows_count);

        vector(start_column->get_data(), end_column->get_data(), step_column->get_data(),
               args_null_map->get_data(), nested_column->get_data(), dest_nested_null_map,
               dest_offsets);

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(dest_array_column_ptr), std::move(args_null_map));
        return Status::OK();
    }

private:
    static void vector(const PaddedPODArray<Int32>& start, const PaddedPODArray<Int32>& end,
                       const PaddedPODArray<Int32>& step, NullMap& args_null_map,
                       PaddedPODArray<Int32>& nested_column,
                       PaddedPODArray<UInt8>& dest_nested_null_map,
                       ColumnArray::Offsets64& dest_offsets) {
        int rows = start.size();
        for (auto row = 0; row < rows; ++row) {
            if (args_null_map[row] || start[row] < 0 || end[row] < 0 || step[row] < 0) {
                nested_column.push_back(0);
                dest_offsets.push_back(dest_offsets.back() + 1);
                dest_nested_null_map.push_back(1);
                args_null_map[row] = 1;
            } else {
                int offset = dest_offsets.back();
                for (auto idx = start[row]; idx < end[row]; idx = idx + step[row]) {
                    nested_column.push_back(idx);
                    dest_nested_null_map.push_back(0);
                    offset++;
                }
                dest_offsets.push_back(offset);
            }
        }
    }
};

struct RangeOneImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeInt32>()}; }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        auto start_column = ColumnInt32::create(input_rows_count, 0);
        auto step_column = ColumnInt32::create(input_rows_count, 1);
        block.insert({std::move(start_column), std::make_shared<DataTypeInt32>(), "start_column"});
        block.insert({std::move(step_column), std::make_shared<DataTypeInt32>(), "step_column"});
        ColumnNumbers temp_arguments = {block.columns() - 2, arguments[0], block.columns() - 1};
        return RangeImplUtil::range_execute(block, temp_arguments, result, input_rows_count);
    }
};

struct RangeTwoImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        auto step_column = ColumnInt32::create(input_rows_count, 1);
        block.insert({std::move(step_column), std::make_shared<DataTypeInt32>(), "step_column"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};
        return RangeImplUtil::range_execute(block, temp_arguments, result, input_rows_count);
    }
};

struct RangeThreeImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        return RangeImplUtil::range_execute(block, arguments, result, input_rows_count);
    }
};

void register_function_array_range(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayRange<RangeOneImpl>>();
    factory.register_function<FunctionArrayRange<RangeTwoImpl>>();
    factory.register_function<FunctionArrayRange<RangeThreeImpl>>();
}

} // namespace doris::vectorized
