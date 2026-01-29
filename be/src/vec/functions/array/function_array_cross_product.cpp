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

#include <gen_cpp/Types_types.h>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

class FunctionArrayCrossProduct : public IFunction {
public:
    using DataType = PrimitiveTypeTraits<TYPE_DOUBLE>::DataType;
    using ColumnType = PrimitiveTypeTraits<TYPE_DOUBLE>::ColumnType;

    static constexpr auto name = "cross_product";
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionArrayCrossProduct>(); }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 2) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Invalid number of arguments for function {}", get_name());
        }

        if (arguments[0]->get_primitive_type() != TYPE_ARRAY ||
            arguments[1]->get_primitive_type() != TYPE_ARRAY) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Arguments for function {} must be arrays", get_name());
        }

        // return ARRAY<DOUBLE>
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& arg1 = block.get_by_position(arguments[0]);
        const auto& arg2 = block.get_by_position(arguments[1]);

        const IColumn* col1 = arg1.column.get();
        const IColumn* col2 = arg2.column.get();

        const ColumnConst* col1_const = nullptr;
        const ColumnConst* col2_const = nullptr;

        if (is_column_const(*col1)) {
            col1_const = assert_cast<const ColumnConst*>(col1);
            col1 = &col1_const->get_data_column();
        }
        if (is_column_const(*col2)) {
            col2_const = assert_cast<const ColumnConst*>(col2);
            col2 = &col2_const->get_data_column();
        }

        const ColumnArray* arr1 = nullptr;
        const ColumnArray* arr2 = nullptr;

        if (col1->is_nullable()) {
            auto nullable1 = assert_cast<const ColumnNullable*>(col1);
            arr1 = assert_cast<const ColumnArray*>(nullable1->get_nested_column_ptr().get());
        } else {
            arr1 = assert_cast<const ColumnArray*>(col1);
        }
        if (col2->is_nullable()) {
            auto nullable2 = assert_cast<const ColumnNullable*>(col2);
            arr2 = assert_cast<const ColumnArray*>(nullable2->get_nested_column_ptr().get());
        } else {
            arr2 = assert_cast<const ColumnArray*>(col2);
        }

        const ColumnFloat64* float1 = nullptr;
        const ColumnFloat64* float2 = nullptr;

        if (arr1->get_data_ptr()->is_nullable()) {
            if (arr1->get_data_ptr()->has_null()) {
                return Status::InvalidArgument(
                        "First argument for function {} cannot have null elements", get_name());
            }
            auto nullable1 = assert_cast<const ColumnNullable*>(arr1->get_data_ptr().get());
            float1 = assert_cast<const ColumnFloat64*>(nullable1->get_nested_column_ptr().get());
        } else {
            float1 = assert_cast<const ColumnFloat64*>(arr1->get_data_ptr().get());
        }

        if (arr2->get_data_ptr()->is_nullable()) {
            if (arr2->get_data_ptr()->has_null()) {
                return Status::InvalidArgument(
                        "Second argument for function {} cannot have null elements", get_name());
            }
            auto nullable2 = assert_cast<const ColumnNullable*>(arr2->get_data_ptr().get());
            float2 = assert_cast<const ColumnFloat64*>(nullable2->get_nested_column_ptr().get());
        } else {
            float2 = assert_cast<const ColumnFloat64*>(arr2->get_data_ptr().get());
        }

        const auto* offset1 =
                assert_cast<const ColumnArray::ColumnOffsets*>(arr1->get_offsets_ptr().get());
        const auto* offset2 =
                assert_cast<const ColumnArray::ColumnOffsets*>(arr2->get_offsets_ptr().get());

        // prepare result data
        auto nested_res = ColumnFloat64::create();
        auto& nested_data = nested_res->get_data();
        nested_data.resize(3 * input_rows_count);

        auto offsets_res = ColumnArray::ColumnOffsets::create();
        auto& offsets_data = offsets_res->get_data();
        offsets_data.resize(input_rows_count);

        size_t current_offset = 0;
        for (ssize_t row = 0; row < input_rows_count; ++row) {
            ssize_t row1 = col1_const ? 0 : row;
            ssize_t row2 = col2_const ? 0 : row;

            ssize_t prev_offset1 = (row1 == 0) ? 0 : offset1->get_data()[row1 - 1];
            ssize_t prev_offset2 = (row2 == 0) ? 0 : offset2->get_data()[row2 - 1];

            ssize_t size1 = offset1->get_data()[row] - prev_offset1;
            ssize_t size2 = offset2->get_data()[row] - prev_offset2;

            if (size1 == 0 || size2 == 0) {
                nested_data[current_offset] = 0;
                nested_data[current_offset + 1] = 0;
                nested_data[current_offset + 2] = 0;

                current_offset += 3;
                offsets_data[row] = current_offset;
                continue;
            }

            if (size1 != 3 || size2 != 3) {
                return Status::InvalidArgument("function {} requires arrays of size 3", get_name());
            }

            ssize_t base1 = prev_offset1;
            ssize_t base2 = prev_offset2;

            double a1 = float1->get_data()[base1];
            double a2 = float1->get_data()[base1 + 1];
            double a3 = float1->get_data()[base1 + 2];

            double b1 = float2->get_data()[base2];
            double b2 = float2->get_data()[base2 + 1];
            double b3 = float2->get_data()[base2 + 2];

            nested_data[current_offset] = a2 * b3 - a3 * b2;
            nested_data[current_offset + 1] = a3 * b1 - a1 * b3;
            nested_data[current_offset + 2] = a1 * b2 - a2 * b1;

            current_offset += 3;
            offsets_data[row] = current_offset;
        }

        auto result_col = ColumnArray::create(
                ColumnNullable::create(std::move(nested_res),
                                       ColumnUInt8::create(nested_res->size(), 0)),
                std::move(offsets_res));

        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

void register_function_array_cross_product(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCrossProduct>();
}

} // namespace doris::vectorized