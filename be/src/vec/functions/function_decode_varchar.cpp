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

#include <fmt/core.h>

#include <cstddef>
#include <limits>
#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "util/simd/reverse_copy_bytes.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename IntegerType>
class FunctionDecodeAsVarchar : public IFunction {
public:
    static constexpr auto name = "decode_as_varchar";
    static FunctionPtr create() { return std::make_shared<FunctionDecodeAsVarchar>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (std::is_same_v<IntegerType, Int16>) {
            return {std::make_shared<DataTypeInt16>()};
        } else if constexpr (std::is_same_v<IntegerType, Int32>) {
            return {std::make_shared<DataTypeInt32>()};
        } else if constexpr (std::is_same_v<IntegerType, Int64>) {
            return {std::make_shared<DataTypeInt64>()};
        } else if constexpr (std::is_same_v<IntegerType, Int128>) {
            return {std::make_shared<DataTypeInt128>()};
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid IntegerType");
        }
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 1) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Function {} requires 1 arguments, got {}", name,
                                   arguments.size());
        }

        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnVector<IntegerType>* col_source = assert_cast<const ColumnVector<IntegerType>*>(
                block.get_by_position(arguments[0]).column.get());

        auto col_res = ColumnString::create();

        ColumnString::Chars& col_res_data = col_res->get_chars();
        ColumnString::Offsets& col_res_offset = col_res->get_offsets();
        col_res_data.resize(input_rows_count * sizeof(IntegerType));
        col_res_offset.resize(input_rows_count);

        for (Int32 i = 0; i < input_rows_count; ++i) {
            IntegerType value = col_source->get_element(i);
            const UInt8* const __restrict ui8_ptr = reinterpret_cast<const UInt8*>(&value);
            UInt32 str_size = static_cast<UInt32>(*ui8_ptr) & 0x7F;

            if (str_size >= sizeof(IntegerType)) {
                const auto& type_ptr = block.get_by_position(arguments[0]).type;
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Invalid input of function {}, input type {} value {}, "
                                       "string size {}, should not be larger than {}",
                                       name, type_ptr->get_name(), value, str_size,
                                       sizeof(IntegerType));
            }

            // col_res_offset[-1] is valid for PaddedPODArray, will get 0
            col_res_offset[i] = col_res_offset[i - 1] + str_size;
            value <<= 1;

            simd::reverse_copy_bytes(col_res_data.data() + col_res_offset[i - 1], str_size,
                                     ui8_ptr + sizeof(IntegerType) - str_size, str_size);
        }

        block.get_by_position(result).column = std::move(col_res);

        return Status::OK();
    }
};

void register_function_decode_as_varchar(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDecodeAsVarchar<Int16>>();
    factory.register_function<FunctionDecodeAsVarchar<Int32>>();
    factory.register_function<FunctionDecodeAsVarchar<Int64>>();
    factory.register_function<FunctionDecodeAsVarchar<Int128>>();
}

} // namespace doris::vectorized
