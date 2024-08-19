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

struct EncodeAsSmallInt {
    static constexpr auto name = "encode_as_smallint";
};

struct EncodeAsInt {
    static constexpr auto name = "encode_as_int";
};

struct EncodeAsBigInt {
    static constexpr auto name = "encode_as_bigint";
};

struct EncodeAsLargeInt {
    static constexpr auto name = "encode_as_largeint";
};

template <typename Name, typename ReturnType>
class FunctionEncodeVarchar : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionEncodeVarchar>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeNumber<ReturnType>>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnString* col_str =
                assert_cast<const ColumnString*>(block.get_by_position(arguments[0]).column.get());

        // max_row_byte_size = size of string + size of offset value
        size_t max_str_size = col_str->get_max_row_byte_size() - sizeof(UInt32);

        if (max_str_size > sizeof(ReturnType) - 1) {
            return Status::InternalError(
                    "String is too long to encode, input string size {}, max valid string "
                    "size for {} is {}",
                    max_str_size, name, sizeof(ReturnType) - 1);
        }

        auto col_res = ColumnVector<ReturnType>::create(input_rows_count, 0);
        auto& col_res_data = col_res->get_data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            const char* str_ptr = col_str->get_data_at(i).data;
            UInt8 str_size = static_cast<UInt8>(col_str->get_data_at(i).size);
            ReturnType* res = &col_res_data[i];
            UInt8* __restrict ui8_ptr = reinterpret_cast<UInt8*>(res);

            // "reverse" the order of string on little endian machine.
            simd::reverse_copy_bytes(ui8_ptr, sizeof(ReturnType), str_ptr, str_size);
            // Lowest byte of Integer stores the size of the string, bit left shiflted by 1 so that we can get
            // correct size after right shifting by 1
            memset(ui8_ptr, str_size << 1, 1);
            *res >>= 1;
            // operator &= can not be applied to Int128
            *res = *res & std::numeric_limits<ReturnType>::max();
        }

        block.get_by_position(result).column = std::move(col_res);

        return Status::OK();
    }
};

void register_function_encode_varchar(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionEncodeVarchar<EncodeAsSmallInt, Int16>>();
    factory.register_function<FunctionEncodeVarchar<EncodeAsInt, Int32>>();
    factory.register_function<FunctionEncodeVarchar<EncodeAsBigInt, Int64>>();
    factory.register_function<FunctionEncodeVarchar<EncodeAsLargeInt, Int128>>();
}

} // namespace doris::vectorized
