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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/status.h"
#include "olap/hll.h"
#include "util/simd/vstring_function.h" //place this header file at last to compile
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
template <typename Impl>
class FunctionHexVariadic : public IFunction {
public:
    static constexpr auto name = "hex";

    static FunctionPtr create() { return std::make_shared<FunctionHexVariadic>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr& argument_column = block.get_by_position(arguments[0]).column;

        auto result_data_column = ColumnString::create();
        auto& result_data = result_data_column->get_chars();
        auto& result_offset = result_data_column->get_offsets();

        RETURN_IF_ERROR(
                Impl::vector(argument_column, input_rows_count, result_data, result_offset));
        block.replace_by_position(result, std::move(result_data_column));
        return Status::OK();
    }
};

static void hex_encode(const unsigned char* source, size_t srclen, unsigned char*& dst_data_ptr,
                       size_t& offset) {
    if (srclen != 0) {
        doris::simd::VStringFunctions::hex_encode(source, srclen,
                                                  reinterpret_cast<char*>(dst_data_ptr));
        dst_data_ptr += (srclen * 2);
        offset += (srclen * 2);
    }
}

struct HexStringImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeString>()}; }

    static Status vector(ColumnPtr argument_column, size_t input_rows_count,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_column.get());
        auto& data = str_col->get_chars();
        auto& offsets = str_col->get_offsets();
        dst_offsets.resize(input_rows_count);
        dst_data.resize(data.size() * 2);

        size_t offset = 0;
        auto dst_data_ptr = dst_data.data();
        for (int i = 0; i < input_rows_count; ++i) {
            auto source = reinterpret_cast<const unsigned char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];
            hex_encode(source, srclen, dst_data_ptr, offset);
            dst_offsets[i] = offset;
        }
        return Status::OK();
    }
};

struct HexIntImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeInt64>()};
    }

    static std::string_view hex(uint64_t num, char* ans) {
        static constexpr auto hex_table = "0123456789ABCDEF";
        // uint64_t max value 0xFFFFFFFFFFFFFFFF , 16 'F'
        if (num == 0) {
            return {hex_table, 1};
        }

        int i = 0;
        while (num) {
            ans[i++] = hex_table[num & 15];
            num = num >> 4;
        }
        ans[i] = '\0';

        // reverse
        for (int k = 0, j = i - 1; k <= j && k <= 16; k++, j--) {
            char tmp = ans[j];
            ans[j] = ans[k];
            ans[k] = tmp;
        }

        return {ans, static_cast<size_t>(i)};
    }

    static Status vector(ColumnPtr argument_column, size_t input_rows_count,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        const auto* str_col = check_and_get_column<ColumnVector<Int64>>(argument_column.get());
        auto& data = str_col->get_data();

        res_offsets.resize(input_rows_count);
        char ans[17];
        for (size_t i = 0; i < input_rows_count; ++i) {
            StringOP::push_value_string(hex(data[i], ans), i, res_data, res_offsets);
        }
        return Status::OK();
    }
};

struct HexHLLImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeHLL>()};
    }

    static Status vector(ColumnPtr argument_column, size_t input_rows_count,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        const auto* str_col = check_and_get_column<ColumnHLL>(argument_column.get());
        const auto& hll_data = str_col->get_data();
        res_offsets.resize(input_rows_count);
        size_t total_length = 0, offset = 0;
        std::string hll_str;
        unsigned char* dst_data_ptr = nullptr;

        for (size_t i = 0; i < input_rows_count; ++i) {
            hll_str.resize(hll_data[i].max_serialized_size(), '0');
            size_t actual_size = hll_data[i].serialize((uint8_t*)hll_str.data());
            hll_str.resize(actual_size);
            total_length += actual_size;

            res_data.resize(total_length * 2 + (i + 1));
            dst_data_ptr = res_data.data() + offset;
            hex_encode(reinterpret_cast<const unsigned char*>(hll_str.data()), hll_str.length(),
                       dst_data_ptr, offset);
            res_offsets[i] = offset;
            hll_str.clear();
        }
        return Status::OK();
    }
};

void register_function_hex_variadic(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHexVariadic<HexStringImpl>>();
    factory.register_function<FunctionHexVariadic<HexIntImpl>>();
    factory.register_function<FunctionHexVariadic<HexHLLImpl>>();
}
} // namespace doris::vectorized
