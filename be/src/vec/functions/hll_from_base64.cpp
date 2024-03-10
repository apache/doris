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

#include "olap/hll.h"
#include "util/url_coding.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct HllFromBase64 {
    using ArgumentType = DataTypeString;

    static constexpr auto name = "hll_from_base64";

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         std::vector<HyperLogLog>& res, NullMap& null_map,
                         size_t input_rows_count) {
        res.reserve(input_rows_count);
        if (offsets.size() == 0 && input_rows_count == 1) {
            // For NULL constant
            res.emplace_back();
            null_map[0] = 1;
            return Status::OK();
        }
        std::string decode_buff;
        int last_decode_buff_len = 0;
        int curr_decode_buff_len = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            const char* src_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int64_t src_size = offsets[i] - offsets[i - 1];
            if (0 != src_size % 4) {
                res.emplace_back();
                null_map[i] = 1;
                continue;
            }
            curr_decode_buff_len = src_size + 3;
            if (curr_decode_buff_len > last_decode_buff_len) {
                decode_buff.resize(curr_decode_buff_len);
                last_decode_buff_len = curr_decode_buff_len;
            }
            int outlen = base64_decode(src_str, src_size, decode_buff.data());
            if (outlen < 0) {
                res.emplace_back();
                null_map[i] = 1;
            } else {
                doris::Slice decoded_slice(decode_buff.data(), outlen);
                doris::HyperLogLog hll;
                if (!hll.deserialize(decoded_slice)) {
                    return Status::RuntimeError(
                            fmt::format("hll_from_base64 decode failed: base64: {}", src_str));
                } else {
                    res.emplace_back(std::move(hll));
                }
            }
        }
        return Status::OK();
    }
};

template <typename Impl>
class FunctionHllAlwaysNull : public IFunction {
public:
    static constexpr auto name = Impl::name;

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionHllAlwaysNull>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeHLL>());
    }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto res_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res_data_column = ColumnHLL::create();
        auto& null_map = res_null_map->get_data();
        auto& res = res_data_column->get_data();

        ColumnPtr& argument_column = block.get_by_position(arguments[0]).column;
        if constexpr (std::is_same_v<typename Impl::ArgumentType, DataTypeString>) {
            const auto& str_column = static_cast<const ColumnString&>(*argument_column);
            const ColumnString::Chars& data = str_column.get_chars();
            const ColumnString::Offsets& offsets = str_column.get_offsets();
            RETURN_IF_ERROR(Impl::vector(data, offsets, res, null_map, input_rows_count));
        } else if constexpr (std::is_same_v<typename Impl::ArgumentType, DataTypeArray>) {
            auto argument_type = remove_nullable(
                    assert_cast<const DataTypeArray&>(*block.get_by_position(arguments[0]).type)
                            .get_nested_type());
            const auto& array_column = static_cast<const ColumnArray&>(*argument_column);
            const auto& offset_column_data = array_column.get_offsets();
            const auto& nested_nullable_column =
                    static_cast<const ColumnNullable&>(array_column.get_data());
            const auto& nested_column = nested_nullable_column.get_nested_column();
            const auto& nested_null_map = nested_nullable_column.get_null_map_column().get_data();

            WhichDataType which_type(argument_type);
            if (which_type.is_int8()) {
                RETURN_IF_ERROR(Impl::template vector<ColumnInt8>(offset_column_data, nested_column,
                                                                  nested_null_map, res, null_map));
            } else if (which_type.is_uint8()) {
                RETURN_IF_ERROR(Impl::template vector<ColumnUInt8>(
                        offset_column_data, nested_column, nested_null_map, res, null_map));
            } else if (which_type.is_int16()) {
                RETURN_IF_ERROR(Impl::template vector<ColumnInt16>(
                        offset_column_data, nested_column, nested_null_map, res, null_map));
            } else if (which_type.is_int32()) {
                RETURN_IF_ERROR(Impl::template vector<ColumnInt32>(
                        offset_column_data, nested_column, nested_null_map, res, null_map));
            } else if (which_type.is_int64()) {
                RETURN_IF_ERROR(Impl::template vector<ColumnInt64>(
                        offset_column_data, nested_column, nested_null_map, res, null_map));
            } else {
                return Status::RuntimeError("Illegal column {} of argument of function {}",
                                            block.get_by_position(arguments[0]).column->get_name(),
                                            get_name());
            }
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_data_column), std::move(res_null_map));
        return Status::OK();
    }
};

using FunctionHllFromBase64 = FunctionHllAlwaysNull<HllFromBase64>;

void register_function_hll_from_base64(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHllFromBase64>();
}

} // namespace doris::vectorized