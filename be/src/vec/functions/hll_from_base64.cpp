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
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionHllFromBase64 : public IFunction {
public:
    static constexpr auto name = "hll_from_base64";

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionHllFromBase64>(); }

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
        const auto& str_column = static_cast<const ColumnString&>(*argument_column);
        const ColumnString::Chars& data = str_column.get_chars();
        const ColumnString::Offsets& offsets = str_column.get_offsets();

        res.reserve(input_rows_count);

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
            auto outlen = base64_decode(src_str, src_size, decode_buff.data());
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

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_data_column), std::move(res_null_map));
        return Status::OK();
    }
};

void register_function_hll_from_base64(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHllFromBase64>();
}

} // namespace doris::vectorized