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

#include <glog/logging.h>

#include <cstddef>
#include <memory>

#include "common/status.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_varbinary.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_varbinary.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/functions/string_hex_util.h"

namespace doris::vectorized {

class FunctionToBinary : public IFunction {
public:
    static constexpr auto name = "to_binary";

    static FunctionPtr create() { return std::make_shared<FunctionToBinary>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeVarbinary>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto& col_ptr = block.get_by_position(arguments[0]).column;
        if (const auto* col = check_and_get_column<ColumnString>(col_ptr.get())) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            auto col_res = ColumnVarbinary::create();
            const auto& data = col->get_chars();
            const auto& offsets = col->get_offsets();

            std::array<char, string_hex::MAX_STACK_CIPHER_LEN> stack_buf;
            std::vector<char> heap_buf;
            for (int i = 0; i < input_rows_count; ++i) {
                const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                ColumnString::Offset srclen = offsets[i] - offsets[i - 1];

                auto cipher_len = srclen / 2;
                char* dst = nullptr;
                if (cipher_len <= stack_buf.size()) {
                    dst = stack_buf.data();
                } else {
                    heap_buf.resize(cipher_len);
                    dst = heap_buf.data();
                }
                int outlen = string_hex::hex_decode(source, srclen, dst);

                // if empty string or decode failed, may return NULL
                if (outlen == 0) {
                    null_map->get_data()[i] = 1;
                    col_res->insert_default();
                    continue;
                }
                col_res->insert_data(dst, outlen);
            }
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        return Status::OK();
    }
};

class FunctionFromBinary : public IFunction {
public:
    static constexpr auto name = "from_binary";

    static FunctionPtr create() { return std::make_shared<FunctionFromBinary>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto& col_ptr = block.get_by_position(arguments[0]).column;
        if (const auto* col = check_and_get_column<ColumnVarbinary>(col_ptr.get())) {
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            auto col_res = ColumnString::create();
            auto& data = col_res->get_chars();
            auto& offsets = col_res->get_offsets();
            offsets.resize(input_rows_count);
            size_t total_len = 0;
            for (size_t i = 0; i < input_rows_count; ++i) {
                total_len += col->get_data()[i].size() * 2;
            }
            data.resize(total_len);

            size_t offset = 0;
            auto* dst_ptr = reinterpret_cast<unsigned char*>(data.data());
            for (int i = 0; i < input_rows_count; ++i) {
                const auto& val = col->get_data()[i];
                string_hex::hex_encode(reinterpret_cast<const unsigned char*>(val.data()),
                                       val.size(), dst_ptr, offset);
                offsets[i] = cast_set<uint32_t>(offset);
            }
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        return Status::OK();
    }
};

void register_function_binary(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionToBinary>();
    factory.register_function<FunctionFromBinary>();
    factory.register_alias("from_binary", "from_hex");
    factory.register_alias("to_binary", "to_hex");
}

} // namespace doris::vectorized
