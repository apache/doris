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

#include "vec/functions/function_varbinary.h"

#include <glog/logging.h>

#include <cstddef>
#include <memory>

#include "common/status.h"
#include "util/url_coding.h"
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
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/functions/string_hex_util.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

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
            col_res->get_data().assign(input_rows_count, StringView());

            for (int i = 0; i < input_rows_count; ++i) {
                const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                ColumnString::Offset srclen = offsets[i] - offsets[i - 1];

                int cipher_len = srclen / 2;
                auto [cipher_inline, dst] = VarBinaryOP::alloc(col_res.get(), i, cipher_len);

                int outlen = string_hex::hex_decode(source, srclen, dst);

                // if empty string or decode failed, may return NULL
                if (outlen == 0) {
                    null_map->get_data()[i] = 1;
                    continue;
                }
                VarBinaryOP::check_and_insert_data(col_res->get_data()[i], dst,
                                                   cast_set<uint32_t>(outlen), cipher_inline);
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

struct NameVarbinaryLength {
    static constexpr auto name = "length";
};

struct VarbinaryLengthImpl {
    using ReturnType = DataTypeInt32;
    using ReturnColumnType = ColumnInt32;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_VARBINARY;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeVarbinary>()};
    }

    static Status vector(const PaddedPODArray<doris::StringView>& data,
                         PaddedPODArray<Int32>& res) {
        size_t rows_count = data.size();
        res.resize(rows_count);
        for (size_t i = 0; i < rows_count; ++i) {
            res[i] = data[i].size();
        }
        return Status::OK();
    }
};

using FunctionBinaryLength = FunctionUnaryToType<VarbinaryLengthImpl, NameVarbinaryLength>;

struct ToBase64BinaryImpl {
    static constexpr auto name = "to_base64_binary";
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_VARBINARY;

    static Status vector(const PaddedPODArray<doris::StringView>& data,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets) {
        auto rows_count = data.size();
        dst_offsets.resize(rows_count);

        size_t total_size = 0;
        for (size_t i = 0; i < rows_count; i++) {
            total_size += 4 * ((data[i].size() + 2) / 3);
        }
        ColumnString::check_chars_length(total_size, rows_count);
        dst_data.resize(total_size);
        auto* dst_data_ptr = dst_data.data();
        size_t offset = 0;

        for (size_t i = 0; i < rows_count; i++) {
            auto binary = data[i];
            auto binlen = binary.size();

            if (UNLIKELY(binlen == 0)) {
                dst_offsets[i] = cast_set<uint32_t>(offset);
                continue;
            }

            auto outlen = doris::base64_encode(
                    reinterpret_cast<const unsigned char*>(binary.data()), binlen,
                    reinterpret_cast<unsigned char*>(dst_data_ptr + offset));

            offset += outlen;
            dst_offsets[i] = cast_set<uint32_t>(offset);
        }

        dst_data.pop_back(total_size - offset);

        return Status::OK();
    }
};

using FunctionToBase64Binary = FunctionStringEncode<ToBase64BinaryImpl, false>;

struct FromBase64BinaryImpl {
    static constexpr auto name = "from_base64_binary";
    using ReturnType = DataTypeVarbinary;
    using ColumnType = ColumnVarbinary;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnVarbinary* res, NullMap& null_map) {
        auto rows_count = offsets.size();
        res->get_data().assign(rows_count, StringView());

        for (size_t i = 0; i < rows_count; i++) {
            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            ColumnString::Offset slen = offsets[i] - offsets[i - 1];

            if (UNLIKELY(slen == 0)) {
                continue;
            }

            int cipher_len = slen / 4 * 3;
            auto [cipher_inline, dst] = VarBinaryOP::alloc(res, i, cipher_len);

            auto outlen = doris::base64_decode(source, slen, dst);

            if (outlen < 0) {
                null_map[i] = 1;
            } else {
                VarBinaryOP::check_and_insert_data(res->get_data()[i], dst,
                                                   cast_set<uint32_t>(outlen), cipher_inline);
            }
        }

        return Status::OK();
    }
};

using FunctionFromBase64Binary = FunctionStringOperateToNullType<FromBase64BinaryImpl>;

void register_function_binary(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBinaryLength>();
    factory.register_function<FunctionToBase64Binary>();
    factory.register_function<FunctionFromBase64Binary>();
    factory.register_function<FunctionSubBinary>();
    factory.register_function<FunctionToBinary>();
    factory.register_function<FunctionFromBinary>();
    factory.register_alias("from_binary", "from_hex");
    factory.register_alias("to_binary", "to_hex");
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
