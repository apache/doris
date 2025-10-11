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
#include "vec/utils/stringop_substring.h"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"

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
        int rows_count = data.size();
        res.resize(rows_count);
        for (int i = 0; i < rows_count; ++i) {
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

        std::array<char, string_hex::MAX_STACK_CIPHER_LEN> stack_buf;
        std::vector<char> heap_buf;
        for (size_t i = 0; i < rows_count; i++) {
            auto binary = data[i];
            auto binlen = binary.size();

            if (binlen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            char* dst = nullptr;
            auto cipher_len = 4 * ((binlen + 2) / 3);
            if (cipher_len <= stack_buf.size()) {
                dst = stack_buf.data();
            } else {
                heap_buf.resize(cipher_len);
                dst = heap_buf.data();
            }

            auto outlen =
                    doris::base64_encode(reinterpret_cast<const unsigned char*>(binary.data()),
                                         binlen, reinterpret_cast<unsigned char*>(dst));

            StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data, dst_offsets);
        }

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

        std::array<char, string_hex::MAX_STACK_CIPHER_LEN> stack_buf;
        std::vector<char> heap_buf;
        for (size_t i = 0; i < rows_count; i++) {
            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            ColumnString::Offset slen = offsets[i] - offsets[i - 1];

            if (slen == 0) {
                res->insert_default();
                continue;
            }

            UInt32 cipher_len = slen;
            char* dst = nullptr;
            if (cipher_len <= stack_buf.size()) {
                dst = stack_buf.data();
            } else {
                heap_buf.resize(cipher_len);
                dst = heap_buf.data();
            }

            auto outlen = doris::base64_decode(source, slen, dst);

            if (outlen < 0) {
                null_map[i] = 1;
                res->insert_default();
            } else {
                res->insert_data(dst, outlen);
            }
        }

        return Status::OK();
    }
};

using FunctionFromBase64Binary = FunctionStringOperateToNullType<FromBase64BinaryImpl>;

struct SubBinary3Impl {
    static DataTypes get_variadic_argument_types() {
        return DataTypes {std::make_shared<DataTypeVarbinary>(), std::make_shared<DataTypeInt32>(),
                          std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        SubBinaryUtil::sub_binary_execute(block, arguments, result, input_rows_count);
        return Status::OK();
    }
};

struct SubBinary2Impl {
    static DataTypes get_variadic_argument_types() {
        return DataTypes {std::make_shared<DataTypeVarbinary>(), std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto col_len = ColumnInt32::create(input_rows_count);
        auto& strlen_data = col_len->get_data();

        ColumnPtr binary_col;
        bool binary_const;
        std::tie(binary_col, binary_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);

        const auto& binarys = assert_cast<const ColumnVarbinary*>(binary_col.get());

        if (binary_const) {
            std::fill(strlen_data.begin(), strlen_data.end(), binarys->get_data()[0].size());
        } else {
            for (int i = 0; i < input_rows_count; ++i) {
                strlen_data[i] = binarys->get_data()[i].size();
            }
        }

        // we complete the column2(strlen) with the default value - each row's strlen.
        block.insert({std::move(col_len), std::make_shared<DataTypeInt32>(), "strlen"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};

        SubBinaryUtil::sub_binary_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

void register_function_binary(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBinaryLength>();
    factory.register_function<FunctionToBase64Binary>();
    factory.register_function<FunctionFromBase64Binary>();
    factory.register_function<FunctionSubBinary<SubBinary2Impl>>();
    factory.register_function<FunctionSubBinary<SubBinary3Impl>>();
    factory.register_function<FunctionToBinary>();
    factory.register_function<FunctionFromBinary>();
    factory.register_alias("from_binary", "from_hex");
    factory.register_alias("to_binary", "to_hex");
}

#include "common/compile_check_avoid_end.h"
} // namespace doris::vectorized
