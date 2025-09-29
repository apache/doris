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

#include "util/url_coding.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"

static constexpr int MAX_STACK_CIPHER_LEN = 1024 * 64;

struct NameVarbinaryLength {
    static constexpr auto name = "length";
};

struct VarbinaryLengthImpl {
    using ReturnType = DataTypeInt32;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_VARBINARY;
    using ReturnColumnType = ColumnInt32;

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
    static constexpr auto is_nullable = false;

    using ReturnType = DataTypeString;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto& col_ptr = block.get_by_position(arguments[0]).column;
        if (const auto* col = check_and_get_column<ColumnVarbinary>(col_ptr.get())) {
            auto result_column = ColumnString::create();
            result_column->get_offsets().reserve(input_rows_count);

            for (size_t i = 0; i < input_rows_count; i++) {
                auto binary = col->get_data_at(i);

                if (binary.size == 0) {
                    result_column->insert_default();
                    continue;
                }

                char dst_array[MAX_STACK_CIPHER_LEN];
                char* dst = dst_array;

                int cipher_len = 4 * ((binary.size + 2) / 3);
                std::unique_ptr<char[]> dst_uptr;
                if (cipher_len > MAX_STACK_CIPHER_LEN) {
                    dst_uptr.reset(new char[cipher_len]);
                    dst = dst_uptr.get();
                }

                auto len = doris::base64_encode(reinterpret_cast<const unsigned char*>(binary.data),
                                                binary.size, reinterpret_cast<unsigned char*>(dst));

                result_column->insert_data(dst, len);
            }
            block.replace_by_position(result, std::move(result_column));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        ToBase64BinaryImpl::name);
        }

        return Status::OK();
    }
};

using FunctionToBase64Binary = FunctionBinaryUnary<ToBase64BinaryImpl>;

struct FromBase64BinaryImpl {
    static constexpr auto name = "from_base64_binary";
    static constexpr auto is_nullable = true;

    using ReturnType = DataTypeVarbinary;

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto& col_ptr = block.get_by_position(arguments[0]).column;
        if (const auto* col = check_and_get_column<ColumnString>(col_ptr.get())) {
            auto result_column = ColumnVarbinary::create();
            auto null_map = ColumnUInt8::create(input_rows_count, 0);

            for (size_t i = 0; i < input_rows_count; i++) {
                auto base64_string = col->get_data_at(i);

                if (base64_string.size == 0) {
                    result_column->insert_default();
                    continue;
                }

                char dst_array[MAX_STACK_CIPHER_LEN];
                char* dst = dst_array;

                int cipher_len = base64_string.size;
                std::unique_ptr<char[]> dst_uptr;
                if (cipher_len > MAX_STACK_CIPHER_LEN) {
                    dst_uptr.reset(new char[cipher_len]);
                    dst = dst_uptr.get();
                }

                auto len = doris::base64_decode(base64_string.data, base64_string.size, dst);

                if (len < 0) {
                    null_map->get_data()[i] = 1;
                    result_column->insert_default();
                } else {
                    result_column->insert_data(dst, len);
                }
            }

            block.replace_by_position(
                    result, ColumnNullable::create(std::move(result_column), std::move(null_map)));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        FromBase64BinaryImpl::name);
        }
        return Status::OK();
    }
};

using FunctionFromBase64Binary = FunctionBinaryUnary<FromBase64BinaryImpl>;

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
}

#include "common/compile_check_avoid_end.h"
} // namespace doris::vectorized
