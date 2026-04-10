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
#include <string_view>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_string.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/md5.h"
#include "util/sha.h"
#include "util/sm3.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

struct SM3Sum {
    static constexpr auto name = "sm3sum";
    using ObjectData = SM3Digest;
};

struct MD5Sum {
    static constexpr auto name = "md5sum";
    using ObjectData = Md5Digest;
};

template <typename Impl>
class FunctionStringDigestMulti : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionStringDigestMulti>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);

        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();
        res_offset.resize(input_rows_count);

        std::vector<ColumnPtr> argument_columns(arguments.size());
        std::vector<uint8_t> is_const(arguments.size(), 0);
        for (size_t i = 0; i < arguments.size(); ++i) {
            std::tie(argument_columns[i], is_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        if (check_and_get_column<ColumnString>(argument_columns[0].get())) {
            vector_execute<ColumnString>(block, input_rows_count, argument_columns, is_const,
                                         res_data, res_offset);
        } else if (check_and_get_column<ColumnVarbinary>(argument_columns[0].get())) {
            vector_execute<ColumnVarbinary>(block, input_rows_count, argument_columns, is_const,
                                            res_data, res_offset);
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        argument_columns[0]->get_name(), get_name());
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }

private:
    template <typename ColumnType>
    void vector_execute(Block& block, size_t input_rows_count,
                        const std::vector<ColumnPtr>& argument_columns,
                        const std::vector<uint8_t>& is_const, ColumnString::Chars& res_data,
                        ColumnString::Offsets& res_offset) const {
        using ObjectData = typename Impl::ObjectData;
        for (size_t i = 0; i < input_rows_count; ++i) {
            ObjectData digest;
            for (size_t j = 0; j < argument_columns.size(); ++j) {
                const auto* col = assert_cast<const ColumnType*>(argument_columns[j].get());
                StringRef data_ref = col->get_data_at(is_const[j] ? 0 : i);
                if (data_ref.size < 1) {
                    continue;
                }
                digest.update(data_ref.data, data_ref.size);
            }
            digest.digest();
            StringOP::push_value_string(std::string_view(digest.hex().c_str(), digest.hex().size()),
                                        i, res_data, res_offset);
        }
    }
};

class FunctionStringDigestSHA1 : public IFunction {
public:
    static constexpr auto name = "sha1";
    static FunctionPtr create() { return std::make_shared<FunctionStringDigestSHA1>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 1);
        ColumnPtr data_col = block.get_by_position(arguments[0]).column;

        auto res_col = ColumnString::create();
        auto& res_data = res_col->get_chars();
        auto& res_offset = res_col->get_offsets();
        res_offset.resize(input_rows_count);
        if (const auto* str_col = check_and_get_column<ColumnString>(data_col.get())) {
            vector_execute(str_col, input_rows_count, res_data, res_offset);
        } else if (const auto* vb_col = check_and_get_column<ColumnVarbinary>(data_col.get())) {
            vector_execute(vb_col, input_rows_count, res_data, res_offset);
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        data_col->get_name(), get_name());
        }

        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }

private:
    template <typename ColumnType>
    void vector_execute(const ColumnType* col, size_t input_rows_count,
                        ColumnString::Chars& res_data, ColumnString::Offsets& res_offset) const {
        SHA1Digest digest;
        for (size_t i = 0; i < input_rows_count; ++i) {
            StringRef data_ref = col->get_data_at(i);
            digest.reset(data_ref.data, data_ref.size);
            std::string_view ans = digest.digest();

            StringOP::push_value_string(ans, i, res_data, res_offset);
        }
    }
};

class FunctionStringDigestSHA2 : public IFunction {
public:
    static constexpr auto name = "sha2";
    static FunctionPtr create() { return std::make_shared<FunctionStringDigestSHA2>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK(!is_column_const(*block.get_by_position(arguments[0]).column));

        ColumnPtr data_col = block.get_by_position(arguments[0]).column;

        [[maybe_unused]] const auto& [right_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        auto digest_length = assert_cast<const ColumnInt32*>(right_column.get())->get_data()[0];

        auto res_col = ColumnString::create();
        auto& res_data = res_col->get_chars();
        auto& res_offset = res_col->get_offsets();
        res_offset.resize(input_rows_count);

        if (digest_length == 224) {
            execute_base<SHA224Digest>(data_col, input_rows_count, res_data, res_offset);
        } else if (digest_length == 256) {
            execute_base<SHA256Digest>(data_col, input_rows_count, res_data, res_offset);
        } else if (digest_length == 384) {
            execute_base<SHA384Digest>(data_col, input_rows_count, res_data, res_offset);
        } else if (digest_length == 512) {
            execute_base<SHA512Digest>(data_col, input_rows_count, res_data, res_offset);
        } else {
            return Status::InvalidArgument(
                    "sha2's digest length only support 224/256/384/512 but meet {}", digest_length);
        }

        block.replace_by_position(result, std::move(res_col));
        return Status::OK();
    }

private:
    template <typename T>
    void execute_base(ColumnPtr data_col, int input_rows_count, ColumnString::Chars& res_data,
                      ColumnString::Offsets& res_offset) const {
        if (const auto* str_col = check_and_get_column<ColumnString>(data_col.get())) {
            vector_execute<T>(str_col, input_rows_count, res_data, res_offset);
        } else if (const auto* vb_col = check_and_get_column<ColumnVarbinary>(data_col.get())) {
            vector_execute<T>(vb_col, input_rows_count, res_data, res_offset);
        } else {
            throw Exception(ErrorCode::RUNTIME_ERROR,
                            "Illegal column {} of argument of function {}", data_col->get_name(),
                            get_name());
        }
    }

    template <typename DigestType, typename ColumnType>
    void vector_execute(const ColumnType* col, size_t input_rows_count,
                        ColumnString::Chars& res_data, ColumnString::Offsets& res_offset) const {
        DigestType digest;
        for (size_t i = 0; i < input_rows_count; ++i) {
            StringRef data_ref = col->get_data_at(i);
            digest.reset(data_ref.data, data_ref.size);
            std::string_view ans = digest.digest();

            StringOP::push_value_string(ans, i, res_data, res_offset);
        }
    }
};

void register_function_string_digest(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStringDigestMulti<SM3Sum>>();
    factory.register_function<FunctionStringDigestMulti<MD5Sum>>();
    factory.register_function<FunctionStringDigestSHA1>();
    factory.register_function<FunctionStringDigestSHA2>();

    factory.register_alias(FunctionStringDigestMulti<MD5Sum>::name, "md5");
    factory.register_alias(FunctionStringDigestMulti<SM3Sum>::name, "sm3");
    factory.register_alias(FunctionStringDigestSHA1::name, "sha");
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
