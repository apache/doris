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
#include <cstring>
#include <string>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/simd/vstring_function.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"
class FunctionStrcmp : public IFunction {
public:
    static constexpr auto name = "strcmp";

    static FunctionPtr create() { return std::make_shared<FunctionStrcmp>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& [arg0_column, arg0_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [arg1_column, arg1_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        auto result_column = ColumnInt8::create(input_rows_count);

        if (auto arg0 = check_and_get_column<ColumnString>(arg0_column.get())) {
            if (auto arg1 = check_and_get_column<ColumnString>(arg1_column.get())) {
                if (arg0_const) {
                    scalar_vector(arg0->get_data_at(0), *arg1, *result_column);
                } else if (arg1_const) {
                    vector_scalar(*arg0, arg1->get_data_at(0), *result_column);
                } else {
                    vector_vector(*arg0, *arg1, *result_column);
                }
            }
        }

        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

private:
    static void scalar_vector(const StringRef str, const ColumnString& vec1, ColumnInt8& res) {
        size_t size = vec1.size();
        for (size_t i = 0; i < size; ++i) {
            res.get_data()[i] = str.compare(vec1.get_data_at(i));
        }
    }

    static void vector_scalar(const ColumnString& vec0, const StringRef str, ColumnInt8& res) {
        size_t size = vec0.size();
        for (size_t i = 0; i < size; ++i) {
            res.get_data()[i] = vec0.get_data_at(i).compare(str);
        }
    }

    static void vector_vector(const ColumnString& vec0, const ColumnString& vec1, ColumnInt8& res) {
        size_t size = vec0.size();
        for (size_t i = 0; i < size; ++i) {
            res.get_data()[i] = vec0.get_data_at(i).compare(vec1.get_data_at(i));
        }
    }
};

template <typename Impl>
class FunctionSubstring : public IFunction {
public:
    static constexpr auto name = SubstringUtil::name;
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionSubstring<Impl>>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct Substr3Impl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>(),
                std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        SubstringUtil::substring_execute(block, arguments, result, input_rows_count);
        return Status::OK();
    }
};

struct Substr2Impl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto col_len = ColumnInt32::create(input_rows_count);
        auto& strlen_data = col_len->get_data();

        ColumnPtr str_col;
        bool str_const;
        std::tie(str_col, str_const) = unpack_if_const(block.get_by_position(arguments[0]).column);

        const auto& str_offset = assert_cast<const ColumnString*>(str_col.get())->get_offsets();

        if (str_const) {
            std::fill(strlen_data.begin(), strlen_data.end(), str_offset[0] - str_offset[-1]);
        } else {
            for (int i = 0; i < input_rows_count; ++i) {
                strlen_data[i] = str_offset[i] - str_offset[i - 1];
            }
        }

        // we complete the column2(strlen) with the default value - each row's strlen.
        block.insert({std::move(col_len), std::make_shared<DataTypeInt32>(), "strlen"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};

        SubstringUtil::substring_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

class FunctionLeft : public IFunction {
public:
    static constexpr auto name = "left";
    static FunctionPtr create() { return std::make_shared<FunctionLeft>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 2);
        auto res = ColumnString::create();
        bool col_const[2];
        ColumnPtr argument_columns[2];
        for (int i = 0; i < 2; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto& str_col = assert_cast<const ColumnString&>(*argument_columns[0]);
        const auto& len_col = assert_cast<const ColumnInt32&>(*argument_columns[1]);
        const auto is_ascii = str_col.is_ascii();

        std::visit(
                [&](auto is_ascii, auto str_const, auto len_const) {
                    _execute<is_ascii, str_const, len_const>(str_col, len_col, *res,
                                                             input_rows_count);
                },
                make_bool_variant(is_ascii), make_bool_variant(col_const[0]),
                make_bool_variant(col_const[1]));

        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }

    template <bool is_ascii, bool str_const, bool len_const>
    static void _execute(const ColumnString& str_col, const ColumnInt32& len_col, ColumnString& res,
                         size_t size) {
        auto& res_chars = res.get_chars();
        auto& res_offsets = res.get_offsets();
        res_offsets.resize(size);
        const auto& len_data = len_col.get_data();

        if constexpr (str_const) {
            res_chars.reserve(size * (str_col.get_chars().size()));
        } else {
            res_chars.reserve(str_col.get_chars().size());
        }

        for (int i = 0; i < size; ++i) {
            auto str = str_col.get_data_at(index_check_const<str_const>(i));
            int len = len_data[index_check_const<len_const>(i)];
            if (len <= 0 || str.empty()) {
                StringOP::push_empty_string(i, res_chars, res_offsets);
                continue;
            }

            const char* begin = str.begin();
            const char* p = begin;

            if constexpr (is_ascii) {
                p = begin + std::min(len, static_cast<int>(str.size));
            } else {
                const char* end = str.end();
                for (size_t ni = 0, char_size = 0; ni < len && p < end; ++ni, p += char_size) {
                    char_size = UTF8_BYTE_LENGTH[static_cast<uint8_t>(*p)];
                }
            }

            StringOP::push_value_string_reserved_and_allow_overflow({begin, p}, i, res_chars,
                                                                    res_offsets);
        }
    }
};

class FunctionRight : public IFunction {
public:
    static constexpr auto name = "right";
    static FunctionPtr create() { return std::make_shared<FunctionRight>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto params1 = ColumnInt32::create(input_rows_count);
        auto params2 = ColumnInt32::create(input_rows_count);
        size_t num_columns_without_result = block.columns();

        // params1 = max(arg[1], -len(arg))
        auto& index_data = params1->get_data();
        auto& strlen_data = params2->get_data();

        auto str_col =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto* str_column = assert_cast<const ColumnString*>(str_col.get());
        auto pos_col =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto& pos_data = assert_cast<const ColumnInt32*>(pos_col.get())->get_data();

        for (int i = 0; i < input_rows_count; ++i) {
            auto str = str_column->get_data_at(i);
            strlen_data[i] = simd::VStringFunctions::get_char_len(str.data, str.size);
        }

        for (int i = 0; i < input_rows_count; ++i) {
            index_data[i] = std::max(-pos_data[i], -strlen_data[i]);
        }

        block.insert({std::move(params1), int_type, "index"});
        block.insert({std::move(params2), int_type, "strlen"});

        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = arguments[0];
        temp_arguments[1] = num_columns_without_result;
        temp_arguments[2] = num_columns_without_result + 1;
        SubstringUtil::substring_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

struct NullOrEmptyImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeUInt8>()}; }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count, bool reverse) {
        auto res_map = ColumnUInt8::create(input_rows_count, 0);

        auto column = block.get_by_position(arguments[0]).column;
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*column)) {
            column = nullable->get_nested_column_ptr();
            VectorizedUtils::update_null_map(res_map->get_data(), nullable->get_null_map_data());
        }
        auto str_col = assert_cast<const ColumnString*>(column.get());
        const auto& offsets = str_col->get_offsets();

        auto& res_map_data = res_map->get_data();
        for (int i = 0; i < input_rows_count; ++i) {
            int size = offsets[i] - offsets[i - 1];
            res_map_data[i] |= (size == 0);
        }
        if (reverse) {
            for (int i = 0; i < input_rows_count; ++i) {
                res_map_data[i] = !res_map_data[i];
            }
        }

        block.replace_by_position(result, std::move(res_map));
        return Status::OK();
    }
};

class FunctionNullOrEmpty : public IFunction {
public:
    static constexpr auto name = "null_or_empty";
    static FunctionPtr create() { return std::make_shared<FunctionNullOrEmpty>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        RETURN_IF_ERROR(NullOrEmptyImpl::execute(context, block, arguments, result,
                                                 input_rows_count, false));
        return Status::OK();
    }
};

class FunctionNotNullOrEmpty : public IFunction {
public:
    static constexpr auto name = "not_null_or_empty";
    static FunctionPtr create() { return std::make_shared<FunctionNotNullOrEmpty>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        RETURN_IF_ERROR(NullOrEmptyImpl::execute(context, block, arguments, result,
                                                 input_rows_count, true));
        return Status::OK();
    }
};

void register_function_string_basic(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSubstring<Substr3Impl>>();
    factory.register_function<FunctionSubstring<Substr2Impl>>();
    factory.register_function<FunctionLeft>();
    factory.register_function<FunctionRight>();
    factory.register_function<FunctionNullOrEmpty>();
    factory.register_function<FunctionNotNullOrEmpty>();
    factory.register_function<FunctionStrcmp>();

    factory.register_alias(FunctionLeft::name, "strleft");
    factory.register_alias(FunctionRight::name, "strright");
    factory.register_alias(SubstringUtil::name, "substr");
    factory.register_alias(SubstringUtil::name, "mid");
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
