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
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/string_functions.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/stringop_substring.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct RegexpReplaceImpl {
    static constexpr auto name = "regexp_replace";
    // 3 args
    static void execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                             size_t input_rows_count, ColumnString::Chars& result_data,
                             ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<false>(context, str_col, pattern_col, replace_col, result_data,
                                       result_offset, null_map, i);
        }
    }
    static void execute_impl_const_args(FunctionContext* context, ColumnPtr argument_columns[],
                                        size_t input_rows_count, ColumnString::Chars& result_data,
                                        ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<true>(context, str_col, pattern_col, replace_col, result_data,
                                      result_offset, null_map, i);
        }
    }
    template <bool Const>
    static void _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                    const ColumnString* pattern_col,
                                    const ColumnString* replace_col,
                                    ColumnString::Chars& result_data,
                                    ColumnString::Offsets& result_offset, NullMap& null_map,
                                    const size_t index_now) {
        re2::RE2* re = reinterpret_cast<re2::RE2*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<re2::RE2> scoped_re; // destroys re if state->re is nullptr
        if (re == nullptr) {
            std::string error_str;
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, Const));
            bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(), scoped_re);
            if (!st) {
                context->add_warning(error_str.c_str());
                StringOP::push_null_string(index_now, result_data, result_offset, null_map);
                return;
            }
            re = scoped_re.get();
        }

        re2::StringPiece replace_str = re2::StringPiece(
                replace_col->get_data_at(index_check_const(index_now, Const)).to_string_view());

        std::string result_str(str_col->get_data_at(index_now).to_string());
        re2::RE2::GlobalReplace(&result_str, *re, replace_str);
        StringOP::push_value_string(result_str, index_now, result_data, result_offset);
    }
};

struct RegexpReplaceOneImpl {
    static constexpr auto name = "regexp_replace_one";

    static void execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                             size_t input_rows_count, ColumnString::Chars& result_data,
                             ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());
        // 3 args
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<false>(context, str_col, pattern_col, replace_col, result_data,
                                       result_offset, null_map, i);
        }
    }

    static void execute_impl_const_args(FunctionContext* context, ColumnPtr argument_columns[],
                                        size_t input_rows_count, ColumnString::Chars& result_data,
                                        ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());
        // 3 args
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<true>(context, str_col, pattern_col, replace_col, result_data,
                                      result_offset, null_map, i);
        }
    }
    template <bool Const>
    static void _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                    const ColumnString* pattern_col,
                                    const ColumnString* replace_col,
                                    ColumnString::Chars& result_data,
                                    ColumnString::Offsets& result_offset, NullMap& null_map,
                                    const size_t index_now) {
        re2::RE2* re = reinterpret_cast<re2::RE2*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<re2::RE2> scoped_re; // destroys re if state->re is nullptr
        if (re == nullptr) {
            std::string error_str;
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, Const));
            bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(), scoped_re);
            if (!st) {
                context->add_warning(error_str.c_str());
                StringOP::push_null_string(index_now, result_data, result_offset, null_map);
                return;
            }
            re = scoped_re.get();
        }

        re2::StringPiece replace_str = re2::StringPiece(
                replace_col->get_data_at(index_check_const(index_now, Const)).to_string_view());

        std::string result_str(str_col->get_data_at(index_now).to_string());
        re2::RE2::Replace(&result_str, *re, replace_str);
        StringOP::push_value_string(result_str, index_now, result_data, result_offset);
    }
};

template <bool ReturnNull>
struct RegexpExtractImpl {
    static constexpr auto name = ReturnNull ? "regexp_extract_or_null" : "regexp_extract";
    // 3 args
    static void execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                             size_t input_rows_count, ColumnString::Chars& result_data,
                             ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* index_col =
                check_and_get_column<ColumnVector<Int64>>(argument_columns[2].get());
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            const auto& index_data = index_col->get_int(i);
            if (index_data < 0) {
                ReturnNull ? StringOP::push_null_string(i, result_data, result_offset, null_map)
                           : StringOP::push_empty_string(i, result_data, result_offset);
                continue;
            }
            _execute_inner_loop<false>(context, str_col, pattern_col, index_data, result_data,
                                       result_offset, null_map, i);
        }
    }

    static void execute_impl_const_args(FunctionContext* context, ColumnPtr argument_columns[],
                                        size_t input_rows_count, ColumnString::Chars& result_data,
                                        ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* index_col =
                check_and_get_column<ColumnVector<Int64>>(argument_columns[2].get());

        const auto& index_data = index_col->get_int(0);
        if (index_data < 0) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                ReturnNull ? StringOP::push_null_string(i, result_data, result_offset, null_map)
                           : StringOP::push_empty_string(i, result_data, result_offset);
            }
            return;
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }

            _execute_inner_loop<true>(context, str_col, pattern_col, index_data, result_data,
                                      result_offset, null_map, i);
        }
    }
    template <bool Const>
    static void _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                    const ColumnString* pattern_col, const Int64 index_data,
                                    ColumnString::Chars& result_data,
                                    ColumnString::Offsets& result_offset, NullMap& null_map,
                                    const size_t index_now) {
        re2::RE2* re = reinterpret_cast<re2::RE2*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<re2::RE2> scoped_re;
        if (re == nullptr) {
            std::string error_str;
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, Const));
            bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(), scoped_re);
            if (!st) {
                context->add_warning(error_str.c_str());
                StringOP::push_null_string(index_now, result_data, result_offset, null_map);
                return;
            }
            re = scoped_re.get();
        }
        const auto& str = str_col->get_data_at(index_now);
        re2::StringPiece str_sp = re2::StringPiece(str.data, str.size);

        int max_matches = 1 + re->NumberOfCapturingGroups();
        if (index_data >= max_matches) {
            ReturnNull ? StringOP::push_null_string(index_now, result_data, result_offset, null_map)
                       : StringOP::push_empty_string(index_now, result_data, result_offset);
            return;
        }

        std::vector<re2::StringPiece> matches(max_matches);
        bool success =
                re->Match(str_sp, 0, str.size, re2::RE2::UNANCHORED, &matches[0], max_matches);
        if (!success) {
            ReturnNull ? StringOP::push_null_string(index_now, result_data, result_offset, null_map)
                       : StringOP::push_empty_string(index_now, result_data, result_offset);
            return;
        }
        const re2::StringPiece& match = matches[index_data];
        StringOP::push_value_string(std::string_view(match.data(), match.size()), index_now,
                                    result_data, result_offset);
    }
};

struct RegexpExtractAllImpl {
    static constexpr auto name = "regexp_extract_all";

    template <bool first_const, bool second_const, bool third_const>
    static void execute_impl(FunctionContext* context, const ColumnPtr* argument_columns,
                             size_t input_rows_count, ColumnArray::MutablePtr& result_column) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* group_idx_col = check_and_get_column<ColumnInt32>(argument_columns[2].get());

        auto& result_array_col = assert_cast<ColumnArray&>(*result_column);
        if constexpr (second_const && third_const) {
            auto* re = reinterpret_cast<re2::RE2*>(
                    context->get_function_state(FunctionContext::THREAD_LOCAL));
            if (re != nullptr) {
                auto group_idx = group_idx_col->get_int(0);

                if (re->NumberOfCapturingGroups() < group_idx) {
                    result_array_col.insert_many_defaults(input_rows_count);
                    return;
                }
            }
        }

        auto& column_nullable = assert_cast<ColumnNullable&>(result_array_col.get_data());
        auto& null_map = column_nullable.get_null_map_data();
        auto& column_string = assert_cast<ColumnString&>(column_nullable.get_nested_column());
        auto& offsets = result_array_col.get_offsets();

        for (int i = 0; i < input_rows_count; ++i) {
            _execute_inner_loop<first_const, second_const, third_const>(
                    context, str_col, pattern_col, group_idx_col, i, column_string, null_map,
                    offsets);
        }
    }

    template <bool first_const, bool second_const, bool third_const>
    static void _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                    const ColumnString* pattern_col,
                                    const ColumnInt32* group_idx_col, const size_t index_now,
                                    ColumnString& result_string_column, NullMap& null_map,
                                    ColumnArray::Offsets64& result_offsets) {
        auto* re = reinterpret_cast<re2::RE2*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<re2::RE2> scoped_re;

        if (re == nullptr) {
            std::string error_str;
            const auto& pattern =
                    pattern_col->get_data_at(index_check_const(index_now, second_const));
            bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(), scoped_re);
            if (!st) {
                context->add_warning(error_str.c_str());
                null_map.push_back(1);
                result_string_column.insert_default();
                result_offsets.emplace_back(result_offsets.back() + 1);
                return;
            }
            re = scoped_re.get();
        }

        auto group_idx = group_idx_col->get_element(index_check_const(index_now, third_const));

        if (re->NumberOfCapturingGroups() < group_idx || group_idx < 0) {
            result_offsets.emplace_back(result_offsets.back());
            return;
        }

        const auto& str = str_col->get_data_at(index_check_const(index_now, first_const));
        int max_matches = 1 + group_idx;
        std::vector<re2::StringPiece> res_matches;
        size_t pos = 0;
        while (pos < str.size) {
            const auto* str_pos = str.data + pos;
            auto str_size = str.size - pos;
            re2::StringPiece str_sp = re2::StringPiece(str_pos, str_size);
            std::vector<re2::StringPiece> matches(max_matches);
            bool success = re->Match(str_sp, 0, str_size, re2::RE2::UNANCHORED, matches.data(),
                                     max_matches);
            if (!success) {
                break;
            }

            if (matches[0].empty()) {
                pos += 1;
                continue;
            }

            res_matches.push_back(matches[group_idx]);
            auto offset = std::string(str_pos, str_size).find(std::string(matches[0].as_string()));
            pos += offset + matches[0].size();
        }

        if (res_matches.empty()) {
            result_offsets.emplace_back(result_offsets.back());
            return;
        }

        for (auto res_match : res_matches) {
            result_string_column.insert_data(res_match.data(), res_match.size());
            null_map.push_back(0);
        }
        result_offsets.emplace_back(result_offsets.back() + res_matches.size());
    }
};

// template FunctionRegexpFunctionality is used for regexp_xxxx series functions, not for regexp match.
template <typename Impl>
class FunctionRegexpFunctionality : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionRegexpFunctionality>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override {
        if constexpr (std::is_same_v<Impl, RegexpExtractAllImpl>) {
            return 0;
        } else {
            return 3;
        }
    }

    bool is_variadic() const override { return std::is_same_v<Impl, RegexpExtractAllImpl>; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if constexpr (std::is_same_v<Impl, RegexpExtractAllImpl>) {
            return std::make_shared<DataTypeArray>(
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
        } else {
            return make_nullable(std::make_shared<DataTypeString>());
        }
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            if (context->is_col_constant(1)) {
                DCHECK(!context->get_function_state(scope));
                const auto pattern_col = context->get_constant_col(1)->column_ptr;
                const auto& pattern = pattern_col->get_data_at(0);
                if (pattern.size == 0) {
                    return Status::OK();
                }

                std::string error_str;
                std::unique_ptr<re2::RE2> scoped_re;
                bool st =
                        StringFunctions::compile_regex(pattern, &error_str, StringRef(), scoped_re);
                if (!st) {
                    context->set_error(error_str.c_str());
                    return Status::InvalidArgument(error_str);
                }
                std::shared_ptr<re2::RE2> re(scoped_re.release());
                context->set_function_state(scope, re);
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        size_t argument_size = arguments.size();

        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < argument_size; ++i) {
            col_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
        }
        argument_columns[0] = col_const[0] ? static_cast<const ColumnConst&>(
                                                     *block.get_by_position(arguments[0]).column)
                                                     .convert_to_full_column()
                                           : block.get_by_position(arguments[0]).column;
        if constexpr (std::is_same_v<Impl, RegexpExtractAllImpl>) {
            DCHECK_LE(argument_size, 3);
            DCHECK_GE(argument_size, 2);
            auto actual_arguments = arguments;
            if (argument_size == 2) {
                // Default value of is 1.
                auto third_arg_column =
                        ColumnConst::create(ColumnInt32::create(1, 1), input_rows_count);
                auto third_arg_idx = block.columns();
                block.insert({std::move(third_arg_column), std::make_shared<DataTypeInt32>(),
                              "group_idx"});
                col_const[2] = true;
                actual_arguments.emplace_back(third_arg_idx);
            }

            default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block,
                                                 actual_arguments);

            auto result_column = ColumnArray::create(
                    ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()),
                    ColumnArray::ColumnOffsets::create());

            std::visit(
                    [&](auto first_const, auto second_const, auto third_const) {
                        Impl::template execute_impl<first_const, second_const, third_const>(
                                context, argument_columns, input_rows_count, result_column);
                    },
                    make_bool_variant(col_const[0]), make_bool_variant(col_const[1]),
                    make_bool_variant(col_const[2]));
            block.get_by_position(result).column = std::move(result_column);
            return Status::OK();
        } else {
            default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block,
                                                 arguments);

            auto result_null_map = ColumnUInt8::create(input_rows_count, 0);
            auto result_data_column = ColumnString::create();
            auto& result_data = result_data_column->get_chars();
            auto& result_offset = result_data_column->get_offsets();
            result_offset.resize(input_rows_count);
            if (col_const[1] && col_const[2]) {
                Impl::execute_impl_const_args(context, argument_columns, input_rows_count,
                                              result_data, result_offset,
                                              result_null_map->get_data());
            } else {
                Impl::execute_impl(context, argument_columns, input_rows_count, result_data,
                                   result_offset, result_null_map->get_data());
            }

            block.get_by_position(result).column = ColumnNullable::create(
                    std::move(result_data_column), std::move(result_null_map));
            return Status::OK();
        }
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }
};

void register_function_regexp_extract(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionRegexpFunctionality<RegexpReplaceImpl>>();
    factory.register_function<FunctionRegexpFunctionality<RegexpExtractImpl<true>>>();
    factory.register_function<FunctionRegexpFunctionality<RegexpExtractImpl<false>>>();
    factory.register_function<FunctionRegexpFunctionality<RegexpReplaceOneImpl>>();
    factory.register_function<FunctionRegexpFunctionality<RegexpExtractAllImpl>>();
}

} // namespace doris::vectorized
