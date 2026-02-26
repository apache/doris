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

#include <boost/regex.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/string_functions.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/function_context.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/stringop_substring.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// Helper structure to hold either RE2 or Boost.Regex
struct RegexpExtractEngine {
    std::unique_ptr<re2::RE2> re2_regex;
    std::unique_ptr<boost::regex> boost_regex;

    bool is_boost() const { return boost_regex != nullptr; }
    bool is_re2() const { return re2_regex != nullptr; }

    // Try to compile with RE2 first, fallback to Boost.Regex if RE2 fails
    static bool compile(const StringRef& pattern, std::string* error_str,
                        RegexpExtractEngine& engine, bool enable_extended_regex) {
        re2::RE2::Options options;
        options.set_log_errors(false); // avoid RE2 printing to stderr; we handle errors ourselves
        options.set_dot_nl(true); // make '.' match '\n' by default, consistent with REGEXP/LIKE
        engine.re2_regex =
                std::make_unique<re2::RE2>(re2::StringPiece(pattern.data, pattern.size), options);

        if (engine.re2_regex->ok()) {
            return true;
        } else if (!enable_extended_regex) {
            *error_str = fmt::format(
                    "Invalid regex pattern: {}. Error: {}. If you need advanced regex features, "
                    "try setting enable_extended_regex=true",
                    std::string(pattern.data, pattern.size), engine.re2_regex->error());
            return false;
        }

        // RE2 failed, try Boost.Regex for advanced features like zero-width assertions
        engine.re2_regex.reset();
        try {
            boost::regex::flag_type flags = boost::regex::normal;
            engine.boost_regex = std::make_unique<boost::regex>(pattern.data,
                                                                pattern.data + pattern.size, flags);
            return true;
        } catch (const boost::regex_error& e) {
            if (error_str) {
                *error_str = fmt::format("Invalid regex pattern: {}. Error: {}",
                                         std::string(pattern.data, pattern.size), e.what());
            }
            return false;
        }
    }

    // Get number of capturing groups
    int number_of_capturing_groups() const {
        if (is_re2()) {
            return re2_regex->NumberOfCapturingGroups();
        } else if (is_boost()) {
            return static_cast<int>(boost_regex->mark_count());
        }
        return 0;
    }

    // Match function for extraction
    bool match_and_extract(const char* data, size_t size, int index, std::string& result) const {
        if (is_re2()) {
            int max_matches = 1 + re2_regex->NumberOfCapturingGroups();
            if (index >= max_matches) {
                return false;
            }
            std::vector<re2::StringPiece> matches(max_matches);
            bool success = re2_regex->Match(re2::StringPiece(data, size), 0, size,
                                            re2::RE2::UNANCHORED, matches.data(), max_matches);
            if (success && index < matches.size()) {
                const re2::StringPiece& match = matches[index];
                result.assign(match.data(), match.size());
                return true;
            }
            return false;
        } else if (is_boost()) {
            boost::cmatch matches;
            bool success = boost::regex_search(data, data + size, matches, *boost_regex);
            if (success && index < matches.size()) {
                result = matches[index].str();
                return true;
            }
            return false;
        }
        return false;
    }

    // Match all occurrences and extract the first capturing group
    void match_all_and_extract(const char* data, size_t size,
                               std::vector<std::string>& results) const {
        if (is_re2()) {
            int max_matches = 1 + re2_regex->NumberOfCapturingGroups();
            if (max_matches < 2) {
                return; // No capturing groups
            }

            size_t pos = 0;
            while (pos < size) {
                const char* str_pos = data + pos;
                size_t str_size = size - pos;
                std::vector<re2::StringPiece> matches(max_matches);
                bool success = re2_regex->Match(re2::StringPiece(str_pos, str_size), 0, str_size,
                                                re2::RE2::UNANCHORED, matches.data(), max_matches);
                if (!success) {
                    break;
                }
                if (matches[0].empty()) {
                    pos += 1;
                    continue;
                }
                // Extract first capturing group
                if (matches.size() > 1 && !matches[1].empty()) {
                    results.emplace_back(matches[1].data(), matches[1].size());
                }
                // Move position forward
                auto offset = std::string(str_pos, str_size)
                                      .find(std::string(matches[0].data(), matches[0].size()));
                pos += offset + matches[0].size();
            }
        } else if (is_boost()) {
            const char* search_start = data;
            const char* search_end = data + size;
            boost::match_results<const char*> matches;

            while (boost::regex_search(search_start, search_end, matches, *boost_regex)) {
                if (matches.size() > 1 && matches[1].matched) {
                    results.emplace_back(matches[1].str());
                }
                if (matches[0].length() == 0) {
                    if (search_start == search_end) {
                        break;
                    }
                    search_start += 1;
                } else {
                    search_start = matches[0].second;
                }
            }
        }
    }
};

struct RegexpCountImpl {
    static void execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                             size_t input_rows_count, ColumnInt32::Container& result_data) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        for (int i = 0; i < input_rows_count; ++i) {
            result_data[i] = _execute_inner_loop(context, str_col, pattern_col, i);
        }
    }
    static int _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                   const ColumnString* pattern_col, const size_t index_now) {
        re2::RE2* re = reinterpret_cast<re2::RE2*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<re2::RE2> scoped_re;
        if (re == nullptr) {
            std::string error_str;
            DCHECK(pattern_col);
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, false));
            bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(), StringRef(),
                                                     scoped_re);
            if (!st) {
                context->add_warning(error_str.c_str());
                throw Exception(Status::InvalidArgument(error_str));
                return 0;
            }
            re = scoped_re.get();
        }

        const auto& str = str_col->get_data_at(index_now);
        int count = 0;
        size_t pos = 0;
        while (pos < str.size) {
            auto str_pos = str.data + pos;
            auto str_size = str.size - pos;
            re2::StringPiece str_sp_current = re2::StringPiece(str_pos, str_size);
            re2::StringPiece match;

            bool success = re->Match(str_sp_current, 0, str_size, re2::RE2::UNANCHORED, &match, 1);
            if (!success) {
                break;
            }
            if (match.empty()) {
                pos += 1;
                continue;
            }
            count++;
            size_t match_start = match.data() - str_sp_current.data();
            pos += match_start + match.size();
        }

        return count;
    }
};

class FunctionRegexpCount : public IFunction {
public:
    static constexpr auto name = "regexp_count";

    static FunctionPtr create() { return std::make_shared<FunctionRegexpCount>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt32>();
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
                bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(),
                                                         StringRef(), scoped_re);
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
        auto result_data_column = ColumnInt32::create(input_rows_count);
        auto& result_data = result_data_column->get_data();

        ColumnPtr argument_columns[2];

        argument_columns[0] = block.get_by_position(arguments[0]).column;
        argument_columns[1] = block.get_by_position(arguments[1]).column;
        RegexpCountImpl::execute_impl(context, argument_columns, input_rows_count, result_data);

        block.get_by_position(result).column = std::move(result_data_column);
        return Status::OK();
    }
};

struct ThreeParamTypes {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }
};

struct FourParamTypes {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }
};

// template FunctionRegexpFunctionality is used for regexp_replace/regexp_replace_one
template <typename Impl, typename ParamTypes>
class FunctionRegexpReplace : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionRegexpReplace>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return ParamTypes::get_variadic_argument_types();
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
                StringRef options_value;
                if constexpr (std::is_same_v<FourParamTypes, ParamTypes>) {
                    DCHECK_EQ(context->get_num_args(), 4);
                    DCHECK(context->is_col_constant(3));
                    const auto options_col = context->get_constant_col(3)->column_ptr;
                    options_value = options_col->get_data_at(0);
                }

                bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(),
                                                         options_value, scoped_re);
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

        auto result_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto result_data_column = ColumnString::create();
        auto& result_data = result_data_column->get_chars();
        auto& result_offset = result_data_column->get_offsets();
        result_offset.resize(input_rows_count);

        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            col_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
        }
        argument_columns[0] = col_const[0] ? static_cast<const ColumnConst&>(
                                                     *block.get_by_position(arguments[0]).column)
                                                     .convert_to_full_column()
                                           : block.get_by_position(arguments[0]).column;

        default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block, arguments);

        StringRef options_value;
        if (col_const[1] && col_const[2]) {
            Impl::execute_impl_const_args(context, argument_columns, options_value,
                                          input_rows_count, result_data, result_offset,
                                          result_null_map->get_data());
        } else {
            // the options have check in FE, so is always const, and get idx of 0
            if (argument_size == 4) {
                options_value = block.get_by_position(arguments[3]).column->get_data_at(0);
            }
            Impl::execute_impl(context, argument_columns, options_value, input_rows_count,
                               result_data, result_offset, result_null_map->get_data());
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_data_column), std::move(result_null_map));
        return Status::OK();
    }
};

struct RegexpReplaceImpl {
    static constexpr auto name = "regexp_replace";
    static void execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                             const StringRef& options_value, size_t input_rows_count,
                             ColumnString::Chars& result_data, ColumnString::Offsets& result_offset,
                             NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<false>(context, str_col, pattern_col, replace_col, options_value,
                                       result_data, result_offset, null_map, i);
        }
    }
    static void execute_impl_const_args(FunctionContext* context, ColumnPtr argument_columns[],
                                        const StringRef& options_value, size_t input_rows_count,
                                        ColumnString::Chars& result_data,
                                        ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<true>(context, str_col, pattern_col, replace_col, options_value,
                                      result_data, result_offset, null_map, i);
        }
    }
    template <bool Const>
    static void _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                    const ColumnString* pattern_col,
                                    const ColumnString* replace_col, const StringRef& options_value,
                                    ColumnString::Chars& result_data,
                                    ColumnString::Offsets& result_offset, NullMap& null_map,
                                    const size_t index_now) {
        re2::RE2* re = reinterpret_cast<re2::RE2*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<re2::RE2> scoped_re; // destroys re if state->re is nullptr
        if (re == nullptr) {
            std::string error_str;
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, Const));
            bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(),
                                                     options_value, scoped_re);
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
                             const StringRef& options_value, size_t input_rows_count,
                             ColumnString::Chars& result_data, ColumnString::Offsets& result_offset,
                             NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());
        // 3 args
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<false>(context, str_col, pattern_col, replace_col, options_value,
                                       result_data, result_offset, null_map, i);
        }
    }

    static void execute_impl_const_args(FunctionContext* context, ColumnPtr argument_columns[],
                                        const StringRef& options_value, size_t input_rows_count,
                                        ColumnString::Chars& result_data,
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
            _execute_inner_loop<true>(context, str_col, pattern_col, replace_col, options_value,
                                      result_data, result_offset, null_map, i);
        }
    }
    template <bool Const>
    static void _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                    const ColumnString* pattern_col,
                                    const ColumnString* replace_col, const StringRef& options_value,
                                    ColumnString::Chars& result_data,
                                    ColumnString::Offsets& result_offset, NullMap& null_map,
                                    const size_t index_now) {
        re2::RE2* re = reinterpret_cast<re2::RE2*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<re2::RE2> scoped_re; // destroys re if state->re is nullptr
        if (re == nullptr) {
            std::string error_str;
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, Const));
            bool st = StringFunctions::compile_regex(pattern, &error_str, StringRef(),
                                                     options_value, scoped_re);
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
        const auto* index_col = check_and_get_column<ColumnInt64>(argument_columns[2].get());
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
        const auto* index_col = check_and_get_column<ColumnInt64>(argument_columns[2].get());

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
        auto* engine = reinterpret_cast<RegexpExtractEngine*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<RegexpExtractEngine> scoped_engine;

        if (engine == nullptr) {
            std::string error_str;
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, Const));
            scoped_engine = std::make_unique<RegexpExtractEngine>();
            bool st = RegexpExtractEngine::compile(pattern, &error_str, *scoped_engine,
                                                   context->state()->enable_extended_regex());
            if (!st) {
                context->add_warning(error_str.c_str());
                StringOP::push_null_string(index_now, result_data, result_offset, null_map);
                return;
            }
            engine = scoped_engine.get();
        }

        const auto& str = str_col->get_data_at(index_now);

        int max_matches = 1 + engine->number_of_capturing_groups();
        if (index_data >= max_matches) {
            ReturnNull ? StringOP::push_null_string(index_now, result_data, result_offset, null_map)
                       : StringOP::push_empty_string(index_now, result_data, result_offset);
            return;
        }

        std::string match_result;
        bool success = engine->match_and_extract(str.data, str.size, static_cast<int>(index_data),
                                                 match_result);

        if (!success) {
            ReturnNull ? StringOP::push_null_string(index_now, result_data, result_offset, null_map)
                       : StringOP::push_empty_string(index_now, result_data, result_offset);
            return;
        }

        StringOP::push_value_string(std::string_view(match_result.data(), match_result.size()),
                                    index_now, result_data, result_offset);
    }
};

struct RegexpExtractAllImpl {
    static constexpr auto name = "regexp_extract_all";

    size_t get_number_of_arguments() const { return 2; }

    static void execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                             size_t input_rows_count, ColumnString::Chars& result_data,
                             ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<false>(context, str_col, pattern_col, result_data, result_offset,
                                       null_map, i);
        }
    }

    static void execute_impl_const_args(FunctionContext* context, ColumnPtr argument_columns[],
                                        size_t input_rows_count, ColumnString::Chars& result_data,
                                        ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            _execute_inner_loop<true>(context, str_col, pattern_col, result_data, result_offset,
                                      null_map, i);
        }
    }
    template <bool Const>
    static void _execute_inner_loop(FunctionContext* context, const ColumnString* str_col,
                                    const ColumnString* pattern_col,
                                    ColumnString::Chars& result_data,
                                    ColumnString::Offsets& result_offset, NullMap& null_map,
                                    const size_t index_now) {
        auto* engine = reinterpret_cast<RegexpExtractEngine*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        std::unique_ptr<RegexpExtractEngine> scoped_engine;

        if (engine == nullptr) {
            std::string error_str;
            const auto& pattern = pattern_col->get_data_at(index_check_const(index_now, Const));
            scoped_engine = std::make_unique<RegexpExtractEngine>();
            bool st = RegexpExtractEngine::compile(pattern, &error_str, *scoped_engine,
                                                   context->state()->enable_extended_regex());
            if (!st) {
                context->add_warning(error_str.c_str());
                StringOP::push_null_string(index_now, result_data, result_offset, null_map);
                return;
            }
            engine = scoped_engine.get();
        }

        if (engine->number_of_capturing_groups() == 0) {
            StringOP::push_empty_string(index_now, result_data, result_offset);
            return;
        }
        const auto& str = str_col->get_data_at(index_now);
        std::vector<std::string> res_matches;
        engine->match_all_and_extract(str.data, str.size, res_matches);

        if (res_matches.empty()) {
            StringOP::push_empty_string(index_now, result_data, result_offset);
            return;
        }

        std::string res = "[";
        for (int j = 0; j < res_matches.size(); ++j) {
            res += "'" + res_matches[j] + "'";
            if (j < res_matches.size() - 1) {
                res += ",";
            }
        }
        res += "]";
        StringOP::push_value_string(std::string_view(res), index_now, result_data, result_offset);
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
            return 2;
        }
        return 3;
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
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
                auto engine = std::make_shared<RegexpExtractEngine>();
                bool st = RegexpExtractEngine::compile(pattern, &error_str, *engine,
                                                       context->state()->enable_extended_regex());
                if (!st) {
                    context->set_error(error_str.c_str());
                    return Status::InvalidArgument(error_str);
                }
                context->set_function_state(scope, engine);
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        size_t argument_size = arguments.size();

        auto result_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto result_data_column = ColumnString::create();
        auto& result_data = result_data_column->get_chars();
        auto& result_offset = result_data_column->get_offsets();
        result_offset.resize(input_rows_count);

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
            default_preprocess_parameter_columns(argument_columns, col_const, {1}, block,
                                                 arguments);
        } else {
            default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block,
                                                 arguments);
        }

        if constexpr (std::is_same_v<Impl, RegexpExtractAllImpl>) {
            if (col_const[1]) {
                Impl::execute_impl_const_args(context, argument_columns, input_rows_count,
                                              result_data, result_offset,
                                              result_null_map->get_data());
            } else {
                Impl::execute_impl(context, argument_columns, input_rows_count, result_data,
                                   result_offset, result_null_map->get_data());
            }
        } else {
            if (col_const[1] && col_const[2]) {
                Impl::execute_impl_const_args(context, argument_columns, input_rows_count,
                                              result_data, result_offset,
                                              result_null_map->get_data());
            } else {
                Impl::execute_impl(context, argument_columns, input_rows_count, result_data,
                                   result_offset, result_null_map->get_data());
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_data_column), std::move(result_null_map));
        return Status::OK();
    }
};

void register_function_regexp_extract(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionRegexpReplace<RegexpReplaceImpl, ThreeParamTypes>>();
    factory.register_function<FunctionRegexpReplace<RegexpReplaceImpl, FourParamTypes>>();
    factory.register_function<FunctionRegexpReplace<RegexpReplaceOneImpl, ThreeParamTypes>>();
    factory.register_function<FunctionRegexpReplace<RegexpReplaceOneImpl, FourParamTypes>>();
    factory.register_function<FunctionRegexpFunctionality<RegexpExtractImpl<true>>>();
    factory.register_function<FunctionRegexpFunctionality<RegexpExtractImpl<false>>>();
    factory.register_function<FunctionRegexpFunctionality<RegexpExtractAllImpl>>();
    factory.register_function<FunctionRegexpCount>();
}

} // namespace doris::vectorized
