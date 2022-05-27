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

#include <re2/re2.h>

#include <random>

#include "exprs/string_functions.h"
#include "runtime/string_value.h"
#include "udf/udf.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"
namespace doris::vectorized {

template <typename Impl>
class FunctionRegexp : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionRegexp>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope != FunctionContext::FRAGMENT_LOCAL) {
            return Status::OK();
        }

        if (context->is_col_constant(1)) {
            const auto pattern_col = context->get_constant_col(1)->column_ptr;
            const auto& pattern = pattern_col->get_data_at(0).to_string_val();
            if (pattern.is_null) {
                return Status::OK();
            }

            std::string error_str;
            re2::RE2* re = StringFunctions::compile_regex(pattern, &error_str, StringVal::null());
            if (re == nullptr) {
                context->set_error(error_str.c_str());
                return Status::InvalidArgument(error_str);
            }
            context->set_function_state(scope, re);
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        size_t argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];
        auto result_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto result_data_column = ColumnString::create();

        auto& result_data = result_data_column->get_chars();
        auto& result_offset = result_data_column->get_offsets();
        result_offset.resize(input_rows_count);

        for (int i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                VectorizedUtils::update_null_map(result_null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        Impl::execute_impl(context, argument_columns, input_rows_count, result_data, result_offset,
                           result_null_map->get_data());

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_data_column), std::move(result_null_map));
        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            re2::RE2* re = reinterpret_cast<re2::RE2*>(context->get_function_state(scope));
            delete re;
        }
        return Status::OK();
    }
};

struct RegexpReplaceImpl {
    static constexpr auto name = "regexp_replace";

    static Status execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                               size_t input_rows_count, ColumnString::Chars& result_data,
                               ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* replace_col = check_and_get_column<ColumnString>(argument_columns[2].get());

        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            re2::RE2* re = reinterpret_cast<re2::RE2*>(
                    context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
            std::unique_ptr<re2::RE2> scoped_re; // destroys re if state->re is nullptr
            if (re == nullptr) {
                std::string error_str;
                const auto& pattern = pattern_col->get_data_at(i).to_string_val();
                re = StringFunctions::compile_regex(pattern, &error_str, StringVal::null());
                if (re == nullptr) {
                    context->add_warning(error_str.c_str());
                    StringOP::push_null_string(i, result_data, result_offset, null_map);
                    continue;
                }
                scoped_re.reset(re);
            }

            re2::StringPiece replace_str =
                    re2::StringPiece(replace_col->get_data_at(i).to_string_view());

            std::string result_str(str_col->get_data_at(i).to_string());
            re2::RE2::GlobalReplace(&result_str, *re, replace_str);
            StringOP::push_value_string(result_str, i, result_data, result_offset);
        }

        return Status::OK();
    }
};

struct RegexpExtractImpl {
    static constexpr auto name = "regexp_extract";

    static Status execute_impl(FunctionContext* context, ColumnPtr argument_columns[],
                               size_t input_rows_count, ColumnString::Chars& result_data,
                               ColumnString::Offsets& result_offset, NullMap& null_map) {
        const auto* str_col = check_and_get_column<ColumnString>(argument_columns[0].get());
        const auto* pattern_col = check_and_get_column<ColumnString>(argument_columns[1].get());
        const auto* index_col =
                check_and_get_column<ColumnVector<Int64>>(argument_columns[2].get());
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            const auto& index_data = index_col->get_int(i);
            if (index_data < 0) {
                StringOP::push_empty_string(i, result_data, result_offset);
                continue;
            }
            re2::RE2* re = reinterpret_cast<re2::RE2*>(
                    context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
            std::unique_ptr<re2::RE2> scoped_re;
            if (re == nullptr) {
                std::string error_str;
                const auto& pattern = pattern_col->get_data_at(i).to_string_val();
                re = StringFunctions::compile_regex(pattern, &error_str, StringVal::null());
                if (re == nullptr) {
                    context->add_warning(error_str.c_str());
                    StringOP::push_null_string(i, result_data, result_offset, null_map);
                    continue;
                }
                scoped_re.reset(re);
            }
            const auto& str = str_col->get_data_at(i);
            re2::StringPiece str_sp = re2::StringPiece(str.data, str.size);

            int max_matches = 1 + re->NumberOfCapturingGroups();
            if (index_data >= max_matches) {
                StringOP::push_empty_string(i, result_data, result_offset);
                continue;
            }

            std::vector<re2::StringPiece> matches(max_matches);
            bool success =
                    re->Match(str_sp, 0, str.size, re2::RE2::UNANCHORED, &matches[0], max_matches);
            if (!success) {
                StringOP::push_empty_string(i, result_data, result_offset);
                continue;
            }
            const re2::StringPiece& match = matches[index_data];
            StringOP::push_value_string(std::string_view(match.data(), match.size()), i,
                                        result_data, result_offset);
        }
        return Status::OK();
    }
};

void register_function_regexp_extract(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionRegexp<RegexpReplaceImpl>>();
    factory.register_function<FunctionRegexp<RegexpExtractImpl>>();
}

} // namespace doris::vectorized
