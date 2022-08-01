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

#include "vec/functions/like.h"

#include "runtime/string_value.h"
#include "runtime/string_value.hpp"
#include "vec/columns/column_const.h"
#include "vec/columns/column_set.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
// A regex to match any regex pattern is equivalent to a substring search.
static const RE2 SUBSTRING_RE(
        "(?:\\.\\*)*([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)(?:\\.\\*)*");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 ENDS_WITH_RE("(?:\\.\\*)*([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\$");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 STARTS_WITH_RE("\\^([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)(?:\\.\\*)*");

// A regex to match any regex pattern which is equivalent to a constant string match.
static const RE2 EQUALS_RE("\\^([^\\.\\^\\{\\[\\(\\|\\)\\]\\}\\+\\*\\?\\$\\\\]*)\\$");

// Like patterns
static const re2::RE2 LIKE_SUBSTRING_RE("(?:%+)(((\\\\%)|(\\\\_)|([^%_]))+)(?:%+)");
static const re2::RE2 LIKE_ENDS_WITH_RE("(?:%+)(((\\\\%)|(\\\\_)|([^%_]))+)");
static const re2::RE2 LIKE_STARTS_WITH_RE("(((\\\\%)|(\\\\_)|([^%_]))+)(?:%+)");
static const re2::RE2 LIKE_EQUALS_RE("(((\\\\%)|(\\\\_)|([^%_]))+)");

Status FunctionLikeBase::constant_starts_with_fn(LikeSearchState* state, const StringValue& val,
                                                 const StringValue& pattern,
                                                 unsigned char* result) {
    *result = (val.len >= state->search_string_sv.len) &&
              (state->search_string_sv == val.substring(0, state->search_string_sv.len));
    return Status::OK();
}

Status FunctionLikeBase::constant_ends_with_fn(LikeSearchState* state, const StringValue& val,
                                               const StringValue& pattern, unsigned char* result) {
    *result = (val.len >= state->search_string_sv.len) &&
              (state->search_string_sv ==
               val.substring(val.len - state->search_string_sv.len, state->search_string_sv.len));
    return Status::OK();
}

Status FunctionLikeBase::constant_equals_fn(LikeSearchState* state, const StringValue& val,
                                            const StringValue& pattern, unsigned char* result) {
    *result = (val == state->search_string_sv);
    return Status::OK();
}

Status FunctionLikeBase::constant_substring_fn(LikeSearchState* state, const StringValue& val,
                                               const StringValue& pattern, unsigned char* result) {
    if (state->search_string_sv.len == 0) {
        *result = true;
        return Status::OK();
    }
    StringValue pattern_value = StringValue::from_string_val(val.ptr);
    *result = state->substring_pattern.search(&pattern_value) != -1;
    return Status::OK();
}

Status FunctionLikeBase::constant_regex_fn(LikeSearchState* state, const StringValue& val,
                                           const StringValue& pattern, unsigned char* result) {
    auto ret = hs_scan(state->hs_database.get(), val.ptr, val.len, 0, state->hs_scratch.get(),
                       state->hs_match_handler, (void*)result);
    if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
        return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
    }

    return Status::OK();
}

Status FunctionLikeBase::regexp_fn(LikeSearchState* state, const StringValue& val,
                                   const StringValue& pattern, unsigned char* result) {
    std::string re_pattern(pattern.ptr, pattern.len);

    hs_database_t* database = nullptr;
    hs_scratch_t* scratch = nullptr;
    RETURN_IF_ERROR(hs_prepare(nullptr, re_pattern.c_str(), &database, &scratch));

    auto ret =
            hs_scan(database, val.ptr, val.len, 0, scratch, state->hs_match_handler, (void*)result);
    if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
        return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
    }

    hs_free_scratch(scratch);
    hs_free_database(database);

    return Status::OK();
}

// hyperscan compile expression to database and allocate scratch space
Status FunctionLikeBase::hs_prepare(FunctionContext* context, const char* expression,
                                    hs_database_t** database, hs_scratch_t** scratch) {
    hs_compile_error_t* compile_err;

    if (hs_compile(expression, HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY, HS_MODE_BLOCK, NULL, database,
                   &compile_err) != HS_SUCCESS) {
        *database = nullptr;
        if (context) context->set_error("hs_compile regex pattern error");
        auto status = Status::RuntimeError("hs_compile regex pattern error:" +
                                           std::string(compile_err->message));
        hs_free_compile_error(compile_err);
        return status;
    }
    hs_free_compile_error(compile_err);

    if (hs_alloc_scratch(*database, scratch) != HS_SUCCESS) {
        hs_free_database(*database);
        *database = nullptr;
        *scratch = nullptr;
        if (context) context->set_error("hs_alloc_scratch allocate scratch space error");
        return Status::RuntimeError("hs_alloc_scratch allocate scratch space error");
    }

    return Status::OK();
}

Status FunctionLikeBase::execute_impl(FunctionContext* context, Block& block,
                                      const ColumnNumbers& arguments, size_t result,
                                      size_t /*input_rows_count*/) {
    // values and patterns
    const auto values_col =
            block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
    const auto pattern_col =
            block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

    const auto* values = check_and_get_column<ColumnString>(values_col.get());
    const auto* patterns = check_and_get_column<ColumnString>(pattern_col.get());

    if (!values || !patterns) {
        return Status::InternalError("Not supported input arguments types");
    }

    // result column
    auto res = ColumnUInt8::create();
    ColumnUInt8::Container& vec_res = res->get_data();
    // set default value to 0, and match functions only need to set 1/true
    vec_res.resize_fill(values->size());

    auto* state = reinterpret_cast<LikeState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));

    vector_vector(values->get_chars(), values->get_offsets(), patterns->get_chars(),
                  patterns->get_offsets(), vec_res, state->function, &state->search_state);

    block.replace_by_position(result, std::move(res));
    return Status::OK();
}

Status FunctionLikeBase::close(FunctionContext* context,
                               FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        auto* state = reinterpret_cast<LikeState*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
    return Status::OK();
}

Status FunctionLikeBase::vector_vector(const ColumnString::Chars& values,
                                       const ColumnString::Offsets& value_offsets,
                                       const ColumnString::Chars& patterns,
                                       const ColumnString::Offsets& pattern_offsets,
                                       ColumnUInt8::Container& result, const LikeFn& function,
                                       LikeSearchState* search_state) {
    // for constant_substring_fn, use long run length search for performance
    if (constant_substring_fn ==
        *(function.target<doris::Status (*)(LikeSearchState * state, const StringValue&,
                                            const StringValue&, unsigned char*)>())) {
        // treat continous multi string data as a long string data
        const UInt8* begin = values.data();
        const UInt8* end = begin + values.size();
        const UInt8* pos = begin;

        /// Current index in the array of strings.
        size_t i = 0;
        size_t needle_size = search_state->substring_pattern.get_pattern_length();

        /// We will search for the next occurrence in all strings at once.
        while (pos < end) {
            // search return matched substring start offset
            pos = (UInt8*)search_state->substring_pattern.search((char*)pos, end - pos);
            if (pos >= end) break;

            /// Determine which index it refers to.
            /// begin + value_offsets[i] is the start offset of string at i+1
            while (begin + value_offsets[i] <= pos) ++i;

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle_size < begin + value_offsets[i]) {
                result[i] = 1;
            }

            // move to next string offset
            pos = begin + value_offsets[i];
            ++i;
        }

        return Status::OK();
    }

    const auto size = value_offsets.size();

    for (int i = 0; i < size; ++i) {
        char* val_raw_str = (char*)(&values[value_offsets[i - 1]]);
        UInt32 val_str_size = value_offsets[i] - value_offsets[i - 1] - 1;

        char* pattern_raw_str = (char*)(&patterns[pattern_offsets[i - 1]]);
        UInt32 patter_str_size = pattern_offsets[i] - pattern_offsets[i - 1] - 1;
        RETURN_IF_ERROR((function)(search_state, StringValue(val_raw_str, val_str_size),
                                   StringValue(pattern_raw_str, patter_str_size), &result[i]));
    }
    return Status::OK();
}

Status FunctionLike::like_fn(LikeSearchState* state, const StringValue& val,
                             const StringValue& pattern, unsigned char* result) {
    std::string re_pattern;
    convert_like_pattern(state, std::string(pattern.ptr, pattern.len), &re_pattern);

    return regexp_fn(state, val, {re_pattern.c_str(), (int)re_pattern.size()}, result);
}

void FunctionLike::convert_like_pattern(LikeSearchState* state, const std::string& pattern,
                                        std::string* re_pattern) {
    re_pattern->clear();

    // add ^ to pattern head to match line head
    if (pattern.size() > 0 && pattern[0] != '%') {
        re_pattern->append("^");
    }

    bool is_escaped = false;
    for (size_t i = 0; i < pattern.size(); ++i) {
        if (!is_escaped && pattern[i] == '%') {
            re_pattern->append(".*");
        } else if (!is_escaped && pattern[i] == '_') {
            re_pattern->append(".");
            // check for escape char before checking for regex special chars, they might overlap
        } else if (!is_escaped && pattern[i] == state->escape_char) {
            is_escaped = true;
        } else if (pattern[i] == '.' || pattern[i] == '[' || pattern[i] == ']' ||
                   pattern[i] == '{' || pattern[i] == '}' || pattern[i] == '(' ||
                   pattern[i] == ')' || pattern[i] == '\\' || pattern[i] == '*' ||
                   pattern[i] == '+' || pattern[i] == '?' || pattern[i] == '|' ||
                   pattern[i] == '^' || pattern[i] == '$') {
            // escape all regex special characters; see list at
            re_pattern->append("\\");
            re_pattern->append(1, pattern[i]);
            is_escaped = false;
        } else {
            // regular character or escaped special character
            re_pattern->append(1, pattern[i]);
            is_escaped = false;
        }
    }

    // add $ to pattern tail to match line tail
    if (pattern.size() > 0 && pattern[pattern.size() - 1] != '%') {
        re_pattern->append("$");
    }
}

void FunctionLike::remove_escape_character(std::string* search_string) {
    std::string tmp_search_string;
    tmp_search_string.swap(*search_string);
    int len = tmp_search_string.length();
    for (int i = 0; i < len;) {
        if (tmp_search_string[i] == '\\' && i + 1 < len &&
            (tmp_search_string[i + 1] == '%' || tmp_search_string[i + 1] == '_')) {
            search_string->append(1, tmp_search_string[i + 1]);
            i += 2;
        } else {
            search_string->append(1, tmp_search_string[i]);
            i++;
        }
    }
}

Status FunctionLike::prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    auto* state = new LikeState();
    context->set_function_state(scope, state);
    state->function = like_fn;
    if (context->is_col_constant(1)) {
        const auto pattern_col = context->get_constant_col(1)->column_ptr;
        const auto& pattern = pattern_col->get_data_at(0);

        std::string pattern_str = pattern.to_string();
        std::string search_string;
        if (RE2::FullMatch(pattern_str, LIKE_EQUALS_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->search_state.set_search_string(search_string);
            state->function = constant_equals_fn;
        } else if (RE2::FullMatch(pattern_str, LIKE_STARTS_WITH_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->search_state.set_search_string(search_string);
            state->function = constant_starts_with_fn;
        } else if (RE2::FullMatch(pattern_str, LIKE_ENDS_WITH_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->search_state.set_search_string(search_string);
            state->function = constant_ends_with_fn;
        } else if (RE2::FullMatch(pattern_str, LIKE_SUBSTRING_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->search_state.set_search_string(search_string);
            state->function = constant_substring_fn;
        } else {
            std::string re_pattern;
            convert_like_pattern(&state->search_state, pattern_str, &re_pattern);

            hs_database_t* database = nullptr;
            hs_scratch_t* scratch = nullptr;
            RETURN_IF_ERROR(hs_prepare(context, re_pattern.c_str(), &database, &scratch));

            state->search_state.hs_database.reset(database);
            state->search_state.hs_scratch.reset(scratch);

            state->function = constant_regex_fn;
        }
    }
    return Status::OK();
}

Status FunctionRegexp::prepare(FunctionContext* context,
                               FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    auto* state = new LikeState();
    context->set_function_state(scope, state);
    state->function = regexp_fn;
    if (context->is_col_constant(1)) {
        const auto pattern_col = context->get_constant_col(1)->column_ptr;
        const auto& pattern = pattern_col->get_data_at(0);

        std::string pattern_str = pattern.to_string();
        std::string search_string;
        if (RE2::FullMatch(pattern_str, EQUALS_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_equals_fn;
        } else if (RE2::FullMatch(pattern_str, STARTS_WITH_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_starts_with_fn;
        } else if (RE2::FullMatch(pattern_str, ENDS_WITH_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_ends_with_fn;
        } else if (RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_substring_fn;
        } else {
            hs_database_t* database = nullptr;
            hs_scratch_t* scratch = nullptr;
            RETURN_IF_ERROR(hs_prepare(context, pattern_str.c_str(), &database, &scratch));

            state->search_state.hs_database.reset(database);
            state->search_state.hs_scratch.reset(scratch);

            state->function = constant_regex_fn;
        }
    }
    return Status::OK();
}

} // namespace doris::vectorized
