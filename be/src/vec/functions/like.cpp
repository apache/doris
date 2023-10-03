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

#include "like.h"

#include <fmt/format.h>
#include <hs/hs_compile.h>
#include <re2/stringpiece.h>

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
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
// A regex to match .*
static const RE2 ALLPASS_RE("(\\\\.\\*)+");

// Like patterns
static const re2::RE2 LIKE_SUBSTRING_RE("(?:%+)(((\\\\_)|([^%_\\\\]))+)(?:%+)");
static const re2::RE2 LIKE_ENDS_WITH_RE("(?:%+)(((\\\\_)|([^%_]))+)");
static const re2::RE2 LIKE_STARTS_WITH_RE("(((\\\\%)|(\\\\_)|([^%_\\\\]))+)(?:%+)");
static const re2::RE2 LIKE_EQUALS_RE("(((\\\\_)|([^%_]))+)");
static const re2::RE2 LIKE_ALLPASS_RE("%+");

Status LikeSearchState::clone(LikeSearchState& cloned) {
    cloned.escape_char = escape_char;
    cloned.set_search_string(search_string);

    std::string re_pattern;
    FunctionLike::convert_like_pattern(this, pattern_str, &re_pattern);
    if (hs_database) { // use hyperscan
        hs_database_t* database = nullptr;
        hs_scratch_t* scratch = nullptr;
        RETURN_IF_ERROR(FunctionLike::hs_prepare(nullptr, re_pattern.c_str(), &database, &scratch));

        cloned.hs_database.reset(database);
        cloned.hs_scratch.reset(scratch);
    } else { // fallback to re2
        cloned.hs_database.reset();
        cloned.hs_scratch.reset();

        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        cloned.regex = std::make_unique<RE2>(re_pattern, opts);
        if (!cloned.regex->ok()) {
            return Status::InternalError("Invalid regex expression: {}", re_pattern);
        }
    }

    return Status::OK();
}

Status FunctionLikeBase::constant_allpass_fn(LikeSearchState* state, const ColumnString& val,
                                             const StringRef& pattern,
                                             ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        result[i] = 1;
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_starts_with_fn(LikeSearchState* state, const ColumnString& val,
                                                 const StringRef& pattern,
                                                 ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        const auto& str_ref = val.get_data_at(i);
        result[i] = (str_ref.size >= state->search_string_sv.size) &&
                    str_ref.start_with(state->search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_ends_with_fn(LikeSearchState* state, const ColumnString& val,
                                               const StringRef& pattern,
                                               ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        const auto& str_ref = val.get_data_at(i);
        result[i] = (str_ref.size >= state->search_string_sv.size) &&
                    str_ref.end_with(state->search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_equals_fn(LikeSearchState* state, const ColumnString& val,
                                            const StringRef& pattern,
                                            ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        result[i] = (val.get_data_at(i) == state->search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_substring_fn(LikeSearchState* state, const ColumnString& val,
                                               const StringRef& pattern,
                                               ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        if (state->search_string_sv.size == 0) {
            result[i] = true;
        }
        result[i] = state->substring_pattern.search(val.get_data_at(i)) != -1;
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_allpass_fn_scalar(LikeSearchState* state, const StringRef& val,
                                                    const StringRef& pattern,
                                                    unsigned char* result) {
    *result = 1;
    return Status::OK();
}

Status FunctionLikeBase::constant_starts_with_fn_scalar(LikeSearchState* state,
                                                        const StringRef& val,
                                                        const StringRef& pattern,
                                                        unsigned char* result) {
    *result = (val.size >= state->search_string_sv.size) &&
              (state->search_string_sv == val.substring(0, state->search_string_sv.size));
    return Status::OK();
}

Status FunctionLikeBase::constant_ends_with_fn_scalar(LikeSearchState* state, const StringRef& val,
                                                      const StringRef& pattern,
                                                      unsigned char* result) {
    *result = (val.size >= state->search_string_sv.size) &&
              (state->search_string_sv == val.substring(val.size - state->search_string_sv.size,
                                                        state->search_string_sv.size));
    return Status::OK();
}

Status FunctionLikeBase::constant_equals_fn_scalar(LikeSearchState* state, const StringRef& val,
                                                   const StringRef& pattern,
                                                   unsigned char* result) {
    *result = (val == state->search_string_sv);
    return Status::OK();
}

Status FunctionLikeBase::constant_substring_fn_scalar(LikeSearchState* state, const StringRef& val,
                                                      const StringRef& pattern,
                                                      unsigned char* result) {
    if (state->search_string_sv.size == 0) {
        *result = true;
        return Status::OK();
    }
    *result = state->substring_pattern.search(val) != -1;
    return Status::OK();
}

Status FunctionLikeBase::constant_regex_fn_scalar(LikeSearchState* state, const StringRef& val,
                                                  const StringRef& pattern, unsigned char* result) {
    if (state->hs_database) { // use hyperscan
        auto ret = hs_scan(state->hs_database.get(), val.data, val.size, 0, state->hs_scratch.get(),
                           doris::vectorized::LikeSearchState::hs_match_handler, (void*)result);
        if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
            return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
        }
    } else { // fallback to re2
        *result = RE2::PartialMatch(re2::StringPiece(val.data, val.size), *state->regex.get());
    }

    return Status::OK();
}

Status FunctionLikeBase::regexp_fn_scalar(LikeSearchState* state, const StringRef& val,
                                          const StringRef& pattern, unsigned char* result) {
    std::string re_pattern(pattern.data, pattern.size);

    RE2::Options opts;
    opts.set_never_nl(false);
    opts.set_dot_nl(true);
    re2::RE2 re(re_pattern, opts);
    if (re.ok()) {
        *result = RE2::PartialMatch(re2::StringPiece(val.data, val.size), re);
    } else {
        return Status::RuntimeError("Invalid pattern: {}", pattern.debug_string());
    }

    return Status::OK();
}

Status FunctionLikeBase::constant_regex_fn(LikeSearchState* state, const ColumnString& val,
                                           const StringRef& pattern,
                                           ColumnUInt8::Container& result) {
    auto sz = val.size();
    if (state->hs_database) { // use hyperscan
        for (size_t i = 0; i < sz; i++) {
            const auto& str_ref = val.get_data_at(i);
            auto ret = hs_scan(state->hs_database.get(), str_ref.data, str_ref.size, 0,
                               state->hs_scratch.get(),
                               doris::vectorized::LikeSearchState::hs_match_handler,
                               (void*)(result.data() + i));
            if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
                return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
            }
        }
    } else { // fallback to re2
        for (size_t i = 0; i < sz; i++) {
            const auto& str_ref = val.get_data_at(i);
            *(result.data() + i) = RE2::PartialMatch(re2::StringPiece(str_ref.data, str_ref.size),
                                                     *state->regex.get());
        }
    }

    return Status::OK();
}

Status FunctionLikeBase::regexp_fn(LikeSearchState* state, const ColumnString& val,
                                   const StringRef& pattern, ColumnUInt8::Container& result) {
    std::string re_pattern(pattern.data, pattern.size);

    hs_database_t* database = nullptr;
    hs_scratch_t* scratch = nullptr;
    if (hs_prepare(nullptr, re_pattern.c_str(), &database, &scratch).ok()) { // use hyperscan
        auto sz = val.size();
        for (size_t i = 0; i < sz; i++) {
            const auto& str_ref = val.get_data_at(i);
            auto ret = hs_scan(database, str_ref.data, str_ref.size, 0, scratch,
                               doris::vectorized::LikeSearchState::hs_match_handler,
                               (void*)(result.data() + i));
            if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
                return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
            }
        }

        hs_free_scratch(scratch);
        hs_free_database(database);
    } else { // fallback to re2
        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        re2::RE2 re(re_pattern, opts);
        if (re.ok()) {
            auto sz = val.size();
            for (size_t i = 0; i < sz; i++) {
                const auto& str_ref = val.get_data_at(i);
                *(result.data() + i) =
                        RE2::PartialMatch(re2::StringPiece(str_ref.data, str_ref.size), re);
            }
        } else {
            return Status::RuntimeError("Invalid pattern: {}", pattern.debug_string());
        }
    }

    return Status::OK();
}

// hyperscan compile expression to database and allocate scratch space
Status FunctionLikeBase::hs_prepare(FunctionContext* context, const char* expression,
                                    hs_database_t** database, hs_scratch_t** scratch) {
    hs_compile_error_t* compile_err;
    auto res = hs_compile(expression, HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                          HS_MODE_BLOCK, nullptr, database, &compile_err);

    if (res != HS_SUCCESS) {
        *database = nullptr;
        if (context) {
            context->set_error("hs_compile regex pattern error");
        }
        return Status::RuntimeError("hs_compile regex pattern error:" +
                                    std::string(compile_err->message));
        hs_free_compile_error(compile_err);
    }
    hs_free_compile_error(compile_err);

    if (hs_alloc_scratch(*database, scratch) != HS_SUCCESS) {
        hs_free_database(*database);
        *database = nullptr;
        *scratch = nullptr;
        if (context) {
            context->set_error("hs_alloc_scratch allocate scratch space error");
        }
        return Status::RuntimeError("hs_alloc_scratch allocate scratch space error");
    }

    return Status::OK();
}

Status FunctionLikeBase::execute_impl(FunctionContext* context, Block& block,
                                      const ColumnNumbers& arguments, size_t result,
                                      size_t input_rows_count) const {
    const auto values_col =
            block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
    const auto* values = check_and_get_column<ColumnString>(values_col.get());

    if (!values) {
        return Status::InternalError("Not supported input arguments types");
    }
    // result column
    auto res = ColumnUInt8::create();
    ColumnUInt8::Container& vec_res = res->get_data();
    // set default value to 0, and match functions only need to set 1/true
    vec_res.resize_fill(input_rows_count);
    auto* state = reinterpret_cast<LikeState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    // for constant_substring_fn, use long run length search for performance
    if (constant_substring_fn ==
        *(state->function.target<doris::Status (*)(LikeSearchState* state, const ColumnString&,
                                                   const StringRef&, ColumnUInt8::Container&)>())) {
        RETURN_IF_ERROR(execute_substring(values->get_chars(), values->get_offsets(), vec_res,
                                          &state->search_state));
    } else {
        const auto pattern_col = block.get_by_position(arguments[1]).column;

        if (const auto* str_patterns = check_and_get_column<ColumnString>(pattern_col.get())) {
            for (int i = 0; i < input_rows_count; i++) {
                const auto pattern_val = str_patterns->get_data_at(i);
                const auto value_val = values->get_data_at(i);
                static_cast<void>((state->scalar_function)(
                        const_cast<vectorized::LikeSearchState*>(&state->search_state), value_val,
                        pattern_val, &vec_res[i]));
            }
        } else if (const auto* const_patterns =
                           check_and_get_column<ColumnConst>(pattern_col.get())) {
            const auto& pattern_val = const_patterns->get_data_at(0);
            RETURN_IF_ERROR(vector_const(*values, &pattern_val, vec_res, state->function,
                                         &state->search_state));
        } else {
            return Status::InternalError("Not supported input arguments types");
        }
    }
    block.replace_by_position(result, std::move(res));
    return Status::OK();
}

Status FunctionLikeBase::close(FunctionContext* context,
                               FunctionContext::FunctionStateScope scope) {
    return Status::OK();
}

Status FunctionLikeBase::execute_substring(const ColumnString::Chars& values,
                                           const ColumnString::Offsets& value_offsets,
                                           ColumnUInt8::Container& result,
                                           LikeSearchState* search_state) const {
    // treat continuous multi string data as a long string data
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
        if (pos >= end) {
            break;
        }

        /// Determine which index it refers to.
        /// begin + value_offsets[i] is the start offset of string at i+1
        while (begin + value_offsets[i] < pos) {
            ++i;
        }

        /// We check that the entry does not pass through the boundaries of strings.
        if (pos + needle_size <= begin + value_offsets[i]) {
            result[i] = 1;
        }

        // move to next string offset
        pos = begin + value_offsets[i];
        ++i;
    }

    return Status::OK();
}

Status FunctionLikeBase::vector_const(const ColumnString& values, const StringRef* pattern_val,
                                      ColumnUInt8::Container& result, const LikeFn& function,
                                      LikeSearchState* search_state) const {
    RETURN_IF_ERROR((function)(search_state, values,
                               *reinterpret_cast<const StringRef*>(pattern_val), result));
    return Status::OK();
}

Status FunctionLike::like_fn(LikeSearchState* state, const ColumnString& val,
                             const StringRef& pattern, ColumnUInt8::Container& result) {
    std::string re_pattern;
    convert_like_pattern(state, std::string(pattern.data, pattern.size), &re_pattern);

    return regexp_fn(state, val, {re_pattern.c_str(), re_pattern.size()}, result);
}

Status FunctionLike::like_fn_scalar(LikeSearchState* state, const StringRef& val,
                                    const StringRef& pattern, unsigned char* result) {
    std::string re_pattern;
    convert_like_pattern(state, std::string(pattern.data, pattern.size), &re_pattern);

    return regexp_fn_scalar(state, StringRef(val.data, val.size),
                            {re_pattern.c_str(), re_pattern.size()}, result);
}

void FunctionLike::convert_like_pattern(LikeSearchState* state, const std::string& pattern,
                                        std::string* re_pattern) {
    re_pattern->clear();

    if (pattern.empty()) {
        re_pattern->append("^$");
        return;
    }

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
    if (pattern.size() > 0 && re_pattern->back() != '*') {
        re_pattern->append("$");
    }
}

void FunctionLike::remove_escape_character(std::string* search_string) {
    std::string tmp_search_string;
    tmp_search_string.swap(*search_string);
    int len = tmp_search_string.length();
    for (int i = 0; i < len;) {
        if (tmp_search_string[i] == '\\' && i + 1 < len &&
            (tmp_search_string[i + 1] == '%' || tmp_search_string[i + 1] == '_' ||
             tmp_search_string[i + 1] == '\\')) {
            search_string->append(1, tmp_search_string[i + 1]);
            i += 2;
        } else {
            search_string->append(1, tmp_search_string[i]);
            i++;
        }
    }
}

bool re2_full_match(const std::string& str, const RE2& re, std::vector<std::string>& results) {
    if (!re.ok()) {
        return false;
    }

    std::vector<RE2::Arg> arguments;
    std::vector<RE2::Arg*> arguments_ptrs;
    std::size_t args_count = re.NumberOfCapturingGroups();
    arguments.resize(args_count);
    arguments_ptrs.resize(args_count);
    results.resize(args_count);
    for (std::size_t i = 0; i < args_count; ++i) {
        arguments[i] = &results[i];
        arguments_ptrs[i] = &arguments[i];
    }

    return RE2::FullMatchN(str, re, arguments_ptrs.data(), args_count);
}

void verbose_log_match(const std::string& str, const std::string& pattern_name, const RE2& re) {
    std::vector<std::string> results;
    VLOG_DEBUG << "arg str: " << str << ", size: " << str.size() << ", pattern " << pattern_name
               << ": " << re.pattern() << ", size: " << re.pattern().size();
    if (re2_full_match(str, re, results)) {
        for (int i = 0; i < results.size(); ++i) {
            VLOG_DEBUG << "match " << i << ": " << results[i] << ", size: " << results[i].size();
        }
    } else {
        VLOG_DEBUG << "no match";
    }
}

Status FunctionLike::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    std::shared_ptr<LikeState> state = std::make_shared<LikeState>();
    context->set_function_state(scope, state);
    state->function = like_fn;
    state->scalar_function = like_fn_scalar;
    if (context->is_col_constant(1)) {
        const auto pattern_col = context->get_constant_col(1)->column_ptr;
        const auto& pattern = pattern_col->get_data_at(0);

        std::string pattern_str = pattern.to_string();
        state->search_state.pattern_str = pattern_str;
        std::string search_string;

        if (!pattern_str.empty() && RE2::FullMatch(pattern_str, LIKE_ALLPASS_RE)) {
            state->search_state.set_search_string("");
            state->function = constant_allpass_fn;
            state->scalar_function = constant_allpass_fn_scalar;
        } else if (pattern_str.empty() ||
                   RE2::FullMatch(pattern_str, LIKE_EQUALS_RE, &search_string)) {
            if (VLOG_DEBUG_IS_ON) {
                verbose_log_match(pattern_str, "LIKE_EQUALS_RE", LIKE_EQUALS_RE);
                VLOG_DEBUG << "search_string : " << search_string
                           << ", size: " << search_string.size();
            }
            remove_escape_character(&search_string);
            if (VLOG_DEBUG_IS_ON) {
                VLOG_DEBUG << "search_string escape removed: " << search_string
                           << ", size: " << search_string.size();
            }
            state->search_state.set_search_string(search_string);
            state->function = constant_equals_fn;
            state->scalar_function = constant_equals_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, LIKE_STARTS_WITH_RE, &search_string)) {
            if (VLOG_DEBUG_IS_ON) {
                verbose_log_match(pattern_str, "LIKE_STARTS_WITH_RE", LIKE_STARTS_WITH_RE);
                VLOG_DEBUG << "search_string : " << search_string
                           << ", size: " << search_string.size();
            }
            remove_escape_character(&search_string);
            if (VLOG_DEBUG_IS_ON) {
                VLOG_DEBUG << "search_string escape removed: " << search_string
                           << ", size: " << search_string.size();
            }
            state->search_state.set_search_string(search_string);
            state->function = constant_starts_with_fn;
            state->scalar_function = constant_starts_with_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, LIKE_ENDS_WITH_RE, &search_string)) {
            if (VLOG_DEBUG_IS_ON) {
                verbose_log_match(pattern_str, "LIKE_ENDS_WITH_RE", LIKE_ENDS_WITH_RE);
                VLOG_DEBUG << "search_string : " << search_string
                           << ", size: " << search_string.size();
            }
            remove_escape_character(&search_string);
            if (VLOG_DEBUG_IS_ON) {
                VLOG_DEBUG << "search_string escape removed: " << search_string
                           << ", size: " << search_string.size();
            }
            state->search_state.set_search_string(search_string);
            state->function = constant_ends_with_fn;
            state->scalar_function = constant_ends_with_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, LIKE_SUBSTRING_RE, &search_string)) {
            if (VLOG_DEBUG_IS_ON) {
                verbose_log_match(pattern_str, "LIKE_SUBSTRING_RE", LIKE_SUBSTRING_RE);
                VLOG_DEBUG << "search_string : " << search_string
                           << ", size: " << search_string.size();
            }
            remove_escape_character(&search_string);
            if (VLOG_DEBUG_IS_ON) {
                VLOG_DEBUG << "search_string escape removed: " << search_string
                           << ", size: " << search_string.size();
            }
            state->search_state.set_search_string(search_string);
            state->function = constant_substring_fn;
            state->scalar_function = constant_substring_fn_scalar;
        } else {
            std::string re_pattern;
            convert_like_pattern(&state->search_state, pattern_str, &re_pattern);
            if (VLOG_DEBUG_IS_ON) {
                VLOG_DEBUG << "hyperscan, pattern str: " << pattern_str
                           << ", size: " << pattern_str.size() << ", re pattern: " << re_pattern
                           << ", size: " << re_pattern.size();
            }

            hs_database_t* database = nullptr;
            hs_scratch_t* scratch = nullptr;
            if (hs_prepare(context, re_pattern.c_str(), &database, &scratch).ok()) {
                // use hyperscan
                state->search_state.hs_database.reset(database);
                state->search_state.hs_scratch.reset(scratch);
            } else {
                // fallback to re2
                // reset hs_database to nullptr to indicate not use hyperscan
                state->search_state.hs_database.reset();
                state->search_state.hs_scratch.reset();

                RE2::Options opts;
                opts.set_never_nl(false);
                opts.set_dot_nl(true);
                state->search_state.regex = std::make_unique<RE2>(re_pattern, opts);
                if (!state->search_state.regex->ok()) {
                    return Status::InternalError("Invalid regex expression: {}", pattern_str);
                }
            }

            state->function = constant_regex_fn;
            state->scalar_function = constant_regex_fn_scalar;
        }
    }
    return Status::OK();
}

Status FunctionRegexp::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    std::shared_ptr<LikeState> state = std::make_shared<LikeState>();
    context->set_function_state(scope, state);
    state->function = regexp_fn;
    state->scalar_function = regexp_fn_scalar;
    if (context->is_col_constant(1)) {
        const auto pattern_col = context->get_constant_col(1)->column_ptr;
        const auto& pattern = pattern_col->get_data_at(0);

        std::string pattern_str = pattern.to_string();
        std::string search_string;
        if (RE2::FullMatch(pattern_str, ALLPASS_RE)) {
            state->search_state.set_search_string("");
            state->function = constant_allpass_fn;
            state->scalar_function = constant_allpass_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, EQUALS_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_equals_fn;
            state->scalar_function = constant_equals_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, STARTS_WITH_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_starts_with_fn;
            state->scalar_function = constant_starts_with_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, ENDS_WITH_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_ends_with_fn;
            state->scalar_function = constant_ends_with_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_substring_fn;
            state->scalar_function = constant_substring_fn_scalar;
        } else {
            hs_database_t* database = nullptr;
            hs_scratch_t* scratch = nullptr;
            if (hs_prepare(context, pattern_str.c_str(), &database, &scratch).ok()) {
                // use hyperscan
                state->search_state.hs_database.reset(database);
                state->search_state.hs_scratch.reset(scratch);
            } else {
                // fallback to re2
                // reset hs_database to nullptr to indicate not use hyperscan
                state->search_state.hs_database.reset();
                state->search_state.hs_scratch.reset();
                RE2::Options opts;
                opts.set_never_nl(false);
                opts.set_dot_nl(true);
                state->search_state.regex = std::make_unique<RE2>(pattern_str, opts);
                if (!state->search_state.regex->ok()) {
                    return Status::InternalError("Invalid regex expression: {}", pattern_str);
                }
            }
            state->function = constant_regex_fn;
            state->scalar_function = constant_regex_fn_scalar;
        }
    }
    return Status::OK();
}

void register_function_like(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLike>();
}

void register_function_regexp(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionRegexp>();
    factory.register_alias(FunctionRegexp::name, FunctionRegexp::alias);
}

} // namespace doris::vectorized
