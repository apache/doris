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

#include "exprs/like_predicate.h"

#include <string.h>

#include <sstream>

#include "exprs/string_functions.h"
#include "runtime/string_value.hpp"

namespace doris {

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

static const re2::RE2 LIKE_SUBSTRING_RE("(?:%+)(((\\\\%)|(\\\\_)|([^%_]))+)(?:%+)");
static const re2::RE2 LIKE_ENDS_WITH_RE("(?:%+)(((\\\\%)|(\\\\_)|([^%_]))+)");
static const re2::RE2 LIKE_STARTS_WITH_RE("(((\\\\%)|(\\\\_)|([^%_]))+)(?:%+)");
static const re2::RE2 LIKE_EQUALS_RE("(((\\\\%)|(\\\\_)|([^%_]))+)");

void LikePredicate::init() {}

void LikePredicate::like_prepare(FunctionContext* context,
                                 FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return;
    }
    LikePredicateState* state = new LikePredicateState();
    state->function = like_fn;
    context->set_function_state(scope, state);
    if (context->is_arg_constant(1)) {
        StringVal pattern_val = *reinterpret_cast<StringVal*>(context->get_constant_arg(1));
        if (pattern_val.is_null) {
            return;
        }
        StringValue pattern = StringValue::from_string_val(pattern_val);
        std::string pattern_str(pattern.ptr, pattern.len);
        std::string search_string;
        if (RE2::FullMatch(pattern_str, LIKE_ENDS_WITH_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->set_search_string(search_string);
            state->function = constant_ends_with_fn;
        } else if (RE2::FullMatch(pattern_str, LIKE_SUBSTRING_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->set_search_string(search_string);
            state->function = constant_substring_fn;
        } else if (RE2::FullMatch(pattern_str, LIKE_EQUALS_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->set_search_string(search_string);
            state->function = constant_equals_fn;
        } else if (RE2::FullMatch(pattern_str, LIKE_STARTS_WITH_RE, &search_string)) {
            remove_escape_character(&search_string);
            state->set_search_string(search_string);
            state->function = constant_starts_with_fn;
        } else {
            std::string re_pattern;
            convert_like_pattern(context,
                                 *reinterpret_cast<StringVal*>(context->get_constant_arg(1)),
                                 &re_pattern);
            RE2::Options opts;
            opts.set_never_nl(false);
            opts.set_dot_nl(true);
            state->regex.reset(new RE2(re_pattern, opts));
            if (!state->regex->ok()) {
                context->set_error("Invalid regex: $0");
            }
        }
    }
}

BooleanVal LikePredicate::like(FunctionContext* context, const StringVal& val,
                               const StringVal& pattern) {
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    return (state->function)(context, val, pattern);
}

void LikePredicate::like_close(FunctionContext* context,
                               FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
}

void LikePredicate::regex_prepare(FunctionContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return;
    }
    LikePredicateState* state = new LikePredicateState();
    context->set_function_state(scope, state);
    state->function = regex_fn;
    if (context->is_arg_constant(1)) {
        StringVal* pattern = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
        if (pattern->is_null) {
            return;
        }
        std::string pattern_str(reinterpret_cast<const char*>(pattern->ptr), pattern->len);
        std::string search_string;
        // The following four conditionals check if the pattern is a constant string,
        // starts with a constant string and is followed by any number of wildcard characters,
        // ends with a constant string and is preceded by any number of wildcard characters or
        // has a constant substring surrounded on both sides by any number of wildcard
        // characters. In any of these conditions, we can search for the pattern more
        // efficiently by using our own string match functions rather than regex matching.
        if (RE2::FullMatch(pattern_str, EQUALS_RE, &search_string)) {
            state->set_search_string(search_string);
            state->function = constant_equals_fn;
        } else if (RE2::FullMatch(pattern_str, STARTS_WITH_RE, &search_string)) {
            state->set_search_string(search_string);
            state->function = constant_starts_with_fn;
        } else if (RE2::FullMatch(pattern_str, ENDS_WITH_RE, &search_string)) {
            state->set_search_string(search_string);
            state->function = constant_ends_with_fn;
        } else if (RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
            state->set_search_string(search_string);
            state->function = constant_substring_fn;
        } else {
            RE2::Options opts;
            opts.set_never_nl(false);
            opts.set_dot_nl(true);
            state->regex.reset(new RE2(pattern_str, opts));
            if (!state->regex->ok()) {
                std::stringstream error;
                error << "Invalid regex expression" << pattern->ptr;
                context->set_error(error.str().c_str());
            }
            state->function = constant_regex_fn_partial;
        }
    }
}

BooleanVal LikePredicate::regex(FunctionContext* context, const StringVal& val,
                                const StringVal& pattern) {
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    return (state->function)(context, val, pattern);
}

// This prepare function is used only when 3 parameters are passed to the regexp_like()
// function. For the 2 parameter version, the RegexPrepare() function is used to prepare.
void LikePredicate::regexp_like_prepare(FunctionContext* context,
                                        FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return;
    }
    LikePredicateState* state = new LikePredicateState();
    context->set_function_state(scope, state);
    // If both the pattern and the match parameter are constant, we pre-compile the
    // regular expression once here. Otherwise, the RE is compiled per row in RegexpLike()
    if (context->is_arg_constant(1) && context->is_arg_constant(2)) {
        StringVal* pattern = nullptr;
        pattern = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
        if (pattern->is_null) {
            return;
        }
        StringVal* match_parameter = reinterpret_cast<StringVal*>(context->get_constant_arg(2));
        std::stringstream error;
        if (match_parameter->is_null) {
            error << "match parameter is null";
            context->set_error(error.str().c_str());
            return;
        }
        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        std::string error_str;
        if (!StringFunctions::set_re2_options(*match_parameter, &error_str, &opts)) {
            context->set_error(error_str.c_str());
            return;
        }
        std::string pattern_str(reinterpret_cast<const char*>(pattern->ptr), pattern->len);
        state->regex.reset(new RE2(pattern_str, opts));
        if (!state->regex->ok()) {
            error << "Invalid regex expression" << pattern->ptr;
            context->set_error(error.str().c_str());
        }
    }
}

// This is used only for the 3 parameter version of regexp_like(). The 2 parameter
// version calls Regex() directly.
BooleanVal LikePredicate::regexp_like(FunctionContext* context, const StringVal& val,
                                      const StringVal& pattern, const StringVal& match_parameter) {
    if (val.is_null || pattern.is_null) {
        return BooleanVal::null();
    }
    // If either the pattern or the third optional match parameter are not constant, we
    // have to recompile the RE for every row.
    if (!context->is_arg_constant(2) || !context->is_arg_constant(1)) {
        if (match_parameter.is_null) {
            return BooleanVal::null();
        }
        RE2::Options opts;
        std::string error_str;
        if (!StringFunctions::set_re2_options(match_parameter, &error_str, &opts)) {
            context->set_error(error_str.c_str());
            return BooleanVal(false);
        }
        std::string re_pattern(reinterpret_cast<const char*>(pattern.ptr), pattern.len);
        re2::RE2 re(re_pattern, opts);
        if (re.ok()) {
            return RE2::PartialMatch(
                    re2::StringPiece(reinterpret_cast<const char*>(val.ptr), val.len), re);
        } else {
            context->set_error("Invalid regex: $0");
            return BooleanVal(false);
        }
    }
    return constant_regex_fn_partial(context, val, pattern);
}

void LikePredicate::regex_close(FunctionContext* context,
                                FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        delete state;
    }
}

BooleanVal LikePredicate::regex_fn(FunctionContext* context, const StringVal& val,
                                   const StringVal& pattern) {
    return regex_match(context, val, pattern, false);
}

BooleanVal LikePredicate::like_fn(FunctionContext* context, const StringVal& val,
                                  const StringVal& pattern) {
    return regex_match(context, val, pattern, true);
}

BooleanVal LikePredicate::constant_substring_fn(FunctionContext* context, const StringVal& val,
                                                const StringVal& pattern) {
    if (val.is_null) {
        return BooleanVal::null();
    }
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (state->search_string_sv.len == 0) {
        return BooleanVal(true);
    }
    StringValue pattern_value = StringValue::from_string_val(val);
    return BooleanVal(state->substring_pattern.search(&pattern_value) != -1);
}

BooleanVal LikePredicate::constant_starts_with_fn(FunctionContext* context, const StringVal& val,
                                                  const StringVal& pattern) {
    if (val.is_null) {
        return BooleanVal::null();
    }
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (val.len < state->search_string_sv.len) {
        return BooleanVal(false);
    } else {
        StringValue v = StringValue(reinterpret_cast<char*>(val.ptr), state->search_string_sv.len);
        return BooleanVal(state->search_string_sv.eq((v)));
    }
}

BooleanVal LikePredicate::constant_ends_with_fn(FunctionContext* context, const StringVal& val,
                                                const StringVal& pattern) {
    if (val.is_null) {
        return BooleanVal::null();
    }
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (val.len < state->search_string_sv.len) {
        return BooleanVal(false);
    } else {
        char* ptr = reinterpret_cast<char*>(val.ptr) + val.len - state->search_string_sv.len;
        int len = state->search_string_sv.len;
        StringValue v = StringValue(ptr, len);
        return BooleanVal(state->search_string_sv.eq(v));
    }
}

BooleanVal LikePredicate::constant_equals_fn(FunctionContext* context, const StringVal& val,
                                             const StringVal& pattern) {
    if (val.is_null) {
        return BooleanVal::null();
    }
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    return BooleanVal(state->search_string_sv.eq(StringValue::from_string_val(val)));
}

BooleanVal LikePredicate::constant_regex_fn_partial(FunctionContext* context, const StringVal& val,
                                                    const StringVal& pattern) {
    if (val.is_null) {
        return BooleanVal::null();
    }
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    re2::StringPiece operand_sp(reinterpret_cast<const char*>(val.ptr), val.len);
    return RE2::PartialMatch(operand_sp, *state->regex);
}

BooleanVal LikePredicate::constant_regex_fn(FunctionContext* context, const StringVal& val,
                                            const StringVal& pattern) {
    if (val.is_null) {
        return BooleanVal::null();
    }
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    re2::StringPiece operand_sp(reinterpret_cast<const char*>(val.ptr), val.len);
    return RE2::FullMatch(operand_sp, *state->regex);
}

BooleanVal LikePredicate::regex_match(FunctionContext* context, const StringVal& operand_value,
                                      const StringVal& pattern_value, bool is_like_pattern) {
    if (operand_value.is_null || pattern_value.is_null) {
        return BooleanVal::null();
    }
    if (context->is_arg_constant(1)) {
        LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
                context->get_function_state(FunctionContext::THREAD_LOCAL));
        if (is_like_pattern) {
            return RE2::FullMatch(re2::StringPiece(reinterpret_cast<const char*>(operand_value.ptr),
                                                   operand_value.len),
                                  *state->regex.get());
        } else {
            return RE2::PartialMatch(
                    re2::StringPiece(reinterpret_cast<const char*>(operand_value.ptr),
                                     operand_value.len),
                    *state->regex.get());
        }
    } else {
        std::string re_pattern;
        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        if (is_like_pattern) {
            convert_like_pattern(context, pattern_value, &re_pattern);
        } else {
            re_pattern = std::string(reinterpret_cast<const char*>(pattern_value.ptr),
                                     pattern_value.len);
        }
        re2::RE2 re(re_pattern, opts);
        if (re.ok()) {
            if (is_like_pattern) {
                return RE2::FullMatch(
                        re2::StringPiece(reinterpret_cast<const char*>(operand_value.ptr),
                                         operand_value.len),
                        re);
            } else {
                return RE2::PartialMatch(
                        re2::StringPiece(reinterpret_cast<const char*>(operand_value.ptr),
                                         operand_value.len),
                        re);
            }
        } else {
            context->set_error("Invalid regex: $0");
            return BooleanVal(false);
        }
    }
}

void LikePredicate::convert_like_pattern(FunctionContext* context, const StringVal& pattern,
                                         std::string* re_pattern) {
    re_pattern->clear();
    LikePredicateState* state = reinterpret_cast<LikePredicateState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    bool is_escaped = false;
    for (int i = 0; i < pattern.len; ++i) {
        if (!is_escaped && pattern.ptr[i] == '%') {
            re_pattern->append(".*");
        } else if (!is_escaped && pattern.ptr[i] == '_') {
            re_pattern->append(".");
            // check for escape char before checking for regex special chars, they might overlap
        } else if (!is_escaped && pattern.ptr[i] == state->escape_char) {
            is_escaped = true;
        } else if (pattern.ptr[i] == '.' || pattern.ptr[i] == '[' || pattern.ptr[i] == ']' ||
                   pattern.ptr[i] == '{' || pattern.ptr[i] == '}' || pattern.ptr[i] == '(' ||
                   pattern.ptr[i] == ')' || pattern.ptr[i] == '\\' || pattern.ptr[i] == '*' ||
                   pattern.ptr[i] == '+' || pattern.ptr[i] == '?' || pattern.ptr[i] == '|' ||
                   pattern.ptr[i] == '^' || pattern.ptr[i] == '$') {
            // escape all regex special characters; see list at
            re_pattern->append("\\");
            re_pattern->append(1, pattern.ptr[i]);
            is_escaped = false;
        } else {
            // regular character or escaped special character
            re_pattern->append(1, pattern.ptr[i]);
            is_escaped = false;
        }
    }
}

void LikePredicate::remove_escape_character(std::string* search_string) {
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

} // namespace doris
