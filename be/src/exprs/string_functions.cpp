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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/string-functions.cpp
// and modified by Doris

#include "exprs/string_functions.h"

#include <re2/re2.h>

#include <algorithm>

#include "exprs/anyval_util.h"
#include "math_functions.h"
#include "util/simd/vstring_function.h"
#include "util/url_parser.h"

// NOTE: be careful not to use string::append.  It is not performant.
namespace doris {

void StringFunctions::init() {}

size_t get_char_len(const StringVal& str, std::vector<size_t>* str_index) {
    size_t char_len = 0;
    for (size_t i = 0, char_size = 0; i < str.len; i += char_size) {
        char_size = get_utf8_byte_length((unsigned)(str.ptr)[i]);
        str_index->push_back(i);
        ++char_len;
    }
    return char_len;
}

// This behaves identically to the mysql implementation, namely:
//  - 1-indexed positions
//  - supported negative positions (count from the end of the string)
//  - [optional] len.  No len indicates longest substr possible
StringVal StringFunctions::substring(FunctionContext* context, const StringVal& str,
                                     const IntVal& pos, const IntVal& len) {
    if (str.is_null || pos.is_null || len.is_null) {
        return StringVal::null();
    }
    if (len.val <= 0 || str.len == 0 || pos.val == 0 || pos.val > str.len) {
        return StringVal();
    }

    // create index indicate every char start byte
    // e.g.  "hello word 你好" => [0,1,2,3,4,5,6,7,8,9,10,11,14] 你 and 好 are 3 bytes
    // why use a vector as index? It is unnecessary if there is no negative pos val,
    // but if has pos is negative it is not easy to determine where to start, so need a
    // index save every character's length
    size_t byte_pos = 0;
    std::vector<size_t> index;
    for (size_t i = 0, char_size = 0; i < str.len; i += char_size) {
        char_size = get_utf8_byte_length((unsigned)(str.ptr)[i]);
        index.push_back(i);
        if (pos.val > 0 && index.size() > pos.val + len.val) {
            break;
        }
    }

    int fixed_pos = pos.val;
    if (fixed_pos < 0) {
        fixed_pos = index.size() + fixed_pos + 1;
    }
    if (fixed_pos > index.size()) {
        return StringVal::null();
    }
    byte_pos = index[fixed_pos - 1];
    int fixed_len = str.len - byte_pos;
    if (fixed_pos + len.val <= index.size()) {
        fixed_len = index[fixed_pos + len.val - 1] - byte_pos;
    }
    if (byte_pos <= str.len && fixed_len > 0) {
        return StringVal(str.ptr + byte_pos, fixed_len);
    } else {
        return StringVal();
    }
}

StringVal StringFunctions::substring(FunctionContext* context, const StringVal& str,
                                     const IntVal& pos) {
    // StringVal.len is an int => INT32_MAX
    return substring(context, str, pos, IntVal(INT32_MAX));
}

// Implementation of Left.  The signature is
//    string left(string input, int len)
// This behaves identically to the mysql implementation.
StringVal StringFunctions::left(FunctionContext* context, const StringVal& str, const IntVal& len) {
    if (len.val >= str.len) return str;
    return substring(context, str, 1, len);
}

// Implementation of Right.  The signature is
//    string right(string input, int len)
// This behaves identically to the mysql implementation.
StringVal StringFunctions::right(FunctionContext* context, const StringVal& str,
                                 const IntVal& len) {
    // Don't index past the beginning of str, otherwise we'll get an empty string back
    int32_t pos = std::max(-len.val, static_cast<int32_t>(-str.len));
    return substring(context, str, IntVal(pos), len);
}

BooleanVal StringFunctions::starts_with(FunctionContext* context, const StringVal& str,
                                        const StringVal& prefix) {
    if (str.is_null || prefix.is_null) {
        return BooleanVal::null();
    }
    re2::StringPiece str_sp(reinterpret_cast<char*>(str.ptr), str.len);
    re2::StringPiece prefix_sp(reinterpret_cast<char*>(prefix.ptr), prefix.len);
    return BooleanVal(str_sp.starts_with(prefix_sp));
}

BooleanVal StringFunctions::ends_with(FunctionContext* context, const StringVal& str,
                                      const StringVal& suffix) {
    if (str.is_null || suffix.is_null) {
        return BooleanVal::null();
    }
    re2::StringPiece str_sp(reinterpret_cast<char*>(str.ptr), str.len);
    re2::StringPiece suffix_sp(reinterpret_cast<char*>(suffix.ptr), suffix.len);
    return BooleanVal(str_sp.ends_with(suffix_sp));
}

BooleanVal StringFunctions::null_or_empty(FunctionContext* context, const StringVal& str) {
    if (str.is_null || str.len == 0) {
        return 1;
    } else {
        return 0;
    }
}

StringVal StringFunctions::space(FunctionContext* context, const IntVal& len) {
    if (len.is_null) {
        return StringVal::null();
    }
    if (len.val <= 0) {
        return StringVal();
    }
    int32_t space_size = std::min(len.val, 65535);
    // TODO pengyubing
    // StringVal result = StringVal::create_temp_string_val(context, space_size);
    StringVal result(context, space_size);
    memset(result.ptr, ' ', space_size);
    return result;
}

StringVal StringFunctions::repeat(FunctionContext* context, const StringVal& str, const IntVal& n) {
    if (str.is_null || n.is_null) {
        return StringVal::null();
    }
    if (str.len == 0 || n.val <= 0) {
        return StringVal();
    }

    // TODO pengyubing
    // StringVal result = StringVal::create_temp_string_val(context, str.len * n.val);
    StringVal result(context, str.len * n.val);
    if (UNLIKELY(result.is_null)) {
        return result;
    }
    uint8_t* ptr = result.ptr;
    for (int64_t i = 0; i < n.val; ++i) {
        memcpy(ptr, str.ptr, str.len);
        ptr += str.len;
    }
    return result;
}

StringVal StringFunctions::lpad(FunctionContext* context, const StringVal& str, const IntVal& len,
                                const StringVal& pad) {
    if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
        return StringVal::null();
    }

    std::vector<size_t> str_index;
    size_t str_char_size = get_char_len(str, &str_index);
    std::vector<size_t> pad_index;
    size_t pad_char_size = get_char_len(pad, &pad_index);

    // Corner cases: Shrink the original string, or leave it alone.
    // TODO: Hive seems to go into an infinite loop if pad.len == 0,
    // so we should pay attention to Hive's future solution to be compatible.
    if (len.val <= str_char_size || pad.len == 0) {
        if (len.val > str_index.size()) {
            return StringVal::null();
        }
        if (len.val == str_index.size()) {
            return StringVal(str.ptr, str.len);
        }
        return StringVal(str.ptr, str_index[len.val]);
    }

    // TODO pengyubing
    // StringVal result = StringVal::create_temp_string_val(context, len.val);
    int32_t pad_byte_len = 0;
    int32_t pad_times = (len.val - str_char_size) / pad_char_size;
    int32_t pad_remainder = (len.val - str_char_size) % pad_char_size;
    pad_byte_len = pad_times * pad.len;
    pad_byte_len += pad_index[pad_remainder];
    int32_t byte_len = str.len + pad_byte_len;
    StringVal result(context, byte_len);
    if (result.is_null) {
        return result;
    }
    int pad_idx = 0;
    int result_index = 0;
    uint8_t* ptr = result.ptr;

    // Prepend chars of pad.
    while (result_index < pad_byte_len) {
        ptr[result_index++] = pad.ptr[pad_idx++];
        pad_idx = pad_idx % pad.len;
    }

    // Append given string.
    memcpy(ptr + result_index, str.ptr, str.len);
    return result;
}

StringVal StringFunctions::rpad(FunctionContext* context, const StringVal& str, const IntVal& len,
                                const StringVal& pad) {
    if (str.is_null || len.is_null || pad.is_null || len.val < 0) {
        return StringVal::null();
    }

    std::vector<size_t> str_index;
    size_t str_char_size = get_char_len(str, &str_index);
    std::vector<size_t> pad_index;
    size_t pad_char_size = get_char_len(pad, &pad_index);

    // Corner cases: Shrink the original string, or leave it alone.
    // TODO: Hive seems to go into an infinite loop if pad->len == 0,
    // so we should pay attention to Hive's future solution to be compatible.
    if (len.val <= str_char_size || pad.len == 0) {
        if (len.val > str_index.size()) {
            return StringVal::null();
        }
        if (len.val == str_index.size()) {
            return StringVal(str.ptr, str.len);
        }
        return StringVal(str.ptr, str_index[len.val]);
    }

    // TODO pengyubing
    // StringVal result = StringVal::create_temp_string_val(context, len.val);
    int32_t pad_byte_len = 0;
    int32_t pad_times = (len.val - str_char_size) / pad_char_size;
    int32_t pad_remainder = (len.val - str_char_size) % pad_char_size;
    pad_byte_len = pad_times * pad.len;
    pad_byte_len += pad_index[pad_remainder];
    int32_t byte_len = str.len + pad_byte_len;
    StringVal result(context, byte_len);
    if (UNLIKELY(result.is_null)) {
        return result;
    }
    memcpy(result.ptr, str.ptr, str.len);

    // Append chars of pad until desired length
    uint8_t* ptr = result.ptr;
    int pad_idx = 0;
    int result_len = str.len;
    while (result_len < byte_len) {
        ptr[result_len++] = pad.ptr[pad_idx++];
        pad_idx = pad_idx % pad.len;
    }
    return result;
}

StringVal StringFunctions::append_trailing_char_if_absent(
        doris_udf::FunctionContext* context, const doris_udf::StringVal& str,
        const doris_udf::StringVal& trailing_char) {
    if (str.is_null || trailing_char.is_null || trailing_char.len != 1) {
        return StringVal::null();
    }
    if (str.len == 0) {
        return trailing_char;
    }
    if (str.ptr[str.len - 1] == trailing_char.ptr[0]) {
        return str;
    }

    StringVal result(context, str.len + 1);
    memcpy(result.ptr, str.ptr, str.len);
    result.ptr[str.len] = trailing_char.ptr[0];
    return result;
}

// Implementation of LENGTH
//   int length(string input)
// Returns the length in bytes of input. If input == nullptr, returns
// nullptr per MySQL
IntVal StringFunctions::length(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return IntVal::null();
    }
    return IntVal(str.len);
}

// Implementation of CHAR_LENGTH
//   int char_utf8_length(string input)
// Returns the length of characters of input. If input == nullptr, returns
// nullptr per MySQL
IntVal StringFunctions::char_utf8_length(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return IntVal::null();
    }
    size_t char_len = 0;
    for (size_t i = 0, char_size = 0; i < str.len; i += char_size) {
        char_size = get_utf8_byte_length((unsigned)(str.ptr)[i]);
        ++char_len;
    }
    return IntVal(char_len);
}

StringVal StringFunctions::lower(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return StringVal::null();
    }
    StringVal result(context, str.len);
    if (UNLIKELY(result.is_null)) {
        return result;
    }
    simd::VStringFunctions::to_lower(str.ptr, str.len, result.ptr);
    return result;
}

StringVal StringFunctions::upper(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return StringVal::null();
    }
    StringVal result(context, str.len);
    if (UNLIKELY(result.is_null)) {
        return result;
    }
    simd::VStringFunctions::to_upper(str.ptr, str.len, result.ptr);
    return result;
}

StringVal StringFunctions::initcap(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return StringVal::null();
    }
    StringVal result(context, str.len);

    simd::VStringFunctions::to_lower(str.ptr, str.len, result.ptr);

    bool need_capitalize = true;
    for (int64_t i = 0; i < str.len; ++i) {
        if (!::isalnum(result.ptr[i])) {
            need_capitalize = true;
        } else if (need_capitalize) {
            result.ptr[i] = ::toupper(result.ptr[i]);
            need_capitalize = false;
        }
    }

    return result;
}

StringVal StringFunctions::reverse(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return StringVal::null();
    }

    StringVal result(context, str.len);
    if (UNLIKELY(result.is_null)) {
        return result;
    }

    simd::VStringFunctions::reverse(str, result);
    return result;
}

StringVal StringFunctions::trim(FunctionContext* context, const StringVal& str) {
    return simd::VStringFunctions::trim(str);
}

StringVal StringFunctions::ltrim(FunctionContext* context, const StringVal& str) {
    return simd::VStringFunctions::ltrim(str);
}

StringVal StringFunctions::rtrim(FunctionContext* context, const StringVal& str) {
    return simd::VStringFunctions::rtrim(str);
}

IntVal StringFunctions::ascii(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return IntVal::null();
    }
    // Hive returns 0 when given an empty string.
    return IntVal((str.len == 0) ? 0 : static_cast<int32_t>(str.ptr[0]));
}

IntVal StringFunctions::instr(FunctionContext* context, const StringVal& str,
                              const StringVal& substr) {
    if (str.is_null || substr.is_null) {
        return IntVal::null();
    }
    if (substr.len == 0) {
        return IntVal(1);
    }
    StringValue str_sv = StringValue::from_string_val(str);
    StringValue substr_sv = StringValue::from_string_val(substr);
    StringSearch search(&substr_sv);
    // Hive returns positions starting from 1.
    int loc = search.search(&str_sv);
    if (loc > 0) {
        size_t char_len = 0;
        for (size_t i = 0, char_size = 0; i < loc; i += char_size) {
            char_size = get_utf8_byte_length((unsigned)(str.ptr)[i]);
            ++char_len;
        }
        loc = char_len;
    }

    return IntVal(loc + 1);
}

IntVal StringFunctions::locate(FunctionContext* context, const StringVal& substr,
                               const StringVal& str) {
    return instr(context, str, substr);
}

IntVal StringFunctions::locate_pos(FunctionContext* context, const StringVal& substr,
                                   const StringVal& str, const IntVal& start_pos) {
    if (str.is_null || substr.is_null || start_pos.is_null) {
        return IntVal::null();
    }
    if (substr.len == 0) {
        if (start_pos.val <= 0) {
            return IntVal(0);
        } else if (start_pos.val == 1) {
            return IntVal(1);
        } else if (start_pos.val > str.len) {
            return IntVal(0);
        } else {
            return IntVal(start_pos.val);
        }
    }
    // Hive returns 0 for *start_pos <= 0,
    // but throws an exception for *start_pos > str->len.
    // Since returning 0 seems to be Hive's error condition, return 0.
    std::vector<size_t> index;
    size_t char_len = get_char_len(str, &index);
    if (start_pos.val <= 0 || start_pos.val > str.len || start_pos.val > char_len) {
        return IntVal(0);
    }
    StringValue substr_sv = StringValue::from_string_val(substr);
    StringSearch search(&substr_sv);
    // Input start_pos.val starts from 1.
    StringValue adjusted_str(reinterpret_cast<char*>(str.ptr) + index[start_pos.val - 1],
                             str.len - index[start_pos.val - 1]);
    int32_t match_pos = search.search(&adjusted_str);
    if (match_pos >= 0) {
        // Hive returns the position in the original string starting from 1.
        size_t char_len = 0;
        for (size_t i = 0, char_size = 0; i < match_pos; i += char_size) {
            char_size = get_utf8_byte_length((unsigned)(adjusted_str.ptr)[i]);
            ++char_len;
        }
        match_pos = char_len;
        return IntVal(start_pos.val + match_pos);
    } else {
        return IntVal(0);
    }
}

// This function sets options in the RE2 library before pattern matching.
bool StringFunctions::set_re2_options(const StringVal& match_parameter, std::string* error_str,
                                      re2::RE2::Options* opts) {
    for (int i = 0; i < match_parameter.len; i++) {
        char match = match_parameter.ptr[i];
        switch (match) {
        case 'i':
            opts->set_case_sensitive(false);
            break;
        case 'c':
            opts->set_case_sensitive(true);
            break;
        case 'm':
            opts->set_posix_syntax(true);
            opts->set_one_line(false);
            break;
        case 'n':
            opts->set_never_nl(false);
            opts->set_dot_nl(true);
            break;
        default:
            std::stringstream error;
            error << "Illegal match parameter " << match;
            *error_str = error.str();
            return false;
        }
    }
    return true;
}

// The caller owns the returned regex. Returns nullptr if the pattern could not be compiled.
re2::RE2* StringFunctions::compile_regex(const StringVal& pattern, std::string* error_str,
                                         const StringVal& match_parameter) {
    re2::StringPiece pattern_sp(reinterpret_cast<char*>(pattern.ptr), pattern.len);
    re2::RE2::Options options;
    // Disable error logging in case e.g. every row causes an error
    options.set_log_errors(false);
    // ATTN(cmy): no set it, or the lazy mode of regex won't work. See Doris #6587
    // Return the leftmost longest match (rather than the first match).
    // options.set_longest_match(true);
    options.set_dot_nl(true);
    if (!match_parameter.is_null &&
        !StringFunctions::set_re2_options(match_parameter, error_str, &options)) {
        return nullptr;
    }
    re2::RE2* re = new re2::RE2(pattern_sp, options);
    if (!re->ok()) {
        std::stringstream ss;
        ss << "Could not compile regexp pattern: " << AnyValUtil::to_string(pattern) << std::endl
           << "Error: " << re->error();
        *error_str = ss.str();
        delete re;
        return nullptr;
    }
    return re;
}

void StringFunctions::regexp_prepare(FunctionContext* context,
                                     FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    if (!context->is_arg_constant(1)) {
        return;
    }
    StringVal* pattern = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
    if (pattern->is_null) {
        return;
    }
    std::string error_str;
    re2::RE2* re = compile_regex(*pattern, &error_str, StringVal::null());
    if (re == nullptr) {
        context->set_error(error_str.c_str());
        return;
    }
    context->set_function_state(scope, re);
}

void StringFunctions::regexp_close(FunctionContext* context,
                                   FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    re2::RE2* re = reinterpret_cast<re2::RE2*>(context->get_function_state(scope));
    delete re;
}

StringVal StringFunctions::regexp_extract(FunctionContext* context, const StringVal& str,
                                          const StringVal& pattern, const BigIntVal& index) {
    if (str.is_null || pattern.is_null || index.is_null) {
        return StringVal::null();
    }
    if (index.val < 0) {
        return StringVal();
    }

    re2::RE2* re = reinterpret_cast<re2::RE2*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::unique_ptr<re2::RE2> scoped_re; // destroys re if we have to locally compile it
    if (re == nullptr) {
        DCHECK(!context->is_arg_constant(1));
        std::string error_str;
        re = compile_regex(pattern, &error_str, StringVal::null());
        if (re == nullptr) {
            context->add_warning(error_str.c_str());
            return StringVal::null();
        }
        scoped_re.reset(re);
    }

    re2::StringPiece str_sp(reinterpret_cast<char*>(str.ptr), str.len);
    int max_matches = 1 + re->NumberOfCapturingGroups();
    if (index.val >= max_matches) {
        return StringVal();
    }
    // Use a vector because clang complains about non-POD varlen arrays
    // TODO: fix this
    std::vector<re2::StringPiece> matches(max_matches);
    bool success = re->Match(str_sp, 0, str.len, re2::RE2::UNANCHORED, &matches[0], max_matches);
    if (!success) {
        return StringVal();
    }
    // matches[0] is the whole string, matches[1] the first group, etc.
    const re2::StringPiece& match = matches[index.val];
    return AnyValUtil::from_buffer_temp(context, match.data(), match.size());
}

StringVal StringFunctions::regexp_replace(FunctionContext* context, const StringVal& str,
                                          const StringVal& pattern, const StringVal& replace) {
    if (str.is_null || pattern.is_null || replace.is_null) {
        return StringVal::null();
    }

    re2::RE2* re = reinterpret_cast<re2::RE2*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::unique_ptr<re2::RE2> scoped_re; // destroys re if state->re is nullptr
    if (re == nullptr) {
        DCHECK(!context->is_arg_constant(1));
        std::string error_str;
        re = compile_regex(pattern, &error_str, StringVal::null());
        if (re == nullptr) {
            context->add_warning(error_str.c_str());
            return StringVal::null();
        }
        scoped_re.reset(re);
    }

    re2::StringPiece replace_str =
            re2::StringPiece(reinterpret_cast<char*>(replace.ptr), replace.len);
    std::string result_str = AnyValUtil::to_string(str);
    re2::RE2::GlobalReplace(&result_str, *re, replace_str);
    return AnyValUtil::from_string_temp(context, result_str);
}

StringVal StringFunctions::concat(FunctionContext* context, int num_children,
                                  const StringVal* strs) {
    DCHECK_GE(num_children, 1);

    // Pass through if there's only one argument
    if (num_children == 1) {
        return strs[0];
    }

    // Loop once to compute the final size and reserve space.
    int32_t total_size = 0;
    for (int32_t i = 0; i < num_children; ++i) {
        if (strs[i].is_null) {
            return StringVal::null();
        }
        total_size += strs[i].len;
    }

    StringVal result(context, total_size);
    uint8_t* ptr = result.ptr;

    // Loop again to append the data.
    for (int32_t i = 0; i < num_children; ++i) {
        memcpy(ptr, strs[i].ptr, strs[i].len);
        ptr += strs[i].len;
    }
    return result;
}

StringVal StringFunctions::concat_ws(FunctionContext* context, const StringVal& sep,
                                     int num_children, const StringVal* strs) {
    DCHECK_GE(num_children, 1);
    if (sep.is_null) {
        return StringVal::null();
    }

    // Loop once to compute the final size and reserve space.
    int32_t total_size = 0;
    bool not_first = false;
    for (int32_t i = 0; i < num_children; ++i) {
        if (strs[i].is_null) {
            continue;
        }
        if (not_first) {
            total_size += sep.len;
        }
        total_size += strs[i].len;
        not_first = true;
    }

    StringVal result(context, total_size);
    uint8_t* ptr = result.ptr;
    not_first = false;
    // Loop again to append the data.
    for (int32_t i = 0; i < num_children; ++i) {
        if (strs[i].is_null) {
            continue;
        }
        if (not_first) {
            memcpy(ptr, sep.ptr, sep.len);
            ptr += sep.len;
        }
        memcpy(ptr, strs[i].ptr, strs[i].len);
        ptr += strs[i].len;
        not_first = true;
    }
    return result;
}

StringVal StringFunctions::elt(FunctionContext* context, const IntVal& pos, int num_children,
                               const StringVal* strs) {
    if (pos.is_null || pos.val < 1 || num_children == 0 || pos.val > num_children) {
        return StringVal::null();
    }

    return strs[pos.val - 1];
}

IntVal StringFunctions::find_in_set(FunctionContext* context, const StringVal& str,
                                    const StringVal& str_set) {
    if (str.is_null || str_set.is_null) {
        return IntVal::null();
    }
    // Check str for commas.
    for (int i = 0; i < str.len; ++i) {
        if (str.ptr[i] == ',') {
            return IntVal(0);
        }
    }
    // The result index starts from 1 since 0 is an error condition.
    int32_t token_index = 1;
    int32_t start = 0;
    int32_t end;
    StringValue str_sv = StringValue::from_string_val(str);
    do {
        end = start;
        // Position end.
        while (end < str_set.len && str_set.ptr[end] != ',') {
            ++end;
        }
        StringValue token(reinterpret_cast<char*>(str_set.ptr) + start, end - start);
        if (str_sv.eq(token)) {
            return IntVal(token_index);
        }

        // Re-position start and end past ','
        start = end + 1;
        ++token_index;
    } while (start < str_set.len);
    return IntVal(0);
}

void StringFunctions::parse_url_prepare(FunctionContext* ctx,
                                        FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    if (!ctx->is_arg_constant(1)) {
        return;
    }
    StringVal* part = reinterpret_cast<StringVal*>(ctx->get_constant_arg(1));
    if (part->is_null) {
        return;
    }
    UrlParser::UrlPart* url_part = new UrlParser::UrlPart;
    *url_part = UrlParser::get_url_part(StringValue::from_string_val(*part));
    if (*url_part == UrlParser::INVALID) {
        std::stringstream ss;
        ss << "Invalid URL part: " << AnyValUtil::to_string(*part) << std::endl
           << "(Valid URL parts are 'PROTOCOL', 'HOST', 'PATH', 'REF', 'AUTHORITY', 'FILE', "
           << "'USERINFO', 'PORT' and 'QUERY')";
        ctx->set_error(ss.str().c_str());
        return;
    }
    ctx->set_function_state(scope, url_part);
}

StringVal StringFunctions::parse_url(FunctionContext* ctx, const StringVal& url,
                                     const StringVal& part) {
    if (url.is_null || part.is_null) {
        return StringVal::null();
    }
    std::string part_str = std::string(reinterpret_cast<const char*>(part.ptr), part.len);
    transform(part_str.begin(), part_str.end(), part_str.begin(), ::toupper);
    StringVal newPart = AnyValUtil::from_string_temp(ctx, part_str);
    void* state = ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    UrlParser::UrlPart url_part;
    if (state != nullptr) {
        url_part = *reinterpret_cast<UrlParser::UrlPart*>(state);
    } else {
        DCHECK(!ctx->is_arg_constant(1));
        url_part = UrlParser::get_url_part(StringValue::from_string_val(newPart));
    }

    StringValue result;
    if (!UrlParser::parse_url(StringValue::from_string_val(url), url_part, &result)) {
        // url is malformed, or url_part is invalid.
        if (url_part == UrlParser::INVALID) {
            std::stringstream ss;
            ss << "Invalid URL part: " << AnyValUtil::to_string(newPart);
            ctx->add_warning(ss.str().c_str());
        } else {
            std::stringstream ss;
            ss << "Could not parse URL: " << AnyValUtil::to_string(url);
            ctx->add_warning(ss.str().c_str());
        }
        return StringVal::null();
    }
    StringVal result_sv;
    result.to_string_val(&result_sv);
    return result_sv;
}

void StringFunctions::parse_url_close(FunctionContext* ctx,
                                      FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }
    UrlParser::UrlPart* url_part =
            reinterpret_cast<UrlParser::UrlPart*>(ctx->get_function_state(scope));
    delete url_part;
}

StringVal StringFunctions::parse_url_key(FunctionContext* ctx, const StringVal& url,
                                         const StringVal& part, const StringVal& key) {
    if (url.is_null || part.is_null || key.is_null) {
        return StringVal::null();
    }
    void* state = ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    UrlParser::UrlPart url_part;
    if (state != nullptr) {
        url_part = *reinterpret_cast<UrlParser::UrlPart*>(state);
    } else {
        DCHECK(!ctx->is_arg_constant(1));
        url_part = UrlParser::get_url_part(StringValue::from_string_val(part));
    }

    StringValue result;
    if (!UrlParser::parse_url_key(StringValue::from_string_val(url), url_part,
                                  StringValue::from_string_val(key), &result)) {
        // url is malformed, or url_part is invalid.
        if (url_part == UrlParser::INVALID) {
            std::stringstream ss;
            ss << "Invalid URL part: " << AnyValUtil::to_string(part);
            ctx->add_warning(ss.str().c_str());
        } else {
            std::stringstream ss;
            ss << "Could not parse URL: " << AnyValUtil::to_string(url);
            ctx->add_warning(ss.str().c_str());
        }
        return StringVal::null();
    }
    StringVal result_sv;
    result.to_string_val(&result_sv);
    return result_sv;
}

StringVal StringFunctions::money_format(FunctionContext* context, const DoubleVal& v) {
    if (v.is_null) {
        return StringVal::null();
    }
    double v_cent = MathFunctions::my_double_round(v.val, 2, false, false);
    return do_money_format(context, fmt::format("{:.2f}", v_cent));
}

StringVal StringFunctions::money_format(FunctionContext* context, const DecimalV2Val& v) {
    if (v.is_null) {
        return StringVal::null();
    }

    DecimalV2Value rounded(0);
    DecimalV2Value::from_decimal_val(v).round(&rounded, 2, HALF_UP);
    return do_money_format<int64_t, 26>(context, rounded.int_value(),
                                        abs(rounded.frac_value() / 10000000));
}

StringVal StringFunctions::money_format(FunctionContext* context, const BigIntVal& v) {
    if (v.is_null) {
        return StringVal::null();
    }
    return do_money_format<int64_t, 26>(context, v.val);
}

StringVal StringFunctions::money_format(FunctionContext* context, const LargeIntVal& v) {
    if (v.is_null) {
        return StringVal::null();
    }
    return do_money_format<__int128_t, 52>(context, v.val);
}

static int index_of(const uint8_t* source, int source_offset, int source_count,
                    const uint8_t* target, int target_offset, int target_count, int from_index) {
    if (from_index >= source_count) {
        return (target_count == 0 ? source_count : -1);
    }
    if (from_index < 0) {
        from_index = 0;
    }
    if (target_count == 0) {
        return from_index;
    }
    const uint8_t first = target[target_offset];
    int max = source_offset + (source_count - target_count);
    for (int i = source_offset + from_index; i <= max; i++) {
        while (i <= max && source[i] != first) i++; // Look for first character
        if (i <= max) { // Found first character, now look at the rest of v2
            int j = i + 1;
            int end = j + target_count - 1;
            for (int k = target_offset + 1; j < end && source[j] == target[k]; j++, k++)
                ;
            if (j == end) {
                return i - source_offset; // Found whole string.
            }
        }
    }
    return -1;
}

StringVal StringFunctions::split_part(FunctionContext* context, const StringVal& content,
                                      const StringVal& delimiter, const IntVal& field) {
    if (content.is_null || delimiter.is_null || field.is_null || field.val <= 0) {
        return StringVal::null();
    }
    std::vector<int> find(field.val, -1); //store substring position
    int from = 0;
    for (int i = 1; i <= field.val; i++) { // find
        int last_index = i - 1;
        find[last_index] =
                index_of(content.ptr, 0, content.len, delimiter.ptr, 0, delimiter.len, from);
        from = find[last_index] + delimiter.len;
        if (find[last_index] == -1) {
            break;
        }
    }
    if ((field.val > 1 && find[field.val - 2] == -1) ||
        (field.val == 1 && find[field.val - 1] == -1)) {
        // field not find return null
        return StringVal::null();
    }
    int start_pos;
    if (field.val == 1) { // find need split first part
        start_pos = 0;
    } else {
        start_pos = find[field.val - 2] + delimiter.len;
    }
    int len = (find[field.val - 1] == -1 ? content.len : find[field.val - 1]) - start_pos;
    return StringVal(content.ptr + start_pos, len);
}

StringVal StringFunctions::replace(FunctionContext* context, const StringVal& origStr,
                                   const StringVal& oldStr, const StringVal& newStr) {
    if (origStr.is_null || oldStr.is_null || newStr.is_null) {
        return StringVal::null();
    }
    // Empty string is a substring of all strings.
    // If old str is an empty string, the std::string.find(oldStr) is always return 0.
    // With an empty old str, there is no need to do replace.
    if (oldStr.len == 0) {
        return origStr;
    }
    std::string orig_str = std::string(reinterpret_cast<const char*>(origStr.ptr), origStr.len);
    std::string old_str = std::string(reinterpret_cast<const char*>(oldStr.ptr), oldStr.len);
    std::string new_str = std::string(reinterpret_cast<const char*>(newStr.ptr), newStr.len);
    std::string::size_type pos = 0;
    std::string::size_type oldLen = old_str.size();
    std::string::size_type newLen = new_str.size();
    while ((pos = orig_str.find(old_str, pos)) != std::string::npos) {
        orig_str.replace(pos, oldLen, new_str);
        pos += newLen;
    }
    return AnyValUtil::from_string_temp(context, orig_str);
}
// Implementation of BIT_LENGTH
//   int bit_length(string input)
// Returns the length in bits of input. If input == nullptr, returns
// nullptr per MySQL
IntVal StringFunctions::bit_length(FunctionContext* context, const StringVal& str) {
    if (str.is_null) {
        return IntVal::null();
    }
    return IntVal(str.len * 8);
}
} // namespace doris
