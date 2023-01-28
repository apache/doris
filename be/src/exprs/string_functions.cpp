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

} // namespace doris
