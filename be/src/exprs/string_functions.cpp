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
#include <re2/stringpiece.h>

#include <sstream>

#include "util/string_util.h"

// NOTE: be careful not to use string::append.  It is not performant.
namespace doris {

// This function sets options in the RE2 library before pattern matching.
bool StringFunctions::set_re2_options(const StringRef& match_parameter, std::string* error_str,
                                      re2::RE2::Options* opts) {
    for (int i = 0; i < match_parameter.size; i++) {
        char match = match_parameter.data[i];
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
bool StringFunctions::compile_regex(const StringRef& pattern, std::string* error_str,
                                    const StringRef& match_parameter,
                                    const StringRef& options_value, std::unique_ptr<re2::RE2>& re) {
    re2::StringPiece pattern_sp(pattern.data, pattern.size);
    re2::RE2::Options options;
    // Disable error logging in case e.g. every row causes an error
    options.set_log_errors(false);
    // ATTN(cmy): no set it, or the lazy mode of regex won't work. See Doris #6587
    // Return the leftmost longest match (rather than the first match).
    // options.set_longest_match(true);
    options.set_dot_nl(true);

    if ((options_value.data != nullptr) && (options_value.size > 0)) {
        auto options_split = split(options_value.to_string(), ",");
        for (const auto& option : options_split) {
            if (iequal("ignore_invalid_escape", option)) {
                options.set_ignore_replace_escape(true);
            } else {
                // "none" do nothing, and could add more options for future extensibility.
            }
        }
    }

    if (match_parameter.size > 0 &&
        !StringFunctions::set_re2_options(match_parameter, error_str, &options)) {
        return false;
    }
    re.reset(new re2::RE2(pattern_sp, options));
    if (!re->ok()) {
        std::stringstream ss;
        ss << "Could not compile regexp pattern: " << std::string(pattern.data, pattern.size)
           << std::endl
           << "Error: " << re->error();
        *error_str = ss.str();
        re.reset();
        return false;
    }
    return true;
}

} // namespace doris
