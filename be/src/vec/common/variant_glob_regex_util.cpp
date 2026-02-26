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

#include "vec/common/variant_glob_regex_util.h"

#include <cstddef>
#include <string>

#include "re2/re2.h"

namespace doris::vectorized::variant_util {
namespace {

inline void append_escaped_regex_char(std::string* regex_output, char ch) {
    switch (ch) {
    case '.':
    case '^':
    case '$':
    case '+':
    case '*':
    case '?':
    case '(':
    case ')':
    case '|':
    case '{':
    case '}':
    case '[':
    case ']':
    case '\\':
        regex_output->push_back('\\');
        regex_output->push_back(ch);
        break;
    default:
        regex_output->push_back(ch);
        break;
    }
}

} // namespace

Status glob_to_regex(const std::string& glob_pattern, std::string* regex_pattern) {
    regex_pattern->clear();
    regex_pattern->append("^");
    bool is_escaped = false;
    size_t pattern_length = glob_pattern.size();
    for (size_t index = 0; index < pattern_length; ++index) {
        char current_char = glob_pattern[index];
        if (is_escaped) {
            append_escaped_regex_char(regex_pattern, current_char);
            is_escaped = false;
            continue;
        }
        if (current_char == '\\') {
            is_escaped = true;
            continue;
        }
        if (current_char == '*') {
            regex_pattern->append(".*");
            continue;
        }
        if (current_char == '?') {
            regex_pattern->append(".");
            continue;
        }
        if (current_char == '[') {
            size_t class_index = index + 1;
            bool class_closed = false;
            bool is_class_escaped = false;
            std::string class_buffer;
            if (class_index < pattern_length &&
                (glob_pattern[class_index] == '!' || glob_pattern[class_index] == '^')) {
                class_buffer.push_back('^');
                ++class_index;
            }
            for (; class_index < pattern_length; ++class_index) {
                char class_char = glob_pattern[class_index];
                if (is_class_escaped) {
                    class_buffer.push_back(class_char);
                    is_class_escaped = false;
                    continue;
                }
                if (class_char == '\\') {
                    is_class_escaped = true;
                    continue;
                }
                if (class_char == ']') {
                    class_closed = true;
                    break;
                }
                class_buffer.push_back(class_char);
            }
            if (!class_closed) {
                return Status::InvalidArgument("Unclosed character class in glob pattern: {}",
                                               glob_pattern);
            }
            regex_pattern->append("[");
            regex_pattern->append(class_buffer);
            regex_pattern->append("]");
            index = class_index;
            continue;
        }
        append_escaped_regex_char(regex_pattern, current_char);
    }
    if (is_escaped) {
        append_escaped_regex_char(regex_pattern, '\\');
    }
    regex_pattern->append("$");
    return Status::OK();
}

bool glob_match_re2(const std::string& glob_pattern, const std::string& candidate_path) {
    std::string regex_pattern;
    auto st = glob_to_regex(glob_pattern, &regex_pattern);
    if (!st.ok()) {
        return false;
    }

    RE2 re2(regex_pattern);
    if (!re2.ok()) {
        return false;
    }
    return RE2::FullMatch(candidate_path, re2);
}

} // namespace doris::vectorized::variant_util
