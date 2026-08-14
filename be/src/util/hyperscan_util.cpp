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

#include "util/hyperscan_util.h"

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <charconv>
#include <string>

namespace doris {
namespace {

bool is_larger_than_fifty(std::string_view str) {
    int number = 0;
    auto [_, error] = std::from_chars(str.data(), str.data() + str.size(), number);
    return error == std::errc() && number > 50;
}

std::string mask_escaped_characters_and_character_classes(std::string_view regexp) {
    std::string masked_regexp(regexp);
    bool escaped = false;
    bool in_character_class = false;
    bool character_class_can_close = false;
    bool character_class_can_negate = false;
    for (char& masked_character : masked_regexp) {
        const char current = masked_character;
        if (escaped) {
            masked_character = ' ';
            escaped = false;
            if (in_character_class) {
                character_class_can_close = true;
                character_class_can_negate = false;
            }
            continue;
        }
        if (current == '\\') {
            masked_character = ' ';
            escaped = true;
            continue;
        }
        if (in_character_class) {
            masked_character = ' ';
            if (current == ']' && character_class_can_close) {
                in_character_class = false;
            } else if (current == '^' && character_class_can_negate) {
                character_class_can_negate = false;
            } else {
                character_class_can_close = true;
                character_class_can_negate = false;
            }
            continue;
        }
        if (current == '[') {
            masked_character = ' ';
            in_character_class = true;
            character_class_can_close = false;
            character_class_can_negate = true;
        }
    }
    return masked_regexp;
}

class SlowWithHyperscanChecker {
public:
    SlowWithHyperscanChecker()
            : _searcher_one_repeat(R"(\{\s*([\d]+)\s*,?\s*})"),
              _searcher_two_repeats(R"(\{\s*([\d]+)\s*,\s*([\d]+)\s*\})") {}

    bool is_slow(std::string_view regexp) const {
        const std::string masked_regexp = mask_escaped_characters_and_character_classes(regexp);
        return is_slow_one_repeat(masked_regexp) || is_slow_two_repeats(masked_regexp);
    }

private:
    bool is_slow_one_repeat(std::string_view regexp) const {
        re2::StringPiece haystack(regexp.data(), regexp.size());
        re2::StringPiece matches[2];
        size_t start_pos = 0;
        while (start_pos < haystack.size()) {
            if (!_searcher_one_repeat.Match(haystack, start_pos, haystack.size(),
                                            re2::RE2::Anchor::UNANCHORED, matches, 2)) {
                break;
            }

            start_pos = matches[0].data() - haystack.data() + matches[0].size();
            if (is_larger_than_fifty({matches[1].data(), matches[1].size()})) {
                return true;
            }
        }
        return false;
    }

    bool is_slow_two_repeats(std::string_view regexp) const {
        re2::StringPiece haystack(regexp.data(), regexp.size());
        re2::StringPiece matches[3];
        size_t start_pos = 0;
        while (start_pos < haystack.size()) {
            if (!_searcher_two_repeats.Match(haystack, start_pos, haystack.size(),
                                             re2::RE2::Anchor::UNANCHORED, matches, 3)) {
                break;
            }

            start_pos = matches[0].data() - haystack.data() + matches[0].size();
            if (is_larger_than_fifty({matches[1].data(), matches[1].size()}) ||
                is_larger_than_fifty({matches[2].data(), matches[2].size()})) {
                return true;
            }
        }
        return false;
    }

    re2::RE2 _searcher_one_repeat;
    re2::RE2 _searcher_two_repeats;
};

} // namespace

bool is_hyperscan_regexp_expensive(std::string_view regexp) {
    static const SlowWithHyperscanChecker slow_with_hyperscan_checker;
    return slow_with_hyperscan_checker.is_slow(regexp);
}

} // namespace doris
