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

namespace doris {
namespace {

bool is_larger_than_fifty(std::string_view str) {
    int number = 0;
    auto [_, error] = std::from_chars(str.data(), str.data() + str.size(), number);
    return error == std::errc() && number > 50;
}

class SlowWithHyperscanChecker {
public:
    SlowWithHyperscanChecker()
            : _searcher_one_repeat(R"(\{\s*([\d]+)\s*,?\s*})"),
              _searcher_two_repeats(R"(\{\s*([\d]+)\s*,\s*([\d]+)\s*\})") {}

    bool is_slow(std::string_view regexp) const {
        return is_slow_one_repeat(regexp) || is_slow_two_repeats(regexp);
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
