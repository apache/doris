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

#pragma once

#include <unicode/uniset.h>
#include <unicode/utext.h>

#include <algorithm>
#include <memory>
#include <vector>

namespace doris::segment_v2::inverted_index {

class CharMatcher {
public:
    CharMatcher() = default;
    virtual ~CharMatcher() = default;

    virtual bool is_token_char(UChar32 c) = 0;
};
using CharMatcherPtr = std::shared_ptr<CharMatcher>;

class BasicCharMatcher : public CharMatcher {
public:
    enum Type { LETTER, DIGIT, WHITESPACE, PUNCTUATION, SYMBOL };

    explicit BasicCharMatcher(Type type) : _type(type) {}

    bool is_token_char(UChar32 c) override {
        switch (_type) {
        case Type::LETTER:
            return u_isalpha(c);
        case Type::DIGIT:
            return u_isdigit(c);
        case Type::WHITESPACE:
            return u_isWhitespace(c);
        case Type::PUNCTUATION: {
            auto charType = u_charType(c);
            return (charType == U_START_PUNCTUATION || charType == U_END_PUNCTUATION ||
                    charType == U_OTHER_PUNCTUATION || charType == U_CONNECTOR_PUNCTUATION ||
                    charType == U_DASH_PUNCTUATION || charType == U_INITIAL_PUNCTUATION ||
                    charType == U_FINAL_PUNCTUATION);
        }
        case Type::SYMBOL: {
            auto charType = u_charType(c);
            return (charType == U_CURRENCY_SYMBOL || charType == U_MATH_SYMBOL ||
                    charType == U_OTHER_SYMBOL || charType == U_MODIFIER_SYMBOL);
        }
        default:
            return false;
        }
    }

private:
    Type _type;
};

class CustomMatcher : public CharMatcher {
public:
    CustomMatcher(const std::string& chars) {
        icu::UnicodeString uniStr = icu::UnicodeString::fromUTF8(icu::StringPiece(chars));
        _code_points.addAll(uniStr);
    }

    bool is_token_char(UChar32 c) override { return _code_points.contains(c); }

private:
    icu::UnicodeSet _code_points;
};

class CompositeMatcher : public CharMatcher {
public:
    CompositeMatcher(std::vector<CharMatcherPtr> matchers) : _matchers(std::move(matchers)) {}
    ~CompositeMatcher() override = default;

    bool is_token_char(UChar32 c) override {
        return std::ranges::any_of(_matchers,
                                   [c](const auto& matcher) { return matcher->is_token_char(c); });
    }

private:
    std::vector<CharMatcherPtr> _matchers;
};

class CharMatcherBuilder {
public:
    void add(const CharMatcherPtr& matcher) { _matchers.push_back(matcher); }

    CharMatcherPtr build() { return std::make_shared<CompositeMatcher>(std::move(_matchers)); }

private:
    std::vector<CharMatcherPtr> _matchers;
};

} // namespace doris::segment_v2::inverted_index