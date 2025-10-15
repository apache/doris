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

#include "char_group_tokenizer_factory.h"

#include <unicode/uscript.h>

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/char/char_tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

void CharGroupTokenizerFactory::initialize(const Settings& settings) {
    _max_token_length = settings.get_int("max_token_length", CharTokenizer::DEFAULT_MAX_WORD_LEN);

    for (const auto& str : settings.get_entry_list("tokenize_on_chars")) {
        if (str.length() == 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "[tokenize_on_chars] cannot contain empty characters");
        }

        icu::UnicodeString unicode_str = icu::UnicodeString::fromUTF8(str);

        if (unicode_str.countChar32() == 1) {
            _tokenize_on_chars.insert(unicode_str.char32At(0));
        } else if (unicode_str.length() > 0 && unicode_str.charAt(0) == '\\') {
            _tokenize_on_chars.insert(parse_escaped_char(unicode_str));
        } else {
            if (str == "letter") {
                _tokenize_on_letter = true;
            } else if (str == "digit") {
                _tokenize_on_digit = true;
            } else if (str == "whitespace") {
                _tokenize_on_space = true;
            } else if (str == "punctuation") {
                _tokenize_on_punctuation = true;
            } else if (str == "symbol") {
                _tokenize_on_symbol = true;
            } else if (str == "cjk") {
                _tokenize_on_cjk = true;
            } else {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Invalid escaped char in [" + str + "]");
            }
        }
    }
}

UChar32 CharGroupTokenizerFactory::parse_escaped_char(const icu::UnicodeString& unicode_str) {
    icu::UnicodeString unescaped = unicode_str.unescape();

    if (unescaped.countChar32() != 1) {
        std::string s;
        unicode_str.toUTF8String(s);
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid escaped char [" + s + "]");
    }

    return unescaped.char32At(0);
}

TokenizerPtr CharGroupTokenizerFactory::create() {
    struct CharGroupConfig {
        bool tokenize_on_space = false;
        bool tokenize_on_letter = false;
        bool tokenize_on_digit = false;
        bool tokenize_on_punctuation = false;
        bool tokenize_on_symbol = false;
        bool tokenize_on_cjk = false;
        std::unordered_set<UChar32> tokenize_on_chars;

        CharGroupConfig(bool space, bool letter, bool digit, bool punct, bool symbol, bool cjk,
                        std::unordered_set<UChar32> chars)
                : tokenize_on_space(space),
                  tokenize_on_letter(letter),
                  tokenize_on_digit(digit),
                  tokenize_on_punctuation(punct),
                  tokenize_on_symbol(symbol),
                  tokenize_on_cjk(cjk),
                  tokenize_on_chars(std::move(chars)) {}
    };

    class CharGroupTokenizerImpl : public CharTokenizer {
    public:
        CharGroupTokenizerImpl(CharGroupConfig config) : _config(std::move(config)) {}
        ~CharGroupTokenizerImpl() override = default;

        bool is_cjk_char(UChar32 c) override {
            if (!_config.tokenize_on_cjk) {
                return false;
            }

            UErrorCode status = U_ZERO_ERROR;
            UScriptCode script = uscript_getScript(c, &status);
            if (!U_SUCCESS(status)) {
                return false;
            }

            return script == USCRIPT_HAN || script == USCRIPT_HIRAGANA ||
                   script == USCRIPT_KATAKANA || script == USCRIPT_HANGUL;
        }

        bool is_token_char(UChar32 c) override {
            if (_config.tokenize_on_space && u_isspace(c)) {
                return false;
            }
            if (_config.tokenize_on_letter && u_isalpha(c)) {
                return false;
            }
            if (_config.tokenize_on_digit && u_isdigit(c)) {
                return false;
            }
            if (_config.tokenize_on_punctuation && u_ispunct(c)) {
                return false;
            }
            if (_config.tokenize_on_symbol) {
                int8_t char_type = u_charType(c);
                if (char_type == U_MATH_SYMBOL || char_type == U_CURRENCY_SYMBOL ||
                    char_type == U_MODIFIER_SYMBOL || char_type == U_OTHER_SYMBOL) {
                    return false;
                }
            }
            if (_config.tokenize_on_chars.contains(c)) {
                return false;
            }
            return true;
        }

    private:
        CharGroupConfig _config;
    };

    CharGroupConfig config(_tokenize_on_space, _tokenize_on_letter, _tokenize_on_digit,
                           _tokenize_on_punctuation, _tokenize_on_symbol, _tokenize_on_cjk,
                           _tokenize_on_chars);

    auto tokenzier = std::make_shared<CharGroupTokenizerImpl>(std::move(config));
    tokenzier->initialize(_max_token_length);
    return tokenzier;
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index