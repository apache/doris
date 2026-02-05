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

#include "pinyin_formatter.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <regex>
#include <unordered_map>

#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

namespace {
// Tone mark mapping: vowel -> [tone1, tone2, tone3, tone4]
// Using unordered_map for O(1) lookup
const std::unordered_map<char, std::array<const char*, 4>> TONE_MARKS = {
        {'a', {"ā", "á", "ǎ", "à"}}, {'e', {"ē", "é", "ě", "è"}},
        {'i', {"ī", "í", "ǐ", "ì"}}, {'o', {"ō", "ó", "ǒ", "ò"}},
        {'u', {"ū", "ú", "ǔ", "ù"}}, {'v', {"ǖ", "ǘ", "ǚ", "ǜ"}}, // v represents ü
};

constexpr const char* VOWELS = "aeiouv";

// Pre-compiled regex patterns (compile once, reuse many times for performance)
// Only use regex for patterns that require pattern matching (character classes, etc.)
const std::regex TONE_NUMBER_REGEX("[1-5]");
const std::regex PINYIN_VALIDATION_REGEX("[a-z]*[1-5]?");
const std::regex PINYIN_WITH_TONE_REGEX("[a-z]*[1-5]");

// Helper function for simple string replacement (faster than regex for fixed strings)
inline std::string replaceAll(const std::string& str, const std::string& from,
                              const std::string& to) {
    if (from.empty()) return str;
    std::string result = str;
    size_t pos = 0;
    while ((pos = result.find(from, pos)) != std::string::npos) {
        result.replace(pos, from.length(), to);
        pos += to.length();
    }
    return result;
}
} // namespace

std::string PinyinFormatter::formatPinyin(const std::string& pinyin_str,
                                          const PinyinFormat& format) {
    if (pinyin_str.empty()) {
        return pinyin_str;
    }

    std::string result = pinyin_str;

    PinyinFormat working_format = format;

    if (format.getToneType() == ToneType::WITH_ABBR) {
        return abbr(result);
    } else {
        if (working_format.getToneType() == ToneType::WITH_TONE_MARK &&
            (working_format.getYuCharType() == YuCharType::WITH_V ||
             working_format.getYuCharType() == YuCharType::WITH_U_AND_COLON)) {
            // ToneType.WITH_TONE_MARK force YuCharType.WITH_U_UNICODE
            working_format.setYuCharType(YuCharType::WITH_U_UNICODE);
        }

        switch (working_format.getToneType()) {
        case ToneType::WITHOUT_TONE:
            result = std::regex_replace(result, TONE_NUMBER_REGEX, "");
            break;
        case ToneType::WITH_TONE_MARK:
            result = replaceAll(result, "u:", "v");
            result = convertToneNumber2ToneMark(result);
            break;
        case ToneType::WITH_TONE_NUMBER:
        default:
            break;
        }

        if (working_format.getToneType() != ToneType::WITH_TONE_MARK) {
            switch (working_format.getYuCharType()) {
            case YuCharType::WITH_V:
                result = replaceAll(result, "u:", "v");
                break;
            case YuCharType::WITH_U_UNICODE:
                result = replaceAll(result, "u:", "ü");
                break;
            case YuCharType::WITH_U_AND_COLON:
            default:
                break;
            }
        }
    }

    switch (working_format.getCaseType()) {
    case CaseType::UPPERCASE:
        std::transform(result.begin(), result.end(), result.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        break;
    case CaseType::CAPITALIZE:
        result = capitalize(result);
        break;
    case CaseType::LOWERCASE:
    default:
        break;
    }

    return result;
}

std::string PinyinFormatter::abbr(const std::string& str) {
    if (str.empty()) {
        return str;
    }

    const char* str_ptr = str.c_str();
    int str_len = static_cast<int>(str.length());
    int byte_pos = 0;
    UChar32 first_char;

    U8_NEXT(str_ptr, byte_pos, str_len, first_char);

    if (first_char < 0) {
        return str.substr(0, 1);
    }

    std::string result;
    char utf8_buffer[4];
    int32_t utf8_len = 0;
    U8_APPEND_UNSAFE(utf8_buffer, utf8_len, first_char);
    result.assign(utf8_buffer, utf8_len);

    return result;
}

std::string PinyinFormatter::capitalize(const std::string& str) {
    if (str.empty()) {
        return str;
    }
    std::string result = str;
    result[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(result[0])));
    return result;
}

std::string PinyinFormatter::convertToneNumber2ToneMark(const std::string& pinyin_str) {
    std::string lower_case_pinyin = pinyin_str;
    std::transform(lower_case_pinyin.begin(), lower_case_pinyin.end(), lower_case_pinyin.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (!std::regex_match(lower_case_pinyin, PINYIN_VALIDATION_REGEX)) {
        return lower_case_pinyin;
    }

    const char DEFAULT_CHAR_VALUE = '$';
    const int DEFAULT_INDEX_VALUE = -1;

    char unmarked_vowel = DEFAULT_CHAR_VALUE;
    int index_of_unmarked_vowel = DEFAULT_INDEX_VALUE;

    if (std::regex_match(lower_case_pinyin, PINYIN_WITH_TONE_REGEX)) {
        int tune_number = lower_case_pinyin.back() - '0';

        size_t index_of_a = lower_case_pinyin.find('a');
        size_t index_of_e = lower_case_pinyin.find('e');
        size_t ou_index = lower_case_pinyin.find("ou");

        if (index_of_a != std::string::npos) {
            index_of_unmarked_vowel = static_cast<int>(index_of_a);
            unmarked_vowel = 'a';
        } else if (index_of_e != std::string::npos) {
            index_of_unmarked_vowel = static_cast<int>(index_of_e);
            unmarked_vowel = 'e';
        } else if (ou_index != std::string::npos) {
            index_of_unmarked_vowel = static_cast<int>(ou_index);
            unmarked_vowel = 'o';
        } else {
            // Find the last vowel
            for (int i = static_cast<int>(lower_case_pinyin.length()) - 1; i >= 0; i--) {
                char c = lower_case_pinyin[i];
                if (std::string(VOWELS).find(c) != std::string::npos) {
                    index_of_unmarked_vowel = i;
                    unmarked_vowel = c;
                    break;
                }
            }
        }

        if (unmarked_vowel != DEFAULT_CHAR_VALUE &&
            index_of_unmarked_vowel != DEFAULT_INDEX_VALUE) {
            // Look up the marked vowel from the map
            auto it = TONE_MARKS.find(unmarked_vowel);
            if (it != TONE_MARKS.end() && tune_number >= 1 && tune_number <= 4) {
                const char* marked_vowel = it->second[tune_number - 1];

                std::string result;
                std::string prefix = lower_case_pinyin.substr(0, index_of_unmarked_vowel);
                result += replaceAll(prefix, "v", "ü");
                result += marked_vowel;

                if (index_of_unmarked_vowel + 1 <
                    static_cast<int>(lower_case_pinyin.length()) - 1) {
                    std::string suffix = lower_case_pinyin.substr(
                            index_of_unmarked_vowel + 1,
                            lower_case_pinyin.length() - 1 - index_of_unmarked_vowel - 1);
                    result += replaceAll(suffix, "v", "ü");
                }

                return result;
            }
        }
    } else {
        return replaceAll(lower_case_pinyin, "v", "ü");
    }

    return lower_case_pinyin;
}

} // namespace doris::segment_v2::inverted_index
