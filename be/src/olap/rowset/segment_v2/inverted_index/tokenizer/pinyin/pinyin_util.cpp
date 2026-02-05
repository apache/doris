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

#include "pinyin_util.h"

#include <unicode/utf8.h>

#include <fstream>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "pinyin_formatter.h"
#include "smart_get_word.h"

namespace doris::segment_v2::inverted_index {

namespace {
// CJK Unified Ideographs basic block range
constexpr uint32_t CJK_START = 0x4E00; // CJK Unified Ideographs start
constexpr uint32_t CJK_END = 0x9FA5;   // CJK Unified Ideographs end
// Dictionary file paths: dynamically obtained from configuration, referencing other tokenizers
inline std::string get_pinyin_dict_path() {
    return config::inverted_index_dict_path + "/pinyin/pinyin.txt";
}
inline std::string get_polyphone_dict_path() {
    return config::inverted_index_dict_path + "/pinyin/polyphone.txt";
}
} // namespace

PinyinUtil& PinyinUtil::instance() {
    static PinyinUtil inst;
    return inst;
}

PinyinUtil::PinyinUtil() : max_polyphone_len_(2) {
    load_pinyin_mapping();
    load_polyphone_mapping();
}

void PinyinUtil::load_pinyin_mapping() {
    _pinyin_dict.clear();
    _pinyin_dict.resize(static_cast<size_t>(CJK_END - CJK_START + 1));

    std::string pinyin_path = get_pinyin_dict_path();
    std::ifstream in(pinyin_path);
    if (!in.is_open()) {
        return;
    }
    std::string line;
    size_t idx = 0;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') {
            ++idx;
            continue;
        }
        size_t pos = line.find('=');
        std::string value;
        if (pos == std::string::npos) {
            value = "";
        } else {
            value = line.substr(pos + 1);
        }
        if (idx < _pinyin_dict.size()) {
            _pinyin_dict[idx] = value;
        }
        ++idx;
    }
}

std::string PinyinUtil::to_pinyin(uint32_t cp) const {
    return to_raw_pinyin(cp);
}

void PinyinUtil::load_polyphone_mapping() {
    polyphone_dict_ = std::make_unique<PolyphoneForest>();

    std::string polyphone_path = get_polyphone_dict_path();
    std::ifstream in(polyphone_path);
    if (!in.is_open()) {
        return;
    }

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        size_t pos = line.find('=');
        if (pos == std::string::npos || pos + 1 >= line.length()) {
            continue;
        }

        std::string word = line.substr(0, pos);
        std::string pinyin_str = line.substr(pos + 1);

        std::vector<std::string> pinyins;
        std::istringstream iss(pinyin_str);
        std::string pinyin;
        while (iss >> pinyin) {
            pinyins.push_back(pinyin);
        }

        if (!word.empty() && !pinyins.empty()) {
            polyphone_dict_->add(word, pinyins);
            if (static_cast<int>(word.length()) > max_polyphone_len_) {
                max_polyphone_len_ = static_cast<int>(word.length());
            }
        }
    }
}

void PinyinUtil::insertPinyin(const std::string& word, const std::vector<std::string>& pinyins) {
    if (word.empty() || pinyins.empty()) {
        return;
    }

    if (polyphone_dict_) {
        polyphone_dict_->add(word, pinyins);
    }
}

std::vector<std::string> PinyinUtil::convert_with_raw_pinyin(const std::string& text) const {
    std::vector<std::string> result;
    if (text.empty()) return result;

    const char* text_ptr = text.c_str();
    int text_len = static_cast<int>(text.length());
    std::vector<UChar32> chars;
    std::vector<int>
            char_byte_starts; // Record byte start position of each character in original UTF-8

    int byte_pos = 0;
    while (byte_pos < text_len) {
        UChar32 cp;
        U8_NEXT(text_ptr, byte_pos, text_len, cp);
        char_byte_starts.push_back(byte_pos - U8_LENGTH(cp));
        chars.push_back(cp);
    }

    result.resize(chars.size());

    PolyphoneGetWord word_matcher(polyphone_dict_.get(), text);

    std::vector<bool> processed(chars.size(), false);

    std::string matched_word;
    while ((matched_word = word_matcher.getFrontWords()) != word_matcher.getNullResult() &&
           !matched_word.empty()) {
        int match_start_byte = word_matcher.offe;
        int match_end_byte = match_start_byte + static_cast<int>(matched_word.length());

        int char_start = -1, char_end = -1;

        for (size_t i = 0; i < char_byte_starts.size(); ++i) {
            int char_byte_start = char_byte_starts[i];
            int char_byte_end = (i + 1 < char_byte_starts.size()) ? char_byte_starts[i + 1]
                                                                  : static_cast<int>(text.length());

            if (match_start_byte >= char_byte_start && match_start_byte < char_byte_end) {
                char_start = static_cast<int>(i);
                break;
            }
        }

        for (size_t i = 0; i < char_byte_starts.size(); ++i) {
            int char_byte_start = char_byte_starts[i];

            if (char_byte_start >= match_end_byte) {
                char_end = static_cast<int>(i);
                break;
            }
        }
        if (char_end == -1) char_end = static_cast<int>(chars.size());

        const auto& pinyins = word_matcher.getParam();
        int word_char_count = char_end - char_start;

        for (int i = 0; i < word_char_count && i < static_cast<int>(pinyins.size()); ++i) {
            int char_idx = char_start + i;
            if (char_idx >= 0 && char_idx < static_cast<int>(result.size())) {
                result[char_idx] = pinyins[i];
                processed[char_idx] = true;
            }
        }
    }

    for (size_t i = 0; i < chars.size(); ++i) {
        if (!processed[i]) {
            std::string pinyin = to_raw_pinyin(static_cast<uint32_t>(chars[i]));
            result[i] = pinyin;
        }
    }

    return result;
}

std::string PinyinUtil::to_raw_pinyin(uint32_t cp) const {
    if (cp < CJK_START || cp > CJK_END) {
        std::string result;
        if (cp <= 0x7F) {
            result.push_back(static_cast<char>(cp));
        } else {
            char buffer[8];
            int32_t buffer_index = 0;
            U8_APPEND_UNSAFE(buffer, buffer_index, cp);
            result.assign(buffer, buffer_index);
        }
        return result;
    }

    size_t idx = static_cast<size_t>(cp - CJK_START);
    if (idx >= _pinyin_dict.size()) return "";
    const std::string& raw = _pinyin_dict[idx];
    if (raw.empty()) return "";
    size_t comma = raw.find(',');
    if (comma == std::string::npos) return raw;
    return raw.substr(0, comma);
}

std::vector<std::string> PinyinUtil::convert(const std::vector<UChar32>& codepoints,
                                             const PinyinFormat& format) const {
    if (codepoints.empty()) {
        return {};
    }

    std::vector<std::string> result(codepoints.size());

    if (!polyphone_dict_) {
        for (size_t i = 0; i < codepoints.size(); ++i) {
            std::string pinyin = to_pinyin(static_cast<uint32_t>(codepoints[i]));
            if (pinyin.empty()) {
                if (!format.isOnlyPinyin()) {
                    result[i] = "";
                }
            } else {
                if (codepoints[i] < CJK_START || codepoints[i] > CJK_END) {
                    result[i] = pinyin;
                } else {
                    result[i] = PinyinFormatter::formatPinyin(pinyin, format);
                }
            }
        }
        return result;
    }

    std::string text;
    for (UChar32 cp : codepoints) {
        char utf8_buffer[4];
        int32_t utf8_len = 0;
        U8_APPEND_UNSAFE(utf8_buffer, utf8_len, cp);
        text.append(utf8_buffer, utf8_len);
    }

    std::vector<std::string> raw_result = convert_with_raw_pinyin(text);

    result.clear();
    result.reserve(raw_result.size());

    for (const std::string& pinyin : raw_result) {
        if (pinyin.empty()) {
            if (!format.isOnlyPinyin()) {
                result.push_back("");
            }
        } else {
            std::string formatted = PinyinFormatter::formatPinyin(pinyin, format);
            if (!formatted.empty() || !format.isOnlyPinyin()) {
                result.push_back(formatted);
            }
        }
    }

    return result;
}

} // namespace doris::segment_v2::inverted_index
