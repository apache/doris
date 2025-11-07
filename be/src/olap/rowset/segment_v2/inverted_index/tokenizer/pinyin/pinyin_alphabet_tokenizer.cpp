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

#include "pinyin_alphabet_tokenizer.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>

#include "common/config.h"

namespace doris::segment_v2::inverted_index {

namespace {

constexpr int kPinyinMaxLength = 6;

inline std::string get_alphabet_dict_path() {
    return config::inverted_index_dict_path + "/pinyin/pinyin_alphabet.dict";
}

static inline bool is_letter(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}
} // namespace

PinyinAlphabetDict& PinyinAlphabetDict::instance() {
    static PinyinAlphabetDict inst;
    return inst;
}

PinyinAlphabetDict::PinyinAlphabetDict() {
    load();
}

void PinyinAlphabetDict::load() {
    std::string dict_path = get_alphabet_dict_path();
    std::ifstream in(dict_path);
    if (!in.is_open()) {
        _CLTHROWA(CL_ERR_IO, ("pinyin alphabet dictionary file not found: " + dict_path).c_str());
    }
    std::string line;
    _alphabet.clear();
    while (std::getline(in, line)) {
        while (!line.empty() &&
               (line.back() == '\r' || line.back() == '\n' || isspace(line.back()))) {
            line.pop_back();
        }
        size_t i = 0;
        while (i < line.size() && isspace(static_cast<unsigned char>(line[i]))) {
            ++i;
        }
        std::string token = line.substr(i);
        if (!token.empty()) {
            std::transform(token.begin(), token.end(), token.begin(),
                           [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
            _alphabet.emplace_back(std::move(token));
        }
    }
    std::sort(_alphabet.begin(), _alphabet.end());
}

bool PinyinAlphabetDict::match(const std::string& token) const {
    if (token.empty()) return false;
    return std::binary_search(_alphabet.begin(), _alphabet.end(), token);
}

std::vector<std::string> PinyinAlphabetTokenizer::walk(const std::string& text) {
    return segPinyinStr(text);
}

std::vector<std::string> PinyinAlphabetTokenizer::segPinyinStr(const std::string& content) {
    std::string lower = content;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });

    std::vector<std::string> pinyin_str_list = splitByNoletter(lower);
    std::vector<std::string> pinyin_list;
    pinyin_list.reserve(pinyin_str_list.size());
    for (const std::string& pinyin_text : pinyin_str_list) {
        if (pinyin_text.size() == 1) {
            pinyin_list.push_back(pinyin_text);
        } else {
            std::vector<std::string> forward = positiveMaxMatch(pinyin_text, kPinyinMaxLength);
            if (forward.size() == 1) {
                pinyin_list.insert(pinyin_list.end(), forward.begin(), forward.end());
            } else {
                std::vector<std::string> backward = reverseMaxMatch(pinyin_text, kPinyinMaxLength);
                if (forward.size() <= backward.size()) {
                    pinyin_list.insert(pinyin_list.end(), forward.begin(), forward.end());
                } else {
                    pinyin_list.insert(pinyin_list.end(), backward.begin(), backward.end());
                }
            }
        }
    }
    return pinyin_list;
}

std::vector<std::string> PinyinAlphabetTokenizer::splitByNoletter(const std::string& pinyin_str) {
    std::vector<std::string> result;
    std::string buf;
    bool last_word = true;
    for (char c : pinyin_str) {
        if (is_letter(c)) {
            if (!last_word) {
                result.push_back(buf);
                buf.clear();
            }
            buf.push_back(c);
            last_word = true;
        } else {
            if (last_word && !buf.empty()) {
                result.push_back(buf);
                buf.clear();
            }
            buf.push_back(c);
            last_word = false;
        }
    }
    if (!buf.empty()) {
        result.push_back(buf);
    }
    return result;
}

std::vector<std::string> PinyinAlphabetTokenizer::positiveMaxMatch(const std::string& pinyin_text,
                                                                   int max_length) {
    std::vector<std::string> pinyin_list;
    std::string no_match_buffer;

    for (size_t start = 0; start < pinyin_text.size();) {
        size_t end = start + static_cast<size_t>(max_length);
        if (end > pinyin_text.size()) end = pinyin_text.size();
        if (start == end) break;

        std::string six_str = pinyin_text.substr(start, end - start);
        bool match = false;
        for (size_t j = 0; j < six_str.size(); ++j) {
            std::string guess = six_str.substr(0, six_str.size() - j);
            if (PinyinAlphabetDict::instance().match(guess)) {
                pinyin_list.push_back(guess);
                start += guess.size();
                match = true;
                break;
            }
        }
        if (!match) {
            no_match_buffer.append(six_str.substr(0, 1));
            start += 1;
        } else {
            if (!no_match_buffer.empty()) {
                pinyin_list.push_back(no_match_buffer);
                no_match_buffer.clear();
            }
        }
    }
    if (!no_match_buffer.empty()) {
        pinyin_list.push_back(no_match_buffer);
        no_match_buffer.clear();
    }
    return pinyin_list;
}

std::vector<std::string> PinyinAlphabetTokenizer::reverseMaxMatch(const std::string& pinyin_text,
                                                                  int max_length) {
    std::vector<std::string> pinyin_list;
    std::string no_match_buffer;

    int end = static_cast<int>(pinyin_text.size());
    while (end >= 0) {
        int start = end - max_length;
        if (start < 0) start = 0;
        if (start == end) break;

        bool match = false;
        std::string six_str = pinyin_text.substr(start, end - start);
        for (size_t j = 0; j < six_str.size(); ++j) {
            std::string guess = six_str.substr(j);
            if (PinyinAlphabetDict::instance().match(guess)) {
                pinyin_list.push_back(guess);
                end -= static_cast<int>(guess.size());
                match = true;
                break;
            }
        }
        if (!match) {
            no_match_buffer.push_back(six_str.back());
            end -= 1;
        } else {
            if (!no_match_buffer.empty()) {
                pinyin_list.push_back(no_match_buffer);
                no_match_buffer.clear();
            }
        }
    }
    if (!no_match_buffer.empty()) {
        pinyin_list.push_back(no_match_buffer);
        no_match_buffer.clear();
    }

    std::reverse(pinyin_list.begin(), pinyin_list.end());
    return pinyin_list;
}

} // namespace doris::segment_v2::inverted_index
