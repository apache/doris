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

#include "olap/rowset/segment_v2/inverted_index/setting.h"
#include "token_filter_factory.h"
#include "word_delimiter_filter.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class WordDelimiterFilterFactory : public TokenFilterFactory {
    friend class WordDelimiterFilterFactoryTest;

public:
    WordDelimiterFilterFactory() = default;
    ~WordDelimiterFilterFactory() override = default;

    void initialize(const Settings& settings) override {
        std::vector<std::string> char_type_table_values = settings.get_entry_list("type_table");
        if (char_type_table_values.empty()) {
            _char_type_table = WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE;
        } else {
            _char_type_table = parse_types(char_type_table_values);
        }
        int32_t flags = 0;
        flags |= get_flag(WordDelimiterFilter::GENERATE_WORD_PARTS, settings, "generate_word_parts",
                          true);
        flags |= get_flag(WordDelimiterFilter::GENERATE_NUMBER_PARTS, settings,
                          "generate_number_parts", true);
        flags |= get_flag(WordDelimiterFilter::CATENATE_WORDS, settings, "catenate_words", false);
        flags |= get_flag(WordDelimiterFilter::CATENATE_NUMBERS, settings, "catenate_numbers",
                          false);
        flags |= get_flag(WordDelimiterFilter::CATENATE_ALL, settings, "catenate_all", false);
        flags |= get_flag(WordDelimiterFilter::SPLIT_ON_CASE_CHANGE, settings,
                          "split_on_case_change", true);
        flags |= get_flag(WordDelimiterFilter::PRESERVE_ORIGINAL, settings, "preserve_original",
                          false);
        flags |= get_flag(WordDelimiterFilter::SPLIT_ON_NUMERICS, settings, "split_on_numerics",
                          true);
        flags |= get_flag(WordDelimiterFilter::STEM_ENGLISH_POSSESSIVE, settings,
                          "stem_english_possessive", true);
        _protected_words = settings.get_word_set("protected_words");
        _flags = flags;
    }

    TokenFilterPtr create(const TokenStreamPtr& in) override {
        return std::make_shared<WordDelimiterFilter>(in, _char_type_table, _flags,
                                                     _protected_words);
    }

    static int32_t get_flag(int32_t flag, const Settings& settings, const std::string& key,
                            bool default_value) {
        if (settings.get_bool(key, default_value)) {
            return flag;
        }
        return 0;
    }

    std::vector<char> parse_types(const std::vector<std::string>& rules) {
        static std::regex pattern(R"((.*)\s*=>\s*(.*)\s*$)");

        std::map<UChar32, char> type_map;
        for (const std::string& rule : rules) {
            std::smatch m;
            if (!regex_search(rule, m, pattern)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Invalid Mapping Rule : [" + rule + "]");
            }
            std::string lhs = parse_string(boost::trim_copy(m[1].str()));
            char rhs = parse_type(boost::trim_copy(m[2].str()));
            if (lhs.length() < 1 || lhs.length() > 2) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid Mapping Rule : [" + rule + "]. Only allow one to two bytes.");
            } else {
                if (lhs.length() == 2) {
                    int32_t expectedLen = U8_COUNT_TRAIL_BYTES(lhs[0]) + 1;
                    if (expectedLen == 1) {
                        throw Exception(ErrorCode::INVALID_ARGUMENT,
                                        "Invalid Mapping Rule : [" + rule +
                                                "]. Only one character is allowed.");
                    }
                }
            }
            if (rhs == 0) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Invalid Mapping Rule : [" + rule + "]. Illegal type.");
            }
            UChar32 c = U_UNASSIGNED;
            U8_GET((const uint8_t*)lhs.data(), 0, 0, lhs.length(), c);
            if (c < 0) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid UTF-8 sequence in the last key of type_map: \"" + lhs + "\".");
            }
            type_map[c] = rhs;
        }

        size_t table_size = std::max(static_cast<size_t>(type_map.rbegin()->first),
                                     WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE.size());
        std::vector<char> types(table_size, 0);
        for (size_t i = 0; i < types.size(); ++i) {
            types[i] = WordDelimiterIterator::get_type(static_cast<int32_t>(i));
        }
        for (const auto& mapping : type_map) {
            types[mapping.first] = mapping.second;
        }
        return types;
    }

private:
    static char parse_type(const std::string& s) {
        if (s == "LOWER") {
            return WordDelimiterFilter::LOWER;
        } else if (s == "UPPER") {
            return WordDelimiterFilter::UPPER;
        } else if (s == "ALPHA") {
            return WordDelimiterFilter::ALPHA;
        } else if (s == "DIGIT") {
            return WordDelimiterFilter::DIGIT;
        } else if (s == "ALPHANUM") {
            return WordDelimiterFilter::ALPHANUM;
        } else if (s == "SUBWORD_DELIM") {
            return WordDelimiterFilter::SUBWORD_DELIM;
        } else {
            return 0;
        }
    }

    static std::string parse_string(const std::string& s) {
        std::string out;
        size_t len = s.length();
        size_t read_pos = 0;
        while (read_pos < len) {
            char c = s[read_pos++];
            if (c == '\\') {
                if (read_pos >= len) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "Invalid escaped char in [" + s + "]");
                }
                c = s[read_pos++];
                switch (c) {
                case '\\':
                    c = '\\';
                    break;
                case 'n':
                    c = '\n';
                    break;
                case 't':
                    c = '\t';
                    break;
                case 'r':
                    c = '\r';
                    break;
                case 'b':
                    c = '\b';
                    break;
                case 'f':
                    c = '\f';
                    break;
                case 'u': {
                    if (read_pos + 3 >= len) {
                        throw Exception(ErrorCode::INVALID_ARGUMENT,
                                        "Invalid escaped char in [" + s + "]");
                    }
                    std::string hex = s.substr(read_pos, 4);
                    c = static_cast<char>(stoi(hex, nullptr, 16));
                    read_pos += 4;
                    break;
                }
                default:
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "Invalid escape sequence in [" + s + "]");
                }
            }
            out += c;
        }
        return out;
    }

    int32_t _flags = 0;
    std::vector<char> _char_type_table;
    std::unordered_set<std::string> _protected_words;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index