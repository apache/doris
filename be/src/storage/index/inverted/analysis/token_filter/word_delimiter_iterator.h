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

#include <unicode/utext.h>

#include <iomanip>
#include <memory>
#include <sstream>
#include <vector>

namespace doris::segment_v2::inverted_index {

class WordDelimiterIterator {
public:
    WordDelimiterIterator(std::vector<char> char_type_table, bool split_on_case_change,
                          bool split_on_numerics, bool stem_english_possessive);
    ~WordDelimiterIterator() = default;

    int32_t next();

    int32_t type() const;
    void set_text(const char* text, int32_t _length);
    bool is_single_word() const;
    void set_bounds();

    static char get_type(UChar32 ch);

    static bool is_alpha(int32_t type) { return (type & ALPHA) != 0; }
    static bool is_digit(int32_t type) { return (type & DIGIT) != 0; }
    static bool is_subword_delim(int32_t type) { return (type & SUBWORD_DELIM) != 0; }
    static bool is_upper(int32_t type) { return (type & UPPER) != 0; }

    std::string to_string() const;

    static constexpr int32_t LOWER = 0x01;
    static constexpr int32_t UPPER = 0x02;
    static constexpr int32_t DIGIT = 0x04;
    static constexpr int32_t SUBWORD_DELIM = 0x08;

    static constexpr int32_t ALPHA = 0x03;
    static constexpr int32_t ALPHANUM = 0x07;

    static constexpr int32_t DONE = -1;

    static const std::vector<char> DEFAULT_WORD_DELIM_TABLE;

private:
    bool is_break(int32_t lastType, int32_t type) const;
    bool ends_with_possessive(int32_t pos);
    int32_t char_type(int32_t ch) const;

    static std::vector<char> initializeDelimTable();

public:
    const char* _text = nullptr;
    int32_t _length = 0;
    int32_t _current = 0;
    int32_t _end = 0;
    int32_t _start_bounds = 0;
    int32_t _end_bounds = 0;

private:
    std::vector<char> _char_type_table;
    bool _split_on_case_change = false;
    bool _split_on_numerics = false;
    bool _stem_english_possessive = false;
    bool _has_final_possessive = false;
    bool _skip_possessive = false;
};
using WordDelimiterIteratorPtr = std::unique_ptr<WordDelimiterIterator>;

} // namespace doris::segment_v2::inverted_index