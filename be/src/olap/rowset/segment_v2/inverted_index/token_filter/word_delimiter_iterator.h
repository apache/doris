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
                          bool split_on_numerics, bool stem_english_possessive)
            : _char_type_table(std::move(char_type_table)),
              _split_on_case_change(split_on_case_change),
              _split_on_numerics(split_on_numerics),
              _stem_english_possessive(stem_english_possessive) {}

    ~WordDelimiterIterator() = default;

    int32_t next() {
        _current = _end;
        if (_current == DONE) {
            return DONE;
        }

        if (_skip_possessive) {
            _current += 2;
            _skip_possessive = false;
        }

        int32_t lastType = 0;

        while (_current < _end_bounds &&
               (is_subword_delim(lastType = char_type(_text[_current])))) {
            _current++;
        }

        if (_current >= _end_bounds) {
            return _end = DONE;
        }

        for (_end = _current + 1; _end < _end_bounds; _end++) {
            int32_t type = char_type(_text[_end]);
            if (is_break(lastType, type)) {
                break;
            }
            lastType = type;
        }

        if (_end < _end_bounds - 1 && ends_with_possessive(_end + 2)) {
            _skip_possessive = true;
        }

        return _end;
    }

    int32_t type() const {
        if (_end == DONE) {
            return 0;
        }

        int32_t type = char_type(_text[_current]);
        switch (type) {
        case LOWER:
        case UPPER:
            return ALPHA;
        default:
            return type;
        }
    }

    void set_text(char* text, int32_t _length) {
        _text = text;
        _length = _end_bounds = _length;
        _current = _start_bounds = _end = 0;
        _skip_possessive = _has_final_possessive = false;
        set_bounds();
    }

    bool is_single_word() const {
        if (_has_final_possessive) {
            return _current == _start_bounds && _end == _end_bounds - 2;
        } else {
            return _current == _start_bounds && _end == _end_bounds;
        }
    }

    void set_bounds() {
        while (_start_bounds < _length && (is_subword_delim(char_type(_text[_start_bounds])))) {
            _start_bounds++;
        }

        while (_end_bounds > _start_bounds &&
               (is_subword_delim(char_type(_text[_end_bounds - 1])))) {
            _end_bounds--;
        }
        if (ends_with_possessive(_end_bounds)) {
            _has_final_possessive = true;
        }
        _current = _start_bounds;
    }

    int32_t get_type(int32_t ch) const {
        const auto char_type = u_charType(ch);

        switch (char_type) {
        case U_UPPERCASE_LETTER:
            return UPPER;
        case U_LOWERCASE_LETTER:
            return LOWER;
        case U_TITLECASE_LETTER:
        case U_MODIFIER_LETTER:
        case U_OTHER_LETTER:
        case U_NON_SPACING_MARK:
        case U_ENCLOSING_MARK:
        case U_COMBINING_SPACING_MARK:
            return ALPHA;
        case U_DECIMAL_DIGIT_NUMBER:
        case U_LETTER_NUMBER:
        case U_OTHER_NUMBER:
            return DIGIT;
        case U_SURROGATE:
            return ALPHA | DIGIT;
        default:
            return SUBWORD_DELIM;
        }
    }

    static bool is_alpha(int32_t type) { return (type & ALPHA) != 0; }

    static bool is_digit(int32_t type) { return (type & DIGIT) != 0; }

    static bool is_subword_delim(int32_t type) { return (type & SUBWORD_DELIM) != 0; }

    static bool is_upper(int32_t type) { return (type & UPPER) != 0; }

    std::string to_string() const {
        if (_end == DONE) {
            return "DONE";
        }
        std::ostringstream oss;
        std::string substring(_text + _current, _end - _current);
        oss << substring << " [" << _current << "-" << _end << "]"
            << " type=0x" << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int32_t>(type());
        return oss.str();
    }

    static constexpr int32_t LOWER = 0x01;
    static constexpr int32_t UPPER = 0x02;
    static constexpr int32_t DIGIT = 0x04;
    static constexpr int32_t SUBWORD_DELIM = 0x08;

    static constexpr int32_t ALPHA = 0x03;
    static constexpr int32_t ALPHANUM = 0x07;

    static constexpr int32_t DONE = -1;

private:
    bool is_break(int32_t lastType, int32_t type) const {
        if ((type & lastType) != 0) {
            return false;
        }

        if (!_split_on_case_change && is_alpha(lastType) && is_alpha(type)) {
            return false;
        } else if (is_upper(lastType) && is_alpha(type)) {
            return false;
        } else if (!_split_on_numerics && ((is_alpha(lastType) && is_digit(type)) ||
                                           (is_digit(lastType) && is_alpha(type)))) {
            return false;
        }

        return true;
    }

    bool ends_with_possessive(int32_t pos) {
        return (_stem_english_possessive && pos > 2 && _text[pos - 2] == '\'' &&
                (_text[pos - 1] == 's' || _text[pos - 1] == 'S') &&
                is_alpha(char_type(_text[pos - 3])) &&
                (pos == _end_bounds || is_subword_delim(char_type(_text[pos]))));
    }

    int32_t char_type(int32_t ch) const {
        if (ch < _char_type_table.size()) {
            return _char_type_table[ch];
        }
        return get_type(ch);
    }

public:
    char* _text = nullptr;
    int32_t _current = 0;
    int32_t _end = 0;

private:
    std::vector<char> _char_type_table;
    bool _split_on_case_change = false;
    bool _split_on_numerics = false;
    bool _stem_english_possessive = false;
    bool _has_final_possessive = false;
    bool _skip_possessive = false;

    int32_t _length = 0;
    int32_t _start_bounds = 0;
    int32_t _end_bounds = 0;
};
using WordDelimiterIteratorPtr = std::unique_ptr<WordDelimiterIterator>;

} // namespace doris::segment_v2::inverted_index