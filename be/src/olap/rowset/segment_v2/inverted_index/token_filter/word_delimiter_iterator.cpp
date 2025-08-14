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

#include "word_delimiter_iterator.h"

#include <unicode/utf8.h>

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

const std::vector<char> WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE = initializeDelimTable();

std::vector<char> WordDelimiterIterator::initializeDelimTable() {
    std::vector<char> tab(256);
    for (int32_t i = 0; i < 256; i++) {
        char code = 0;
        if (u_isULowercase(i)) {
            code |= LOWER;
        } else if (u_isUUppercase(i)) {
            code |= UPPER;
        } else if (u_isdigit(i)) {
            code |= DIGIT;
        }
        if (code == 0) {
            code = SUBWORD_DELIM;
        }
        tab[i] = code;
    }
    return tab;
}

WordDelimiterIterator::WordDelimiterIterator(std::vector<char> char_type_table,
                                             bool split_on_case_change, bool split_on_numerics,
                                             bool stem_english_possessive)
        : _char_type_table(std::move(char_type_table)),
          _split_on_case_change(split_on_case_change),
          _split_on_numerics(split_on_numerics),
          _stem_english_possessive(stem_english_possessive) {}

int32_t WordDelimiterIterator::next() {
    _current = _end;
    if (_current == DONE) {
        return DONE;
    }

    if (_skip_possessive) {
        _current += 2;
        _skip_possessive = false;
    }

    int32_t last_type = 0;
    int32_t next_pos = 0;
    while (_current < _end_bounds) {
        next_pos = _current;
        UChar32 c = U_UNASSIGNED;
        U8_NEXT(_text, next_pos, _end_bounds, c);
        if (c < 0) {
            _current++;
            continue;
        }
        last_type = char_type(c);
        if (!is_subword_delim(last_type)) {
            break;
        }
        _current = next_pos;
    }

    if (_current >= _end_bounds) {
        return _end = DONE;
    }

    _end = next_pos;
    while (next_pos < _end_bounds) {
        next_pos = _end;
        UChar32 c = U_UNASSIGNED;
        U8_NEXT(_text, next_pos, _end_bounds, c);
        if (c < 0) {
            _end++;
            continue;
        }
        int32_t type = char_type(c);
        if (is_break(last_type, type)) {
            break;
        }
        last_type = type;
        _end = next_pos;
    }

    if (_end < _end_bounds - 1 && ends_with_possessive(_end + 2)) {
        _skip_possessive = true;
    }

    return _end;
}

int32_t WordDelimiterIterator::type() const {
    if (_end == DONE) {
        return 0;
    }

    UChar32 c = U_UNASSIGNED;
    U8_GET((const uint8_t*)_text, 0, _current, _end, c);
    if (c < 0) {
        return SUBWORD_DELIM;
    }
    int32_t type = char_type(c);
    switch (type) {
    case LOWER:
    case UPPER:
        return ALPHA;
    default:
        return type;
    }
}

void WordDelimiterIterator::set_text(const char* text, int32_t length) {
    _text = text;
    _length = _end_bounds = length;
    _current = _start_bounds = _end = 0;
    _skip_possessive = _has_final_possessive = false;
    set_bounds();
}

bool WordDelimiterIterator::is_single_word() const {
    if (_has_final_possessive) {
        return _current == _start_bounds && _end == _end_bounds - 2;
    } else {
        return _current == _start_bounds && _end == _end_bounds;
    }
}

void WordDelimiterIterator::set_bounds() {
    int32_t pos = 0;
    while (pos < _length) {
        const int32_t prev_pos = pos;
        UChar32 c = U_UNASSIGNED;
        U8_NEXT(_text, pos, _length, c);
        if (c < 0) {
            pos++;
            continue;
        }
        if (!is_subword_delim(char_type(c))) {
            _start_bounds = prev_pos;
            break;
        }
    }

    pos = _length;
    while (pos > _start_bounds) {
        int32_t prev_pos = pos;
        UChar32 c = U_UNASSIGNED;
        U8_PREV(_text, 0, prev_pos, c);
        if (c < 0) {
            pos--;
            continue;
        }
        if (!is_subword_delim(char_type(c))) {
            _end_bounds = pos;
            break;
        }
        pos = prev_pos;
    }

    if (ends_with_possessive(_end_bounds)) {
        _has_final_possessive = true;
    }
    _current = _start_bounds;
}

char WordDelimiterIterator::get_type(UChar32 ch) {
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
    case U_MODIFIER_SYMBOL:
    case U_OTHER_SYMBOL: {
        if (U_IS_UNICODE_CHAR(ch) && ch >= 0x10000 && ch <= 0x10FFFF) {
            return ALPHA | DIGIT;
        }
    }
    default:
        return SUBWORD_DELIM;
    }
}

std::string WordDelimiterIterator::to_string() const {
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

bool WordDelimiterIterator::is_break(int32_t last_type, int32_t type) const {
    if ((type & last_type) != 0) {
        return false;
    }

    if (!_split_on_case_change && is_alpha(last_type) && is_alpha(type)) {
        return false;
    } else if (is_upper(last_type) && is_alpha(type)) {
        return false;
    } else if (!_split_on_numerics && ((is_alpha(last_type) && is_digit(type)) ||
                                       (is_digit(last_type) && is_alpha(type)))) {
        return false;
    }

    return true;
}

bool WordDelimiterIterator::ends_with_possessive(int32_t pos) {
    if (!_stem_english_possessive || pos <= 2) {
        return false;
    }

    if (_text[pos - 2] != '\'' || (_text[pos - 1] != 's' && _text[pos - 1] != 'S')) {
        return false;
    }

    int32_t i = pos - 2;
    UChar32 c;
    U8_PREV(_text, 0, i, c);
    if (c < 0) {
        return false;
    }
    if (!is_alpha(char_type(c))) {
        return false;
    }

    if (pos == _end_bounds) {
        return true;
    }

    i = pos;
    U8_NEXT(_text, i, _end_bounds, c);
    if (c < 0) {
        return false;
    }
    return is_subword_delim(char_type(c));
}

int32_t WordDelimiterIterator::char_type(UChar32 ch) const {
    if (ch < _char_type_table.size()) {
        return _char_type_table[ch];
    }
    return get_type(ch);
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index