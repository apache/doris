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

#include "word_delimiter_filter.h"

#include <iostream>
#include <memory>
#include <string_view>

#include "olap/rowset/segment_v2/inverted_index/token_filter/token_filter.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

WordDelimiterFilter::WordDelimiterFilter(const TokenStreamPtr& in,
                                         std::vector<char> char_type_table,
                                         int32_t configuration_flags,
                                         std::unordered_set<std::string> prot_words)
        : DorisTokenFilter(in),
          _flags(configuration_flags),
          _prot_words(std::move(prot_words)),
          _states(8) {
    _iterator = std::make_unique<WordDelimiterIterator>(char_type_table, has(SPLIT_ON_CASE_CHANGE),
                                                        has(SPLIT_ON_NUMERICS),
                                                        has(STEM_ENGLISH_POSSESSIVE));
    _concat = std::make_unique<WordDelimiterConcatenation>(*this);
    _concat_all = std::make_unique<WordDelimiterConcatenation>(*this);
}

Token* WordDelimiterFilter::next(Token* t) {
    while (true) {
        if (!_has_saved_state) {
            if (!_in->next(t)) {
                return nullptr;
            }
            // todo: has(IGNORE_KEYWORDS)
            char* term_buffer = t->termBuffer<char>();
            auto term_length = static_cast<int32_t>(t->termLength<char>());
            std::string_view term(term_buffer, term_length);

            _accum_pos_inc += get_position_increment(t);
            _iterator->set_text(term.data(), static_cast<int32_t>(term.size()));
            _iterator->next();

            if ((_iterator->_current == 0 && _iterator->_end == term_length) ||
                (!_prot_words.empty() &&
                 _prot_words.find(std::string(term)) != _prot_words.end())) {
                set_position_increment(t, _accum_pos_inc);
                _accum_pos_inc = 0;
                _first = false;
                return t;
            }

            if (_iterator->_end == WordDelimiterIterator::DONE && !has(PRESERVE_ORIGINAL)) {
                if (get_position_increment(t) == 1 && !_first) {
                    _accum_pos_inc--;
                }
                continue;
            }

            save_state(term);

            _has_output_token = false;
            _has_output_following_original = !has(PRESERVE_ORIGINAL);
            _last_concat_count = 0;

            if (has(PRESERVE_ORIGINAL)) {
                set_position_increment(t, _accum_pos_inc);
                _accum_pos_inc = 0;
                _first = false;
                return t;
            }
        }

        if (_iterator->_end == WordDelimiterIterator::DONE) {
            if (!_concat->is_empty()) {
                if (flush_concatenation(_concat)) {
                    buffer();
                    continue;
                }
            }

            if (!_concat_all->is_empty()) {
                if (_concat_all->_subword_count > _last_concat_count) {
                    _concat_all->write_and_clear();
                    buffer();
                    continue;
                }
                _concat_all->clear();
            }

            if (_buffered_pos < _buffered_len) {
                if (_buffered_pos == 0) {
                    std::sort(_states.begin(), _states.begin() + _buffered_len,
                              [](const Attribute& a, const Attribute& b) {
                                  if (a.start_off != b.start_off) {
                                      return a.start_off < b.start_off;
                                  }
                                  return a.pos_inc > b.pos_inc;
                              });
                }
                const auto& term = _states[_buffered_pos].buffered;
                int32_t position = _states[_buffered_pos].pos_inc;
                _buffered_pos++;
                set(t, term, position);
                if (_first && get_position_increment(t) == 0) {
                    set_position_increment(t, 1);
                }
                _first = false;
                return t;
            }

            _buffered_pos = _buffered_len = 0;
            _has_saved_state = false;
            continue;
        }

        if (_iterator->is_single_word()) {
            generate_part(true);
            _iterator->next();
            _first = false;
            set(t, _attribute.buffered, _attribute.pos_inc);
            return t;
        }

        int32_t word_type = _iterator->type();
        if (!_concat->is_empty() && (_concat->_type & word_type) == 0) {
            if (flush_concatenation(_concat)) {
                _has_output_token = false;
                buffer();
                continue;
            }
            _has_output_token = false;
        }

        if (should_concatenate(word_type)) {
            if (_concat->is_empty()) {
                _concat->_type = word_type;
            }
            concatenate(_concat);
        }

        if (has(CATENATE_ALL)) {
            concatenate(_concat_all);
        }

        if (should_generate_parts(word_type)) {
            generate_part(false);
            buffer();
        }

        _iterator->next();
    }
}

void WordDelimiterFilter::reset() {
    DorisTokenFilter::reset();
    _has_saved_state = false;
    _concat->clear();
    _concat_all->clear();
    _accum_pos_inc = 0;
    _buffered_pos = 0;
    _buffered_len = 0;
    _first = true;
}

void WordDelimiterFilter::save_state(const std::string_view& term) {
    _saved_buffer = term;
    _iterator->_text = _saved_buffer.data();
    _has_saved_state = true;
}

bool WordDelimiterFilter::flush_concatenation(const WordDelimiterConcatenationPtr& concatenation) {
    _last_concat_count = concatenation->_subword_count;
    if (concatenation->_subword_count != 1 || !should_generate_parts(concatenation->_type)) {
        concatenation->write_and_clear();
        return true;
    }
    concatenation->clear();
    return false;
}

void WordDelimiterFilter::buffer() {
    if (_buffered_len == _states.size()) {
        _states.resize(_states.size() * 2);
    }
    _states[_buffered_len].buffered = _attribute.buffered;
    _states[_buffered_len].start_off = _attribute.start_off;
    _states[_buffered_len].pos_inc = _attribute.pos_inc;
    _buffered_len++;
}

void WordDelimiterFilter::generate_part(bool is_single_word) {
    _attribute.buffered =
            _saved_buffer.substr(_iterator->_current, _iterator->_end - _iterator->_current);
    _attribute.start_off = _iterator->_current;
    _attribute.pos_inc = position(false);
}

int32_t WordDelimiterFilter::position(bool inject) {
    int32_t pos_inc = _accum_pos_inc;

    if (_has_output_token) {
        _accum_pos_inc = 0;
        return inject ? 0 : std::max(1, pos_inc);
    }

    _has_output_token = true;

    if (!_has_output_following_original) {
        _has_output_following_original = true;
        return 0;
    }
    _accum_pos_inc = 0;
    return std::max(1, pos_inc);
}

void WordDelimiterFilter::concatenate(const WordDelimiterConcatenationPtr& concatenation) {
    if (concatenation->is_empty()) {
        concatenation->_start_offset = _iterator->_current;
    }
    concatenation->append(_saved_buffer.data(), _iterator->_current,
                          _iterator->_end - _iterator->_current);
}

bool WordDelimiterFilter::should_concatenate(int32_t word_type) {
    return (has(CATENATE_WORDS) && is_alpha(word_type)) ||
           (has(CATENATE_NUMBERS) && is_digit(word_type));
}

bool WordDelimiterFilter::should_generate_parts(int32_t word_type) {
    return (has(GENERATE_WORD_PARTS) && is_alpha(word_type)) ||
           (has(GENERATE_NUMBER_PARTS) && is_digit(word_type));
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index