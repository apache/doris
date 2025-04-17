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

WordDelimiterFilter::WordDelimiterFilter(const TokenStreamPtr& in,
                                         std::vector<char> char_type_table,
                                         int32_t configuration_flags,
                                         std::unordered_set<std::string> prot_words)
        : DorisTokenFilter(in),
          _flags(configuration_flags),
          _prot_words(std::move(prot_words)),
          _saved_buffer(1024, 0),
          _states(8) {
    _attribute = std::make_shared<Token>();
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
            int32_t term_length = t->termLength<char>();
            std::string_view term(term_buffer, term_length);

            _accum_pos_inc += t->getPositionIncrement();
            _iterator->set_text(term_buffer, term_length);
            _iterator->next();

            if ((_iterator->_current == 0 && _iterator->_end == term_length) ||
                (!_prot_words.empty() &&
                 _prot_words.find(std::string(term)) != _prot_words.end())) {
                t->setPositionIncrement(_accum_pos_inc);
                _accum_pos_inc = 0;
                _first = false;
                return t;
            }

            if (_iterator->_end == WordDelimiterIterator::DONE && !has(PRESERVE_ORIGINAL)) {
                if (t->getPositionIncrement() == 1 && !_first) {
                    _accum_pos_inc--;
                }
                continue;
            }

            save_state(term);

            _has_output_token = false;
            _has_output_following_original = !has(PRESERVE_ORIGINAL);
            _last_concat_count = 0;

            if (has(PRESERVE_ORIGINAL)) {
                t->setPositionIncrement(_accum_pos_inc);
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
                              [](const State& a, const State& b) {
                                  if (a._start_off != b._start_off) {
                                      return a._start_off < b._start_off;
                                  }
                                  return a._pos_inc > b._pos_inc;
                              });
                }
                const auto& term = _states[_buffered_pos]._buffered;
                int32_t position = _states[_buffered_pos]._pos_inc;
                _buffered_pos++;
                t->set(term.data(), 0, term.size());
                t->setPositionIncrement(position);
                if (_first && t->getPositionIncrement() == 0) {
                    t->setPositionIncrement(1);
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
            t->set(_attribute->termBuffer<char>(), 0, _attribute->termLength<char>());
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
    _states[_buffered_len]._start_off = _attribute->startOffset();
    _states[_buffered_len]._pos_inc = _attribute->getPositionIncrement();
    _states[_buffered_len]._buffered =
            std::string_view(_attribute->termBuffer<char>(), _attribute->termLength<char>());
    _buffered_len++;
}

void WordDelimiterFilter::generate_part(bool is_single_word) {
    _attribute->set(_saved_buffer.data() + _iterator->_current, 0,
                    _iterator->_end - _iterator->_current);
    _attribute->setStartOffset(_iterator->_current);
    _attribute->setPositionIncrement(position(false));
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

} // namespace doris::segment_v2::inverted_index