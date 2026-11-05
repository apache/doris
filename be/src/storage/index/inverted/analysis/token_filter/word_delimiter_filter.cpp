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

#include "storage/index/inverted/analysis/token_filter/word_delimiter_filter.h"

#include <algorithm>
#include <memory>
#include <string_view>

namespace doris::segment_v2::inverted_index {

WordDelimiterFilter::WordDelimiterFilter(std::unique_ptr<TextTokenStream> in,
                                         std::vector<char> char_type_table,
                                         int32_t configuration_flags,
                                         std::unordered_set<std::string> prot_words)
        : PerTokenFilter(std::move(in)),
          _flags(configuration_flags),
          _prot_words(std::move(prot_words)),
          _states(INITIAL_BUFFERED_STATES) {
    _iterator = std::make_unique<WordDelimiterIterator>(char_type_table, has(SPLIT_ON_CASE_CHANGE),
                                                        has(SPLIT_ON_NUMERICS),
                                                        has(STEM_ENGLISH_POSSESSIVE));
    _concat = std::make_unique<WordDelimiterConcatenation>(*this);
    _concat_all = std::make_unique<WordDelimiterConcatenation>(*this);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size): keep the token emission state machine together.
bool WordDelimiterFilter::next_token(TokenRef& token) {
    while (true) {
        if (!_has_saved_state) {
            if (!input_token(token)) {
                return false;
            }
            // todo: has(IGNORE_KEYWORDS)
            const std::string_view term = token.term();
            const auto term_length = static_cast<int32_t>(term.size());

            _accum_pos_inc += token.position_increment;
            _iterator->set_text(term.data(), term_length);
            _iterator->next();

            if ((_iterator->_current == 0 && _iterator->_end == term_length) ||
                (!_prot_words.empty() &&
                 _prot_words.find(std::string(term)) != _prot_words.end())) {
                token.position_increment = _accum_pos_inc;
                _accum_pos_inc = 0;
                _first = false;
                return true;
            }

            if (_iterator->_end == WordDelimiterIterator::DONE && !has(PRESERVE_ORIGINAL)) {
                if (token.position_increment == 1 && !_first) {
                    _accum_pos_inc--;
                }
                continue;
            }

            save_state(term);

            _has_output_token = false;
            _has_output_following_original = !has(PRESERVE_ORIGINAL);
            _last_concat_count = 0;

            if (has(PRESERVE_ORIGINAL)) {
                token.position_increment = _accum_pos_inc;
                _accum_pos_inc = 0;
                _first = false;
                return true;
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
                const Attribute& state = _states[_buffered_pos];
                _buffered_pos++;
                int32_t position = state.pos_inc;
                if (_first && position == 0) {
                    position = 1;
                }
                token = {.data = state.buffered.data(),
                         .size = static_cast<uint32_t>(state.buffered.size()),
                         .position_increment = position};
                _first = false;
                return true;
            }

            _buffered_pos = _buffered_len = 0;
            _has_saved_state = false;
            continue;
        }

        if (_iterator->is_single_word()) {
            generate_part(true);
            _iterator->next();
            _first = false;
            token = {.data = _attribute.buffered.data(),
                     .size = static_cast<uint32_t>(_attribute.buffered.size()),
                     .position_increment = _attribute.pos_inc};
            return true;
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

void WordDelimiterFilter::on_reset() {
    _has_saved_state = false;
    _concat->clear();
    _concat_all->clear();
    _accum_pos_inc = 0;
    _buffered_pos = 0;
    _buffered_len = 0;
    _first = true;
    _concat->release_oversized_buffers();
    _concat_all->release_oversized_buffers();
    release_oversized_scratch(_attribute.buffered);
    if (_states.capacity() * sizeof(Attribute) > ANALYZER_SCRATCH_HIGH_WATER_BYTES) {
        std::vector<Attribute>(INITIAL_BUFFERED_STATES).swap(_states);
    } else {
        std::ranges::for_each(_states,
                              [](Attribute& state) { release_oversized_scratch(state.buffered); });
    }
}

size_t WordDelimiterFilter::reserved_bytes() const {
    size_t bytes = PerTokenFilter::reserved_bytes() + _attribute.buffered.capacity() +
                   _concat->scratch_capacity_bytes() + _concat_all->scratch_capacity_bytes() +
                   _states.capacity() * sizeof(Attribute);
    for (const Attribute& state : _states) {
        bytes += state.buffered.capacity();
    }
    return bytes;
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

} // namespace doris::segment_v2::inverted_index
