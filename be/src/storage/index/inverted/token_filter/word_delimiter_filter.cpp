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

#include "storage/index/inverted/token_filter/word_delimiter_filter.h"

#include <unicode/utf8.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string_view>

#include "storage/index/inverted/token_filter/token_filter.h"

namespace doris::segment_v2::inverted_index {

WordDelimiterFilter::WordDelimiterFilter(const TokenStreamPtr& in,
                                         std::vector<char> char_type_table,
                                         int32_t configuration_flags,
                                         std::unordered_set<std::string> prot_words)
        : DorisTokenFilter(in),
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
            _saved_start_offset = t->startOffset();
            _saved_end_offset = t->endOffset();
            save_source_state(term);

            _accum_pos_inc += get_position_increment(t);
            _iterator->set_text(term.data(), static_cast<int32_t>(term.size()));
            _iterator->next();

            if ((_iterator->_current == 0 && _iterator->_end == term_length) ||
                (!_prot_words.empty() &&
                 _prot_words.find(std::string(term)) != _prot_words.end())) {
                set_position_increment(t, _accum_pos_inc);
                _accum_pos_inc = 0;
                _first = false;
                _current_source_byte_offsets = _saved_source_byte_offsets;
                _current_source_byte_end_offsets = _saved_source_byte_end_offsets;
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
                _current_source_byte_offsets = _saved_source_byte_offsets;
                _current_source_byte_end_offsets = _saved_source_byte_end_offsets;
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
                t->setStartOffset(_states[_buffered_pos - 1].token_start_offset);
                t->setEndOffset(_states[_buffered_pos - 1].token_end_offset);
                _current_source_byte_offsets = _states[_buffered_pos - 1].source_byte_offsets;
                _current_source_byte_end_offsets =
                        _states[_buffered_pos - 1].source_byte_end_offsets;
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
            t->setStartOffset(_attribute.token_start_offset);
            t->setEndOffset(_attribute.token_end_offset);
            _current_source_byte_offsets = _attribute.source_byte_offsets;
            _current_source_byte_end_offsets = _attribute.source_byte_end_offsets;
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
    _saved_source_byte_offsets.clear();
    _saved_source_byte_end_offsets.clear();
    _saved_token_byte_offsets.clear();
    _current_source_byte_offsets.clear();
    _current_source_byte_end_offsets.clear();
    release_oversized_scratch(_saved_source_byte_offsets);
    release_oversized_scratch(_saved_source_byte_end_offsets);
    release_oversized_scratch(_saved_token_byte_offsets);
    release_oversized_scratch(_current_source_byte_offsets);
    release_oversized_scratch(_current_source_byte_end_offsets);
    _concat->release_oversized_buffers();
    _concat_all->release_oversized_buffers();
    auto release_attribute = [](Attribute& attribute) {
        release_oversized_scratch(attribute.buffered);
        release_oversized_scratch(attribute.source_byte_offsets);
        release_oversized_scratch(attribute.source_byte_end_offsets);
    };
    release_attribute(_attribute);
    if (_states.capacity() * sizeof(Attribute) > ANALYZER_SCRATCH_HIGH_WATER_BYTES) {
        std::vector<Attribute>(INITIAL_BUFFERED_STATES).swap(_states);
    } else {
        std::ranges::for_each(_states, release_attribute);
    }
}

size_t WordDelimiterFilter::scratch_capacity_bytes_for_test() const {
    auto offsets_bytes = [](const std::vector<int32_t>& offsets) {
        return offsets.capacity() * sizeof(int32_t);
    };
    auto attribute_bytes = [&](const Attribute& attribute) {
        return attribute.buffered.capacity() + offsets_bytes(attribute.source_byte_offsets) +
               offsets_bytes(attribute.source_byte_end_offsets);
    };
    size_t bytes = offsets_bytes(_saved_source_byte_offsets) +
                   offsets_bytes(_saved_source_byte_end_offsets) +
                   offsets_bytes(_saved_token_byte_offsets) +
                   offsets_bytes(_current_source_byte_offsets) +
                   offsets_bytes(_current_source_byte_end_offsets) + attribute_bytes(_attribute) +
                   _concat->scratch_capacity_bytes() + _concat_all->scratch_capacity_bytes() +
                   _states.capacity() * sizeof(Attribute);
    for (const auto& state : _states) {
        bytes += attribute_bytes(state);
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
    _states[_buffered_len].source_byte_offsets = _attribute.source_byte_offsets;
    _states[_buffered_len].source_byte_end_offsets = _attribute.source_byte_end_offsets;
    _states[_buffered_len].token_start_offset = _attribute.token_start_offset;
    _states[_buffered_len].token_end_offset = _attribute.token_end_offset;
    _buffered_len++;
}

void WordDelimiterFilter::generate_part(bool is_single_word) {
    _attribute.buffered =
            _saved_buffer.substr(_iterator->_current, _iterator->_end - _iterator->_current);
    _attribute.start_off = _iterator->_current;
    _attribute.pos_inc = position(false);
    auto [source_byte_offsets, source_byte_end_offsets] =
            slice_source_byte_offsets(_iterator->_current, _iterator->_end);
    set_attribute_source_byte_offsets(std::move(source_byte_offsets),
                                      std::move(source_byte_end_offsets));
}

void WordDelimiterFilter::save_source_state(std::string_view term) {
    auto source_byte_offsets = DorisTokenFilter::get_source_byte_offsets();
    _saved_source_byte_offsets.assign(source_byte_offsets.begin(), source_byte_offsets.end());
    _saved_source_byte_end_offsets.clear();
    _saved_token_byte_offsets.clear();
    if (_saved_source_byte_offsets.empty()) {
        return;
    }

    _saved_token_byte_offsets.push_back(0);
    int32_t offset = 0;
    const auto length = static_cast<int32_t>(term.size());
    while (offset < length) {
        UChar32 codepoint;
        const char* term_data = term.data();
        U8_NEXT(term_data, offset, length, codepoint);
        _saved_token_byte_offsets.push_back(offset);
    }
    if (_saved_token_byte_offsets.size() != _saved_source_byte_offsets.size()) {
        _saved_source_byte_offsets.clear();
        _saved_token_byte_offsets.clear();
        return;
    }
    auto source_byte_end_offsets = DorisTokenFilter::get_source_byte_end_offsets();
    if (source_byte_end_offsets.empty()) {
        _saved_source_byte_end_offsets.assign(_saved_source_byte_offsets.begin() + 1,
                                              _saved_source_byte_offsets.end());
    } else {
        DORIS_CHECK_EQ(source_byte_end_offsets.size() + 1, _saved_source_byte_offsets.size());
        _saved_source_byte_end_offsets.assign(source_byte_end_offsets.begin(),
                                              source_byte_end_offsets.end());
    }
}

std::pair<std::vector<int32_t>, std::vector<int32_t>>
WordDelimiterFilter::slice_source_byte_offsets(int32_t start, int32_t end) const {
    if (_saved_source_byte_offsets.empty()) {
        return {};
    }
    auto start_it = std::ranges::lower_bound(_saved_token_byte_offsets, start);
    auto end_it = std::ranges::lower_bound(_saved_token_byte_offsets, end);
    if (start_it == _saved_token_byte_offsets.end() || *start_it != start ||
        end_it == _saved_token_byte_offsets.end() || *end_it != end || start_it > end_it) {
        return {};
    }
    const auto start_index = std::distance(_saved_token_byte_offsets.begin(), start_it);
    const auto end_index = std::distance(_saved_token_byte_offsets.begin(), end_it);
    std::vector<int32_t> offsets {_saved_source_byte_offsets.begin() + start_index,
                                  _saved_source_byte_offsets.begin() + end_index};
    std::vector<int32_t> ends {_saved_source_byte_end_offsets.begin() + start_index,
                               _saved_source_byte_end_offsets.begin() + end_index};
    DORIS_CHECK(!ends.empty());
    offsets.push_back(ends.back());
    return {std::move(offsets), std::move(ends)};
}

void WordDelimiterFilter::set_attribute_source_byte_offsets(
        std::vector<int32_t> source_byte_offsets, std::vector<int32_t> source_byte_end_offsets) {
    _attribute.token_start_offset = _saved_start_offset;
    _attribute.token_end_offset = _saved_end_offset;
    if (!source_byte_offsets.empty()) {
        const int32_t relative_start = source_byte_offsets.front();
        _attribute.token_start_offset += relative_start;
        _attribute.token_end_offset = _saved_start_offset + source_byte_offsets.back();
        for (int32_t& offset : source_byte_offsets) {
            offset -= relative_start;
        }
        for (int32_t& offset : source_byte_end_offsets) {
            offset -= relative_start;
        }
    }
    _attribute.source_byte_offsets = std::move(source_byte_offsets);
    _attribute.source_byte_end_offsets = std::move(source_byte_end_offsets);
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
