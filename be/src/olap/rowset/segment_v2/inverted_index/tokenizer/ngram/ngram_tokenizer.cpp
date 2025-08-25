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

#include "ngram_tokenizer.h"

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

NGramTokenizer::NGramTokenizer(int32_t min_gram, int32_t max_gram, bool edges_only) {
    init(min_gram, max_gram, edges_only);
}

NGramTokenizer::NGramTokenizer(int32_t min_gram, int32_t max_gram)
        : NGramTokenizer(min_gram, max_gram, false) {}

NGramTokenizer::NGramTokenizer() : NGramTokenizer(DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE) {}

Token* NGramTokenizer::next(Token* token) {
    while (true) {
        if (_buffer_start >= _buffer_end - _max_gram - 1 && !_exhausted) {
            std::copy(_buffer.data() + _buffer_start, _buffer.data() + _buffer_end, _buffer.data());
            _buffer_end -= _buffer_start;
            _last_checked_char -= _buffer_start;
            _last_non_token_char -= _buffer_start;
            _buffer_start = 0;

            if (_char_buffer != nullptr && _char_offset < _char_length) {
                auto [char_buffer_add, buffer_add] = to_code_points(
                        _char_buffer, _char_offset, _char_length, _buffer, _buffer_end);
                _char_offset += char_buffer_add;
                _buffer_end += buffer_add;
            }

            if (_char_buffer == nullptr || _char_offset >= _char_length) {
                _exhausted = true;
            }
        }

        if (_gram_size > _max_gram || (_buffer_start + _gram_size) > _buffer_end) {
            if (_buffer_start + 1 + _min_gram > _buffer_end) {
                assert(_exhausted);
                return nullptr;
            }
            consume();
            _gram_size = _min_gram;
        }

        update_last_non_token_char();

        bool term_contains_non_token_char = _last_non_token_char >= _buffer_start &&
                                            _last_non_token_char < (_buffer_start + _gram_size);
        bool is_edge_and_previous_char_is_token_char =
                _edges_only && _last_non_token_char != _buffer_start - 1;
        if (term_contains_non_token_char || is_edge_and_previous_char_is_token_char) {
            consume();
            _gram_size = _min_gram;
            continue;
        }

        to_chars(_buffer, _buffer_start, _gram_size);
        set(token, _utf8_buffer);
        ++_gram_size;

        return token;
    }
}

void NGramTokenizer::reset() {
    DorisTokenizer::reset();
    _buffer_start = _buffer_end = static_cast<int32_t>(_buffer.size());
    _last_non_token_char = _last_checked_char = _buffer_start - 1;
    _offset = 0;
    _gram_size = _min_gram;
    _exhausted = false;

    _char_buffer = nullptr;
    _char_offset = 0;
    _char_length = _in->read((const void**)&_char_buffer, 0, static_cast<int32_t>(_in->size()));
}

void NGramTokenizer::init(int32_t min_gram, int32_t max_gram, bool edges_only) {
    if (min_gram < 1) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "min_gram must be greater than zero");
    }
    if (min_gram > max_gram) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "min_gram must not be greater than max_gram");
    }
    _min_gram = min_gram;
    _max_gram = max_gram;
    _edges_only = edges_only;
    _buffer.resize(4 * max_gram + 1024);
}

void NGramTokenizer::update_last_non_token_char() {
    int32_t term_end = _buffer_start + _gram_size - 1;
    if (term_end > _last_checked_char) {
        for (int32_t i = term_end; i > _last_checked_char; --i) {
            if (!is_token_char(_buffer[i])) {
                _last_non_token_char = i;
                break;
            }
        }
        _last_checked_char = term_end;
    }
}

std::pair<int32_t, int32_t> NGramTokenizer::to_code_points(const char* char_buffer,
                                                           int32_t char_offset, int32_t char_length,
                                                           std::vector<UChar32>& buffer,
                                                           int32_t buffer_end) const {
    int32_t write_pos = buffer_end;
    int32_t i = char_offset;
    for (; i < char_length && write_pos < buffer.size();) {
        UChar32 c = U_UNASSIGNED;
        U8_NEXT(char_buffer, i, char_length, c);
        if (c < 0) {
            continue;
        }
        buffer[write_pos++] = c;
    }
    return {i - char_offset, write_pos - buffer_end};
}

void NGramTokenizer::to_chars(const std::vector<UChar32>& buffer, int32_t start, int32_t size) {
    icu::UnicodeString unistr;
    for (int32_t i = start; i < start + size; i++) {
        unistr.append(buffer[i]);
    }
    _utf8_buffer.clear();
    unistr.toUTF8String(_utf8_buffer);
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index
