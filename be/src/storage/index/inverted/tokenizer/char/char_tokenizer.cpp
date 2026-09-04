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

#include "storage/index/inverted/tokenizer/char/char_tokenizer.h"

#ifdef BE_TEST
#include <atomic>
#endif

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {

#ifdef BE_TEST
namespace {
std::atomic<uint64_t> g_non_ascii_decode_count {0};
} // namespace

namespace char_tokenizer_testing {

uint64_t non_ascii_decode_count() {
    return g_non_ascii_decode_count.load(std::memory_order_relaxed);
}

void reset_non_ascii_decode_count() {
    g_non_ascii_decode_count.store(0, std::memory_order_relaxed);
}

} // namespace char_tokenizer_testing
#endif

void CharTokenizer::initialize(int32_t max_token_len) {
    if (max_token_len > MAX_TOKEN_LENGTH_LIMIT || max_token_len <= 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "maxTokenLen must be greater than 0 and less than " +
                                std::to_string(MAX_TOKEN_LENGTH_LIMIT) +
                                " passed: " + std::to_string(max_token_len));
    }
    _max_token_len = max_token_len;
    for (size_t value = 0; value < _ascii_char_classes.size(); ++value) {
        const auto c = static_cast<UChar32>(value);
        _ascii_char_classes[value] =
                is_cjk_char(c)
                        ? AsciiCharClass::kCjk
                        : (is_token_char(c) ? AsciiCharClass::kToken : AsciiCharClass::kDelimiter);
    }
}

CharTokenizer::AsciiCharClass CharTokenizer::read_next_char_class() {
    const auto first_byte = static_cast<uint8_t>(_char_buffer[_buffer_index]);
    if (first_byte < _ascii_char_classes.size()) {
        ++_buffer_index;
        return _ascii_char_classes[first_byte];
    }
#ifdef BE_TEST
    g_non_ascii_decode_count.fetch_add(1, std::memory_order_relaxed);
#endif
    UChar32 c = U_UNASSIGNED;
    U8_NEXT(_char_buffer, _buffer_index, _data_len, c);
    if (c < 0) {
        return AsciiCharClass::kInvalid;
    }
    if (is_cjk_char(c)) {
        return AsciiCharClass::kCjk;
    }
    return is_token_char(c) ? AsciiCharClass::kToken : AsciiCharClass::kDelimiter;
}

Token* CharTokenizer::next(Token* token) {
    if (!token) {
        return nullptr;
    }

    int32_t start = -1;
    int32_t end = -1;
    while (true) {
        if (_buffer_index >= _data_len) {
            if (start == -1) {
                return nullptr;
            }
            break;
        }

        const int32_t prev_i = _buffer_index;
        const AsciiCharClass char_class = read_next_char_class();
        if (char_class == AsciiCharClass::kInvalid) {
            continue;
        }

        if (char_class == AsciiCharClass::kCjk) {
            if (start == -1) {
                start = prev_i;
                end = _buffer_index - 1;
            } else {
                _buffer_index = prev_i;
            }
            break;
        } else if (char_class == AsciiCharClass::kToken) {
            if (start == -1) {
                start = prev_i;
            }
            end = _buffer_index - 1;
            int32_t current_length = end - start + 1;
            if (current_length >= _max_token_len) {
                break;
            }
        } else if (start != -1) {
            break;
        }
    }

    int32_t length = end - start + 1;
    std::string_view term(_char_buffer + start, length);
    set(token, term);
    return token;
}

void CharTokenizer::reset() {
    DorisTokenizer::reset();

    _buffer_index = 0;
    _data_len = _in->read((const void**)&_char_buffer, 0, static_cast<int32_t>(_in->size()));
}

} // namespace doris::segment_v2::inverted_index