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

#include "char_tokenizer.h"

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

void CharTokenizer::initialize(int32_t max_token_len) {
    if (max_token_len > MAX_TOKEN_LENGTH_LIMIT || max_token_len <= 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "maxTokenLen must be greater than 0 and less than " +
                                std::to_string(MAX_TOKEN_LENGTH_LIMIT) +
                                " passed: " + std::to_string(max_token_len));
    }
    _max_token_len = max_token_len;
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

        UChar32 c = U_UNASSIGNED;
        const int32_t prev_i = _buffer_index;
        U8_NEXT(_char_buffer, _buffer_index, _data_len, c);
        if (c < 0) {
            continue;
        }

        if (is_cjk_char(c)) {
            if (start == -1) {
                start = prev_i;
                end = _buffer_index - 1;
            } else {
                _buffer_index = prev_i;
            }
            break;
        } else if (is_token_char(c)) {
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

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index