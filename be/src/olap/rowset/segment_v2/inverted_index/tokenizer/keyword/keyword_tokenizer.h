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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class KeywordTokenizer : public DorisTokenizer {
public:
    KeywordTokenizer() = default;
    ~KeywordTokenizer() override = default;

    void initialize(int32_t buffer_size = DEFAULT_BUFFER_SIZE) {
        if (buffer_size > MAX_TOKEN_LENGTH_LIMIT || buffer_size <= 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "maxTokenLen must be greater than 0 and less than " +
                                    std::to_string(MAX_TOKEN_LENGTH_LIMIT) +
                                    " passed: " + std::to_string(buffer_size));
        }
        _buffer_size = std::min(buffer_size, MAX_TOKEN_LENGTH_LIMIT);
    }

    Token* next(Token* token) override {
        if (!_done) {
            _done = true;
            if (_char_buffer == nullptr) {
                return nullptr;
            }
            int32_t length = std::min(_char_length, MAX_TOKEN_LENGTH_LIMIT);
            std::string_view term(_char_buffer, length);
            set(token, term);
            return token;
        }
        return nullptr;
    }

    void reset() override {
        DorisTokenizer::reset();
        _done = false;
        _char_buffer = nullptr;
        _char_length = _in->read((const void**)&_char_buffer, 0, static_cast<int32_t>(_in->size()));
    }

    static constexpr int32_t DEFAULT_BUFFER_SIZE = 256;
    static constexpr int32_t MAX_TOKEN_LENGTH_LIMIT = 8192;

private:
    bool _done = false;
    int32_t _buffer_size = 0;

    const char* _char_buffer = nullptr;
    int32_t _char_length = 0;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index