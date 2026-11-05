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

#include <unicode/utf8.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "common/exception.h"
#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2::inverted_index {

// The whole value as one token, cut to MAX_TOKEN_LENGTH_LIMIT bytes on a rune boundary.
class KeywordTokenizer final : public PerTokenTokenizer {
public:
    KeywordTokenizer() : PerTokenTokenizer(TermLifetime::kUntilReset) {}

    void initialize(int32_t buffer_size = DEFAULT_BUFFER_SIZE) {
        if (buffer_size > MAX_TOKEN_LENGTH_LIMIT || buffer_size <= 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "maxTokenLen must be greater than 0 and less than " +
                                    std::to_string(MAX_TOKEN_LENGTH_LIMIT) +
                                    " passed: " + std::to_string(buffer_size));
        }
        _buffer_size = std::min(buffer_size, MAX_TOKEN_LENGTH_LIMIT);
    }

    // The token is a view of the value.
    size_t reserved_bytes() const override { return 0; }

    static constexpr int32_t DEFAULT_BUFFER_SIZE = 256;
    static constexpr int32_t MAX_TOKEN_LENGTH_LIMIT = 8192;

protected:
    void on_reset(std::string_view text) override { _text = text; }

    bool next_token(TokenRef& token) override {
        if (_text.empty()) {
            return false;
        }
        size_t length = std::min(_text.size(), static_cast<size_t>(MAX_TOKEN_LENGTH_LIMIT));
        while (length > 0 && length < _text.size() &&
               U8_IS_TRAIL(static_cast<uint8_t>(_text[length]))) {
            --length;
        }
        token = {.data = _text.data(),
                 .size = static_cast<uint32_t>(length),
                 .position_increment = 1};
        _text = {};
        return true;
    }

private:
    int32_t _buffer_size = 0;
    // What is left of the value: all of it, or nothing once the token is out.
    std::string_view _text;
};

} // namespace doris::segment_v2::inverted_index
