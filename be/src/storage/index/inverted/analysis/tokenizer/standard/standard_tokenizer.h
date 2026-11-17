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

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/exception.h"
#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.h"

namespace doris::segment_v2::inverted_index {

// The words of the value by the Unicode word break rules (UAX#29); a word longer than
// max_token_length is dropped, and its position is skipped.
class StandardTokenizer final : public PerTokenTokenizer {
public:
    StandardTokenizer() { _scanner = std::make_unique<StandardTokenizerImpl>(); }

    void set_max_token_length(int32_t length) {
        if (length < 1) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "max_token_length must be greater than zero");
        } else if (length > MAX_TOKEN_LENGTH_LIMIT) {
            throw Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "max_token_length may not exceed " + std::to_string(MAX_TOKEN_LENGTH_LIMIT));
        }
        if (length != _max_token_length) {
            _max_token_length = length;
            _scanner->set_buffer_size(length);
        }
    }

    // The scan window, max_token_length bytes.
    size_t reserved_bytes() const override { return _scanner->buffer_capacity(); }

    static constexpr int32_t DEFAULT_MAX_TOKEN_LENGTH = 255;
    static constexpr int32_t MAX_TOKEN_LENGTH_LIMIT = 16383;

protected:
    void on_reset(std::string_view text) override { _scanner->yyreset(text); }

    bool next_token(TokenRef& token) override {
        int32_t skipped_positions = 0;
        while (true) {
            const auto token_type = _scanner->get_next_token();
            if (token_type == StandardTokenizerImpl::TokenType::EOF_TOKEN) {
                return false;
            }
            if (std::cmp_less_equal(_scanner->yylength(), _max_token_length)) {
                const std::string_view term = _scanner->get_text();
                token = {.data = term.data(),
                         .size = static_cast<uint32_t>(term.size()),
                         .position_increment = skipped_positions + 1};
                return true;
            }
            skipped_positions++;
        }
    }

private:
    StandardTokenizerImplPtr _scanner;
    int32_t _max_token_length = DEFAULT_MAX_TOKEN_LENGTH;
};

} // namespace doris::segment_v2::inverted_index
