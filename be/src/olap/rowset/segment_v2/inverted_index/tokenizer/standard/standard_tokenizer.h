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

#include <cstddef>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer.h"
#include "standard_tokenizer_impl.h"

namespace doris::segment_v2::inverted_index {

class StandardTokenizer : public DorisTokenizer {
public:
    StandardTokenizer() { _scanner = std::make_unique<StandardTokenizerImpl>(); }

    Token* next(Token* t) override {
        // First, output any remaining part from a previously split token
        if (!_remaining_token_part.empty()) {
            size_t chunkSize =
                    std::min(_remaining_token_part.size(), static_cast<size_t>(_max_token_length));
            t->set(_remaining_token_part.data(), 0, chunkSize);
            if (chunkSize < _remaining_token_part.size()) {
                _remaining_token_part = _remaining_token_part.substr(chunkSize);
            } else {
                _remaining_token_part = "";
            }
            return t;
        }

        // Only get next token if we don't have any remaining parts to output
        while (true) {
            int32_t tokenType = _scanner->get_next_token();

            if (tokenType == StandardTokenizerImpl::YYEOF) {
                return nullptr;
            }

            std::string_view term = _scanner->get_text();
            size_t tokenLength = _scanner->yylength();
            if (tokenLength <= _max_token_length) {
                t->set(term.data(), 0, term.size());
                return t;
            } else {
                // Split the long token
                size_t chunkSize = _max_token_length;
                t->set(term.data(), 0, chunkSize);
                _remaining_token_part = term.substr(chunkSize);
                return t;
            }
        }

        return nullptr;
    }

    void reset() override {
        DorisTokenizer::reset();
        const char* _char_buffer = nullptr;
        size_t length = _in->read((const void**)&_char_buffer, 0, _in->size());
        _scanner->yyreset({_char_buffer, length});
    };

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
        }
    }

    static constexpr int32_t DEFAULT_MAX_TOKEN_LENGTH = 255;
    static constexpr int32_t MAX_TOKEN_LENGTH_LIMIT = 16383;

private:
    StandardTokenizerImplPtr _scanner;

    std::string_view _remaining_token_part;
    int32_t _max_token_length = DEFAULT_MAX_TOKEN_LENGTH;
};

} // namespace doris::segment_v2::inverted_index