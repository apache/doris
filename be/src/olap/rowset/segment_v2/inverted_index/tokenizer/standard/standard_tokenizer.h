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
#include "standard_tokenizer_impl.h"

namespace doris::segment_v2::inverted_index {

class StandardTokenizer : public DorisTokenizer {
public:
    StandardTokenizer() { _scanner = std::make_unique<StandardTokenizerImpl>(); }

    Token* next(Token* t) override {
        _skipped_positions = 0;
        while (true) {
            int32_t token_type = _scanner->get_next_token();

            if (token_type == StandardTokenizerImpl::YYEOF) {
                return nullptr;
            }

            std::string_view term = _scanner->get_text();
            size_t token_length = _scanner->yylength();
            if (token_length <= _max_token_length) {
                set(t, term, _skipped_positions + 1);
                return t;
            } else {
                _skipped_positions++;
            }
        }
        return nullptr;
    }

    void reset() override {
        DorisTokenizer::reset();
        _scanner->yyreset(_in);
        _skipped_positions = 0;
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
            _scanner->set_buffer_size(length);
        }
    }

    static constexpr int32_t DEFAULT_MAX_TOKEN_LENGTH = 255;
    static constexpr int32_t MAX_TOKEN_LENGTH_LIMIT = 16383;

private:
    StandardTokenizerImplPtr _scanner;

    int32_t _skipped_positions = 0;
    int32_t _max_token_length = DEFAULT_MAX_TOKEN_LENGTH;
};

} // namespace doris::segment_v2::inverted_index