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
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class EmptyTokenizer : public DorisTokenizer {
public:
    EmptyTokenizer() = default;
    ~EmptyTokenizer() override = default;

    void initialize() {}

    Token* next(Token* token) override {
        if (!_done) {
            _done = true;
            if (_char_buffer == nullptr) {
                return nullptr;
            }
            std::string_view term(_char_buffer, _char_length);
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

private:
    bool _done = false;
    const char* _char_buffer = nullptr;
    int32_t _char_length = 0;
};

class EmptyTokenizerFactory : public TokenizerFactory {
public:
    EmptyTokenizerFactory() = default;
    ~EmptyTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {}

    TokenizerPtr create() override {
        auto tokenizer = std::make_shared<EmptyTokenizer>();
        tokenizer->initialize();
        return tokenizer;
    }
};

} // namespace doris::segment_v2::inverted_index