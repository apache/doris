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

#include <string_view>

#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

// The whole value as one token.
class EmptyTokenizer final : public PerTokenTokenizer {
public:
    EmptyTokenizer() : PerTokenTokenizer(TermLifetime::kUntilReset) {}

    void initialize() {}

    // The token is a view of the value.
    size_t reserved_bytes() const override { return 0; }

protected:
    void on_reset(std::string_view text) override { _text = text; }

    bool next_token(TokenRef& token) override {
        if (_text.empty()) {
            return false;
        }
        token = {.data = _text.data(),
                 .size = static_cast<uint32_t>(_text.size()),
                 .position_increment = 1};
        _text = {};
        return true;
    }

private:
    // What is left of the value: all of it, or nothing once the token is out.
    std::string_view _text;
};

class EmptyTokenizerFactory : public TokenizerFactory {
public:
    EmptyTokenizerFactory() = default;
    ~EmptyTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {}

    std::unique_ptr<TextTokenizer> create() const override {
        auto tokenizer = std::make_unique<EmptyTokenizer>();
        tokenizer->initialize();
        return tokenizer;
    }
};

} // namespace doris::segment_v2::inverted_index
