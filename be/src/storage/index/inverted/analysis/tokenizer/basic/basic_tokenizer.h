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

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2::inverted_index {

// Runs of ASCII letters and digits, each Chinese char, and each extra char as tokens.
class BasicTokenizer final : public PerTokenTokenizer {
public:
    BasicTokenizer() : PerTokenTokenizer(TermLifetime::kUntilReset) {}

    void initialize(const std::string& extra_chars = "");

    // The tokens are views of the value; the list of them is kept.
    size_t reserved_bytes() const override {
        return _tokens_text.capacity() * sizeof(std::string_view);
    }

protected:
    void on_reset(std::string_view text) override;
    bool next_token(TokenRef& token) override;

private:
    template <bool HasExtraChars>
    void cut(std::string_view text);

    size_t _buffer_index = 0;
    std::vector<std::string_view> _tokens_text;

    std::array<bool, 128> _extra_char_set {};
    bool _has_extra_chars = false;
};

} // namespace doris::segment_v2::inverted_index
