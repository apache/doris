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

#include "storage/index/inverted/tokenizer/tokenizer.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

class BasicTokenizer : public DorisTokenizer {
public:
    BasicTokenizer() = default;
    BasicTokenizer(bool own_reader);
    ~BasicTokenizer() override = default;

    void initialize(const std::string& extra_chars = "");

    // Set the tokenizer's lowercasing flag without going through
    // a wrapper LowerCaseFilter. When true, `cut()` lowercases each
    // alnum byte inline using the ASCII LUT — same semantics as
    // `LowerCaseFilter` for ASCII input, ~30 % faster end-to-end
    // because we skip ICU + the filter's per-token buffer copy.
    void set_lowercase(bool v) { this->lowercase = v; }

    Token* next(Token* token) override;
    void reset() override;

    // V4 fast path: expose the materialised token list directly so
    // callers can iterate without paying the virtual `next()` +
    // CLucene `Token` wrapping cost per token. After `reset()`
    // returns, `tokens_text()` holds the row's token slices in
    // emission order; positions are implicit (token index ==
    // position). String views point into `_buffer` which stays
    // valid until the next `reset()` call.
    const std::vector<std::string_view>& tokens_text() const { return _tokens_text; }

private:
    template <bool HasExtraChars>
    void cut();

    int32_t _buffer_index = 0;
    int32_t _data_len = 0;
    std::string _buffer;
    std::vector<std::string_view> _tokens_text;

    std::array<bool, 128> _extra_char_set {};
    bool _has_extra_chars = false;
};

} // namespace doris::segment_v2::inverted_index