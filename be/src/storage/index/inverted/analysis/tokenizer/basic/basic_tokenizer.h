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
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/text_token_stream.h"

namespace doris::segment_v2::inverted_index {

// Runs of ASCII letters and digits, cut to MAX_WORD_LEN bytes with the rest of the run dropped;
// each character of five CJK ranges; and each extra char. Invalid bytes and other characters are
// skipped. With `lowercase`, A-Z is folded to a-z, which is all a lowercase filter does to these
// tokens.
//
// Without extra chars it classifies the value a block of BYTE_CLASS_BLOCK bytes at a time, cuts the
// runs at the edges of the letter-and-digit bits and decodes a character only at a byte that can
// start one: of the three bytes of 中 only the first is visited. A batch is handed over at the end
// of the block that fills it. With extra chars it runs the scalar loop, which has no performance
// target. The terms are views of the value or of its lowered copy, which keeps the offsets of the
// value.
class BasicTokenizer final : public TextTokenizer {
public:
    explicit BasicTokenizer(bool lowercase = false) : _lowercase(lowercase) {}

    // ASCII characters that are tokens of their own; none together with `lowercase`.
    void initialize(const std::string& extra_chars = "");

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The lowered copy of the value, which only a lowercasing tokenizer has, and the token list
    // of the scalar loop.
    size_t reserved_bytes() const override {
        return (_lowercase ? _lowered.capacity() : 0) +
               _tokens_text.capacity() * sizeof(std::string_view);
    }

private:
    template <bool kLower>
    bool scan(TokenBatch& batch);
    void cut_with_extra_chars(std::string_view text);
    bool hand_out(TokenBatch& batch);

    const bool _lowercase;
    std::string_view _text;
    // The value with A-Z folded, byte for byte at the offsets of the value; only with _lowercase.
    std::string _lowered;
    size_t _block = 0;       // the next block to classify
    uint64_t _carry = 0;     // 1 when the last byte of the previous block is a letter or digit
    int64_t _run_start = -1; // the offset of the run being read, or -1 between runs

    // The scalar loop cuts the whole value in reset().
    std::vector<std::string_view> _tokens_text;
    size_t _next = 0;

    std::array<bool, 128> _extra_char_set {};
    bool _has_extra_chars = false;
};

} // namespace doris::segment_v2::inverted_index
