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
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

// The tokenizer of the english parser and of an unknown one: the runs of ASCII letters and digits,
// a run longer than MAX_WORD_LEN bytes cut into pieces of at most MAX_WORD_LEN bytes, each one
// position after the last; with `lowercase`, A-Z folded to a-z. CLucene's SimpleTokenizer<char>
// made the same tokens.
//
// It classifies the value a block of BYTE_CLASS_BLOCK bytes at a time and cuts at the edges of the
// letter-and-digit bits. A batch is handed over at the end of the block that fills it, so it may
// hold a few dozen tokens more than kBatchTokens. The terms are views of the value, or of its
// lowered copy, which keeps the offsets of the value.
class EnglishTokenizer final : public TextTokenizer {
public:
    explicit EnglishTokenizer(bool lowercase) : _lowercase(lowercase) {}

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The lowered copy of the value, which only a lowercasing tokenizer has.
    size_t reserved_bytes() const override { return _lowercase ? _lowered.capacity() : 0; }

private:
    template <bool kLower>
    bool scan(TokenBatch& batch);

    const bool _lowercase;
    std::string_view _text;
    // The value with A-Z folded, byte for byte at the offsets of the value; only with _lowercase.
    std::string _lowered;
    size_t _block = 0;       // the next block to classify
    uint64_t _carry = 0;     // 1 when the last byte of the previous block is a letter or digit
    int64_t _run_start = -1; // the offset of the run being read, or -1 between runs
};

// The tokenizer of the builtin english parser; english is no custom tokenizer.
class EnglishTokenizerFactory final : public TokenizerFactory {
public:
    explicit EnglishTokenizerFactory(bool lowercase) : _lowercase(lowercase) {}

    void initialize(const Settings& settings) override {}

    std::unique_ptr<TextTokenizer> create() const override {
        return std::make_unique<EnglishTokenizer>(_lowercase);
    }

private:
    const bool _lowercase;
};

} // namespace doris::segment_v2::inverted_index
