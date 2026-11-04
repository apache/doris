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
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

struct Uax29Table;

// The 33 stop words of the builtin standard and unicode parsers, the English set of Lucene's
// StandardAnalyzer.
std::span<const std::string_view> standard_stop_words();
// Whether a term, as the tokenizer makes it, is one of them: the comparison is byte for byte, so
// "The" is a stop word only once lowercased.
bool is_standard_stop_word(std::string_view term);

// The tokenizer of the builtin standard and unicode parsers, which make the tokens CLucene's
// standard95 tokenizer made. A value that fails lenient_utf8_valid() (utf8_validator.h) has none.
// Otherwise its tokens are the UAX#29 words: a token is scanned while it is shorter than
// MAX_TOKEN_LENGTH bytes, so its last character may end past it, and a token longer than
// MAX_TOKEN_LENGTH is dropped. With `lowercase`, A-Z in WORD tokens is folded to a-z; with
// `stop_words`, a WORD token that is a stop word is dropped. Every token is one position after the
// last kept one: the old tokenizer never set an increment, so a dropped token left no gap.
//
// Lowercasing folds the whole value into a copy at the same offsets, and the scan runs over the
// copy. That makes the same tokens because A-Z and a-z are one class of the DFA, and lowercases
// only WORD tokens because every accepting state reached through a letter is WORD; Uax29TableTest
// checks both on the table.
class Uax29Tokenizer final : public TextTokenizer {
public:
    Uax29Tokenizer(bool lowercase, bool stop_words);

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The lowered copy of the value, which only a lowercasing tokenizer has.
    size_t reserved_bytes() const override { return _lowercase ? _lowered.capacity() : 0; }

    // The scan window from a token start, and the longest token kept.
    static constexpr size_t MAX_TOKEN_LENGTH = 255;

private:
    const Uax29Table& _table;
    const bool _lowercase;
    const bool _stop_words;
    // The value or its lowered copy; empty for a value that failed the UTF-8 rule.
    std::string_view _text;
    // The value with A-Z folded, byte for byte at the offsets of the value; only with _lowercase.
    std::string _lowered;
    size_t _pos = 0; // where the next token starts
};

// The tokenizer of the builtin standard and unicode parsers; neither is a custom tokenizer.
class Uax29TokenizerFactory final : public TokenizerFactory {
public:
    Uax29TokenizerFactory(bool lowercase, bool stop_words)
            : _lowercase(lowercase), _stop_words(stop_words) {}

    void initialize(const Settings& settings) override {}

    std::unique_ptr<TextTokenizer> create() const override {
        return std::make_unique<Uax29Tokenizer>(_lowercase, _stop_words);
    }

private:
    const bool _lowercase;
    const bool _stop_words;
};

} // namespace doris::segment_v2::inverted_index
