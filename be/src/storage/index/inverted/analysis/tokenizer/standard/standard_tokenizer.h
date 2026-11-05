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
#include <string_view>

#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.h"

namespace doris::segment_v2::inverted_index {

struct Uax29Table;

// The custom standard tokenizer: the UAX#29 words of the value, each one position after the last.
// It keeps the rules of StandardTokenizerImpl, the JFlex scanner it was, whose buffer of
// max_token_length bytes is the window a token is matched in: a character that does not fit ends
// the token, so a longer word is cut into pieces, and a character that does not fit an empty window
// ends the value. E.g. with max_token_length=2, "abc" makes "ab" and "c", and "ab中cd" makes "ab"
// and nothing after it.
//
// A value that is valid UTF-8 runs through the DFA of Uax29Table, and its terms are views of the
// value. Any other value runs through the scanner itself, whose reaction to invalid bytes depends
// on where its buffer refills; those terms are copied, because a refill moves the buffer.
class StandardTokenizer final : public TextTokenizer {
public:
    StandardTokenizer();

    void set_max_token_length(int32_t length);

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The scanner's window, max_token_length bytes.
    size_t reserved_bytes() const override { return _scanner->buffer_capacity(); }

    static constexpr int32_t DEFAULT_MAX_TOKEN_LENGTH = 255;
    static constexpr int32_t MAX_TOKEN_LENGTH_LIMIT = 16383;

private:
    bool next_valid_batch(TokenBatch& batch);
    bool next_scanner_batch(TokenBatch& batch);

    const Uax29Table& _table;
    StandardTokenizerImplPtr _scanner;
    int32_t _max_token_length = DEFAULT_MAX_TOKEN_LENGTH;
    std::string_view _text;
    bool _valid = true;      // the value is valid UTF-8
    size_t _pos = 0;         // where the next token of a valid value starts
    bool _exhausted = false; // the scanner reached the end of an invalid value
};

} // namespace doris::segment_v2::inverted_index
