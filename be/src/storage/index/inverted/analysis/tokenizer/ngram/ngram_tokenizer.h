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

#include <unicode/umachine.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2::inverted_index {

// The n-grams of the value, min_gram to max_gram code points long, that hold token chars only;
// with edges_only, only those that start a run of token chars.
class NGramTokenizer : public PerTokenTokenizer {
public:
    NGramTokenizer(int32_t min_gram, int32_t max_gram, bool edges_only);
    NGramTokenizer(int32_t min_gram, int32_t max_gram);
    NGramTokenizer();
    ~NGramTokenizer() override = default;

    // Strict: invalid UTF-8 throws instead of being skipped.
    void set_strict_utf8(bool strict) override { _strict_utf8 = strict; }

    // The code point window, fixed by max_gram, and the scratch of one gram.
    size_t reserved_bytes() const override {
        return _buffer.capacity() * sizeof(UChar32) + _utf8_buffer.capacity();
    }

    static constexpr int32_t DEFAULT_MIN_NGRAM_SIZE = 1;
    static constexpr int32_t DEFAULT_MAX_NGRAM_SIZE = 2;

protected:
    void on_reset(std::string_view text) override;
    bool next_token(TokenRef& token) override;

private:
    void init(int32_t min_gram, int32_t max_gram, bool edges_only);
    void update_last_non_token_char();

    virtual bool is_token_char(UChar32 chr) { return true; }

    std::pair<int32_t, int32_t> to_code_points(const char* char_buffer, int32_t char_offset,
                                               int32_t char_length, std::vector<UChar32>& buffer,
                                               int32_t buffer_end) const;

    void to_chars(const std::vector<UChar32>& buffer, int32_t start, int32_t size);

    int32_t _buffer_start = 0;
    int32_t _buffer_end = 0;
    int32_t _gram_size = 0;
    int32_t _min_gram = 0;
    int32_t _max_gram = 0;
    bool _input_exhausted = false;
    int32_t _last_checked_char = 0;
    int32_t _last_non_token_char = 0;
    bool _edges_only = false;
    bool _strict_utf8 = false;

    const char* _char_buffer = nullptr;
    int32_t _char_offset = 0;
    int32_t _char_length = 0;

    std::vector<UChar32> _buffer;
    std::string _utf8_buffer;
};

} // namespace doris::segment_v2::inverted_index
