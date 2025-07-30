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

#include <unicode/unistr.h>
#include <unicode/utext.h>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class NGramTokenizer : public DorisTokenizer {
public:
    NGramTokenizer(int32_t min_gram, int32_t max_gram, bool edges_only);
    NGramTokenizer(int32_t min_gram, int32_t max_gram);
    NGramTokenizer();
    ~NGramTokenizer() override = default;

    Token* next(Token* token) override;
    void reset() override;

    static constexpr int32_t DEFAULT_MIN_NGRAM_SIZE = 1;
    static constexpr int32_t DEFAULT_MAX_NGRAM_SIZE = 2;

private:
    void init(int32_t min_gram, int32_t max_gram, bool edges_only);
    void update_last_non_token_char();

    void consume() {
        auto c = static_cast<uint8_t>(_buffer[_buffer_start++]);
        _offset += U8_LENGTH(c);
    }

    virtual bool is_token_char(UChar32 chr) { return true; }

    std::pair<int32_t, int32_t> to_code_points(const char* char_buffer, int32_t char_offset,
                                               int32_t char_length, std::vector<UChar32>& buffer,
                                               int32_t buffer_end) const;

    void to_chars(const std::vector<UChar32>& buffer, int32_t start, int32_t size);

    int32_t _buffer_start = 0;
    int32_t _buffer_end = 0;
    int32_t _offset = 0;
    int32_t _gram_size = 0;
    int32_t _min_gram = 0;
    int32_t _max_gram = 0;
    bool _exhausted = false;
    int32_t _last_checked_char = 0;
    int32_t _last_non_token_char = 0;
    bool _edges_only = false;

    const char* _char_buffer = nullptr;
    int32_t _char_offset = 0;
    int32_t _char_length = 0;

    std::vector<UChar32> _buffer;
    std::string _utf8_buffer;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index