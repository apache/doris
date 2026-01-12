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

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class CharTokenizer : public DorisTokenizer {
public:
    CharTokenizer() = default;
    ~CharTokenizer() override = default;

    void initialize(int32_t max_token_len);
    Token* next(Token* token) override;
    void reset() override;

    virtual bool is_cjk_char(UChar32 c) = 0;
    virtual bool is_token_char(UChar32 c) = 0;

    static constexpr int32_t DEFAULT_MAX_WORD_LEN = 255;

private:
    static constexpr int32_t MAX_TOKEN_LENGTH_LIMIT = 16383;

    int32_t _max_token_len = 0;

    int32_t _buffer_index = 0;
    int32_t _data_len = 0;
    const char* _char_buffer = nullptr;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index