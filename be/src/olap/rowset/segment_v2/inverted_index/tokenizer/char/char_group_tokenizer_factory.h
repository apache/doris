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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class CharGroupTokenizerFactory : public TokenizerFactory {
public:
    CharGroupTokenizerFactory() = default;
    ~CharGroupTokenizerFactory() override = default;

    void initialize(const Settings& settings) override;

    TokenizerPtr create() override;

private:
    static UChar32 parse_escaped_char(const icu::UnicodeString& unicode_str);

    std::unordered_set<UChar32> _tokenize_on_chars;

    int32_t _max_token_length = 0;

    bool _tokenize_on_space = false;
    bool _tokenize_on_letter = false;
    bool _tokenize_on_digit = false;
    bool _tokenize_on_punctuation = false;
    bool _tokenize_on_symbol = false;
    bool _tokenize_on_cjk = false;
};
using CharGroupTokenizerFactoryPtr = std::shared_ptr<CharGroupTokenizerFactory>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index