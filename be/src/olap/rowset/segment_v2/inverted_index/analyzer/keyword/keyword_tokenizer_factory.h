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

#include "keyword_tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/setting.h"

namespace doris::segment_v2::inverted_index {

class KeywordTokenizerFactory {
public:
    KeywordTokenizerFactory() = default;
    ~KeywordTokenizerFactory() = default;

    void initialize(const Settings& settings) {
        if (settings.contains("max_token_len")) {
            _max_token_len = std::get<int32_t>(settings.at("max_token_len"));
        } else {
            _max_token_len = KeywordTokenizer::DEFAULT_BUFFER_SIZE;
        }
        if (_max_token_len > KeywordTokenizer::MAX_TOKEN_LENGTH_LIMIT || _max_token_len <= 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "maxTokenLen must be greater than 0 and less than " +
                                    std::to_string(KeywordTokenizer::MAX_TOKEN_LENGTH_LIMIT) +
                                    " passed: " + std::to_string(_max_token_len));
        }
    }

    KeywordTokenizerPtr create() const {
        auto tokenzier = std::make_shared<KeywordTokenizer>();
        tokenzier->initialize(_max_token_len);
        return tokenzier;
    }

private:
    int32_t _max_token_len = 0;
};

} // namespace doris::segment_v2::inverted_index