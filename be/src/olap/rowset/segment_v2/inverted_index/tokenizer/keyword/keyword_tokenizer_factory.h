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
#include "olap/rowset/segment_v2/inverted_index/setting.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class KeywordTokenizerFactory : public TokenizerFactory {
public:
    KeywordTokenizerFactory() = default;
    ~KeywordTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {
        _buffer_size = settings.get_int("buffer_size", KeywordTokenizer::DEFAULT_BUFFER_SIZE);
    }

    TokenizerPtr create() override {
        auto tokenzier = std::make_shared<KeywordTokenizer>();
        tokenzier->initialize(_buffer_size);
        return tokenzier;
    }

private:
    int32_t _buffer_size = 0;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index