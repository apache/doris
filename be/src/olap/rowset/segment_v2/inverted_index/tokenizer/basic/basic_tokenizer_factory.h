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

#include "basic_tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {
class BasicTokenizerFactory : public TokenizerFactory {
public:
    BasicTokenizerFactory() = default;
    ~BasicTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {
        _extra_chars = settings.get_string("extra_chars", "");
        for (uint8_t c : _extra_chars) {
            if (c >= 128) {
                throw Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid extra_chars for basic tokenizer: contains non-ASCII character");
            }
        }
    }

    TokenizerPtr create() override {
        auto tokenzier = std::make_shared<BasicTokenizer>();
        tokenzier->initialize(_extra_chars);
        return tokenzier;
    }

private:
    std::string _extra_chars;
};

} // namespace doris::segment_v2::inverted_index