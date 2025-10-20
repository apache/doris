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

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

enum class BasicTokenizerMode {
    L1 = 1, // English + numbers + Chinese tokenization
    L2 = 2  // L1 + all Unicode characters tokenized
};

class BasicTokenizer : public DorisTokenizer {
public:
    BasicTokenizer() = default;
    BasicTokenizer(bool own_reader);
    ~BasicTokenizer() override = default;

    void initialize(BasicTokenizerMode mode);

    Token* next(Token* token) override;
    void reset() override;

private:
    template <BasicTokenizerMode mode>
    void cut();

    int32_t _buffer_index = 0;
    int32_t _data_len = 0;
    std::string _buffer;
    std::vector<std::string_view> _tokens_text;

    BasicTokenizerMode _mode = BasicTokenizerMode::L1;
};

} // namespace doris::segment_v2::inverted_index