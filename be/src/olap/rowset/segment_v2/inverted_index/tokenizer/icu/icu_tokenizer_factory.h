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

#include "icu_tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class ICUTokenizerFactory : public TokenizerFactory {
public:
    ICUTokenizerFactory() = default;
    ~ICUTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {}

    TokenizerPtr create() override {
        auto tokenizer = std::make_shared<ICUTokenizer>();
        tokenizer->initialize(config::inverted_index_dict_path + "/icu");
        return tokenizer;
    }
};

} // namespace doris::segment_v2::inverted_index