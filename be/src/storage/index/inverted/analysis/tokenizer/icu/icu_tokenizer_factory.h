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

#include "common/config.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

// The custom icu_tokenizer, and with `lowercase` the builtin icu parser, whose tokenizer
// lowercases its own terms.
class ICUTokenizerFactory : public TokenizerFactory {
public:
    explicit ICUTokenizerFactory(bool lowercase = false) : _lowercase(lowercase) {}
    ~ICUTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {}

    std::unique_ptr<TextTokenizer> create() const override {
        auto tokenizer = std::make_unique<ICUTokenizer>(_lowercase);
        tokenizer->initialize(config::inverted_index_dict_path + "/icu");
        return tokenizer;
    }

private:
    const bool _lowercase;
};

} // namespace doris::segment_v2::inverted_index