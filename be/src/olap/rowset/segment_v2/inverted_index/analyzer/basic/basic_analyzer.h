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

#include <memory>

#include "basic_tokenizer.h"

namespace doris::segment_v2 {

class BasicAnalyzer : public Analyzer {
public:
    BasicAnalyzer() {
        _lowercase = true;
        _ownReader = false;
    }

    ~BasicAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override {
        auto* tokenizer = _CLNEW BasicTokenizer(_lowercase, _ownReader);
        tokenizer->reset(reader);
        return (TokenStream*)tokenizer;
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        if (_tokenizer == nullptr) {
            _tokenizer = std::make_unique<BasicTokenizer>(_lowercase, _ownReader);
        }
        _tokenizer->reset(reader);
        return (TokenStream*)_tokenizer.get();
    };

private:
    std::unique_ptr<BasicTokenizer> _tokenizer;
};

} // namespace doris::segment_v2