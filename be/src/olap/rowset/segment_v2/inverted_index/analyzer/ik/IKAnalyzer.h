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

#include "IKTokenizer.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class IKAnalyzer : public Analyzer {
public:
    IKAnalyzer() {
        _lowercase = true;
        _ownReader = false;
        config_ = std::make_shared<Configuration>(true, _lowercase);
    }

    ~IKAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    void initDict(const std::string& dictPath) override {
        try {
            config_->setDictPath(dictPath);
            Dictionary::initial(*config_);
        } catch (const CLuceneError& e) {
            LOG(ERROR) << "Failed to initialize dictionary: " << e.what();
            throw;
        }
    }

    void setMode(bool isSmart) { config_->setUseSmart(isSmart); }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override {
        auto* tokenizer = _CLNEW IKTokenizer(config_, _lowercase, _ownReader);
        tokenizer->reset(reader);
        return (TokenStream*)tokenizer;
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        if (tokenizer_ == nullptr) {
            tokenizer_ = std::make_unique<IKTokenizer>(config_, _lowercase, _ownReader);
        }
        tokenizer_->reset(reader);
        return (TokenStream*)tokenizer_.get();
    };

private:
    std::string dictPath_;
    std::unique_ptr<IKTokenizer> tokenizer_;
    std::shared_ptr<Configuration> config_;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
