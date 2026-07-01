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
#include <string>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analyzer/kuromoji/KuromojiTokenizer.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary.h"

namespace doris::segment_v2 {

class KuromojiAnalyzer : public Analyzer {
public:
    KuromojiAnalyzer() {
        _lowercase = true;
        _ownReader = false;
    }
    ~KuromojiAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    // Loads (once, process-wide) the IPADIC dictionary from `dictPath`. 
    void initDict(const std::string& dictPath) override {
        dict_ = inverted_index::kuromoji::KuromojiDictionary::get_or_load(dictPath);
        if (dict_ == nullptr) {
            throw doris::Exception(
                    doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                    "kuromoji dictionary could not be loaded from {}; ensure system.bin, "
                    "matrix.bin, chardef.bin and unkdict.bin are present in the BE package",
                    dictPath);
        }
    }

    void setMode(KuromojiMode mode) { mode_ = mode; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override {
        auto* tokenizer = _CLNEW KuromojiTokenizer(mode_, _lowercase, _ownReader, dict_);
        tokenizer->reset(reader);
        return (TokenStream*)tokenizer;
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        if (tokenizer_ == nullptr) {
            tokenizer_ = std::make_unique<KuromojiTokenizer>(mode_, _lowercase, _ownReader, dict_);
        }
        tokenizer_->reset(reader);
        return (TokenStream*)tokenizer_.get();
    }

private:
    const inverted_index::kuromoji::KuromojiDictionary* dict_ {nullptr};
    KuromojiMode mode_ {KuromojiMode::Search};
    std::unique_ptr<KuromojiTokenizer> tokenizer_;
};

} // namespace doris::segment_v2
