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

#include "olap/rowset/segment_v2/inverted_index/token_filter/lower_case_filter.h"
#include "olap/rowset/segment_v2/inverted_index/token_stream.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/icu/icu_tokenizer.h"

namespace doris::segment_v2 {

class ICUAnalyzer : public Analyzer {
public:
    ICUAnalyzer() {
        _lowercase = true;
        _ownReader = false;
    }

    ~ICUAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    void initDict(const std::string& dictPath) override { dictPath_ = dictPath; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                        "ICUAnalyzer::tokenStream not supported");
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                        "ICUAnalyzer::reusableTokenStream not supported");
    }

    TokenStream* tokenStream(const TCHAR* fieldName,
                             const inverted_index::ReaderPtr& reader) override {
        auto token_stream = create_components();
        token_stream->set_reader(reader);
        token_stream->get_token_stream()->reset();
        return new inverted_index::TokenStreamWrapper(token_stream->get_token_stream());
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     const inverted_index::ReaderPtr& reader) override {
        if (_reuse_token_stream == nullptr) {
            _reuse_token_stream = create_components();
        }
        _reuse_token_stream->set_reader(reader);
        return _reuse_token_stream->get_token_stream().get();
    };

private:
    inverted_index::TokenStreamComponentsPtr create_components() {
        auto tk = std::make_shared<inverted_index::ICUTokenizer>();
        tk->initialize(dictPath_);
        inverted_index::TokenStreamPtr ts = tk;
        if (_lowercase) {
            auto lower_case_filter = std::make_shared<inverted_index::LowerCaseFilter>(tk);
            lower_case_filter->initialize();
            ts = lower_case_filter;
        }
        return std::make_shared<inverted_index::TokenStreamComponents>(tk, ts);
    }

    std::string dictPath_;
    inverted_index::TokenStreamComponentsPtr _reuse_token_stream;
};

} // namespace doris::segment_v2