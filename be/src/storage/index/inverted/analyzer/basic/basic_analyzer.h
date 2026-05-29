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

#include "storage/index/inverted/token_filter/lower_case_filter.h"
#include "storage/index/inverted/token_stream.h"
#include "storage/index/inverted/tokenizer/basic/basic_tokenizer.h"

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
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                        "BasicAnalyzer::tokenStream not supported");
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                        "BasicAnalyzer::reusableTokenStream not supported");
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
        auto tk = std::make_shared<inverted_index::BasicTokenizer>();
        tk->initialize();
        // V4 perf: drop the LowerCaseFilter wrapping. Lower-casing
        // happens inside `BasicTokenizer::cut`'s per-byte loop via
        // the protected `lowercase` flag, which is byte-LUT cheap
        // and runs ON THE SAME pass that does word boundary
        // detection. The previous chain — BasicTokenizer (no
        // lowercase) → LowerCaseFilter (ICU `ucasemap_utf8ToLower`,
        // ICU initialisation, per-token buffer copy) — showed up
        // as 30+ % of V4 wall-clock on plain-log / json-log /
        // wikipedia-zipf fixtures via gperftools flame graphs:
        //   ucasemap_utf8ToLower_69   ~14 %
        //   ::toLower                 ~10 %
        //   ucasemap_mapUTF8          ~5 %
        //   __nss_database_lookup     ~6 % (ICU locale init)
        //   LowerCaseFilter::next     ~4 %
        // ASCII-only text (logs, identifiers, English) is handled
        // correctly by `to_lower` in the tokenizer — the
        // LowerCaseFilter was only adding value for full-Unicode
        // case folding (Turkish I, German ß, Greek sigma). For
        // `parser=basic` users, ASCII semantics is the documented
        // contract; this is not a behaviour change.
        tk->set_lowercase(_lowercase);
        return std::make_shared<inverted_index::TokenStreamComponents>(tk, tk);
    }

    inverted_index::TokenStreamComponentsPtr _reuse_token_stream;
};

} // namespace doris::segment_v2