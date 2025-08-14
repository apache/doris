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

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/analysis_factory_mgr.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer_config.h"
#include "olap/rowset/segment_v2/inverted_index/setting.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/token_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class TokenStreamComponents;
using TokenStreamComponentsPtr = std::shared_ptr<TokenStreamComponents>;

class CustomAnalyzer;
using CustomAnalyzerPtr = std::shared_ptr<CustomAnalyzer>;

class CustomAnalyzer : public Analyzer {
public:
    class Builder {
    public:
        Builder() = default;
        ~Builder() = default;

        void with_tokenizer(const std::string& name, const Settings& params);
        void add_token_filter(const std::string& name, const Settings& params);
        CustomAnalyzerPtr build();

    private:
        TokenizerFactoryPtr _tokenizer;
        std::vector<TokenFilterFactoryPtr> _token_filters;

        friend class CustomAnalyzer;
    };

    CustomAnalyzer(Builder* builder);
    ~CustomAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override;
    TokenStream* reusableTokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override;

    static CustomAnalyzerPtr build_custom_analyzer(const CustomAnalyzerConfigPtr& config);

private:
    TokenStreamComponentsPtr create_components();

    TokenizerFactoryPtr _tokenizer;
    std::vector<TokenFilterFactoryPtr> _token_filters;

    TokenStreamComponentsPtr _reuse_token_stream;
};

class TokenStreamComponents {
public:
    TokenStreamComponents(TokenizerPtr tokenizer, TokenStreamPtr result)
            : _source(std::move(tokenizer)), _sink(std::move(result)) {}

    void set_reader(CL_NS(util)::Reader* reader);
    TokenStreamPtr get_token_stream();
    TokenizerPtr get_source();

private:
    TokenizerPtr _source;
    TokenStreamPtr _sink;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index