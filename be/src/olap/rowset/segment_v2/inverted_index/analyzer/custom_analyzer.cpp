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

#include "custom_analyzer.h"

#include "olap/rowset/segment_v2/inverted_index/analysis_factory_mgr.h"
#include "runtime/exec_env.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

CustomAnalyzer::CustomAnalyzer(Builder* builder) {
    _tokenizer = builder->_tokenizer;
    _token_filters = builder->_token_filters;
}

TokenStream* CustomAnalyzer::tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) {
    class TokenStreamWrapper : public TokenStream {
    public:
        explicit TokenStreamWrapper(std::shared_ptr<TokenStream> ts) : _impl(std::move(ts)) {}
        ~TokenStreamWrapper() override = default;

        Token* next(Token* token) override { return _impl->next(token); }
        void close() override { _impl->close(); }
        void reset() override { _impl->reset(); }

    private:
        std::shared_ptr<TokenStream> _impl;
    };
    auto token_stream = create_components();
    token_stream->set_reader(reader);
    token_stream->get_token_stream()->reset();
    return new TokenStreamWrapper(token_stream->get_token_stream());
}

TokenStream* CustomAnalyzer::reusableTokenStream(const TCHAR* fieldName,
                                                 lucene::util::Reader* reader) {
    if (_reuse_token_stream == nullptr) {
        _reuse_token_stream = create_components();
    }
    _reuse_token_stream->set_reader(reader);
    return _reuse_token_stream->get_token_stream().get();
}

TokenStreamComponentsPtr CustomAnalyzer::create_components() {
    auto tk = _tokenizer->create();
    TokenStreamPtr ts = tk;
    for (const auto& filter : _token_filters) {
        ts = filter->create(ts);
    }
    return std::make_shared<TokenStreamComponents>(tk, ts);
}

CustomAnalyzerPtr CustomAnalyzer::build_custom_analyzer(const CustomAnalyzerConfigPtr& config) {
    if (config == nullptr) {
        throw Exception(ErrorCode::ILLEGAL_STATE, "Null configuration detected.");
    }
    CustomAnalyzer::Builder builder;
    builder.with_tokenizer(config->get_tokenizer_config()->get_name(),
                           config->get_tokenizer_config()->get_params());
    for (const auto& filter_config : config->get_token_filter_configs()) {
        builder.add_token_filter(filter_config->get_name(), filter_config->get_params());
    }
    return builder.build();
}

void CustomAnalyzer::Builder::with_tokenizer(const std::string& name, const Settings& params) {
    _tokenizer = AnalysisFactoryMgr::instance().create<TokenizerFactory>(name, params);
}

void CustomAnalyzer::Builder::add_token_filter(const std::string& name, const Settings& params) {
    _token_filters.push_back(
            AnalysisFactoryMgr::instance().create<TokenFilterFactory>(name, params));
}

CustomAnalyzerPtr CustomAnalyzer::Builder::build() {
    if (_tokenizer == nullptr) {
        throw Exception(ErrorCode::ILLEGAL_STATE, "You have to set at least a tokenizer.");
    }
    return std::make_shared<CustomAnalyzer>(this);
}

void TokenStreamComponents::set_reader(CL_NS(util)::Reader* reader) {
    _source->set_reader(reader);
}

TokenStreamPtr TokenStreamComponents::get_token_stream() {
    return _sink;
}

TokenizerPtr TokenStreamComponents::get_source() {
    return _source;
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index