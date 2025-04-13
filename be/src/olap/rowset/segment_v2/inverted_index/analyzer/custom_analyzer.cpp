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

#include <memory>
#include <unordered_map>

#include "olap/rowset/segment_v2/inverted_index/token_filter/ascii_folding_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/loser_case_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/edge_ngram_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/standard/standard_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

TokenizerFactoryPtr get_tokenizer_factory(const std::string& name, const Settings& params) {
    using FactoryCreator = std::function<TokenizerFactoryPtr()>;

    static const std::map<std::string, FactoryCreator> factoryCreators = {
            {"standard", []() { return std::make_shared<StandardTokenizerFactory>(); }},
            {"keyword", []() { return std::make_shared<KeywordTokenizerFactory>(); }},
            {"edge_ngram", []() { return std::make_shared<EdgeNGramTokenizerFactory>(); }}};

    auto it = factoryCreators.find(name);
    if (it != factoryCreators.end()) {
        auto tk = it->second();
        tk->initialize(params);
        return tk;
    } else {
        throw std::invalid_argument("Unknown tokenizer name: " + name);
    }
}

TokenFilterFactoryPtr get_token_filter_factory(const std::string& name, const Settings& params) {
    using FactoryCreator = std::function<TokenFilterFactoryPtr()>;

    static const std::map<std::string, FactoryCreator> factoryCreators = {
            {"lowercase", []() { return std::make_shared<LowerCaseFilterFactory>(); }},
            {"asciifolding", []() { return std::make_shared<ASCIIFoldingFilterFactory>(); }},
            {"word_delimiter", []() { return std::make_shared<WordDelimiterFilterFactory>(); }}};

    auto it = factoryCreators.find(name);
    if (it != factoryCreators.end()) {
        auto tk = it->second();
        tk->initialize(params);
        return tk;
    } else {
        throw std::invalid_argument("Unknown token filter name: " + name);
    }
}

CustomAnalyzer::CustomAnalyzer(Builder* builder) {
    _tokenizer = builder->_tokenizer;
    _token_filters = builder->_token_filters;
}

TokenStream* CustomAnalyzer::tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) {
    return nullptr;
}

TokenStream* CustomAnalyzer::reusableTokenStream(const TCHAR* fieldName,
                                                 lucene::util::Reader* reader) {
    if (_reuse_token_stream == nullptr) {
        _reuse_token_stream = create_components();
    }
    _reuse_token_stream->set_reader(reader);
    _reuse_token_stream->get_token_stream()->reset();
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
                           config->get_tokenizer_config()->get_param());
    for (const auto& filter_config : config->get_token_filter_configs()) {
        builder.add_token_filter(filter_config->get_name(), filter_config->get_param());
    }
    return builder.build();
}

void CustomAnalyzer::Builder::with_tokenizer(const std::string& name, const Settings& params) {
    _tokenizer = get_tokenizer_factory(name, params);
}

void CustomAnalyzer::Builder::add_token_filter(const std::string& name, const Settings& params) {
    _token_filters.emplace_back(get_token_filter_factory(name, params));
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

} // namespace doris::segment_v2::inverted_index