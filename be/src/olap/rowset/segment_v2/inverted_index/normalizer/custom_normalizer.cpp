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

#include "custom_normalizer.h"

#include "olap/rowset/segment_v2/inverted_index/analysis_factory_mgr.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer_config.h"
#include "olap/rowset/segment_v2/inverted_index/token_stream.h"

namespace doris::segment_v2::inverted_index {

CustomNormalizer::CustomNormalizer(Builder* builder) {
    _keyword_tokenizer = AnalysisFactoryMgr::instance().create<TokenizerFactory>("keyword", {});

    _char_filters = std::move(builder->_char_filters);
    _token_filters = std::move(builder->_token_filters);
}

ReaderPtr CustomNormalizer::init_reader(ReaderPtr reader) {
    for (const auto& filter : _char_filters) {
        reader = filter->create(reader);
    }
    return reader;
}

TokenStreamComponentsPtr CustomNormalizer::create_components() {
    auto tk = _keyword_tokenizer->create();
    TokenStreamPtr ts = tk;
    for (const auto& filter : _token_filters) {
        ts = filter->create(ts);
    }
    return std::make_shared<TokenStreamComponents>(tk, ts);
}

TokenStream* CustomNormalizer::tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "CustomNormalizer does not support lucene::util::Reader");
}

TokenStream* CustomNormalizer::reusableTokenStream(const TCHAR* fieldName,
                                                   lucene::util::Reader* reader) {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "CustomNormalizer does not support lucene::util::Reader");
}

TokenStream* CustomNormalizer::tokenStream(const TCHAR* fieldName, const ReaderPtr& reader) {
    auto r = init_reader(reader);
    auto token_stream = create_components();
    token_stream->set_reader(r);
    token_stream->get_token_stream()->reset();
    return new TokenStreamWrapper(token_stream->get_token_stream());
}

TokenStream* CustomNormalizer::reusableTokenStream(const TCHAR* fieldName,
                                                   const ReaderPtr& reader) {
    auto r = init_reader(reader);
    if (_reuse_token_stream == nullptr) {
        _reuse_token_stream = create_components();
    }
    _reuse_token_stream->set_reader(r);
    return _reuse_token_stream->get_token_stream().get();
}

CustomNormalizerPtr CustomNormalizer::build_custom_normalizer(
        const CustomNormalizerConfigPtr& config) {
    if (config == nullptr) {
        throw Exception(ErrorCode::ILLEGAL_STATE, "Null configuration detected.");
    }
    CustomNormalizer::Builder builder;
    for (const auto& filter_config : config->get_char_filter_configs()) {
        builder.add_char_filter(filter_config->get_name(), filter_config->get_params());
    }
    for (const auto& filter_config : config->get_token_filter_configs()) {
        builder.add_token_filter(filter_config->get_name(), filter_config->get_params());
    }
    return builder.build();
}

void CustomNormalizer::Builder::add_char_filter(const std::string& name, const Settings& params) {
    _char_filters.push_back(AnalysisFactoryMgr::instance().create<CharFilterFactory>(name, params));
}

void CustomNormalizer::Builder::add_token_filter(const std::string& name, const Settings& params) {
    _token_filters.push_back(
            AnalysisFactoryMgr::instance().create<TokenFilterFactory>(name, params));
}

CustomNormalizerPtr CustomNormalizer::Builder::build() {
    if (_char_filters.empty() && _token_filters.empty()) {
        throw Exception(ErrorCode::ILLEGAL_STATE,
                        "Normalizer must have at least one char_filter or token_filter.");
    }
    return std::make_shared<CustomNormalizer>(this);
}

} // namespace doris::segment_v2::inverted_index