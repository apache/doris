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

#include "storage/index/inverted/analyzer/custom_analyzer.h"

#include <algorithm>
#include <string_view>

#include "common/status.h"
#include "runtime/exec_env.h"
#include "storage/index/inverted/analysis_factory_mgr.h"
#include "storage/index/inverted/token_stream.h"
#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {
namespace {} // namespace

CustomAnalyzer::CustomAnalyzer(Builder* builder) {
    _tokenizer = builder->_tokenizer;
    _char_filters = builder->_char_filters;
    _token_filters = builder->_token_filters;
}

TokenStream* CustomAnalyzer::tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) {
    throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                    "CustomAnalyzer::tokenStream not supported");
}

TokenStream* CustomAnalyzer::reusableTokenStream(const TCHAR* fieldName,
                                                 lucene::util::Reader* reader) {
    throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED,
                    "CustomAnalyzer::reusableTokenStream not supported");
}

TokenStream* CustomAnalyzer::tokenStream(const TCHAR* fieldName, const ReaderPtr& reader) {
    auto r = init_reader(reader);
    auto token_stream = create_components();
    token_stream->set_reader(r);
    token_stream->get_token_stream()->reset();
    return new TokenStreamWrapper(token_stream->get_token_stream());
}

TokenStream* CustomAnalyzer::reusableTokenStream(const TCHAR* fieldName, const ReaderPtr& reader) {
    auto r = init_reader(reader);
    if (_reuse_token_stream == nullptr) {
        _reuse_token_stream = create_components();
    }
    _reuse_token_stream->set_reader(r);
    return _reuse_token_stream->get_token_stream().get();
}

ReaderPtr CustomAnalyzer::init_reader(ReaderPtr reader) {
    for (const auto& filter : _char_filters) {
        reader = filter->create(reader);
    }
    return reader;
}

TokenStreamComponentsPtr CustomAnalyzer::create_components() {
    auto tk = _tokenizer->create();
    TokenStreamPtr ts = tk;
    for (const auto& filter : _token_filters) {
        ts = filter->create(ts);
    }
    return std::make_shared<TokenStreamComponents>(tk, ts);
}

CustomAnalyzerPtr CustomAnalyzer::build_custom_analyzer(
        const ImmutableCustomAnalyzerConfigPtr& config) {
    if (config == nullptr) {
        throw Exception(ErrorCode::ILLEGAL_STATE, "Null configuration detected.");
    }
    CustomAnalyzer::Builder builder;
    for (const auto& filter_config : config->get_char_filter_configs()) {
        builder.add_char_filter(filter_config->get_name(), filter_config->get_params());
    }
    builder.with_tokenizer(config->get_tokenizer_config()->get_name(),
                           config->get_tokenizer_config()->get_params());
    for (const auto& filter_config : config->get_token_filter_configs()) {
        builder.add_token_filter(filter_config->get_name(), filter_config->get_params());
    }
    return builder.build();
}

CustomAnalyzerProvider::CustomAnalyzerProvider(
        ImmutableCustomAnalyzerConfigPtr config,
        std::map<std::string, std::string> /*outer_char_filter_map*/)
        : _config(std::move(config)), _analyzer(CustomAnalyzer::build_custom_analyzer(_config)) {
    // Gram-family detection: when the tokenizer is "ngram", hand its Settings to
    // NGramTokenizerFactory::parse_gram_scheme so the same property mapping is reused (R16,
    // DRY). build_custom_analyzer() has already constructed that tokenizer successfully above
    // (otherwise it would have thrown and aborted construction), so re-parsing the same
    // configuration here cannot fail; should it fail anyway, we fall back to nullopt rather than
    // rethrowing.
    //
    // R22 (fail-safe): an analyzer carrying any char filter or token filter is never treated as
    // gram family. The whole value of the gram family rests on the row invariant "the stored
    // term == GramExtractor.extract(raw column value)", which the query side (phase C) uses to
    // rewrite a regex into a conjunction of grams; a char filter rewrites the text and a token
    // filter rewrites, adds or removes grams, and either one breaks that equality. Better to
    // fall back to a full scan than to drop rows on an invariant that no longer holds.
    const bool has_filters = !_config->get_char_filter_configs().empty() ||
                             !_config->get_token_filter_configs().empty();
    if (const auto& tokenizer_config = _config->get_tokenizer_config();
        !has_filters && tokenizer_config != nullptr && tokenizer_config->get_name() == "ngram") {
        if (Status st = NGramTokenizerFactory::parse_gram_scheme(tokenizer_config->get_params(),
                                                                 &_gram_scheme);
            !st.ok()) {
            _gram_scheme.reset();
        }
    }
}
void CustomAnalyzer::Builder::with_tokenizer(const std::string& name, const Settings& params) {
    _tokenizer = AnalysisFactoryMgr::instance().create<TokenizerFactory>(name, params);
}

void CustomAnalyzer::Builder::add_char_filter(const std::string& name, const Settings& params) {
    _char_filters.push_back(AnalysisFactoryMgr::instance().create<CharFilterFactory>(name, params));
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

void TokenStreamComponents::set_reader(const ReaderPtr& reader) {
    _source->set_reader(reader);
}

TokenStreamPtr TokenStreamComponents::get_token_stream() {
    return _sink;
}

TokenizerPtr TokenStreamComponents::get_source() {
    return _source;
}

} // namespace doris::segment_v2::inverted_index