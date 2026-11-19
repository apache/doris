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

#include "storage/index/inverted/analysis/analyzer/custom_analyzer.h"

#include <algorithm>
#include <string_view>

#include "common/logging.h"
#include "common/status.h"
#include "storage/index/inverted/analysis/analysis_factory_mgr.h"
#include "storage/index/inverted/analysis/analyzer/chain_analyzer.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

TextAnalyzerPtr CustomAnalyzer::build_custom_analyzer(
        const ImmutableCustomAnalyzerConfigPtr& config) {
    if (config == nullptr) {
        throw Exception(ErrorCode::ILLEGAL_STATE, "Null configuration detected.");
    }
    const auto& tokenizer_config = config->get_tokenizer_config();
    DORIS_CHECK(tokenizer_config != nullptr);
    return build_chain(config->get_char_filter_configs(), *tokenizer_config,
                       config->get_token_filter_configs());
}

TextAnalyzerPtr CustomAnalyzer::build_chain(const std::vector<ComponentConfigPtr>& char_filters,
                                            const ComponentConfig& tokenizer,
                                            const std::vector<ComponentConfigPtr>& token_filters) {
    auto& factories = AnalysisFactoryMgr::instance();
    std::vector<CharFilterFactoryPtr> char_filter_factories;
    for (const auto& filter_config : char_filters) {
        char_filter_factories.push_back(factories.create<CharFilterFactory>(
                filter_config->get_name(), filter_config->get_params()));
    }
    TokenizerFactoryPtr tokenizer_factory =
            factories.create<TokenizerFactory>(tokenizer.get_name(), tokenizer.get_params());
    std::vector<TokenFilterFactoryPtr> token_filter_factories;
    for (const auto& filter_config : token_filters) {
        token_filter_factories.push_back(factories.create<TokenFilterFactory>(
                filter_config->get_name(), filter_config->get_params()));
    }
    return std::make_shared<ChainAnalyzer>(std::move(char_filter_factories),
                                           std::move(tokenizer_factory),
                                           std::move(token_filter_factories));
}

CustomAnalyzerProvider::CustomAnalyzerProvider(ImmutableCustomAnalyzerConfigPtr config)
        : _config(std::move(config)) {
    // Built for every configuration: building validates it, gram family included, and it is the
    // analyzer of every configuration outside the gram family.
    TextAnalyzerPtr analyzer = CustomAnalyzer::build_custom_analyzer(_config);
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
    // A gram-family configuration has no filter, so the GramTokenizer is its whole chain.
    if (_gram_scheme.has_value()) {
        _analyzer = std::make_shared<GramTextAnalyzer>(*_gram_scheme);
    } else {
        _analyzer = std::move(analyzer);
    }
}

} // namespace doris::segment_v2::inverted_index
