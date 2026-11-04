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

#include <optional>
#include <vector>

#include "storage/index/inverted/analysis/analyzer/analyzer_provider.h"
#include "storage/index/inverted/analysis/analyzer/custom_analyzer_config.h"
#include "storage/index/inverted/analysis/gram/gram_scheme.h"
#include "storage/index/inverted/analysis/text_analyzer.h"

namespace doris::segment_v2::inverted_index {

class CustomAnalyzer {
public:
    // The chain of a custom analyzer configuration: its char filters, its tokenizer and its token
    // filters. Throws when a component is unknown or its settings are invalid.
    static TextAnalyzerPtr build_custom_analyzer(const ImmutableCustomAnalyzerConfigPtr& config);

    // The chain of the components these configurations name, each made by AnalysisFactoryMgr.
    // Throws when a component is unknown or its settings are invalid.
    static TextAnalyzerPtr build_chain(const std::vector<ComponentConfigPtr>& char_filters,
                                       const ComponentConfig& tokenizer,
                                       const std::vector<ComponentConfigPtr>& token_filters);
};

class CustomAnalyzerProvider final : public AnalyzerProvider {
public:
    explicit CustomAnalyzerProvider(ImmutableCustomAnalyzerConfigPtr config);

    // A GramTextAnalyzer for the gram family, and otherwise the chain of the configuration.
    TextAnalyzerPtr get_analyzer() const override { return _analyzer; }
    // Computed once by the constructor from the analyzer configuration and cached: it holds a
    // value when the tokenizer is "ngram" with a non-empty "mode" property and the analyzer
    // carries no char filter or token filter, and is nullopt otherwise (R22 fail-safe: any
    // filter invalidates the row invariant "the stored term == GramExtractor.extract(raw column
    // value)"). The value is computed by NGramTokenizerFactory::parse_gram_scheme, keeping a
    // single source of truth with the tokenizer's own property parsing.
    std::optional<gram::GramScheme> gram_scheme() const override { return _gram_scheme; }

private:
    ImmutableCustomAnalyzerConfigPtr _config;
    std::optional<gram::GramScheme> _gram_scheme;
    TextAnalyzerPtr _analyzer;
};

} // namespace doris::segment_v2::inverted_index
