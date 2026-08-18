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

#include <array>
#include <map>
#include <optional>

#include "storage/index/inverted/analyzer/analyzer_provider.h"
#include "storage/index/inverted/analyzer/custom_analyzer_config.h"
#include "storage/index/inverted/char_filter/char_filter_factory.h"
#include "storage/index/inverted/setting.h"
#include "storage/index/inverted/token_filter/token_filter_factory.h"
#include "storage/index/inverted/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class CommonWordSet;
class CustomAnalyzer;
using CustomAnalyzerPtr = std::shared_ptr<CustomAnalyzer>;

class CustomAnalyzer : public Analyzer {
public:
    class Builder {
    public:
        Builder() = default;
        ~Builder() = default;

        void with_tokenizer(const std::string& name, const Settings& params);
        void add_char_filter(const std::string& name, const Settings& params);
        void add_token_filter(const std::string& name, const Settings& params);

        CustomAnalyzerPtr build();

    private:
        TokenizerFactoryPtr _tokenizer;
        std::vector<CharFilterFactoryPtr> _char_filters;
        std::vector<TokenFilterFactoryPtr> _token_filters;

        friend class CustomAnalyzer;
    };

    CustomAnalyzer(Builder* builder);
    ~CustomAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override;
    TokenStream* reusableTokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override;

    TokenStream* tokenStream(const TCHAR* fieldName, const ReaderPtr& reader) override;
    TokenStream* reusableTokenStream(const TCHAR* fieldName, const ReaderPtr& reader) override;

    static CustomAnalyzerPtr build_custom_analyzer(const ImmutableCustomAnalyzerConfigPtr& config);
    static CustomAnalyzerPtr build_custom_analyzer(const ImmutableCustomAnalyzerConfigPtr& config,
                                                   AnalysisPurpose purpose);
    static CustomAnalyzerPtr build_custom_analyzer(
            const ImmutableCustomAnalyzerConfigPtr& config, AnalysisPurpose purpose,
            const std::shared_ptr<const CommonWordSet>& common_words);

private:
    ReaderPtr init_reader(ReaderPtr reader);
    TokenStreamComponentsPtr create_components();

    TokenizerFactoryPtr _tokenizer;
    std::vector<CharFilterFactoryPtr> _char_filters;
    std::vector<TokenFilterFactoryPtr> _token_filters;

    TokenStreamComponentsPtr _reuse_token_stream;
};

class CustomAnalyzerProvider final : public AnalyzerProvider {
public:
    // The CommonGrams word list is not a parameter: it is the BE-local
    // CommonWordSet::default_word_set(), and the dictionary identity stamped into segments comes
    // from that set's content. An index policy cannot choose either one.
    explicit CustomAnalyzerProvider(ImmutableCustomAnalyzerConfigPtr config,
                                    std::map<std::string, std::string> outer_char_filter_map = {});

    std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            AnalysisPurpose purpose) const override;
    std::string_view base_analyzer_fingerprint() const override {
        return _base_analyzer_fingerprint;
    }
    bool uses_common_grams() const override { return _uses_common_grams; }
    const CommonGramsQueryIdentity* common_grams_identity() const override {
        return _common_grams_identity ? &*_common_grams_identity : nullptr;
    }
    const CommonWordSet* common_grams_word_set() const override {
        return _uses_common_grams ? _common_words.get() : nullptr;
    }
    const std::shared_ptr<const CommonWordSet>& common_words() const { return _common_words; }

    static std::string calculate_base_analyzer_fingerprint(
            const ImmutableCustomAnalyzerConfigPtr& config,
            const std::map<std::string, std::string>& outer_char_filter_map = {});

private:
    ImmutableCustomAnalyzerConfigPtr _config;
    const std::string _base_analyzer_fingerprint;
    std::shared_ptr<const CommonWordSet> _common_words;
    bool _uses_common_grams = false;
    std::optional<CommonGramsQueryIdentity> _common_grams_identity;
    std::array<std::shared_ptr<lucene::analysis::Analyzer>, 5> _analyzers;
};

} // namespace doris::segment_v2::inverted_index