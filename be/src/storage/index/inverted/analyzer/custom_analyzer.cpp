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
#include "storage/index/inverted/common_grams/common_word_set.h"
#include "storage/index/inverted/token_filter/common_grams_filter_factory.h"
#include "storage/index/inverted/token_stream.h"
#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer_factory.h"
#include "util/sha.h"

namespace doris::segment_v2::inverted_index {
namespace {

bool config_uses_common_grams(const ImmutableCustomAnalyzerConfigPtr& config) {
    DORIS_CHECK(config != nullptr);
    const auto& filter_configs = config->get_token_filter_configs();
    return std::any_of(filter_configs.begin(), filter_configs.end(),
                       [](const auto& entry) { return entry->get_name() == "common_grams"; });
}

void append_canonical_value(std::string_view value, std::string* output) {
    output->append(std::to_string(value.size()));
    output->push_back(':');
    output->append(value);
}

void append_component(std::string_view role, const ComponentConfigPtr& component,
                      std::string* output) {
    append_canonical_value(role, output);
    append_canonical_value(component->get_name(), output);
    const auto entries = component->get_params().sorted_entries();
    append_canonical_value(std::to_string(entries.size()), output);
    for (const auto& [key, value] : entries) {
        append_canonical_value(key, output);
        append_canonical_value(value, output);
    }
}

std::string sha256(std::string_view value) {
    SHA256Digest digest;
    digest.reset(value.data(), value.size());
    return std::string(digest.digest());
}

std::string calculate_base_analyzer_fingerprint_impl(
        const ImmutableCustomAnalyzerConfigPtr& config,
        const std::map<std::string, std::string>& outer_char_filter_map) {
    DORIS_CHECK(config != nullptr);
    std::string base;
    append_canonical_value("doris-common-grams-base-analyzer:v1", &base);
    append_canonical_value("outer_char_filter", &base);
    append_canonical_value(std::to_string(outer_char_filter_map.size()), &base);
    for (const auto& [key, value] : outer_char_filter_map) {
        append_canonical_value(key, &base);
        append_canonical_value(value, &base);
    }
    append_component("tokenizer", config->get_tokenizer_config(), &base);
    const auto char_filters = config->get_char_filter_configs();
    append_canonical_value(std::to_string(char_filters.size()), &base);
    for (const auto& char_filter : char_filters) {
        append_component("char_filter", char_filter, &base);
    }
    const auto token_filters = config->get_token_filter_configs();
    const auto base_token_filter_count = std::count_if(
            token_filters.begin(), token_filters.end(),
            [](const auto& token_filter) { return token_filter->get_name() != "common_grams"; });
    append_canonical_value(std::to_string(base_token_filter_count), &base);
    for (const auto& token_filter : token_filters) {
        if (token_filter->get_name() != "common_grams") {
            append_component("token_filter", token_filter, &base);
        }
    }
    return sha256(base);
}

CommonGramsQueryIdentity build_common_grams_identity(std::string dictionary_identity,
                                                     std::string base_analyzer_fingerprint) {
    std::string common_grams;
    append_canonical_value("doris-common-grams:v1", &common_grams);
    append_canonical_value(std::to_string(COMMON_GRAMS_SEMANTICS_VERSION_V1), &common_grams);
    append_canonical_value(std::to_string(COMMON_GRAMS_KEY_VERSION_V1), &common_grams);
    append_canonical_value(WORDSET_FORMAT_V1, &common_grams);
    append_canonical_value(dictionary_identity, &common_grams);
    return {.common_grams_dictionary_identity = std::move(dictionary_identity),
            .base_analyzer_fingerprint = std::move(base_analyzer_fingerprint),
            .common_grams_fingerprint = sha256(common_grams)};
}

std::array<std::shared_ptr<lucene::analysis::Analyzer>, 5> build_purpose_analyzers(
        const ImmutableCustomAnalyzerConfigPtr& config,
        const std::shared_ptr<const CommonWordSet>& common_words) {
    DORIS_CHECK(config != nullptr);
    const bool has_common_grams = config_uses_common_grams(config);
    if (!has_common_grams) {
        auto analyzer = CustomAnalyzer::build_custom_analyzer(config);
        return {analyzer, analyzer, analyzer, analyzer, analyzer};
    }
    return {CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kIndex, common_words),
            CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kSniiTransientIndex,
                                                  common_words),
            CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kPlainQuery,
                                                  common_words),
            CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kExactPhraseQuery,
                                                  common_words),
            CustomAnalyzer::build_custom_analyzer(config, AnalysisPurpose::kPhrasePrefixQuery,
                                                  common_words)};
}

} // namespace

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

CustomAnalyzerPtr CustomAnalyzer::build_custom_analyzer(
        const ImmutableCustomAnalyzerConfigPtr& config, AnalysisPurpose purpose) {
    return build_custom_analyzer(config, purpose, CommonWordSet::default_word_set());
}

CustomAnalyzerPtr CustomAnalyzer::build_custom_analyzer(
        const ImmutableCustomAnalyzerConfigPtr& config, AnalysisPurpose purpose,
        const std::shared_ptr<const CommonWordSet>& common_words) {
    if (config == nullptr) {
        throw Exception(ErrorCode::ILLEGAL_STATE, "Null configuration detected.");
    }

    CustomAnalyzer::Builder builder;
    for (const auto& filter_config : config->get_char_filter_configs()) {
        builder.add_char_filter(filter_config->get_name(), filter_config->get_params());
    }
    builder.with_tokenizer(config->get_tokenizer_config()->get_name(),
                           config->get_tokenizer_config()->get_params());

    const auto filter_configs = config->get_token_filter_configs();
    const size_t common_grams_count =
            std::count_if(filter_configs.begin(), filter_configs.end(),
                          [](const auto& entry) { return entry->get_name() == "common_grams"; });
    if (common_grams_count == 0) {
        for (const auto& filter_config : filter_configs) {
            builder.add_token_filter(filter_config->get_name(), filter_config->get_params());
        }
        return builder.build();
    }
    if (common_grams_count != 1 || filter_configs.back()->get_name() != "common_grams") {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "common_grams must appear exactly once as the terminal token filter");
    }
    if (builder._tokenizer->position_capability() != PositionCapability::kAlwaysUnitIncrement) {
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "CommonGrams tokenizer does not guarantee unit position increments");
    }

    for (size_t i = 0; i + 1 < filter_configs.size(); ++i) {
        auto factory = AnalysisFactoryMgr::instance().create<TokenFilterFactory>(
                filter_configs[i]->get_name(), filter_configs[i]->get_params());
        if (factory->position_capability() != PositionCapability::kAlwaysUnitIncrement) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "CommonGrams token filter '{}' does not guarantee unit position "
                            "increments",
                            filter_configs[i]->get_name());
        }
        builder._token_filters.push_back(std::move(factory));
    }

    auto common_grams = AnalysisFactoryMgr::instance().create<TokenFilterFactory>(
            filter_configs.back()->get_name(), filter_configs.back()->get_params());
    auto common_grams_factory = std::dynamic_pointer_cast<CommonGramsFilterFactory>(common_grams);
    DORIS_CHECK(common_grams_factory != nullptr);
    common_grams_factory->set_common_words(common_words);
    switch (purpose) {
    case AnalysisPurpose::kIndex:
        common_grams_factory->set_output_mode(CommonGramsOutputMode::kEscapedV1Index);
        builder._token_filters.push_back(std::move(common_grams));
        break;
    case AnalysisPurpose::kSniiTransientIndex:
        common_grams_factory->set_output_mode(CommonGramsOutputMode::kEscapedV1SpimiIndex);
        builder._token_filters.push_back(std::move(common_grams));
        break;
    case AnalysisPurpose::kPlainQuery: {
        auto factory = std::make_shared<CommonGramsPositionFilterFactory>();
        factory->initialize({});
        builder._token_filters.push_back(std::move(factory));
        break;
    }
    case AnalysisPurpose::kExactPhraseQuery: {
        builder._token_filters.push_back(std::move(common_grams));
        auto factory = std::make_shared<CommonGramsQueryFilterFactory>(common_words);
        factory->initialize({});
        builder._token_filters.push_back(std::move(factory));
        break;
    }
    case AnalysisPurpose::kPhrasePrefixQuery: {
        builder._token_filters.push_back(std::move(common_grams));
        auto factory = std::make_shared<CommonGramsPhrasePrefixFilterFactory>(common_words);
        factory->initialize({});
        builder._token_filters.push_back(std::move(factory));
        break;
    }
    }
    return builder.build();
}

CustomAnalyzerProvider::CustomAnalyzerProvider(
        ImmutableCustomAnalyzerConfigPtr config,
        std::map<std::string, std::string> outer_char_filter_map)
        : _config(std::move(config)),
          _base_analyzer_fingerprint(
                  calculate_base_analyzer_fingerprint(_config, outer_char_filter_map)),
          _uses_common_grams(config_uses_common_grams(_config)) {
    _common_words = CommonWordSet::default_word_set();
    _analyzers = build_purpose_analyzers(_config, _common_words);
    if (_uses_common_grams) {
        // Content-derived, so a BE reading a segment grammed against a different word list sees a
        // mismatched identity and falls back to the plain plan instead of trusting its grams.
        _common_grams_identity =
                build_common_grams_identity(_common_words->identity(), _base_analyzer_fingerprint);
    }
    // gram 族识别：tokenizer 是 "ngram" 时，把它的 Settings 交给
    // NGramTokenizerFactory::parse_gram_scheme 复用同一份属性映射（R16 DRY）。
    // build_purpose_analyzers() 已经在上面成功构造出该 tokenizer（否则会抛异常中断构造），
    // 因此这里重新解析同一份配置不会失败；解析失败时保持 nullopt 兜底，不重复上抛。
    //
    // R22（fail-safe）：带任何 char filter 或 token filter 的 analyzer 一律不算 gram 族。
    // gram 族的全部价值建立在"落库 term == GramExtractor.extract(原始列值)"这条行不变式上，
    // 查询侧（阶段 C）据此把正则改写成 gram 的合取式；char filter 会改写文本、token filter
    // 会改写/增删 gram，任一存在都让该等式不再成立。宁可退回全表扫描，也不能凭一个不成立的
    // 不变式漏行。
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

std::string CustomAnalyzerProvider::calculate_base_analyzer_fingerprint(
        const ImmutableCustomAnalyzerConfigPtr& config,
        const std::map<std::string, std::string>& outer_char_filter_map) {
    return calculate_base_analyzer_fingerprint_impl(config, outer_char_filter_map);
}

std::shared_ptr<lucene::analysis::Analyzer> CustomAnalyzerProvider::get_analyzer(
        AnalysisPurpose purpose) const {
    switch (purpose) {
    case AnalysisPurpose::kIndex:
        return _analyzers[0];
    case AnalysisPurpose::kSniiTransientIndex:
        return _analyzers[1];
    case AnalysisPurpose::kPlainQuery:
        return _analyzers[2];
    case AnalysisPurpose::kExactPhraseQuery:
        return _analyzers[3];
    case AnalysisPurpose::kPhrasePrefixQuery:
        return _analyzers[4];
    }
    __builtin_unreachable();
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