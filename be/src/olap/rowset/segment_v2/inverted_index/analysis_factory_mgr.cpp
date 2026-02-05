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

#include "analysis_factory_mgr.h"

#include "olap/rowset/segment_v2/inverted_index/char_filter/char_replace_char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/empty_char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/icu_normalizer_char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/ascii_folding_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/empty_token_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/icu_normalizer_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/lower_case_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/pinyin_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/basic/basic_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/char/char_group_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/empty/empty_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/icu/icu_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/edge_ngram_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/standard/standard_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

void AnalysisFactoryMgr::initialise() {
    static std::once_flag once_flag;
    std::call_once(once_flag, [this]() {
        // char_filter
        registerFactory<CharFilterFactory>(
                "empty", []() { return std::make_shared<EmptyCharFilterFactory>(); });
        registerFactory<CharFilterFactory>(
                "char_replace", []() { return std::make_shared<CharReplaceCharFilterFactory>(); });
        registerFactory<CharFilterFactory>("icu_normalizer", []() {
            return std::make_shared<ICUNormalizerCharFilterFactory>();
        });

        // tokenizer
        registerFactory<TokenizerFactory>(
                "empty", []() { return std::make_shared<EmptyTokenizerFactory>(); });
        registerFactory<TokenizerFactory>(
                "standard", []() { return std::make_shared<StandardTokenizerFactory>(); });
        registerFactory<TokenizerFactory>(
                "keyword", []() { return std::make_shared<KeywordTokenizerFactory>(); });
        registerFactory<TokenizerFactory>(
                "ngram", []() { return std::make_shared<NGramTokenizerFactory>(); });
        registerFactory<TokenizerFactory>(
                "edge_ngram", []() { return std::make_shared<EdgeNGramTokenizerFactory>(); });
        registerFactory<TokenizerFactory>(
                "char_group", []() { return std::make_shared<CharGroupTokenizerFactory>(); });
        registerFactory<TokenizerFactory>(
                "basic", []() { return std::make_shared<BasicTokenizerFactory>(); });
        registerFactory<TokenizerFactory>("icu",
                                          []() { return std::make_shared<ICUTokenizerFactory>(); });
        registerFactory<TokenizerFactory>(
                "pinyin", []() { return std::make_shared<PinyinTokenizerFactory>(); });

        // token_filter
        registerFactory<TokenFilterFactory>(
                "empty", []() { return std::make_shared<EmptyTokenFilterFactory>(); });
        registerFactory<TokenFilterFactory>(
                "lowercase", []() { return std::make_shared<LowerCaseFilterFactory>(); });
        registerFactory<TokenFilterFactory>(
                "asciifolding", []() { return std::make_shared<ASCIIFoldingFilterFactory>(); });
        registerFactory<TokenFilterFactory>(
                "word_delimiter", []() { return std::make_shared<WordDelimiterFilterFactory>(); });
        registerFactory<TokenFilterFactory>(
                "pinyin", []() { return std::make_shared<PinyinFilterFactory>(); });
        registerFactory<TokenFilterFactory>(
                "icu_normalizer", []() { return std::make_shared<ICUNormalizerFilterFactory>(); });
    });
}

template <typename FactoryType>
void AnalysisFactoryMgr::registerFactory(const std::string& name, FactoryCreator creator) {
    RegistryKey key = {std::type_index(typeid(FactoryType)), name};
    registry_[key] = std::move(creator);
}

template <typename FactoryType>
std::shared_ptr<FactoryType> AnalysisFactoryMgr::create(const std::string& name,
                                                        const Settings& params) {
    if (registry_.empty()) {
        initialise();
    }

    RegistryKey key = {std::type_index(typeid(FactoryType)), name};
    auto it = registry_.find(key);
    if (it == registry_.end()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown factory name: {} for type: {}", name,
                        typeid(FactoryType).name());
    }

    auto factory = std::static_pointer_cast<FactoryType>(it->second());
    factory->initialize(params);
    return factory;
}

template std::shared_ptr<TokenizerFactory> AnalysisFactoryMgr::create<TokenizerFactory>(
        const std::string&, const Settings&);

template std::shared_ptr<TokenFilterFactory> AnalysisFactoryMgr::create<TokenFilterFactory>(
        const std::string&, const Settings&);

template std::shared_ptr<CharFilterFactory> AnalysisFactoryMgr::create<CharFilterFactory>(
        const std::string&, const Settings&);

} // namespace doris::segment_v2::inverted_index