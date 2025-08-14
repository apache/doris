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

#include "olap/rowset/segment_v2/inverted_index/token_filter/ascii_folding_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/lower_case_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/char/char_group_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/edge_ngram_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/standard/standard_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

void AnalysisFactoryMgr::initialise() {
    static std::once_flag once_flag;
    std::call_once(once_flag, [this]() {
        // tokenizer
        registerFactory("standard", []() { return std::make_shared<StandardTokenizerFactory>(); });
        registerFactory("keyword", []() { return std::make_shared<KeywordTokenizerFactory>(); });
        registerFactory("ngram", []() { return std::make_shared<NGramTokenizerFactory>(); });
        registerFactory("edge_ngram",
                        []() { return std::make_shared<EdgeNGramTokenizerFactory>(); });
        registerFactory("char_group",
                        []() { return std::make_shared<CharGroupTokenizerFactory>(); });

        // token_filter
        registerFactory("lowercase", []() { return std::make_shared<LowerCaseFilterFactory>(); });
        registerFactory("asciifolding",
                        []() { return std::make_shared<ASCIIFoldingFilterFactory>(); });
        registerFactory("word_delimiter",
                        []() { return std::make_shared<WordDelimiterFilterFactory>(); });
    });
}

void AnalysisFactoryMgr::registerFactory(const std::string& name, FactoryCreator creator) {
    registry_[name] = std::move(creator);
}

template <typename FactoryType>
std::shared_ptr<FactoryType> AnalysisFactoryMgr::create(const std::string& name,
                                                        const Settings& params) {
    if (registry_.empty()) {
        initialise();
    }

    auto it = registry_.find(name);
    if (it == registry_.end()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown factory name: {}", name);
    }

    auto factory = std::static_pointer_cast<FactoryType>(it->second());
    factory->initialize(params);
    return factory;
}

template std::shared_ptr<TokenizerFactory> AnalysisFactoryMgr::create<TokenizerFactory>(
        const std::string&, const Settings&);

template std::shared_ptr<TokenFilterFactory> AnalysisFactoryMgr::create<TokenFilterFactory>(
        const std::string&, const Settings&);

} // namespace doris::segment_v2::inverted_index