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

#include "ngram_tokenizer_factory.h"

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {

std::unordered_map<std::string, CharMatcherPtr> NGramTokenizerFactory::MATCHERS;

void NGramTokenizerFactory::initialize(const Settings& settings) {
    if (settings.contains("min_gram")) {
        _min_gram = std::get<int32_t>(settings.at("min_gram"));
    } else {
        _min_gram = NGramTokenizer::DEFAULT_MIN_NGRAM_SIZE;
    }
    if (settings.contains("max_gram")) {
        _max_gram = std::get<int32_t>(settings.at("max_gram"));
    } else {
        _max_gram = NGramTokenizer::DEFAULT_MAX_NGRAM_SIZE;
    }
    int32_t ngram_diff = _max_gram - _min_gram;
    if (ngram_diff > 1) {
        throw Exception(
                ErrorCode::INVALID_ARGUMENT,
                "The difference between max_gram and min_gram in NGram Tokenizer must be less "
                "than or equal to: [ 1 ] but was [" +
                        std::to_string(ngram_diff) + "]");
    }
    _matcher = parse_token_chars(settings);
}

void NGramTokenizerFactory::initialize_matchers() {
    static std::once_flag once_flag;
    std::call_once(once_flag, []() {
        MATCHERS["letter"] = std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::LETTER);
        MATCHERS["digit"] = std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::DIGIT);
        MATCHERS["whitespace"] =
                std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::WHITESPACE);
        MATCHERS["punctuation"] =
                std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::PUNCTUATION);
        MATCHERS["symbol"] = std::make_shared<BasicCharMatcher>(BasicCharMatcher::Type::SYMBOL);
    });
}

CharMatcherPtr NGramTokenizerFactory::parse_token_chars(const Settings& settings) {
    if (settings.empty()) {
        return nullptr;
    }
    auto iter = settings.find("token_chars");
    if (iter == settings.end()) {
        return nullptr;
    }
    auto characters = std::get<std::vector<std::string>>(iter->second);
    if (characters.empty()) {
        return nullptr;
    }
    CharMatcherBuilder builder;
    for (const auto& character : characters) {
        initialize_matchers();
        auto matcher = MATCHERS.find(character);
        if (matcher == MATCHERS.end()) {
            if (character != "custom") {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown token type: " + character);
            }
            auto iter = settings.find("custom_token_chars");
            if (iter == settings.end()) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Token type: 'custom' requires setting `custom_token_chars`");
            }
            auto custom_token_chars = std::get<std::string>(iter->second);
            auto custom_matcher = std::make_shared<CustomMatcher>(custom_token_chars);
            builder.add(custom_matcher);
        } else {
            builder.add(matcher->second);
        }
    }
    return builder.build();
}

} // namespace doris::segment_v2::inverted_index