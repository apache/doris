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

#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer_factory.h"

#include <map>

#include "common/exception.h"

namespace doris::segment_v2::inverted_index {

std::unordered_map<std::string, CharMatcherPtr> NGramTokenizerFactory::MATCHERS;

Status NGramTokenizerFactory::parse_gram_scheme(const Settings& settings,
                                                std::optional<gram::GramScheme>* out) {
    out->reset();
    // "mode" 出现即进入 gram 族（sparse|dense|auto），与 legacy ngram 的滑窗窗口互斥；
    // gram 族的方案解析、校验、默认值全部委托给 GramScheme::from_properties，这里只负责把
    // tokenizer 属性透传过去。min/max_gram 缺省时由 GramScheme 的成员初值（3/16）给出，
    // 不在这里再注入一份副本——两处默认值早晚会漂移，唯一真源只能有一个。
    if (settings.get_string("mode").empty()) {
        return Status::OK();
    }
    std::map<std::string, std::string> props;
    for (const auto& [k, v] : settings.sorted_entries()) {
        props.emplace(k, v);
    }
    gram::GramScheme scheme;
    RETURN_IF_ERROR(gram::GramScheme::from_properties(props, &scheme));
    *out = scheme;
    return Status::OK();
}

void NGramTokenizerFactory::initialize(const Settings& settings) {
    std::optional<gram::GramScheme> scheme;
    Status st = parse_gram_scheme(settings, &scheme);
    if (!st.ok()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "ngram tokenizer: {}", st.to_string());
    }
    if (scheme.has_value()) {
        _gram_scheme = scheme;
        return; // 跳过 legacy 的 max-min>1 校验与 token_chars 解析
    }

    _min_gram = settings.get_int("min_gram", NGramTokenizer::DEFAULT_MIN_NGRAM_SIZE);
    _max_gram = settings.get_int("max_gram", NGramTokenizer::DEFAULT_MAX_NGRAM_SIZE);
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
    auto characters = settings.get_word_set("token_chars");
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
            auto custom_token_chars = settings.get_string("custom_token_chars");
            if (custom_token_chars.empty()) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Token type: 'custom' requires setting `custom_token_chars`");
            }
            auto custom_matcher = std::make_shared<CustomMatcher>(custom_token_chars);
            builder.add(custom_matcher);
        } else {
            builder.add(matcher->second);
        }
    }
    return builder.build();
}

} // namespace doris::segment_v2::inverted_index