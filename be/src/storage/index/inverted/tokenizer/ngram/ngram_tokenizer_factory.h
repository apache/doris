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

#include "common/status.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/inverted/setting.h"
#include "storage/index/inverted/tokenizer/ngram/char_matcher.h"
#include "storage/index/inverted/tokenizer/ngram/gram_tokenizer.h"
#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer.h"
#include "storage/index/inverted/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class NGramTokenizerFactory : public TokenizerFactory {
public:
    NGramTokenizerFactory() = default;
    ~NGramTokenizerFactory() override = default;

    void initialize(const Settings& settings) override;

    TokenizerPtr create() override {
        if (_gram_scheme.has_value()) {
            return std::make_shared<GramTokenizer>(*_gram_scheme);
        }
        if (_matcher == nullptr) {
            return std::make_shared<NGramTokenizer>(_min_gram, _max_gram);
        } else {
            class NGramTokenizerWithMatcher : public NGramTokenizer {
            public:
                NGramTokenizerWithMatcher(int32_t min_gram, int32_t max_gram,
                                          CharMatcherPtr matcher)
                        : NGramTokenizer(min_gram, max_gram), _matcher(std::move(matcher)) {}

                bool is_token_char(UChar32 chr) override { return _matcher->is_token_char(chr); }

            private:
                CharMatcherPtr _matcher;
            };
            return std::make_shared<NGramTokenizerWithMatcher>(_min_gram, _max_gram, _matcher);
        }
    }

    PositionCapability position_capability() const override {
        return PositionCapability::kAlwaysUnitIncrement;
    }

    // gram 族（tokenizer 属性含 "mode"）时返回方案；legacy ngram（"mode" 缺省）时为空，
    // 供 Task 8/12 判断当前 tokenizer 是否处于 gram 族以及取得其方案参数。
    std::optional<gram::GramScheme> gram_scheme() const { return _gram_scheme; }

    static void initialize_matchers();
    static CharMatcherPtr parse_token_chars(const Settings& settings);

    // 把 tokenizer Settings 映射为 GramScheme：这是该映射关系的唯一真源，initialize() 与
    // CustomAnalyzerProvider（gram/gram_family.h 的使用方）都必须调用它，不得各自复制一份
    // 属性解析逻辑（R16 DRY）。"mode" 缺省时 *out 置为 nullopt 并返回 OK（legacy ngram）；
    // 出现非法取值（如未知的 mode、越界的 min/max_gram）时返回 InvalidArgument，*out 保持 nullopt。
    // 未出现的键一律取 GramScheme 自身的成员初值（min_gram=3 / max_gram=16 等）。
    static Status parse_gram_scheme(const Settings& settings, std::optional<gram::GramScheme>* out);

private:
    static std::unordered_map<std::string, CharMatcherPtr> MATCHERS;

    int32_t _min_gram = 0;
    int32_t _max_gram = 0;
    CharMatcherPtr _matcher;
    std::optional<gram::GramScheme> _gram_scheme; // "mode" 存在时有值，此时 create() 走 gram 族
};

}; // namespace doris::segment_v2::inverted_index
