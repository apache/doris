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

#include "char_matcher.h"
#include "ngram_tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/setting.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class NGramTokenizerFactory : public TokenizerFactory {
public:
    NGramTokenizerFactory() = default;
    ~NGramTokenizerFactory() override = default;

    void initialize(const Settings& settings) override;

    TokenizerPtr create() override {
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

    static void initialize_matchers();
    static CharMatcherPtr parse_token_chars(const Settings& settings);

private:
    static std::unordered_map<std::string, CharMatcherPtr> MATCHERS;

    int32_t _min_gram = 0;
    int32_t _max_gram = 0;
    CharMatcherPtr _matcher;
};

}; // namespace doris::segment_v2::inverted_index