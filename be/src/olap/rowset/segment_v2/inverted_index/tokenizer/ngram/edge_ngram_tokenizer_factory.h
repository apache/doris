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
#include "edge_ngram_tokenizer.h"
#include "ngram_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/setting.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class EdgeNGramTokenizerFactory : public TokenizerFactory {
public:
    EdgeNGramTokenizerFactory() = default;
    ~EdgeNGramTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {
        _min_gram = settings.get_int("min_gram", EdgeNGramTokenizer::DEFAULT_MIN_NGRAM_SIZE);
        _max_gram = settings.get_int("max_gram", EdgeNGramTokenizer::DEFAULT_MAX_NGRAM_SIZE);
        _matcher = NGramTokenizerFactory::parse_token_chars(settings);
    }

    TokenizerPtr create() override {
        if (_matcher == nullptr) {
            return std::make_shared<EdgeNGramTokenizer>(_min_gram, _max_gram);
        } else {
            class EdgeNGramTokenizerWithMatcher : public EdgeNGramTokenizer {
            public:
                EdgeNGramTokenizerWithMatcher(int32_t min_gram, int32_t max_gram,
                                              CharMatcherPtr matcher)
                        : EdgeNGramTokenizer(min_gram, max_gram), _matcher(std::move(matcher)) {}

                bool is_token_char(UChar32 chr) override { return _matcher->is_token_char(chr); }

            private:
                CharMatcherPtr _matcher;
            };
            return std::make_shared<EdgeNGramTokenizerWithMatcher>(_min_gram, _max_gram, _matcher);
        }
    }

private:
    int32_t _min_gram = 0;
    int32_t _max_gram = 0;
    CharMatcherPtr _matcher;
};

} // namespace doris::segment_v2::inverted_index