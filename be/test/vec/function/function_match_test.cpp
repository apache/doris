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

#include <gtest/gtest.h>

#include <memory>

#include "function_test_util.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/functions/match.h"

namespace doris::vectorized {

TEST(FunctionMatchTest, analyse_query_str) {
    FunctionMatchPhrase func_match_phrase;

    {
        auto inverted_index_ctx = nullptr;
        std::vector<std::string> query_tokens;
        func_match_phrase.analyse_query_str_token(&query_tokens, inverted_index_ctx, "a b c",
                                                  "name");
        ASSERT_EQ(query_tokens.size(), 0);
    }

    {
        auto inverted_index_ctx = std::make_unique<InvertedIndexCtx>();
        inverted_index_ctx->parser_type = InvertedIndexParserType::PARSER_NONE;
        std::vector<std::string> query_tokens;
        func_match_phrase.analyse_query_str_token(&query_tokens, inverted_index_ctx.get(), "a b c",
                                                  "name");
        ASSERT_EQ(query_tokens.size(), 1);
    }

    {
        auto inverted_index_ctx = std::make_unique<InvertedIndexCtx>();
        inverted_index_ctx->parser_type = InvertedIndexParserType::PARSER_ENGLISH;
        auto analyzer =
                doris::segment_v2::InvertedIndexReader::create_analyzer(inverted_index_ctx.get());
        inverted_index_ctx->analyzer = analyzer.get();
        std::vector<std::string> query_tokens;
        func_match_phrase.analyse_query_str_token(&query_tokens, inverted_index_ctx.get(), "a b c",
                                                  "name");
        ASSERT_EQ(query_tokens.size(), 3);
    }
}

} // namespace doris::vectorized