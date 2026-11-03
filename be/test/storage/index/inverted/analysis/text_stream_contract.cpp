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

#include "storage/index/inverted/analysis/text_stream_contract.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::inverted_index {

std::ostream& operator<<(std::ostream& out, const CollectedToken& token) {
    return out << "+" << token.position_increment << "\"" << token.term << "\"";
}

std::vector<CollectedToken> collect_checked(TextTokenStream& stream, std::string_view text) {
    constexpr std::string_view kStale = "stale token";
    std::vector<CollectedToken> tokens;
    TokenBatch batch;
    stream.reset(text);
    for (;;) {
        batch.push(kStale, 1);
        if (!stream.next_batch(batch)) {
            break;
        }
        EXPECT_GT(batch.size(), 0) << "next_batch() returned true with no token";
        for (const TokenRef& token : batch.tokens()) {
            EXPECT_GE(token.position_increment, 0);
            tokens.push_back({.term = std::string(token.term()),
                              .position_increment = token.position_increment});
        }
    }
    EXPECT_EQ(batch.size(), 0) << "next_batch() returned false with tokens in the batch";
    batch.push(kStale, 1);
    EXPECT_FALSE(stream.next_batch(batch)) << "next_batch() returned true after it returned false";
    EXPECT_EQ(batch.size(), 0);
    return tokens;
}

void expect_text_stream_contract(const TextStreamFactory& make_stream,
                                 const std::vector<std::string>& texts) {
    const std::unique_ptr<TextTokenStream> reused = make_stream();
    std::vector<std::vector<CollectedToken>> expected;
    for (const std::string& text : texts) {
        const std::unique_ptr<TextTokenStream> fresh = make_stream();
        expected.push_back(collect_checked(*fresh, text));
        EXPECT_EQ(collect_checked(*reused, text), expected.back())
                << "a reused stream disagrees with a new one on \"" << text << "\"";
    }

    // NFKC_CF can fold a value to an empty view that does not point to null.
    const std::string not_null = "x";
    EXPECT_TRUE(collect_checked(*reused, std::string_view()).empty());
    EXPECT_TRUE(collect_checked(*reused, std::string_view(not_null.data(), 0)).empty());

    // Each text cut after its first batch, then the next text, which comes out whole.
    for (size_t i = 0; i < texts.size(); ++i) {
        TokenBatch batch;
        reused->reset(texts[i]);
        reused->next_batch(batch);
        const size_t next = (i + 1) % texts.size();
        EXPECT_EQ(collect_checked(*reused, texts[next]), expected[next])
                << "\"" << texts[next] << "\" after a reset in the middle of \"" << texts[i]
                << "\"";
    }
}

} // namespace doris::segment_v2::inverted_index
