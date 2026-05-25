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

#include "storage/index/inverted/spimi/tee_token_stream.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Minimal stub TokenStream that emits a scripted sequence of (text,
// position_increment) pairs. Lets us exercise TeeTokenStream behavior
// without dragging in the full CLucene analyzer machinery.
class ScriptedTokenStream : public lucene::analysis::TokenStream {
public:
    struct Scripted {
        std::string text;
        int32_t position_increment = 1;
    };

    explicit ScriptedTokenStream(std::vector<Scripted> tokens) : _tokens(std::move(tokens)) {}

    lucene::analysis::Token* next(lucene::analysis::Token* token) override {
        if (_cursor >= _tokens.size()) {
            return nullptr;
        }
        const auto& t = _tokens[_cursor++];
        token->setText<char>(t.text.c_str(), static_cast<int32_t>(t.text.size()));
        token->setPositionIncrement(t.position_increment);
        // TeeTokenStream only consumes termBuffer/termLength/
        // positionIncrement today, but populate offsets and type
        // defensively so future tap behavior expansions (start/end
        // offsets, type tagging) do not silently read uninitialised
        // state when this stub is reused.
        token->setStartOffset(0);
        token->setEndOffset(static_cast<int32_t>(t.text.size()));
        return token;
    }

    void close() override {}

    void reset() override { _cursor = _reset_cursor; }

    // Test seam: where reset() should rewind to. Default is the start
    // (re-emit everything); override to mid-stream to model an analyzer
    // chain that resets midway.
    void set_reset_cursor(size_t cursor) { _reset_cursor = cursor; }

private:
    std::vector<Scripted> _tokens;
    size_t _cursor = 0;
    size_t _reset_cursor = 0;
};

// Drain the tee, collecting the (term, doc_id, position) records into the
// buffer.
void DrainTee(TeeTokenStream& tee, std::vector<SpimiRecord>& sink_into,
              SpimiPostingBuffer& buffer) {
    lucene::analysis::Token tok;
    while (tee.next(&tok) != nullptr) {
        // The tee already appended to `buffer`; just observe the latest record
        // for the test assertions.
        if (!buffer.records().empty()) {
            sink_into.push_back(buffer.records().back());
        }
    }
}

} // namespace

TEST(TeeTokenStreamTest, FirstTokenPositionIsZero) {
    SpimiPostingBuffer buffer;
    ScriptedTokenStream upstream({{"hello", 1}, {"world", 1}});
    TeeTokenStream tee;
    tee.Configure(&upstream, &buffer, /*doc_id=*/7);

    std::vector<SpimiRecord> seen;
    DrainTee(tee, seen, buffer);

    ASSERT_EQ(seen.size(), 2U);
    EXPECT_EQ(seen[0].doc_id, 7U);
    EXPECT_EQ(seen[0].position, 0U) << "first token canonical position is 0";
    EXPECT_EQ(seen[1].position, 1U);
}

TEST(TeeTokenStreamTest, FirstTokenWithZeroIncrementClampsToZero) {
    // H-sec-5 — a synonym overlay as the very first token has
    // positionIncrement = 0. The tee must clamp the first position to
    // 0, not record 0xFFFFFFFF.
    SpimiPostingBuffer buffer;
    ScriptedTokenStream upstream({{"alpha", 0}, {"beta", 1}});
    TeeTokenStream tee;
    tee.Configure(&upstream, &buffer, /*doc_id=*/0);

    std::vector<SpimiRecord> seen;
    DrainTee(tee, seen, buffer);

    ASSERT_EQ(seen.size(), 2U);
    EXPECT_EQ(seen[0].position, 0U) << "first token clamp must hide -1 from the buffer";
    EXPECT_EQ(seen[1].position, 1U);
}

TEST(TeeTokenStreamTest, ResetDoesNotZeroPositionMidDoc) {
    // SPIMI_DESIGN.md § 9.0 — CLucene's reusable-stream protocol can
    // call reset() between filter passes within one doc. The tee must
    // NOT zero `_pos` in reset() because doing so would let two passes
    // overlap at position 0 and silently break phrase recall on chains
    // like CJK + n-gram.
    SpimiPostingBuffer buffer;
    ScriptedTokenStream upstream({{"a", 1}, {"b", 1}, {"c", 1}});
    upstream.set_reset_cursor(0); // reset re-emits from the start

    TeeTokenStream tee;
    tee.Configure(&upstream, &buffer, /*doc_id=*/3);

    // Consume the first two tokens.
    lucene::analysis::Token tok;
    ASSERT_NE(tee.next(&tok), nullptr); // "a" @ 0
    ASSERT_NE(tee.next(&tok), nullptr); // "b" @ 1
    ASSERT_EQ(buffer.RecordCount(), 2U);
    ASSERT_EQ(buffer.records().back().position, 1U);

    // Reset the stream — upstream rewinds, but TeeTokenStream::reset()
    // must NOT zero its own `_pos`. The next token should land at
    // position 2 (continuing the monotonic sequence) or higher, never
    // at 0.
    tee.reset();

    ASSERT_NE(tee.next(&tok), nullptr);
    ASSERT_EQ(buffer.RecordCount(), 3U);
    EXPECT_GT(buffer.records().back().position, buffer.records()[1].position)
            << "reset() must not collapse positions back to 0";
}

TEST(TeeTokenStreamTest, ConfigureIsThePerDocResetPoint) {
    // The intentional per-doc reset path: Configure() zeros `_pos` and
    // re-arms the first-token clamp. Two consecutive Configure() calls
    // produce two independent position streams.
    SpimiPostingBuffer buffer;
    TeeTokenStream tee;

    {
        ScriptedTokenStream upstream({{"x", 1}, {"y", 1}});
        tee.Configure(&upstream, &buffer, /*doc_id=*/0);
        lucene::analysis::Token tok;
        ASSERT_NE(tee.next(&tok), nullptr);
        ASSERT_NE(tee.next(&tok), nullptr);
    }
    EXPECT_EQ(buffer.records()[0].position, 0U);
    EXPECT_EQ(buffer.records()[1].position, 1U);

    {
        ScriptedTokenStream upstream({{"u", 1}, {"v", 1}});
        tee.Configure(&upstream, &buffer, /*doc_id=*/1);
        lucene::analysis::Token tok;
        ASSERT_NE(tee.next(&tok), nullptr);
        ASSERT_NE(tee.next(&tok), nullptr);
    }
    // The second doc's positions restart at 0 because Configure() ran.
    EXPECT_EQ(buffer.records()[2].doc_id, 1U);
    EXPECT_EQ(buffer.records()[2].position, 0U);
    EXPECT_EQ(buffer.records()[3].position, 1U);
}

TEST(TeeTokenStreamTest, EmptyTermsAreDroppedNotAppended) {
    // The tee skips term_len==0 to match what the SPIMI analyzer
    // pipeline guarantees. Make sure that behavior is preserved.
    SpimiPostingBuffer buffer;
    ScriptedTokenStream upstream({{"", 1}, {"x", 1}});
    TeeTokenStream tee;
    tee.Configure(&upstream, &buffer, /*doc_id=*/0);

    lucene::analysis::Token tok;
    ASSERT_NE(tee.next(&tok), nullptr);
    ASSERT_NE(tee.next(&tok), nullptr);

    // Only the non-empty term should have been recorded.
    ASSERT_EQ(buffer.RecordCount(), 1U);
    EXPECT_EQ(buffer.TermFor(buffer.records()[0]), "x");
}

} // namespace doris::segment_v2::inverted_index::spimi
