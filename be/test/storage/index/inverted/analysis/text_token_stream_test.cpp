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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/token_batch.h"

namespace doris::segment_v2::inverted_index {

namespace {

TokenRef ref(std::string_view term, int32_t position_increment) {
    return {.data = term.data(),
            .size = static_cast<uint32_t>(term.size()),
            .position_increment = position_increment};
}

// Words separated by spaces, each built in one reused scratch string: a base that did not copy the
// terms would hand out the last word of a batch over and over.
class WordTokenizer final : public PerTokenTokenizer {
public:
    size_t reserved_bytes() const override { return _scratch.capacity(); }

    // next_token() calls after it returned false.
    int calls_after_end = 0;

protected:
    void on_reset(std::string_view text) override {
        _text = text;
        _position = 0;
        _ended = false;
    }

    bool next_token(TokenRef& token) override {
        if (_ended) {
            ++calls_after_end;
            return false;
        }
        while (_position < _text.size() && _text[_position] == ' ') {
            ++_position;
        }
        if (_position == _text.size()) {
            _ended = true;
            return false;
        }
        const size_t end = std::min(_text.find(' ', _position), _text.size());
        _scratch.assign(_text.substr(_position, end - _position));
        _position = end;
        token = ref(_scratch, 1);
        return true;
    }

private:
    std::string_view _text;
    size_t _position = 0;
    bool _ended = true;
    std::string _scratch;
};

// Each token, then its upper-case twin at the same position: two tokens out of one.
class UpperCaseTwinFilter final : public PerTokenFilter {
public:
    using PerTokenFilter::PerTokenFilter;

    size_t reserved_bytes() const override {
        return _twin.capacity() + PerTokenFilter::reserved_bytes();
    }

protected:
    void on_reset() override { _twin_pending = false; }

    bool next_token(TokenRef& token) override {
        if (_twin_pending) {
            _twin_pending = false;
            token = ref(_twin, 0);
            return true;
        }
        if (!input_token(token)) {
            return false;
        }
        _twin.assign(token.term());
        std::ranges::transform(_twin, _twin.begin(),
                               [](char c) { return static_cast<char>(std::toupper(c)); });
        _twin_pending = true;
        return true;
    }

private:
    std::string _twin;
    bool _twin_pending = false;
};

// Keeps every n-th token and adds the increments of the dropped ones to it, as a stop filter does.
// With n above the token count it drops every token.
class KeepEveryNthFilter final : public PerTokenFilter {
public:
    KeepEveryNthFilter(std::unique_ptr<TextTokenStream> in, size_t n)
            : PerTokenFilter(std::move(in)), _n(n) {}

    size_t reserved_bytes() const override { return PerTokenFilter::reserved_bytes(); }

    // next_token() calls after it returned false.
    int calls_after_end = 0;

protected:
    void on_reset() override {
        _seen = 0;
        _ended = false;
    }

    bool next_token(TokenRef& token) override {
        if (_ended) {
            ++calls_after_end;
            return false;
        }
        int32_t dropped = 0;
        while (input_token(token)) {
            if (++_seen % _n == 0) {
                token.position_increment += dropped;
                return true;
            }
            dropped += token.position_increment;
        }
        _ended = true;
        return false;
    }

private:
    const size_t _n;
    size_t _seen = 0;
    bool _ended = true;
};

// The characters of each token, one token each. It holds the input token while it hands out the
// characters, across next_batch() calls when the token is long, as word_delimiter holds a token
// while it hands out its parts.
class CharacterFilter final : public PerTokenFilter {
public:
    using PerTokenFilter::PerTokenFilter;

    size_t reserved_bytes() const override { return PerTokenFilter::reserved_bytes(); }

protected:
    void on_reset() override {
        _current = TokenRef();
        _next = 0;
    }

    bool next_token(TokenRef& token) override {
        while (_next == _current.size) {
            if (!input_token(_current)) {
                return false;
            }
            _next = 0;
        }
        token = ref(std::string_view(_current.data + _next, 1), 1);
        ++_next;
        return true;
    }

private:
    TokenRef _current;
    size_t _next = 0;
};

std::string words(size_t count, std::string_view prefix = "w") {
    std::string text;
    for (size_t i = 0; i < count; ++i) {
        text += fmt::format("{}{} ", prefix, i);
    }
    return text;
}

std::vector<CollectedToken> word_tokens(size_t count, std::string_view prefix = "w") {
    std::vector<CollectedToken> tokens;
    for (size_t i = 0; i < count; ++i) {
        tokens.push_back({.term = fmt::format("{}{}", prefix, i), .position_increment = 1});
    }
    return tokens;
}

// The sizes of the batches `stream` hands out for `text`.
std::vector<size_t> batch_sizes(TextTokenStream& stream, std::string_view text) {
    std::vector<size_t> sizes;
    TokenBatch batch;
    stream.reset(text);
    while (stream.next_batch(batch)) {
        sizes.push_back(batch.size());
    }
    return sizes;
}

std::unique_ptr<TextTokenStream> word_tokenizer() {
    return std::make_unique<WordTokenizer>();
}

} // namespace

TEST(TokenBatchTest, PushKeepsTheViewAndPushCopyCopies) {
    std::string source = "abc";
    TokenBatch batch;
    batch.push(source, 1);
    batch.push_copy(source, 2);
    char* built = batch.allocate_term(3);
    std::copy_n("def", 3, built);
    batch.push({built, 3}, 0);
    batch.push_copy({}, 1);
    source[0] = 'X';

    ASSERT_EQ(batch.size(), 4);
    EXPECT_EQ(batch.tokens()[0].term(), "Xbc");
    EXPECT_EQ(batch.tokens()[1].term(), "abc");
    EXPECT_EQ(batch.tokens()[1].position_increment, 2);
    EXPECT_EQ(batch.tokens()[2].term(), "def");
    EXPECT_EQ(batch.tokens()[2].position_increment, 0);
    EXPECT_EQ(batch.tokens()[3].size, 0);
}

// Term storage grows by adding blocks; a term copied before keeps its address and its bytes.
TEST(TokenBatchTest, CopiedTermsStayPutWhileTermStorageGrows) {
    TokenBatch batch;
    std::vector<std::string> expected;
    for (size_t i = 0; i < 10000; ++i) {
        expected.push_back(fmt::format("term{}-{}", i, std::string(i % 97, 'x')));
        batch.push_copy(expected.back(), 1);
    }
    // Far more than the first 4 KiB block, so the terms are spread over several blocks.
    ASSERT_GT(batch._terms.size(), 64 * 4096);
    ASSERT_EQ(batch.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(batch.tokens()[i].term(), expected[i]) << i;
    }
}

TEST(TokenBatchTest, ClearKeepsTheTokenArrayAndTheLargestBlock) {
    TokenBatch batch;
    for (size_t i = 0; i < 3000; ++i) {
        batch.push_copy(std::string(40, static_cast<char>('a' + i % 26)), 1);
    }
    const size_t token_capacity = batch._tokens.capacity();
    const TokenRef* token_array = batch._tokens.data();
    const size_t term_bytes = batch._terms.size();

    batch.clear();
    EXPECT_EQ(batch.size(), 0);
    EXPECT_EQ(batch._tokens.capacity(), token_capacity);
    // The blocks before the largest are freed, the largest stays.
    EXPECT_LT(batch._terms.size(), term_bytes);
    EXPECT_GT(batch._terms.size(), 0);

    // A batch the largest block holds allocates nothing.
    const size_t kept = batch.capacity_bytes();
    for (size_t i = 0; i < 100; ++i) {
        batch.push_copy(std::string(40, 'z'), 1);
    }
    EXPECT_EQ(batch.capacity_bytes(), kept);
    EXPECT_EQ(batch._tokens.data(), token_array);
}

TEST(TokenBatchTest, ReleaseOversizedKeepsAtMostTheHighWaterMark) {
    // An ordinary batch keeps its memory.
    TokenBatch ordinary;
    for (size_t i = 0; i < TextTokenStream::kBatchTokens; ++i) {
        ordinary.push_copy("word", 1);
    }
    ordinary.clear();
    const size_t kept = ordinary.capacity_bytes();
    ordinary.release_oversized();
    EXPECT_GT(kept, 0);
    EXPECT_EQ(ordinary.capacity_bytes(), kept);

    // Many tokens, then long terms: each leaves more than the mark behind, and gives it back.
    TokenBatch many;
    for (size_t i = 0; i < 10000; ++i) {
        many.push("w", 1);
    }
    many.clear();
    ASSERT_GT(many.capacity_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
    many.release_oversized();
    EXPECT_LE(many.capacity_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);

    TokenBatch long_terms;
    long_terms.push_copy(std::string(1 << 20, 'x'), 1);
    long_terms.clear();
    ASSERT_GT(long_terms.capacity_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
    long_terms.release_oversized();
    EXPECT_LE(long_terms.capacity_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);

    long_terms.push_copy("again", 1);
    EXPECT_EQ(long_terms.tokens()[0].term(), "again");
}

TEST(PerTokenStreamTest, TokenizerCopiesEveryTermOutOfItsScratch) {
    WordTokenizer tokenizer;
    EXPECT_EQ(collect_checked(tokenizer, " alpha  beta gamma "),
              (std::vector<CollectedToken> {{.term = "alpha", .position_increment = 1},
                                            {.term = "beta", .position_increment = 1},
                                            {.term = "gamma", .position_increment = 1}}));
    EXPECT_EQ(collect_checked(tokenizer, words(1000)), word_tokens(1000));
}

TEST(PerTokenStreamTest, BatchesHoldAtMostKBatchTokens) {
    WordTokenizer tokenizer;
    EXPECT_EQ(batch_sizes(tokenizer, words(1000)), (std::vector<size_t> {256, 256, 256, 232}));
    // A value of exactly one batch: the next call finds it exhausted and returns false.
    EXPECT_EQ(batch_sizes(tokenizer, words(256)), (std::vector<size_t> {256}));
    EXPECT_TRUE(batch_sizes(tokenizer, "   ").empty());
}

TEST(PerTokenStreamTest, NextTokenIsNotCalledAgainAfterItReturnedFalse) {
    WordTokenizer tokenizer;
    TokenBatch batch;
    tokenizer.reset("a b");
    EXPECT_TRUE(tokenizer.next_batch(batch));
    EXPECT_FALSE(tokenizer.next_batch(batch));
    EXPECT_FALSE(tokenizer.next_batch(batch));
    EXPECT_EQ(tokenizer.calls_after_end, 0);

    auto filter = std::make_unique<KeepEveryNthFilter>(word_tokenizer(), 2);
    const std::string ten = words(10);
    filter->reset(ten);
    while (filter->next_batch(batch)) {
    }
    EXPECT_FALSE(filter->next_batch(batch));
    EXPECT_EQ(filter->calls_after_end, 0);
}

TEST(PerTokenStreamTest, NothingComesOutBeforeAValueIsBound) {
    WordTokenizer tokenizer;
    TokenBatch batch;
    EXPECT_FALSE(tokenizer.next_batch(batch));
    UpperCaseTwinFilter filter(word_tokenizer());
    EXPECT_FALSE(filter.next_batch(batch));
    EXPECT_EQ(batch.size(), 0);
}

TEST(PerTokenStreamTest, FilterMakesSeveralTokensOutOfOne) {
    UpperCaseTwinFilter filter(word_tokenizer());
    const std::vector<CollectedToken> tokens = collect_checked(filter, words(300));
    ASSERT_EQ(tokens.size(), 600);
    for (size_t i = 0; i < 300; ++i) {
        EXPECT_EQ(tokens[2 * i],
                  (CollectedToken {.term = fmt::format("w{}", i), .position_increment = 1}));
        EXPECT_EQ(tokens[2 * i + 1],
                  (CollectedToken {.term = fmt::format("W{}", i), .position_increment = 0}));
    }
    EXPECT_EQ(batch_sizes(filter, words(300)), (std::vector<size_t> {256, 256, 88}));
}

// A filter that drops every token of a batch keeps pulling its input: no call returns true with an
// empty batch, and a value whose tokens are all dropped makes the first call return false.
TEST(PerTokenStreamTest, FilterThatDropsTokensReturnsNoEmptyBatch) {
    KeepEveryNthFilter drop_all(word_tokenizer(), 1000);
    TokenBatch batch;
    const std::string text = words(999);
    drop_all.reset(text);
    EXPECT_FALSE(drop_all.next_batch(batch));
    EXPECT_EQ(batch.size(), 0);

    // Three of 1000 tokens survive, from input batches far apart.
    KeepEveryNthFilter keep_few(word_tokenizer(), 300);
    EXPECT_EQ(collect_checked(keep_few, words(1000)),
              (std::vector<CollectedToken> {{.term = "w299", .position_increment = 300},
                                            {.term = "w599", .position_increment = 300},
                                            {.term = "w899", .position_increment = 300}}));
}

// The filter hands out the characters of a 300-character token across two batches while it holds
// the token, and the input batch the token lives in stays as it is meanwhile.
TEST(PerTokenStreamTest, FilterHoldsAnInputTokenAcrossBatches) {
    const std::string first(300, 'a');
    const std::string second = "bcd";
    CharacterFilter filter(word_tokenizer());
    EXPECT_EQ(batch_sizes(filter, first + " " + second), (std::vector<size_t> {256, 47}));
    const std::vector<CollectedToken> tokens = collect_checked(filter, first + " " + second);
    ASSERT_EQ(tokens.size(), 303);
    for (size_t i = 0; i < 300; ++i) {
        EXPECT_EQ(tokens[i].term, "a") << i;
    }
    EXPECT_EQ(tokens[300].term, "b");
    EXPECT_EQ(tokens[302].term, "d");
}

TEST(PerTokenStreamTest, ResetDropsTheRestOfAValue) {
    TokenBatch batch;
    WordTokenizer tokenizer;
    // A value must stay valid while the stream reads it, so it is not a temporary.
    const std::string long_text = words(1000);
    tokenizer.reset(long_text);
    ASSERT_TRUE(tokenizer.next_batch(batch));
    EXPECT_EQ(collect_checked(tokenizer, "x y"),
              (std::vector<CollectedToken> {{.term = "x", .position_increment = 1},
                                            {.term = "y", .position_increment = 1}}));

    UpperCaseTwinFilter twins(word_tokenizer());
    twins.reset(long_text);
    ASSERT_TRUE(twins.next_batch(batch));
    EXPECT_EQ(collect_checked(twins, "x"),
              (std::vector<CollectedToken> {{.term = "x", .position_increment = 1},
                                            {.term = "X", .position_increment = 0}}));

    // Cut in the middle of a held token.
    CharacterFilter characters(word_tokenizer());
    const std::string long_word(300, 'a');
    characters.reset(long_word);
    ASSERT_TRUE(characters.next_batch(batch));
    EXPECT_EQ(collect_checked(characters, "bc"),
              (std::vector<CollectedToken> {{.term = "b", .position_increment = 1},
                                            {.term = "c", .position_increment = 1}}));
}

TEST(PerTokenStreamTest, ReservedBytesAddUpTheChainAndResetReleasesTheInputBatch) {
    UpperCaseTwinFilter filter(word_tokenizer());
    // One word of 1 MiB: the input batch copies it, so it holds more than the mark.
    const std::string huge(1 << 20, 'h');
    TokenBatch batch;
    filter.reset(huge);
    ASSERT_TRUE(filter.next_batch(batch));
    const auto& tokenizer = static_cast<const WordTokenizer&>(*filter._in);
    EXPECT_GT(filter._input.capacity_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
    EXPECT_EQ(filter.reserved_bytes(), filter._twin.capacity() + filter._input.capacity_bytes() +
                                               tokenizer.reserved_bytes());

    filter.reset("short");
    EXPECT_LE(filter._input.capacity_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
    EXPECT_EQ(filter.reserved_bytes(), filter._twin.capacity() + filter._input.capacity_bytes() +
                                               tokenizer.reserved_bytes());
}

TEST(PerTokenStreamTest, TestStreamsKeepTheContract) {
    const std::vector<std::string> texts = {
            "one", " a  b c ", words(1000), words(256), std::string(300, 'a'), "x"};
    expect_text_stream_contract(word_tokenizer, texts);
    expect_text_stream_contract(
            [] { return std::make_unique<UpperCaseTwinFilter>(word_tokenizer()); }, texts);
    expect_text_stream_contract(
            [] { return std::make_unique<KeepEveryNthFilter>(word_tokenizer(), 3); }, texts);
    expect_text_stream_contract([] { return std::make_unique<CharacterFilter>(word_tokenizer()); },
                                texts);
    // Every token dropped, for every text.
    expect_text_stream_contract(
            [] { return std::make_unique<KeepEveryNthFilter>(word_tokenizer(), 100000); }, texts);
}

} // namespace doris::segment_v2::inverted_index
