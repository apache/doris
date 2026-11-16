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

#include "storage/index/inverted/snii/format/dict_gram_summary.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/snii/encoding/byte_sink.h"

namespace doris::snii::format {
namespace {

void add_block(DictGramSummaryBuilder* builder, uint32_t ordinal,
               std::initializer_list<std::string_view> keys) {
    std::vector<uint32_t> codes;
    for (std::string_view key : keys) {
        append_dict_gram_codes(key, &codes);
    }
    builder->add_block(ordinal, &codes);
}

std::vector<uint8_t> finish(const DictGramSummaryBuilder& builder) {
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

uint64_t candidates(const DictGramSummaryReader& reader, std::string_view text, bool* any) {
    std::vector<uint64_t> blocks;
    const Status status = reader.candidate_blocks(text, &blocks, any);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(blocks.size(), (reader.n_blocks() + 63) / 64);
    return blocks.empty() ? 0 : blocks[0];
}

} // namespace

TEST(SniiDictGramSummary, GramCodesAreTheThreeBytes) {
    std::vector<uint32_t> codes;
    append_dict_gram_codes("ab", &codes);
    EXPECT_TRUE(codes.empty());
    append_dict_gram_codes("abcd", &codes);
    EXPECT_EQ(codes, (std::vector<uint32_t> {0x616263U, 0x626364U}));
    codes.clear();
    append_dict_gram_codes("\xff\xfe\xfd", &codes);
    EXPECT_EQ(codes, (std::vector<uint32_t> {0xFFFEFDU}));
}

TEST(SniiDictGramSummary, NamesTheBlocksHoldingEveryGram) {
    DictGramSummaryBuilder builder;
    add_block(&builder, 0, {"apple", "cache"});
    add_block(&builder, 1, {"cab", "zzz"});
    add_block(&builder, 2, {"cached", "error"});
    add_block(&builder, 3, {"xy"});
    add_block(&builder, 4, {"cache", "cache"});
    EXPECT_EQ(builder.n_blocks(), 5U);
    EXPECT_TRUE(builder.prunes());

    DictGramSummaryReader reader;
    const Status status = DictGramSummaryReader::open(finish(builder), &reader);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(reader.n_blocks(), 5U);

    bool any = false;
    EXPECT_EQ(candidates(reader, "cache", &any), (1U << 0) | (1U << 2) | (1U << 4));
    EXPECT_TRUE(any);
    EXPECT_EQ(candidates(reader, "cached", &any), 1U << 2);
    EXPECT_TRUE(any);
    EXPECT_EQ(candidates(reader, "error", &any), 1U << 2);
    EXPECT_EQ(candidates(reader, "zzz", &any), 1U << 1);
    EXPECT_EQ(candidates(reader, "cab", &any), 1U << 1);
    // Grams of a longer text that no single block holds together.
    EXPECT_EQ(candidates(reader, "applezzz", &any), 0U);
    EXPECT_FALSE(any);
    // A gram no key holds.
    EXPECT_EQ(candidates(reader, "qqq", &any), 0U);
    EXPECT_FALSE(any);

    std::vector<uint64_t> blocks;
    EXPECT_FALSE(reader.candidate_blocks("ca", &blocks, &any).ok());
}

TEST(SniiDictGramSummary, RunsSpanConsecutiveBlocks) {
    DictGramSummaryBuilder builder;
    for (uint32_t ordinal = 0; ordinal < 200; ++ordinal) {
        if (ordinal == 3 || ordinal == 70 || ordinal == 71) {
            add_block(&builder, ordinal, {"other"});
        } else {
            add_block(&builder, ordinal, {"running"});
        }
    }
    DictGramSummaryReader reader;
    ASSERT_TRUE(DictGramSummaryReader::open(finish(builder), &reader).ok());
    std::vector<uint64_t> blocks;
    bool any = false;
    ASSERT_TRUE(reader.candidate_blocks("run", &blocks, &any).ok());
    EXPECT_TRUE(any);
    for (uint32_t ordinal = 0; ordinal < 200; ++ordinal) {
        const bool expected = ordinal != 3 && ordinal != 70 && ordinal != 71;
        EXPECT_EQ((blocks[ordinal / 64] >> (ordinal % 64)) & 1U, expected ? 1U : 0U) << ordinal;
    }
    ASSERT_TRUE(reader.candidate_blocks("other", &blocks, &any).ok());
    EXPECT_EQ(blocks[0], 1U << 3);
    EXPECT_EQ(blocks[1], (uint64_t {1} << 6) | (uint64_t {1} << 7));
    EXPECT_EQ(blocks[2], 0U);
    EXPECT_EQ(blocks[3], 0U);
}

// The per-block set collects each gram once, grows past its first size and starts over on clear.
TEST(SniiDictGramSummary, GramSetCollectsEachGramOnce) {
    DictGramSet set;
    std::vector<uint32_t> codes;
    collect_dict_gram_codes("cache", &set, &codes);
    collect_dict_gram_codes("cached", &set, &codes);
    collect_dict_gram_codes("ab", &set, &codes);
    std::vector<uint32_t> expected;
    append_dict_gram_codes("cached", &expected);
    EXPECT_EQ(codes, expected);
    EXPECT_EQ(set.size(), 4U);

    for (uint32_t code = 0; code < 50000; ++code) {
        EXPECT_TRUE(set.insert(code * 7919U % 0xFFFFFFU)) << code;
    }
    EXPECT_EQ(set.size(), 50004U);
    EXPECT_FALSE(set.insert(7919U));
    set.clear();
    EXPECT_EQ(set.size(), 0U);
    codes.clear();
    collect_dict_gram_codes("cache", &set, &codes);
    EXPECT_EQ(codes.size(), 3U);
}

TEST(SniiDictGramSummary, DoesNotPruneUniformOrSingleBlockKeys) {
    DictGramSummaryBuilder single;
    add_block(&single, 0, {"cache", "error"});
    EXPECT_FALSE(single.prunes());

    DictGramSummaryBuilder uniform;
    for (uint32_t ordinal = 0; ordinal < 8; ++ordinal) {
        add_block(&uniform, ordinal, {"deadbeef", "cafe", "feed"});
    }
    EXPECT_FALSE(uniform.prunes());

    DictGramSummaryBuilder empty;
    add_block(&empty, 0, {"ab"});
    add_block(&empty, 1, {"cd"});
    EXPECT_FALSE(empty.prunes());
}

TEST(SniiDictGramSummary, RejectsCorruption) {
    DictGramSummaryBuilder builder;
    add_block(&builder, 0, {"cache"});
    add_block(&builder, 1, {"error"});
    const std::vector<uint8_t> good = finish(builder);
    DictGramSummaryReader reader;
    ASSERT_TRUE(DictGramSummaryReader::open(good, &reader).ok());

    std::vector<uint8_t> flipped = good;
    flipped[flipped.size() / 2] ^= 0x5A;
    EXPECT_FALSE(DictGramSummaryReader::open(flipped, &reader).ok());

    std::vector<uint8_t> truncated(good.begin(), good.end() - 1);
    EXPECT_FALSE(DictGramSummaryReader::open(truncated, &reader).ok());

    std::vector<uint8_t> versioned = good;
    versioned[0] = kDictGramSummaryVersion + 1;
    EXPECT_FALSE(DictGramSummaryReader::open(versioned, &reader).ok());

    EXPECT_FALSE(DictGramSummaryReader::open({}, &reader).ok());
}

} // namespace doris::snii::format
