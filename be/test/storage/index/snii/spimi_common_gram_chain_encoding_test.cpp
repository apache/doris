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

#include <cstdint>
#include <vector>

#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::writer {
namespace {

TEST(SpimiCommonGramChainEncodingTest, NativeDocsOnlyPairUsesStatlessEncoding) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    const PlainTermId left = buffer.intern_plain_term("left");
    const PlainTermId right = buffer.intern_plain_term("right");

    buffer.add_common_gram(left, right, /*docid=*/1, /*pos=*/0,
                           /*retain_positions=*/false);
    buffer.add_common_gram(left, right, /*docid=*/1, /*pos=*/1,
                           /*retain_positions=*/false);
    buffer.add_common_gram(left, right, /*docid=*/4, /*pos=*/0,
                           /*retain_positions=*/false);

    const std::vector<TermPostings> terms = buffer.finalize_sorted();
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1, 4}));
    EXPECT_TRUE(terms[0].freqs.empty());
    EXPECT_TRUE(terms[0].positions_flat.empty());
    EXPECT_FALSE(terms[0].retain_positions);
}

TEST(SpimiCommonGramChainEncodingTest, NativeDocsOnlySingletonDefersPostingArena) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    const PlainTermId left = buffer.intern_plain_term("left");
    const PlainTermId right = buffer.intern_plain_term("right");
    const uint64_t bytes_before = buffer.resident_bytes_for_test();

    buffer.add_common_gram(left, right, /*docid=*/7, /*pos=*/0,
                           /*retain_positions=*/false);

    EXPECT_LT(buffer.resident_bytes_for_test() - bytes_before, 32U << 10);
    const std::vector<TermPostings> terms = buffer.finalize_sorted();
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {7}));
    EXPECT_TRUE(terms[0].freqs.empty());
    EXPECT_TRUE(terms[0].positions_flat.empty());
}

TEST(SpimiCommonGramChainEncodingTest, NativeDocsOnlyBackfillPreservesOutOfOrderDocids) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    const PlainTermId left = buffer.intern_plain_term("left");
    const PlainTermId right = buffer.intern_plain_term("right");

    for (const uint32_t docid : {9U, 7U, 9U, UINT32_MAX, 0U}) {
        buffer.add_common_gram(left, right, docid, /*pos=*/0,
                               /*retain_positions=*/false);
    }

    const std::vector<TermPostings> terms = buffer.finalize_sorted();
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {0U, 7U, 9U, UINT32_MAX}));
}

TEST(SpimiCommonGramChainEncodingTest, NativeDocsOnlySingletonMergesAcrossSpill) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    buffer.set_forced_spill_min_arena_bytes(0);
    const PlainTermId left = buffer.intern_plain_term("left");
    const PlainTermId right = buffer.intern_plain_term("right");

    buffer.request_global_spill_for_test();
    buffer.add_common_gram_and_plain(left, right, /*docid=*/7, /*gram_pos=*/0,
                                     /*plain_pos=*/1, /*retain_gram_positions=*/false);
    ASSERT_EQ(buffer.run_count_for_test(), 1U);
    buffer.add_common_gram(left, right, /*docid=*/7, /*pos=*/2,
                           /*retain_positions=*/false);
    buffer.add_common_gram(left, right, /*docid=*/9, /*pos=*/0,
                           /*retain_positions=*/false);

    const std::vector<TermPostings> terms = buffer.finalize_sorted();
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    const TermPostings* gram = nullptr;
    for (const TermPostings& term : terms) {
        if (!term.retain_positions) {
            gram = &term;
        }
    }
    ASSERT_NE(gram, nullptr);
    EXPECT_EQ(gram->docids, (std::vector<uint32_t> {7U, 9U}));
}

TEST(SpimiCommonGramChainEncodingTest, PositionedNativePairRemainsTagged) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.enable_common_gram_pair_keys();
    const PlainTermId left = buffer.intern_plain_term("left");
    const PlainTermId right = buffer.intern_plain_term("right");

    buffer.add_common_gram(left, right, /*docid=*/1, /*pos=*/2,
                           /*retain_positions=*/true);
    buffer.add_common_gram(left, right, /*docid=*/1, /*pos=*/5,
                           /*retain_positions=*/true);
    buffer.add_common_gram(left, right, /*docid=*/3, /*pos=*/1,
                           /*retain_positions=*/true);

    const std::vector<TermPostings> terms = buffer.finalize_sorted();
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(terms.size(), 1U);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {1, 3}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {2, 1}));
    EXPECT_EQ(terms[0].positions_flat, (std::vector<uint32_t> {2, 5, 1}));
    EXPECT_TRUE(terms[0].retain_positions);
}

} // namespace
} // namespace doris::snii::writer
