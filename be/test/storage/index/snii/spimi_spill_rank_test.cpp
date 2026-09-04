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
#include <string>
#include <vector>

#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::writer {
namespace {

TEST(SpimiSpillRankTest, RebuildsGrowingVocabularyRankOnlyForFinalMerge) {
    testing::reset_string_rank_rebuilds();
    SpimiTermBuffer buffer(/*has_positions=*/false, /*spill_threshold_bytes=*/0);
    buffer.set_max_run_files(/*cap=*/0);
    buffer.set_forced_spill_min_arena_bytes(0);

    buffer.add_token("b", /*docid=*/0, /*pos=*/0);
    buffer.request_global_spill_for_test();
    buffer.add_token("c", /*docid=*/1, /*pos=*/0);
    EXPECT_EQ(testing::string_rank_rebuilds(), 1);

    buffer.add_token("z", /*docid=*/2, /*pos=*/0);
    buffer.add_token("a", /*docid=*/3, /*pos=*/0);
    buffer.add_token("b", /*docid=*/4, /*pos=*/0);
    buffer.request_global_spill_for_test();
    buffer.add_token("m", /*docid=*/5, /*pos=*/0);
    EXPECT_EQ(testing::string_rank_rebuilds(), 1);
    EXPECT_GE(buffer.string_rank_capacity_for_test(), 5);
    EXPECT_GE(buffer.resident_bytes_for_test(),
              buffer.string_rank_capacity_for_test() * sizeof(uint32_t));

    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(buffer.run_count_for_test(), 2);

    const std::vector<TermPostings> terms = buffer.finalize_sorted();

    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    EXPECT_EQ(testing::string_rank_rebuilds(), 2);
    ASSERT_EQ(terms.size(), 5);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[0].docids, std::vector<uint32_t>({3}));
    EXPECT_EQ(terms[0].freqs, std::vector<uint32_t>({1}));
    EXPECT_EQ(terms[1].term, "b");
    EXPECT_EQ(terms[1].docids, std::vector<uint32_t>({0, 4}));
    EXPECT_EQ(terms[1].freqs, std::vector<uint32_t>({1, 1}));
    EXPECT_EQ(terms[2].term, "c");
    EXPECT_EQ(terms[2].docids, std::vector<uint32_t>({1}));
    EXPECT_EQ(terms[2].freqs, std::vector<uint32_t>({1}));
    EXPECT_EQ(terms[3].term, "m");
    EXPECT_EQ(terms[3].docids, std::vector<uint32_t>({5}));
    EXPECT_EQ(terms[3].freqs, std::vector<uint32_t>({1}));
    EXPECT_EQ(terms[4].term, "z");
    EXPECT_EQ(terms[4].docids, std::vector<uint32_t>({2}));
    EXPECT_EQ(terms[4].freqs, std::vector<uint32_t>({1}));
}

TEST(SpimiSpillRankTest, ReusesRankAcrossFixedVocabularySpillsAndFinalMerge) {
    testing::reset_string_rank_rebuilds();
    const std::vector<std::string> vocabulary = {"z", "a", "m"};
    SpimiTermBuffer buffer(&vocabulary, /*has_positions=*/false, /*spill_threshold_bytes=*/1);
    buffer.set_max_run_files(/*cap=*/0);

    buffer.add_token(/*term_id=*/0, /*docid=*/0, /*pos=*/0);
    EXPECT_EQ(testing::string_rank_rebuilds(), 1);
    buffer.add_token(/*term_id=*/1, /*docid=*/1, /*pos=*/0);
    buffer.add_token(/*term_id=*/2, /*docid=*/2, /*pos=*/0);
    buffer.add_token(/*term_id=*/0, /*docid=*/3, /*pos=*/0);

    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(buffer.run_count_for_test(), 4);
    EXPECT_EQ(testing::string_rank_rebuilds(), 1);

    const std::vector<TermPostings> terms = buffer.finalize_sorted();

    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    EXPECT_EQ(testing::string_rank_rebuilds(), 1);
    ASSERT_EQ(terms.size(), 3);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[0].docids, std::vector<uint32_t>({1}));
    EXPECT_EQ(terms[1].term, "m");
    EXPECT_EQ(terms[1].docids, std::vector<uint32_t>({2}));
    EXPECT_EQ(terms[2].term, "z");
    EXPECT_EQ(terms[2].docids, std::vector<uint32_t>({0, 3}));
}

TEST(SpimiSpillRankTest, RefreshesStaleRankBeforeRunCompaction) {
    testing::reset_run_compactions();
    testing::reset_string_rank_rebuilds();
    SpimiTermBuffer buffer(/*has_positions=*/false, /*spill_threshold_bytes=*/0);
    buffer.set_max_run_files(/*cap=*/1);
    buffer.set_forced_spill_min_arena_bytes(0);

    buffer.add_token("h", /*docid=*/0, /*pos=*/0);
    buffer.request_global_spill_for_test();
    buffer.add_token("i", /*docid=*/1, /*pos=*/0);
    EXPECT_EQ(testing::string_rank_rebuilds(), 1);

    buffer.add_token("z", /*docid=*/2, /*pos=*/0);
    buffer.add_token("h", /*docid=*/3, /*pos=*/0);
    buffer.request_global_spill_for_test();
    buffer.add_token("a", /*docid=*/4, /*pos=*/0);
    EXPECT_EQ(testing::string_rank_rebuilds(), 1);

    buffer.add_token("m", /*docid=*/5, /*pos=*/0);
    buffer.request_global_spill_for_test();
    buffer.add_token("b", /*docid=*/6, /*pos=*/0);
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    EXPECT_EQ(testing::run_compactions(), 1);
    EXPECT_EQ(testing::string_rank_rebuilds(), 2);

    const std::vector<TermPostings> terms = buffer.finalize_sorted();

    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    EXPECT_EQ(testing::string_rank_rebuilds(), 2);
    ASSERT_EQ(terms.size(), 6);
    EXPECT_EQ(terms[0].term, "a");
    EXPECT_EQ(terms[0].docids, std::vector<uint32_t>({4}));
    EXPECT_EQ(terms[1].term, "b");
    EXPECT_EQ(terms[1].docids, std::vector<uint32_t>({6}));
    EXPECT_EQ(terms[2].term, "h");
    EXPECT_EQ(terms[2].docids, std::vector<uint32_t>({0, 3}));
    EXPECT_EQ(terms[3].term, "i");
    EXPECT_EQ(terms[3].docids, std::vector<uint32_t>({1}));
    EXPECT_EQ(terms[4].term, "m");
    EXPECT_EQ(terms[4].docids, std::vector<uint32_t>({5}));
    EXPECT_EQ(terms[5].term, "z");
    EXPECT_EQ(terms[5].docids, std::vector<uint32_t>({2}));
}

TEST(SpimiSpillRankTest, PhysicalCommonGramsSurviveSpillAndRunCompaction) {
    namespace inverted_index = doris::segment_v2::inverted_index;
    const std::string first = inverted_index::encode_common_gram("of", "the").value();
    const std::string second = inverted_index::encode_common_gram("the", "world").value();

    auto feed = [&](SpimiTermBuffer* buffer) {
        buffer->add_token(second, 0, 1);
        buffer->add_token("plain", 0, 0);
        buffer->request_global_spill_for_test();
        buffer->add_token(first, 0, 0);
        buffer->add_token(first, 1, 2);
        buffer->request_global_spill_for_test();
        buffer->add_token(second, 2, 3);
        buffer->request_global_spill_for_test();
        buffer->add_token("plain", 3, 4);
    };

    SpimiTermBuffer unspilled(/*has_positions=*/true);
    feed(&unspilled);

    SpimiTermBuffer spilled(/*has_positions=*/true);
    spilled.set_max_run_files(/*cap=*/1);
    spilled.set_forced_spill_min_arena_bytes(0);
    feed(&spilled);

    ASSERT_TRUE(unspilled.status().ok()) << unspilled.status();
    ASSERT_TRUE(spilled.status().ok()) << spilled.status();
    EXPECT_GE(spilled.run_count_for_test(), 1U);

    const std::vector<TermPostings> expected = unspilled.finalize_sorted();
    const std::vector<TermPostings> actual = spilled.finalize_sorted();
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i].term, expected[i].term);
        EXPECT_EQ(actual[i].docids, expected[i].docids);
        EXPECT_EQ(actual[i].freqs, expected[i].freqs);
        EXPECT_EQ(actual[i].positions_flat, expected[i].positions_flat);
    }
}

TEST(SpimiSpillRankTest, PerTermDocsOnlyDeduplicatesAcrossSpillAndCompaction) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    buffer.set_max_run_files(/*cap=*/1);
    buffer.set_forced_spill_min_arena_bytes(0);

    buffer.add_token("docs", /*docid=*/7, /*pos=*/1, /*retain_positions=*/false);
    buffer.request_global_spill_for_test();
    buffer.add_token("docs", /*docid=*/7, /*pos=*/2, /*retain_positions=*/false);
    buffer.request_global_spill_for_test();
    buffer.add_token("positioned", /*docid=*/7, /*pos=*/3, /*retain_positions=*/true);
    buffer.request_global_spill_for_test();
    buffer.add_token("positioned", /*docid=*/7, /*pos=*/4, /*retain_positions=*/true);
    buffer.add_token("docs", /*docid=*/8, /*pos=*/5, /*retain_positions=*/false);

    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    EXPECT_GE(buffer.run_count_for_test(), 1U);
    const std::vector<TermPostings> terms = buffer.finalize_sorted();
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term, "docs");
    EXPECT_FALSE(terms[0].retain_positions);
    EXPECT_EQ(terms[0].docids, (std::vector<uint32_t> {7U, 8U}));
    EXPECT_EQ(terms[0].freqs, (std::vector<uint32_t> {1U, 1U}));
    EXPECT_TRUE(terms[0].positions_flat.empty());
    EXPECT_EQ(terms[1].term, "positioned");
    EXPECT_TRUE(terms[1].retain_positions);
    EXPECT_EQ(terms[1].docids, (std::vector<uint32_t> {7U}));
    EXPECT_EQ(terms[1].freqs, (std::vector<uint32_t> {2U}));
    EXPECT_EQ(terms[1].positions_flat, (std::vector<uint32_t> {3U, 4U}));
}

} // namespace
} // namespace doris::snii::writer
