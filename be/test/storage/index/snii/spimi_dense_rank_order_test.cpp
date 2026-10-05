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

#include <algorithm>
#include <string>
#include <vector>

#include "storage/index/snii/writer/spimi_term_buffer.h"

namespace doris::snii::writer {
namespace {

TEST(SpimiDenseRankOrderTest, PartialBorrowedVocabularyUsesComparisonSort) {
    testing::reset_rank_ordering_counts();
    const std::vector<std::string> vocabulary = {"z", "a", "middle", "b"};
    SpimiTermBuffer buffer(&vocabulary, /*has_positions=*/false);
    buffer.add_token(/*term_id=*/0, /*docid=*/0, /*pos=*/0);
    buffer.add_token(/*term_id=*/2, /*docid=*/0, /*pos=*/1);

    const std::vector<TermPostings> terms = buffer.finalize_sorted();
    ASSERT_TRUE(buffer.status().ok()) << buffer.status();
    ASSERT_EQ(terms.size(), 2U);
    EXPECT_EQ(terms[0].term, "middle");
    EXPECT_EQ(terms[1].term, "z");
    EXPECT_EQ(testing::dense_rank_inversions(), 0U);
    EXPECT_EQ(testing::rank_comparison_sorts(), 1U);
}

} // namespace
} // namespace doris::snii::writer
