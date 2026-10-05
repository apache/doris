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

#include "exec/sink/writer/result_file_name.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace doris {

TEST(ResultFileNameTest, no_padding_preserves_legacy_format) {
    EXPECT_EQ("s3://bucket/exp_abc_0.csv",
              build_result_file_name("s3://bucket/exp_", "abc", 0, 0, "csv"));
    EXPECT_EQ("s3://bucket/exp_abc_11.csv",
              build_result_file_name("s3://bucket/exp_", "abc", 11, 0, "csv"));
}

TEST(ResultFileNameTest, negative_padding_is_treated_as_none) {
    EXPECT_EQ("s3://bucket/exp_abc_7.csv",
              build_result_file_name("s3://bucket/exp_", "abc", 7, -1, "csv"));
}

TEST(ResultFileNameTest, padding_zero_fills_to_width) {
    EXPECT_EQ("s3://bucket/exp_abc_00000.csv",
              build_result_file_name("s3://bucket/exp_", "abc", 0, 5, "csv"));
    EXPECT_EQ("s3://bucket/exp_abc_00011.csv",
              build_result_file_name("s3://bucket/exp_", "abc", 11, 5, "csv"));
}

TEST(ResultFileNameTest, padding_does_not_truncate_when_index_exceeds_width) {
    // Width is a minimum, never a cap: an index wider than `pad` is emitted in full.
    EXPECT_EQ("s3://bucket/exp_abc_123456.csv",
              build_result_file_name("s3://bucket/exp_", "abc", 123456, 5, "csv"));
}

TEST(ResultFileNameTest, padded_names_sort_lexicographically) {
    // The bug this feature fixes: without padding, _11 sorts before _2.
    std::vector<std::string> unpadded;
    std::vector<std::string> padded;
    for (int i : {0, 1, 2, 10, 11, 100}) {
        unpadded.push_back(build_result_file_name("exp_", "id", i, 0, "csv"));
        padded.push_back(build_result_file_name("exp_", "id", i, 5, "csv"));
    }

    std::vector<std::string> unpadded_sorted = unpadded;
    std::sort(unpadded_sorted.begin(), unpadded_sorted.end());
    // Lexicographic sort scrambles the numeric order for unpadded names.
    EXPECT_NE(unpadded, unpadded_sorted);

    std::vector<std::string> padded_sorted = padded;
    std::sort(padded_sorted.begin(), padded_sorted.end());
    // Padded names keep numeric order under a lexicographic sort.
    EXPECT_EQ(padded, padded_sorted);
}

} // namespace doris
