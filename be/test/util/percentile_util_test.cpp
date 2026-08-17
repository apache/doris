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

#include "util/percentile_util.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <vector>

namespace doris {

class PercentileUtilTest : public testing::Test {};

TEST_F(PercentileUtilTest, CountsTotalTest) {
    Counts<int64_t> counts;
    // 1 1 1 2 5 7 7 9 9 19
    // >>> import numpy as np
    // >>> a = np.array([1,1,1,2,5,7,7,9,9,19])
    // >>> p = np.percentile(a, 20)
    counts.increment(1, 3);
    counts.increment(5, 1);
    counts.increment(2, 1);
    counts.increment(9, 1);
    counts.increment(9, 1);
    counts.increment(19, 1);
    counts.increment(7, 2);

    double result = counts.terminate(0.2);
    EXPECT_EQ(1, result);

    auto cs = ColumnString::create();
    BufferWritable bw(*cs);
    counts.serialize(bw);
    bw.commit();

    Counts<int64_t> other;
    StringRef res(cs->get_chars().data(), cs->get_chars().size());
    BufferReadable br(res);
    other.unserialize(br);
    double result1 = other.terminate(0.2);
    EXPECT_EQ(result, result1);

    Counts<int64_t> other1;
    other1.increment(1, 1);
    other1.increment(100, 3);
    other1.increment(50, 3);
    other1.increment(10, 1);
    other1.increment(99, 2);

    // deserialize other1
    cs->clear();
    other1.serialize(bw);
    bw.commit();
    Counts<int64_t> other1_deserialized;
    BufferReadable br1(res);
    other1_deserialized.unserialize(br1);

    Counts<int64_t> merge_res;
    merge_res.merge(&other);
    merge_res.merge(&other1_deserialized);
    // 1 1 1 1 2 5 7 7 9 9 10 19 50 50 50 99 99 100 100 100
    EXPECT_EQ(merge_res.terminate(0.3), 6.4);
}

TEST_F(PercentileUtilTest, CountsBoundaryBehavior) {
    Counts<int64_t> empty_counts;
    EXPECT_DOUBLE_EQ(0.0, empty_counts.terminate(0.5));

    Counts<int64_t> single_counts;
    single_counts.increment(42);
    EXPECT_DOUBLE_EQ(42.0, single_counts.terminate(0.0));
    EXPECT_DOUBLE_EQ(42.0, single_counts.terminate(0.5));
    EXPECT_DOUBLE_EQ(42.0, single_counts.terminate(1.0));
}

TEST_F(PercentileUtilTest, CountsSerializeMergedState) {
    Counts<int64_t> left;
    left.increment(5);
    left.increment(1);

    auto col = ColumnString::create();
    BufferWritable writer(*col);
    left.serialize(writer);
    writer.commit();

    StringRef left_data(col->get_chars().data(), col->get_chars().size());
    BufferReadable left_reader(left_data);
    Counts<int64_t> left_sorted;
    left_sorted.unserialize(left_reader);

    Counts<int64_t> right;
    right.increment(9);
    right.increment(7);

    col->clear();
    right.serialize(writer);
    writer.commit();

    StringRef right_data(col->get_chars().data(), col->get_chars().size());
    BufferReadable right_reader(right_data);
    Counts<int64_t> right_sorted;
    right_sorted.unserialize(right_reader);

    Counts<int64_t> merged;
    merged.merge(&left_sorted);
    merged.merge(&right_sorted);

    col->clear();
    merged.serialize(writer);
    writer.commit();

    StringRef merged_data(col->get_chars().data(), col->get_chars().size());
    BufferReadable merged_reader(merged_data);
    Counts<int64_t> restored;
    restored.unserialize(merged_reader);

    EXPECT_DOUBLE_EQ(1.0, restored.terminate(0.0));
    EXPECT_DOUBLE_EQ(6.0, restored.terminate(0.5));
    EXPECT_DOUBLE_EQ(9.0, restored.terminate(1.0));
}

TEST_F(PercentileUtilTest, CheckQuantileBoundary) {
    EXPECT_NO_THROW(check_quantile(0.0));
    EXPECT_NO_THROW(check_quantile(0.5));
    EXPECT_NO_THROW(check_quantile(1.0));
    EXPECT_THROW(check_quantile(-0.0001), Exception);
    EXPECT_THROW(check_quantile(1.0001), Exception);
}

TEST_F(PercentileUtilTest, EmptyLevelsState) {
    PercentileLevels levels;
    EXPECT_TRUE(levels.empty());
    EXPECT_TRUE(levels.quantiles.empty());
    EXPECT_TRUE(levels.permutation.empty());

    auto col = ColumnString::create();
    BufferWritable writer(*col);
    levels.write(writer);
    writer.commit();

    StringRef data(col->get_chars().data(), col->get_chars().size());
    BufferReadable reader(data);

    PercentileLevels restored;
    restored.read(reader);
    EXPECT_TRUE(restored.empty());
    EXPECT_TRUE(restored.quantiles.empty());
    EXPECT_TRUE(restored.permutation.empty());
}

TEST_F(PercentileUtilTest, SortPermutationKeepsQuantiles) {
    PercentileLevels levels;
    levels.quantiles = {0.56, 0.12, 0.45, 0.23};
    levels.permutation = {0, 1, 2, 3};

    const auto& permutation = levels.get_permutation();

    EXPECT_EQ((std::vector<size_t> {1, 3, 2, 0}), permutation);
    EXPECT_EQ((std::vector<size_t> {1, 3, 2, 0}), levels.permutation);
    EXPECT_EQ((std::vector<double> {0.56, 0.12, 0.45, 0.23}), levels.quantiles);
}

TEST_F(PercentileUtilTest, SortPermutationWithDuplicateQuantiles) {
    PercentileLevels levels;
    levels.quantiles = {0.7, 0.2, 0.2, 0.9, 0.7};
    levels.permutation = {0, 1, 2, 3, 4};

    const auto& permutation = levels.get_permutation();

    ASSERT_EQ(levels.quantiles.size(), permutation.size());
    std::vector<size_t> sorted_perm = permutation;
    std::sort(sorted_perm.begin(), sorted_perm.end());
    EXPECT_EQ((std::vector<size_t> {0, 1, 2, 3, 4}), sorted_perm);
    for (size_t i = 1; i < permutation.size(); ++i) {
        EXPECT_LE(levels.quantiles[permutation[i - 1]], levels.quantiles[permutation[i]]);
    }
}

TEST_F(PercentileUtilTest, WriteReadDefersPermutationSort) {
    PercentileLevels levels;
    levels.quantiles = {0.56, 0.12, 0.45, 0.23};
    levels.permutation = {0, 1, 2, 3};

    auto col = ColumnString::create();
    BufferWritable writer(*col);
    levels.write(writer);
    writer.commit();

    StringRef data(col->get_chars().data(), col->get_chars().size());
    BufferReadable reader(data);

    PercentileLevels restored;
    restored.read(reader);

    EXPECT_EQ(levels.quantiles, restored.quantiles);
    EXPECT_EQ((std::vector<size_t> {0, 1, 2, 3}), restored.permutation);
    EXPECT_EQ((std::vector<size_t> {1, 3, 2, 0}), restored.get_permutation());
}

TEST_F(PercentileUtilTest, ReadBuildsPermutationFromUnsortedQuantiles) {
    auto col = ColumnString::create();
    BufferWritable writer(*col);
    writer.write_binary(4);
    writer.write_binary(0.56);
    writer.write_binary(0.12);
    writer.write_binary(0.45);
    writer.write_binary(0.23);
    writer.commit();

    StringRef data(col->get_chars().data(), col->get_chars().size());
    BufferReadable reader(data);

    PercentileLevels levels;
    levels.read(reader);

    EXPECT_EQ((std::vector<double> {0.56, 0.12, 0.45, 0.23}), levels.quantiles);
    EXPECT_EQ((std::vector<size_t> {0, 1, 2, 3}), levels.permutation);
    EXPECT_EQ((std::vector<size_t> {1, 3, 2, 0}), levels.get_permutation());
}

TEST_F(PercentileUtilTest, MergeFromEmptyCopiesState) {
    PercentileLevels src;
    src.quantiles = {0.56, 0.12, 0.45, 0.23};
    src.permutation = {0, 1, 2, 3};

    PercentileLevels dst;
    dst.merge(src);

    EXPECT_EQ(src.quantiles, dst.quantiles);
    EXPECT_EQ(src.permutation, dst.permutation);
}

TEST_F(PercentileUtilTest, MergeEmptyRightKeepsLeft) {
    PercentileLevels lhs;
    lhs.quantiles = {0.56, 0.12, 0.45, 0.23};
    lhs.permutation = {0, 1, 2, 3};

    PercentileLevels rhs;
    lhs.merge(rhs);

    EXPECT_EQ((std::vector<double> {0.56, 0.12, 0.45, 0.23}), lhs.quantiles);
    EXPECT_EQ((std::vector<size_t> {0, 1, 2, 3}), lhs.permutation);
    EXPECT_EQ((std::vector<size_t> {1, 3, 2, 0}), lhs.get_permutation());
}

TEST_F(PercentileUtilTest, MergeSameStateKeepsState) {
    PercentileLevels lhs;
    lhs.quantiles = {0.56, 0.12, 0.45, 0.23};
    lhs.permutation = {0, 1, 2, 3};

    PercentileLevels rhs;
    rhs.quantiles = lhs.quantiles;
    rhs.permutation = lhs.permutation;

    lhs.merge(rhs);

    EXPECT_EQ((std::vector<double> {0.56, 0.12, 0.45, 0.23}), lhs.quantiles);
    EXPECT_EQ((std::vector<size_t> {0, 1, 2, 3}), lhs.permutation);
    EXPECT_EQ((std::vector<size_t> {1, 3, 2, 0}), lhs.get_permutation());
}

TEST_F(PercentileUtilTest, ClearResetsState) {
    PercentileLevels levels;
    levels.quantiles = {0.56, 0.12, 0.45, 0.23};
    levels.permutation = {1, 3, 2, 0};

    levels.clear();

    EXPECT_TRUE(levels.empty());
    EXPECT_TRUE(levels.quantiles.empty());
    EXPECT_TRUE(levels.permutation.empty());
}

} // namespace doris
