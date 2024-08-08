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

#include <regex>

#include "vec/exec/format/parquet/parquet_common.h"

namespace doris {
namespace vectorized {
class ParquetCorruptStatisticsTest : public testing::Test {
public:
    ParquetCorruptStatisticsTest() = default;
};

TEST_F(ParquetCorruptStatisticsTest, test_only_applies_to_binary) {
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.6.0 (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.6.0 (build abcd)",
                                                            tparquet::Type::FIXED_LEN_BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.6.0 (build abcd)", tparquet::Type::DOUBLE));
}

TEST_F(ParquetCorruptStatisticsTest, test_corrupt_statistics) {
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.6.0 (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.4.2 (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.6.100 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.7.999 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.6.22rc99 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.6.1-SNAPSHOT (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.6.0t-01-abcdefg (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("unparseable string",
                                                            tparquet::Type::BYTE_ARRAY));

    // missing semver
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version  (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));

    // missing build hash
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.6.0 (build )",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.6.0 (build)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version (build)",
                                                            tparquet::Type::BYTE_ARRAY));

    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics("imapla version 1.6.0 (build abcd)",
                                                             tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics("imapla version 1.10.0 (build abcd)",
                                                             tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.8.0 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.8.1 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.8.1rc3 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.8.1rc3-SNAPSHOT (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.9.0 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 2.0.0 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.9.0t-01-abcdefg (build abcd)", tparquet::Type::BYTE_ARRAY));

    // missing semver
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics("impala version (build abcd)",
                                                             tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics("impala version  (build abcd)",
                                                             tparquet::Type::BYTE_ARRAY));

    // missing build hash
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics("impala version 1.6.0 (build )",
                                                             tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics("impala version 1.6.0 (build)",
                                                             tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics("impala version (build)",
                                                             tparquet::Type::BYTE_ARRAY));
}

TEST_F(ParquetCorruptStatisticsTest, test_distribution_corrupt_statistics) {
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.5.0-cdh5.4.999 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.5.0-cdh5.5.0-SNAPSHOT (build "
            "956ed6c14c611b4c4eaaa1d6e5b9a9c6d4dfa336)",
            tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.5.0-cdh5.5.0 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.5.0-cdh5.5.1 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_FALSE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.5.0-cdh5.6.0 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics(
            "parquet-mr version 1.4.10 (build abcd)", tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.5.0 (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.5.1 (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.6.0 (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
    EXPECT_TRUE(CorruptStatistics::should_ignore_statistics("parquet-mr version 1.7.0 (build abcd)",
                                                            tparquet::Type::BYTE_ARRAY));
}

} // namespace vectorized
} // namespace doris
