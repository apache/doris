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

#include "exec/scan/scan_key_bucket_pruner.h"

#include <gtest/gtest.h>

#include "core/column/column_string.h"
#include "storage/olap_utils.h"

namespace doris {
namespace {

std::unique_ptr<OlapScanRange> varchar_point(const std::string& value) {
    auto range = std::make_unique<OlapScanRange>();
    range->has_lower_bound = range->has_upper_bound = true;
    range->begin_scan_range.add_field(Field::create_field<TYPE_VARCHAR>(value));
    range->end_scan_range.add_field(Field::create_field<TYPE_VARCHAR>(value));
    return range;
}

} // namespace

TEST(ScanKeyBucketPrunerTest, MatchesDistributionHashAndPreservesRangeIdentity) {
    const std::vector<std::string> values {"", "a", "address000001", std::string("a\0b", 3),
                                           "中文"};
    // Python zlib.crc32(bytes) % 128, independent of the routing implementation.
    const std::vector<uint32_t> expected_buckets {0, 67, 76, 113, 55};
    auto column = ColumnString::create();
    std::vector<std::unique_ptr<OlapScanRange>> ranges;
    for (const auto& value : values) {
        ranges.push_back(varchar_point(value));
        column->insert_data(value.data(), value.size());
    }
    std::vector<uint32_t> hashes(values.size(), 0);
    column->update_crcs_with_value(hashes.data(), TYPE_VARCHAR,
                                   static_cast<uint32_t>(values.size()), 0, nullptr);
    ScanKeyBucketPruner pruner;
    ASSERT_TRUE(pruner.init(ranges, 128));
    size_t routed_count = 0;
    for (int bucket = 0; bucket < 128; ++bucket) {
        routed_count += pruner.ranges_for_bucket(bucket).size();
    }
    EXPECT_EQ(routed_count, ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
        EXPECT_EQ(hashes[i] % 128, expected_buckets[i]);
        const auto& routed = pruner.ranges_for_bucket(expected_buckets[i]);
        ASSERT_EQ(routed.size(), 1);
        EXPECT_EQ(routed.front(), ranges[i].get());
    }
    EXPECT_TRUE(pruner.ranges_for_bucket(1).empty());
}

TEST(ScanKeyBucketPrunerTest, KeepsDuplicatesAndBucketLocalOrder) {
    std::vector<std::unique_ptr<OlapScanRange>> ranges;
    ranges.push_back(varchar_point("a"));
    ranges.push_back(varchar_point("a"));
    ScanKeyBucketPruner pruner;
    ASSERT_TRUE(pruner.init(ranges, 128));
    EXPECT_EQ(pruner.ranges_for_bucket(67),
              (std::vector<OlapScanRange*> {ranges[0].get(), ranges[1].get()}));
}

TEST(ScanKeyBucketPrunerTest, RejectsIntervalsAndUnsupportedKeys) {
    ScanKeyBucketPruner pruner;
    std::vector<std::unique_ptr<OlapScanRange>> ranges;
    ranges.push_back(varchar_point("a"));
    auto check_fallback = [&](std::unique_ptr<OlapScanRange> range) {
        ASSERT_TRUE(pruner.init(ranges, 128));
        ranges.push_back(std::move(range));
        EXPECT_FALSE(pruner.init(ranges, 128));
        ranges.pop_back();
    };
    check_fallback(std::make_unique<OlapScanRange>());
    auto range = varchar_point("b");
    range->end_scan_range.get_field(0) = Field::create_field<TYPE_VARCHAR>("z");
    check_fallback(std::move(range));
    range = varchar_point("b");
    range->begin_include = false;
    check_fallback(std::move(range));
    range = varchar_point("b");
    range->end_include = false;
    check_fallback(std::move(range));
    range = varchar_point("b");
    range->has_upper_bound = false;
    check_fallback(std::move(range));
    range = varchar_point("b");
    range->begin_scan_range.add_null();
    range->end_scan_range.add_null();
    check_fallback(std::move(range));
    range = varchar_point("b");
    range->begin_scan_range.get_field(0) = Field(TYPE_NULL);
    range->end_scan_range.get_field(0) = Field(TYPE_NULL);
    check_fallback(std::move(range));
    range = varchar_point("b");
    range->begin_scan_range.get_field(0) = Field::create_field<TYPE_INT>(1);
    range->end_scan_range.get_field(0) = Field::create_field<TYPE_INT>(1);
    check_fallback(std::move(range));
}

} // namespace doris
