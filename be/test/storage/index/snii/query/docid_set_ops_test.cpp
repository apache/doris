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

#include "storage/index/snii/query/internal/docid_set_ops.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/status.h"

namespace {

std::vector<uint32_t> Range(uint32_t begin, uint32_t end, uint32_t step = 1) {
    std::vector<uint32_t> out;
    for (uint32_t v = begin; v < end; v += step) {
        out.push_back(v);
    }
    return out;
}

} // namespace

TEST(SniiDocIdSetOps, UnionSortedManyDeduplicatesHighOverlapLists) {
    std::vector<std::vector<uint32_t>> lists;
    lists.reserve(257);
    lists.push_back(Range(0, 10000));
    for (uint32_t i = 0; i < 256; ++i) {
        lists.push_back(Range(i % 17, 10000, 17));
    }

    const std::vector<uint32_t> got = doris::snii::query::internal::union_sorted_many(lists);

    EXPECT_EQ(got, Range(0, 10000));
    EXPECT_TRUE(std::ranges::is_sorted(got));
    EXPECT_EQ(std::ranges::adjacent_find(got), got.end());
}

TEST(SniiDocIdSetOps, UnionSortedManyMergesDisjointLists) {
    const std::vector<std::vector<uint32_t>> lists = {{0, 3, 6}, {1, 4, 7}, {}, {2, 5, 8}};

    const std::vector<uint32_t> got = doris::snii::query::internal::union_sorted_many(lists);

    EXPECT_EQ(got, Range(0, 9));
}
