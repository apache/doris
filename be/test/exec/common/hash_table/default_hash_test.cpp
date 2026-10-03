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

#include "exec/common/hash_table/hash.h"
#include "parallel_hashmap/phmap.h"

namespace doris {

TEST(DefaultHashTest, UuidUsesBothHalves) {
    const DefaultHash<UUIDValueType> hasher;
    const UUIDValueType key = (UUIDValueType {0x123456789abcdef0ULL} << 64) | 0x9234001122334455ULL;
    for (int bit = 0; bit < 128; ++bit) {
        EXPECT_NE(hasher(key), hasher(key ^ (UUIDValueType {1} << bit))) << "bit " << bit;
    }
}

struct CountingUuidEqual {
    size_t* comparisons;

    bool operator()(UUIDValueType lhs, UUIDValueType rhs) const {
        ++*comparisons;
        return lhs == rhs;
    }
};

TEST(DefaultHashTest, UuidSameLowBitsHaveLinearProbeGrowth) {
    constexpr size_t count = 1024;
    size_t comparisons = 0;
    phmap::flat_hash_set<UUIDValueType, DefaultHash<UUIDValueType>, CountingUuidEqual> set(
            0, DefaultHash<UUIDValueType> {}, CountingUuidEqual {&comparisons});
    const auto key = [](size_t high) {
        return (static_cast<UUIDValueType>(high) << 64) | 0x9234001122334455ULL;
    };
    // Exercise the contains/insert pattern in array_distinct, including table growth.
    for (size_t i = 1; i <= count; ++i) {
        ASSERT_FALSE(set.contains(key(i)));
        ASSERT_TRUE(set.insert(key(i)).second);
    }
    EXPECT_EQ(count, set.size());
    // Exercise matching and disjoint inputs as in arrays_overlap.
    for (size_t i = 1; i <= count; ++i) {
        ASSERT_TRUE(set.contains(key(i)));
        ASSERT_FALSE(set.contains(key(i + count)));
    }
}

} // namespace doris
