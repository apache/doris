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
#include "exprs/function/array/function_array_hash.h"
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

template <typename Hash>
void check_uuid_set_probe_growth(size_t count) {
    size_t comparisons = 0;
    phmap::flat_hash_set<UUIDValueType, Hash, CountingUuidEqual> set(
            0, Hash {}, CountingUuidEqual {&comparisons});
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
    EXPECT_LT(comparisons, 64 * count);
}

TEST(DefaultHashTest, UuidSameLowBitsHaveLinearProbeGrowth) {
    for (const size_t count : {1024, 4096, 16384}) {
        check_uuid_set_probe_growth<DefaultHash<UUIDValueType>>(count);
    }
}

TEST(ArraySetHashTest, UuidSetHasLinearProbeGrowth) {
    for (const size_t count : {1024, 4096, 16384}) {
        check_uuid_set_probe_growth<ArraySetHash<UUIDValueType>>(count);
    }
}

TEST(ArraySetHashTest, UuidMapHasLinearProbeGrowth) {
    for (const size_t count : {1024, 4096, 16384}) {
        size_t comparisons = 0;
        phmap::flat_hash_map<UUIDValueType, size_t, ArraySetHash<UUIDValueType>, CountingUuidEqual>
                map(0, ArraySetHash<UUIDValueType> {}, CountingUuidEqual {&comparisons});
        const auto key = [](size_t high) {
            return (static_cast<UUIDValueType>(high) << 64) | 0x9234001122334455ULL;
        };
        // Exercise union/intersect insertion and except_all counting, including duplicates.
        for (size_t i = 1; i <= count; ++i) {
            ++map[key(i)];
            ++map[key(i)];
        }
        EXPECT_EQ(count, map.size());
        for (size_t i = 1; i <= count; ++i) {
            const auto entry = map.find(key(i));
            ASSERT_NE(entry, map.end());
            EXPECT_EQ(2, entry->second);
            --entry->second;
            EXPECT_EQ(map.end(), map.find(key(i + count)));
        }
        EXPECT_LT(comparisons, 64 * count);
    }
}

TEST(ArraySetHashTest, PreserveOtherKeyHashers) {
    static_assert(std::is_same_v<ArraySetHash<Int64>, phmap::Hash<Int64>>);
    static_assert(std::is_same_v<ArraySetHash<Int128>, phmap::Hash<Int128>>);
    static_assert(std::is_same_v<ArraySetHash<Float64>, phmap::Hash<Float64>>);
}

} // namespace doris
