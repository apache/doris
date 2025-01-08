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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "vec/common/hash_table/ph_hash_set.h"

namespace doris::vectorized {

TEST(SmallSizeHashSetTest, testint8) {
    SmallFixedSizeHashSet<int8_t> small_hash_set;
    PHHashSet<int8_t, HashCRC32<int8_t>> ph_hash_set;
    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size());
    EXPECT_EQ(0, small_hash_set.size());
    EXPECT_EQ(0, ph_hash_set.size());

    void* lookup_result_get_mapped = nullptr;

    std::vector<int8_t> input_data = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                      -1, -2, -3, -4, -5, -6, -7, -8, -9, -10};
    for (auto& data : input_data) {
        small_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                    [&](auto& ctor, auto& key_holder) { ctor(key_holder); });
        ph_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                 [&](auto& ctor, auto& key_holder) { ctor(key_holder); });
    }

    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size());

    for (auto& data : input_data) {
        small_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                    [&](auto& ctor, auto& key_holder) {
                                        EXPECT_TRUE(false);
                                        ctor(key_holder);
                                    });
        ph_hash_set.lazy_emplace(data, lookup_result_get_mapped, [&](auto& ctor, auto& key_holder) {
            EXPECT_TRUE(false);
            ctor(key_holder);
        });
    }
    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size());

    std::cout << "hash set size : " << small_hash_set.size() << "\t" << ph_hash_set.size();
}

TEST(SmallSizeHashSetTest, testuint8) {
    SmallFixedSizeHashSet<uint8_t> small_hash_set;
    PHHashSet<int8_t, HashCRC32<uint8_t>> ph_hash_set;
    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size());
    EXPECT_EQ(0, small_hash_set.size());
    EXPECT_EQ(0, ph_hash_set.size());

    void* lookup_result_get_mapped = nullptr;

    std::vector<uint8_t> input_data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 233, 24, 51, 231};
    for (auto& data : input_data) {
        small_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                    [&](auto& ctor, auto& key_holder) { ctor(key_holder); });
        ph_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                 [&](auto& ctor, auto& key_holder) { ctor(key_holder); });
    }

    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size());

    for (auto& data : input_data) {
        small_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                    [&](auto& ctor, auto& key_holder) {
                                        EXPECT_TRUE(false);
                                        ctor(key_holder);
                                    });
        ph_hash_set.lazy_emplace(data, lookup_result_get_mapped, [&](auto& ctor, auto& key_holder) {
            EXPECT_TRUE(false);
            ctor(key_holder);
        });
    }
    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size());
    std::cout << "hash set size : " << small_hash_set.size() << "\t" << ph_hash_set.size();
}
} // namespace doris::vectorized
