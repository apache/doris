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
#include <ostream>
#include <random>
#include <sstream>
#include <vector>

#include "vec/common/hash_table/ph_hash_set.h"

namespace doris::vectorized {

template <typename T>
void test_type_hash_map() {
    SmallFixedSizeHashSet<T> small_hash_set;
    PHHashSet<T, HashCRC32<T>> ph_hash_set;
    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size());
    EXPECT_EQ(0, small_hash_set.size());
    EXPECT_EQ(0, ph_hash_set.size());

    void* lookup_result_get_mapped = nullptr;
    std::vector<T> input_data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<T> dis(std::numeric_limits<T>::min(),
                                         std::numeric_limits<T>::max());

    for (int i = 0; i < 20; ++i) {
        input_data.push_back(dis(gen));
    }

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

    auto print_data = [&]() -> std::string {
        std::stringstream ss;
        for (int x : input_data) {
            ss << x << " ";
        }
        return ss.str();
    };
    EXPECT_EQ(small_hash_set.size(), ph_hash_set.size()) << print_data();

    auto move_small_hash_set = std::move(small_hash_set);
    auto move_ph_hash_set = std::move(ph_hash_set);

    for (auto& data : input_data) {
        move_small_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                         [&](auto& ctor, auto& key_holder) {
                                             EXPECT_TRUE(false);
                                             ctor(key_holder);
                                         });
        move_ph_hash_set.lazy_emplace(data, lookup_result_get_mapped,
                                      [&](auto& ctor, auto& key_holder) {
                                          EXPECT_TRUE(false);
                                          ctor(key_holder);
                                      });
    }

    EXPECT_EQ(move_small_hash_set.size(), move_ph_hash_set.size()) << print_data();
}

TEST(SmallSizeHashSetTest, testint8) {
    for (int i = 0; i < 100; i++) {
        test_type_hash_map<int8_t>();
    }
}

TEST(SmallSizeHashSetTest, testuint8) {
    for (int i = 0; i < 100; i++) {
        test_type_hash_map<uint8_t>();
    }
}

TEST(SmallSizeHashSetTest, testint16) {
    for (int i = 0; i < 100; i++) {
        test_type_hash_map<int16_t>();
    }
}

TEST(SmallSizeHashSetTest, testuint16) {
    for (int i = 0; i < 100; i++) {
        test_type_hash_map<uint16_t>();
    }
}
} // namespace doris::vectorized
