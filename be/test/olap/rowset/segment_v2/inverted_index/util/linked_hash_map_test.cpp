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

#include "olap/rowset/segment_v2/inverted_index/util/linked_hash_map.h"

#include <gtest/gtest.h>

#include <string>

namespace doris::segment_v2::inverted_index {

TEST(LinkedHashMapTest, InsertAndFind) {
    LinkedHashMap<int32_t, std::string> map;

    map.insert(1, "one");
    auto* value = map.find(1);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, "one");

    EXPECT_EQ(map.find(2), nullptr);
}

TEST(LinkedHashMapTest, OrderPreservation) {
    LinkedHashMap<int32_t, std::string> map;

    map.insert(1, "one");
    map.insert(2, "two");
    map.insert(3, "three");

    const auto& order = map.to_vector();
    ASSERT_EQ(order, (std::vector<int32_t> {1, 2, 3}));
}

TEST(LinkedHashMapTest, OverwriteExistingKey) {
    LinkedHashMap<int32_t, std::string> map;

    map.insert(1, "old");
    map.insert(1, "new");

    EXPECT_EQ(*map.find(1), "new");
    EXPECT_EQ(map.size(), 1);
    EXPECT_EQ(map.to_vector(), (std::vector<int32_t> {1}));
}

TEST(LinkedHashMapTest, EraseOperations) {
    LinkedHashMap<int32_t, std::string> map;

    map.insert(1, "one");
    map.insert(2, "two");

    map.erase(1);
    EXPECT_FALSE(map.contains(1));
    EXPECT_EQ(map.size(), 1);
    EXPECT_EQ(map.to_vector(), (std::vector<int32_t> {2}));

    map.erase(99);
    EXPECT_EQ(map.size(), 1);
}

TEST(LinkedHashMapTest, EmptyAndSize) {
    LinkedHashMap<int32_t, std::string> map;

    EXPECT_TRUE(map.empty());
    EXPECT_EQ(map.size(), 0);

    map.insert(1, "one");
    EXPECT_FALSE(map.empty());
    EXPECT_EQ(map.size(), 1);

    map.erase(1);
    EXPECT_TRUE(map.empty());
}

TEST(LinkedHashMapTest, ComplexOperations) {
    LinkedHashMap<std::string, int32_t> map;

    map.insert("first", 1);
    map.insert("second", 2);
    map.insert("third", 3);

    map.erase("second");
    EXPECT_EQ(map.to_vector(), (std::vector<std::string> {"first", "third"}));

    map.insert("second", 22);
    EXPECT_EQ(map.to_vector(), (std::vector<std::string> {"first", "third", "second"}));
    EXPECT_EQ(*map.find("second"), 22);
}

TEST(LinkedHashMapTest, ConstFind) {
    LinkedHashMap<int32_t, std::string> map;
    map.insert(42, "answer");

    const auto& const_map = map;
    const auto* value = const_map.find(42);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, "answer");

    EXPECT_EQ(const_map.find(99), nullptr);
}

TEST(LinkedHashMapTest, EdgeCases) {
    LinkedHashMap<int32_t, void*> map;

    map.insert(0, nullptr);
    auto* value = map.find(0);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, nullptr);

    const int32_t COUNT = 1000;
    for (int32_t i = 0; i < COUNT; ++i) {
        map.insert(i, reinterpret_cast<void*>(i));
    }
    EXPECT_EQ(map.size(), COUNT);

    auto vec = map.to_vector();
    for (int32_t i = 0; i < COUNT; ++i) {
        EXPECT_EQ(vec[i], i);
    }
}

} // namespace doris::segment_v2::inverted_index
