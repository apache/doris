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

#include "vec/common/space_saving.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/logging.h"
#include "vec/common/string_ref.h"

namespace doris::vectorized {

class SapceSavingTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    SapceSavingTest() = default;
    ~SapceSavingTest() override = default;
};

int getDaySeed() {
    std::time_t now = std::time(nullptr);
    std::tm* localTime = std::localtime(&now);
    localTime->tm_sec = 0;
    localTime->tm_min = 0;
    localTime->tm_hour = 0;

    return static_cast<int>(std::mktime(localTime) / (60 * 60 * 24));
}

std::string generateRandomIP() {
    std::string part1 = "127";
    std::string part2 = "0";
    std::string part3 = "0";
    std::string part4 = std::to_string(rand() % 256);

    return part1 + "." + part2 + "." + part3 + "." + part4;
}

int32_t generateRandomNumber() {
    return rand() % 256;
}

TEST_F(SapceSavingTest, test_space_saving_ip) {
    int seed = getDaySeed();
    std::srand(seed);

    SpaceSaving<StringRef> space_saving(256);
    std::unordered_map<StringRef, int32_t> count_map;

    std::vector<std::string> datas;
    for (int32_t i = 0; i < 100000; ++i) {
        datas.emplace_back(generateRandomIP());
    }

    for (auto& data : datas) {
        StringRef ref(data);
        space_saving.insert(ref);
        count_map[ref]++;
    }

    auto counts = space_saving.top_k(256);
    int32_t i = 0;
    int32_t j = 0;
    for (auto& iter : counts) {
        StringRef ref(iter.key);
        EXPECT_EQ(iter.count, count_map[ref]);
        i += iter.count;
        j += count_map[ref];
    }
    EXPECT_EQ(i, j);
}

TEST_F(SapceSavingTest, test_space_saving_number) {
    int seed = getDaySeed();
    std::srand(seed);

    SpaceSaving<int32_t> space_saving(256);
    std::unordered_map<int32_t, int32_t> count_map;

    std::vector<int32_t> datas;
    for (int32_t i = 0; i < 100000; ++i) {
        datas.emplace_back(generateRandomNumber());
    }

    for (auto& data : datas) {
        space_saving.insert(data);
        count_map[data]++;
    }

    auto counts = space_saving.top_k(256);
    int32_t i = 0;
    int32_t j = 0;
    for (auto& iter : counts) {
        EXPECT_EQ(iter.count, count_map[iter.key]);
        i += iter.count;
        j += count_map[iter.key];
    }
    EXPECT_EQ(i, j);
}

TEST_F(SapceSavingTest, test_space_saving_merge) {
    int seed = getDaySeed();
    std::srand(seed);

    SpaceSaving<StringRef> space_saving(256);
    std::unordered_map<StringRef, int32_t> count_map;

    // merge1
    std::vector<std::string> datas1;
    {
        SpaceSaving<StringRef> space_saving1(256);
        std::unordered_map<StringRef, int32_t> count_map1;
        for (int32_t i = 0; i < 100000; ++i) {
            datas1.emplace_back(generateRandomIP());
        }

        for (auto& data : datas1) {
            StringRef ref(data);
            space_saving1.insert(ref);
            count_map1[ref]++;
        }

        space_saving.merge(space_saving1);
        for (auto& iter1 : count_map1) {
            auto iter = count_map.find(iter1.first);
            if (iter != count_map.end()) {
                iter->second += iter1.second;
            } else {
                count_map[iter1.first] = iter1.second;
            }
        }
    }

    // merge2
    std::vector<std::string> datas2;
    {
        SpaceSaving<StringRef> space_saving1(256);
        std::unordered_map<StringRef, int32_t> count_map1;
        for (int32_t i = 0; i < 100000; ++i) {
            datas2.emplace_back(generateRandomIP());
        }

        for (auto& data : datas2) {
            StringRef ref(data);
            space_saving1.insert(ref);
            count_map1[ref]++;
        }

        space_saving.merge(space_saving1);
        for (auto& iter1 : count_map1) {
            auto iter = count_map.find(iter1.first);
            if (iter != count_map.end()) {
                iter->second += iter1.second;
            } else {
                count_map[iter1.first] = iter1.second;
            }
        }
    }

    auto counts = space_saving.top_k(256);
    int32_t i = 0;
    int32_t j = 0;
    for (auto& iter : counts) {
        StringRef ref(iter.key);
        EXPECT_EQ(iter.count, count_map[ref]);
        i += iter.count;
        j += count_map[ref];
    }
    EXPECT_EQ(i, j);
}

} // namespace doris::vectorized