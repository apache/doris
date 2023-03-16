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

#include "util/faststring.h"

#include <gtest/gtest.h>
#include <time.h>

#include <algorithm>
#include <cstring>
#include <memory>

#include "util/random.h"

namespace doris {
class FaststringTest : public ::testing::Test {};

void RandomString(void* dest, size_t n, doris::Random* rng) {
    size_t i = 0;
    uint32_t random = rng->Next();
    char* cdest = static_cast<char*>(dest);
    static const size_t sz = sizeof(random);
    if (n >= sz) {
        for (i = 0; i <= n - sz; i += sz) {
            memcpy(&cdest[i], &random, sizeof(random));
            random = rng->Next();
        }
    }
    memcpy(cdest + i, &random, n - i);
}

TEST_F(FaststringTest, TestShrinkToFit_Empty) {
    faststring s;
    s.shrink_to_fit();
    EXPECT_EQ(faststring::kInitialCapacity, s.capacity());
}

// Test that, if the string contents is shorter than the initial capacity
// of the faststring, shrink_to_fit() leaves the string in the built-in
// array.
TEST_F(FaststringTest, TestShrinkToFit_SmallerThanInitialCapacity) {
    faststring s;
    s.append("hello");
    s.shrink_to_fit();
    EXPECT_EQ(faststring::kInitialCapacity, s.capacity());
}

TEST_F(FaststringTest, TestShrinkToFit_Random) {
    doris::Random r(time(nullptr));
    int kMaxSize = faststring::kInitialCapacity * 2;
    std::unique_ptr<char[]> random_bytes(new char[kMaxSize]);
    RandomString(random_bytes.get(), kMaxSize, &r);

    faststring s;
    for (int i = 0; i < 100; i++) {
        int new_size = r.Uniform(kMaxSize);
        s.resize(new_size);
        memcpy(s.data(), random_bytes.get(), new_size);
        s.shrink_to_fit();
        EXPECT_EQ(0, memcmp(s.data(), random_bytes.get(), new_size));
        EXPECT_EQ(std::max<int>(faststring::kInitialCapacity, new_size), s.capacity());
    }
}

TEST_F(FaststringTest, TestPushBack) {
    faststring s;
    for (int i = 0; i < faststring::kInitialCapacity * 2; ++i) {
        s.push_back('a');
        EXPECT_LE(s.size(), s.capacity());
        EXPECT_EQ(i + 1, s.size());
        if (i + 1 <= faststring::kInitialCapacity) {
            EXPECT_EQ(s.capacity(), faststring::kInitialCapacity);
        }
    }
}

TEST_F(FaststringTest, TestAppend_Simple) {
    faststring s;
    EXPECT_EQ(s.capacity(), faststring::kInitialCapacity);
    EXPECT_EQ(s.size(), 0);

    // append empty string
    s.append("");
    EXPECT_EQ(s.capacity(), faststring::kInitialCapacity);
    EXPECT_EQ(s.size(), 0);

    // len_ < kInitialCapacity
    s.append("hello");
    EXPECT_EQ(s.capacity(), faststring::kInitialCapacity);
    EXPECT_EQ(s.size(), 5);

    // len_ > kInitialCapacity
    std::string tmp(faststring::kInitialCapacity, 'a');
    s.append(tmp);
    EXPECT_GT(s.capacity(), faststring::kInitialCapacity);
    EXPECT_EQ(s.size(), 5 + faststring::kInitialCapacity);
}

TEST_F(FaststringTest, TestAppend_ExponentiallyExpand) {
    size_t initial_capacity = faststring::kInitialCapacity / 2;
    std::string tmp1(initial_capacity, 'a');

    {
        // Less than 150% after expansion
        faststring s;
        std::string tmp2(faststring::kInitialCapacity - 1, 'a');
        s.append(tmp1);
        s.append(tmp2);
        EXPECT_EQ(s.capacity(), faststring::kInitialCapacity * 3 / 2);
    }

    {
        // More than 150% after expansion
        faststring s;
        std::string tmp2(faststring::kInitialCapacity + 1, 'a');
        s.append(tmp1);
        s.append(tmp2);
        EXPECT_GT(s.capacity(), faststring::kInitialCapacity * 3 / 2);
    }
}

} // namespace doris
