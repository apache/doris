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

#include "olap/memtable.h"

namespace doris {

class MemTableSortTest : public ::testing::Test {};

TEST_F(MemTableSortTest, Tie) {
    auto t0 = Tie {0, 0};
    EXPECT_FALSE(t0.iter().next());

    auto tie = Tie {0, 1};
    EXPECT_FALSE(tie.iter().next());

    auto t = Tie {10, 30};
    for (int i = 10; i < 30; i++) {
        EXPECT_EQ(t[i], 1);
    }

    auto it1 = t.iter();
    EXPECT_TRUE(it1.next());
    EXPECT_EQ(it1.left(), 10);
    EXPECT_EQ(it1.right(), 30);

    EXPECT_FALSE(it1.next());

    t[13] = t[14] = t[22] = t[29] = 0;
    auto it2 = t.iter();

    EXPECT_TRUE(it2.next());
    EXPECT_EQ(it2.left(), 10);
    EXPECT_EQ(it2.right(), 13);

    EXPECT_TRUE(it2.next());
    EXPECT_EQ(it2.left(), 14);
    EXPECT_EQ(it2.right(), 22);

    EXPECT_TRUE(it2.next());
    EXPECT_EQ(it2.left(), 22);
    EXPECT_EQ(it2.right(), 29);

    EXPECT_FALSE(it2.next());
    EXPECT_FALSE(it2.next());

    // 100000000...
    for (int i = 11; i < 30; i++) {
        t[i] = 0;
    }
    EXPECT_FALSE(t.iter().next());

    // 000000000...
    t[10] = 0;
    EXPECT_FALSE(t.iter().next());

    // 000000000...001
    t[29] = 1;
    auto it3 = t.iter();
    EXPECT_TRUE(it3.next());
    EXPECT_EQ(it3.left(), 28);
    EXPECT_EQ(it3.right(), 30);

    EXPECT_FALSE(it3.next());
}

} // namespace doris
