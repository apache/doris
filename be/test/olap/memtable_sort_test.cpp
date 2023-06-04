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

#include "olap/memtable.h"

#include <gtest/gtest.h>

namespace doris {

class MemTableSortTest : public ::testing::Test {};

TEST_F(MemTableSortTest, Tie) {
    auto tie = Tie {0, 1};
    EXPECT_FALSE(tie.iter().next());

    auto t = Tie {10, 30};
    for (int i = 10; i < 30; i++) {
        EXPECT_EQ(t[i], 1);
    }

    auto it1 = t.iter();
    EXPECT_EQ(it1.next(), true);
    EXPECT_EQ(it1.left(), 10);
    EXPECT_EQ(it1.right(), 30);

    EXPECT_EQ(it1.next(), false);

    t[13] = t[14] = t[22] = t[29] = 0;
    auto it2 = t.iter();

    EXPECT_EQ(it2.next(), true);
    EXPECT_EQ(it2.left(), 10);
    EXPECT_EQ(it2.right(), 13);

    EXPECT_EQ(it2.next(), true);
    EXPECT_EQ(it2.left(), 14);
    EXPECT_EQ(it2.right(), 22);

    EXPECT_EQ(it2.next(), true);
    EXPECT_EQ(it2.left(), 22);
    EXPECT_EQ(it2.right(), 29);

    EXPECT_EQ(it2.next(), false);

    // 100000000...
    for (int i = 11; i < 30; i++) {
        t[i] = 0;
    }
    auto it3 = t.iter();
    EXPECT_EQ(it3.next(), false);

    // 000000000...
    t[10] = 0;
    auto it4 = t.iter();
    EXPECT_EQ(it4.next(), false);

    // 000000000...001
    t[29] = 1;
    auto it5 = t.iter();
    EXPECT_EQ(it5.next(), true);
    EXPECT_EQ(it5.left(), 28);
    EXPECT_EQ(it5.right(), 30);
    EXPECT_EQ(it5.next(), false);
}

} // namespace doris
