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

TEST(MemTableSort, Tie) {
    auto t = Tie {10, 30};
    for (int i = 10; i < 30; i++) {
        EXPECT_EQ(t[i], 1);
    }

    r[13] = r[14] = r[22] = r[29] = 0;
    auto it = t.iter();

    EXPECT_EQ(it.next(), true);
    EXPECT_EQ(it.left(), 10);
    EXPECT_EQ(it.right(), 13);

    EXPECT_EQ(it.next(), true);
    EXPECT_EQ(it.left(), 14);
    EXPECT_EQ(it.right(), 22);

    EXPECT_EQ(it.next(), true);
    EXPECT_EQ(it.left(), 22);
    EXPECT_EQ(it.right(), 29);

    EXPECT_EQ(it.next(), false);
}

} // namespace doris
