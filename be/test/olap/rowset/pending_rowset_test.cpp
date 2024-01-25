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

#include "olap/rowset/pending_rowset_helper.h"

namespace doris {

TEST(PendingRowsetTest, PendingRowsetGuard) {
    PendingRowsetSet set;
    RowsetId rowset_id;
    rowset_id.init(123);
    EXPECT_FALSE(set.contains(rowset_id));
    {
        auto guard = set.add(rowset_id);
        EXPECT_TRUE(set.contains(rowset_id));
    }
    EXPECT_FALSE(set.contains(rowset_id));
    auto guard = set.add(rowset_id);
    {
        auto guard1 = set.add(rowset_id);
        EXPECT_TRUE(set.contains(rowset_id));
        guard = std::move(guard1);
        EXPECT_TRUE(set.contains(rowset_id));
    }
    EXPECT_TRUE(set.contains(rowset_id));
}

} // namespace doris
