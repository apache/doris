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

#include "vec/core/accurate_comparison.h"

#include <gtest/gtest.h>

namespace doris::vectorized {
TEST(VAccurateComparison, TestsOP) {
    EXPECT_TRUE((EqualsOp<TYPE_INT>::apply(1, 1)));
    EXPECT_FALSE((EqualsOp<TYPE_INT>::apply(1, 2)));
    EXPECT_TRUE((NotEqualsOp<TYPE_INT>::apply(1, 2)));
    EXPECT_FALSE((NotEqualsOp<TYPE_INT>::apply(1, 1)));
    EXPECT_TRUE((LessOp<TYPE_INT>::apply(1, 2)));
    EXPECT_FALSE((LessOp<TYPE_INT>::apply(2, 1)));
    EXPECT_TRUE((GreaterOp<TYPE_INT>::apply(2, 1)));
    EXPECT_FALSE((GreaterOp<TYPE_INT>::apply(1, 2)));
    EXPECT_TRUE((LessOrEqualsOp<TYPE_INT>::apply(1, 2)));
    EXPECT_TRUE((LessOrEqualsOp<TYPE_INT>::apply(1, 1)));
    EXPECT_FALSE((LessOrEqualsOp<TYPE_INT>::apply(2, 1)));
    EXPECT_TRUE((GreaterOrEqualsOp<TYPE_INT>::apply(2, 1)));
    EXPECT_TRUE((GreaterOrEqualsOp<TYPE_INT>::apply(1, 1)));
}

} // namespace doris::vectorized
