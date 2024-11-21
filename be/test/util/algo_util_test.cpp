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

#include <boost/utility/binary.hpp>
#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "util/algorithm_util.h"

namespace doris {

TEST(TestAlgoUtil, DescentByStep) {
    // double descent_by_step(int step_num, int64_t low_bound, int64_t high_bound, int64_t current)
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 100, 200, 101), 0.9);
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 100, 200, 99), 1);
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 200, 100, 101), 1);
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 100, 200, 111), 0.8);
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 100, 200, 188), 0.1);
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 100, 200, 100), 1);
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 100, 200, 200), 0);
    EXPECT_EQ(AlgoUtil::descent_by_step(10, 100, 200, 300), 0);

    EXPECT_EQ(AlgoUtil::descent_by_step(4, 100, 200, 133), 0.5);
    EXPECT_EQ(AlgoUtil::descent_by_step(4, 100, 200, 110), 0.75);
    EXPECT_EQ(AlgoUtil::descent_by_step(4, 100, 200, 125), 0.75);
    EXPECT_EQ(AlgoUtil::descent_by_step(4, 100, 200, 126), 0.5);
}

} // namespace doris
