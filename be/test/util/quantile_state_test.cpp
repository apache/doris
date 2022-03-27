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

#include "util/quantile_state.h"

#include <gtest/gtest.h>

namespace doris {
using DoubleQuantileState = QuantileState<double>;

TEST(QuantileStateTest, merge) {
    DoubleQuantileState empty;
    ASSERT_EQ(EMPTY, empty._type);
    empty.add_value(1);
    ASSERT_EQ(SINGLE, empty._type);
    empty.add_value(2);
    empty.add_value(3);
    empty.add_value(4);
    empty.add_value(5);
    ASSERT_EQ(1, empty.get_value_by_percentile(0));
    ASSERT_EQ(3, empty.get_value_by_percentile(0.5));
    ASSERT_EQ(5, empty.get_value_by_percentile(1));

    DoubleQuantileState another;
    another.add_value(6);
    another.add_value(7);
    another.add_value(8);
    another.add_value(9);
    another.add_value(10);
    ASSERT_EQ(6, another.get_value_by_percentile(0));
    ASSERT_EQ(8, another.get_value_by_percentile(0.5));
    ASSERT_EQ(10, another.get_value_by_percentile(1));

    another.merge(empty);
    ASSERT_EQ(1, another.get_value_by_percentile(0));
    ASSERT_EQ(5.5, another.get_value_by_percentile(0.5));
    ASSERT_EQ(10, another.get_value_by_percentile(1));
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
