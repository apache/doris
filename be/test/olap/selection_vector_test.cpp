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

#include "olap/selection_vector.h"

#include <gtest/gtest.h>

namespace doris {

class SelectionVectorTest : public testing::Test {};

TEST_F(SelectionVectorTest, Normal) {
    SelectionVector sel_vel(10);
    EXPECT_EQ(10, sel_vel.nrows());
    sel_vel.set_all_true();
    EXPECT_EQ("   0: 11111111 11 \n", sel_vel.to_string());
    sel_vel.set_all_false();
    EXPECT_EQ("   0: 00000000 00 \n", sel_vel.to_string());
    sel_vel.set_row_selected(7);
    EXPECT_TRUE(sel_vel.is_row_selected(7));
    EXPECT_TRUE(sel_vel.any_selected());
    EXPECT_EQ("   0: 00000001 00 \n", sel_vel.to_string());
    sel_vel.clear_bit(7);
    EXPECT_EQ("   0: 00000000 00 \n", sel_vel.to_string());
}

} // namespace doris
