
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

#include "vec/columns/column_decimal.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/columns_number.h"

namespace doris::vectorized {

TEST(ColumnDecimalTest, CloneDecimalv2TypeTest) {
    auto column1 = ColumnDecimal128::create(0, 1);
    EXPECT_EQ(false, column1->is_decimalv2_type());
    EXPECT_EQ(false, column1->clone_resized(0)->is_decimalv2_type());
    column1->set_decimalv2_type();
    EXPECT_EQ(true, column1->is_decimalv2_type());
    EXPECT_EQ(true, column1->clone_resized(0)->is_decimalv2_type());

    auto column2 = ColumnDecimal64::create(0, 1);
    EXPECT_EQ(false, column2->is_decimalv2_type());
    EXPECT_EQ(false, column2->clone_resized(0)->is_decimalv2_type());
    column2->set_decimalv2_type();
    EXPECT_EQ(true, column2->is_decimalv2_type());
    EXPECT_EQ(true, column2->clone_resized(0)->is_decimalv2_type());

    auto column3 = ColumnDecimal32::create(0, 1);
    EXPECT_EQ(false, column3->is_decimalv2_type());
    EXPECT_EQ(false, column3->clone_resized(0)->is_decimalv2_type());
    column3->set_decimalv2_type();
    EXPECT_EQ(true, column3->is_decimalv2_type());
    EXPECT_EQ(true, column3->clone_resized(0)->is_decimalv2_type());
}

} // namespace doris::vectorized
