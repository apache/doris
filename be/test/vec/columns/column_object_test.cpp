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

#include "vec/columns/column_object.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

namespace doris::vectorized {

class ColumnObjectTest : public ::testing::Test {};

TEST_F(ColumnObjectTest, test_pop_back) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int(123);
    Field field_string("hello");

    subcolumn.insert(field_int);
    subcolumn.insert(field_string);

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(Int8)");
}

} // namespace doris::vectorized
