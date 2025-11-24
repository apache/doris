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
#include <testutil/column_helper.h>
#include <vec/columns/column_array.h>
#include <vec/columns/column_nothing.h>
#include <vec/columns/column_vector.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_number.h>

#include <vector>

namespace doris::vectorized {

TEST(ColumnNothingTest, Test) {
    ColumnNothing column_nothing(23);

    EXPECT_EQ(column_nothing.size(), 23);

    column_nothing.insert(Field {});

    EXPECT_EQ(column_nothing.size(), 24);

    EXPECT_EQ(column_nothing[0], Field {});

    {
        Field f;
        column_nothing.get(0, f);
        EXPECT_EQ(f, Field {});
    }

    {
        ColumnNothing column_nothing2(column_nothing);
        EXPECT_TRUE(column_nothing2.structure_equals(column_nothing));
    }
}

} // namespace doris::vectorized