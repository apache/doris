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

#include "core/column/column_filter_helper.h"

#include <gtest/gtest.h>

#include "core/assert_cast.h"

namespace doris {

TEST(ColumnFilterHelperTest, ResizeFillAndInsertKeepNullableUInt8NotNull) {
    auto nullable = ColumnNullable::create(ColumnUInt8::create(), ColumnUInt8::create());

    ColumnFilterHelper helper(*nullable);
    helper.reserve(8);
    helper.resize_fill(3, 1);
    helper.insert_value(0);

    EXPECT_EQ(helper.size(), 4);

    const auto& value_column = assert_cast<const ColumnUInt8&>(nullable->get_nested_column());
    const auto& values = value_column.get_data();
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(values[0], 1);
    EXPECT_EQ(values[1], 1);
    EXPECT_EQ(values[2], 1);
    EXPECT_EQ(values[3], 0);

    const auto& null_map = nullable->get_null_map_data();
    ASSERT_EQ(null_map.size(), 4);
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 0);
    EXPECT_EQ(null_map[2], 0);
    EXPECT_EQ(null_map[3], 0);
}

} // namespace doris
