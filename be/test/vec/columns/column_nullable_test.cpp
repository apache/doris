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

#include "vec/columns/column_nullable.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "column_nullable_test.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

TEST(ColumnNullableTest, NullTest) {
    ColumnNullable::MutablePtr null_col = create_column_nullable<Int64>(500, true);
    EXPECT_TRUE(null_col->has_null());

    ColumnNullable::MutablePtr dst_col =
            ColumnNullable::create(create_nested_column<Int64>(10), ColumnUInt8::create(10));
    EXPECT_FALSE(dst_col->has_null());

    ColumnInt64::MutablePtr source_col = ColumnInt64::create();
    source_col->insert_range_of_integer(0, 100);

    dst_col->insert(Field());
    EXPECT_TRUE(dst_col->has_null());
    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_many_from_not_nullable(*source_col, 0, 10);
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from_not_nullable(*source_col, 5);
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_many_from_not_nullable(*source_col, 5, 5);
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_range_from_not_nullable(*source_col, 5, 5);
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_range_from(
            *ColumnNullable::create(std::move(source_col), ColumnUInt8::create(10)), 5, 5);
    EXPECT_FALSE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_null_elements(10);
    EXPECT_TRUE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from(*null_col, 100);
    EXPECT_TRUE(dst_col->has_null());

    auto tmp_col =
            ColumnNullable::create(create_nested_column<Int64>(10), ColumnUInt8::create(10, 1));

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from(*tmp_col, 9);
    EXPECT_TRUE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_range_from(*tmp_col, 0, 3);
    EXPECT_TRUE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from(*tmp_col, 9);
    EXPECT_TRUE(dst_col->has_null());
}

TEST(ColumnNullableTest, PredicateTest) {
    auto nullable_pred =
            ColumnNullable::create(PredicateColumnType<TYPE_DATE>::create(), ColumnUInt8::create());
    nullable_pred->insert_many_defaults(3);
    EXPECT_TRUE(nullable_pred->has_null());
    nullable_pred->insert_null_elements(10);
    EXPECT_TRUE(nullable_pred->has_null());

    nullable_pred->clear();
    EXPECT_FALSE(nullable_pred->has_null());
    nullable_pred->insert_null_elements(10);
    EXPECT_TRUE(nullable_pred->has_null()); // now it have 10 nulls

    auto null_dst = ColumnNullable::create(ColumnDate::create(), ColumnUInt8::create());
    EXPECT_FALSE(null_dst->has_null());

    uint16_t selector[] = {5, 8}; // both null
    EXPECT_EQ(nullable_pred->filter_by_selector(selector, 2, null_dst.get()), Status::OK());
    // filter_by_selector must announce to update has_null to make below right.
    EXPECT_TRUE(null_dst->has_null());
}
} // namespace doris::vectorized