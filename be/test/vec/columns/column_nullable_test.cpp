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
#include "testutil/column_helper.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

TEST(ColumnNullableTest, NullTest) {
    ColumnNullable::MutablePtr null_col = create_column_nullable<TYPE_BIGINT>(500, true);
    EXPECT_TRUE(null_col->has_null());

    ColumnNullable::MutablePtr dst_col =
            ColumnNullable::create(create_nested_column<TYPE_BIGINT>(10), ColumnUInt8::create(10));
    EXPECT_FALSE(dst_col->has_null());

    ColumnInt64::MutablePtr source_col = ColumnInt64::create();
    source_col->insert_range_of_integer(0, 100);

    dst_col->insert(Field());
    EXPECT_TRUE(dst_col->has_null());
    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_range_from(
            *ColumnNullable::create(std::move(source_col), ColumnUInt8::create(10)), 5, 5);
    EXPECT_FALSE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_many_defaults(10);
    EXPECT_TRUE(dst_col->has_null());

    dst_col->clear();
    EXPECT_FALSE(dst_col->has_null());
    dst_col->insert_from(*null_col, 100);
    EXPECT_TRUE(dst_col->has_null());

    auto tmp_col = ColumnNullable::create(create_nested_column<TYPE_BIGINT>(10),
                                          ColumnUInt8::create(10, 1));

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
    nullable_pred->insert_many_defaults(10);
    EXPECT_TRUE(nullable_pred->has_null());

    nullable_pred->clear();
    EXPECT_FALSE(nullable_pred->has_null());
    nullable_pred->insert_many_defaults(10);
    EXPECT_TRUE(nullable_pred->has_null()); // now it have 10 nulls

    auto null_dst = ColumnNullable::create(ColumnDate::create(), ColumnUInt8::create());
    EXPECT_FALSE(null_dst->has_null());

    uint16_t selector[] = {5, 8}; // both null
    EXPECT_EQ(nullable_pred->filter_by_selector(selector, 2, null_dst.get()), Status::OK());
    // filter_by_selector must announce to update has_null to make below right.
    EXPECT_TRUE(null_dst->has_null());
}

TEST(ColumnNullableTest, append_data_by_selector) {
    auto srt_column = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            {true, false, true, false, false, false, true, false, false, false});

    IColumn::Selector selector = {0, 2, 4, 6, 8};

    IColumn::MutablePtr dst_column =
            ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    srt_column->append_data_by_selector(dst_column, selector);

    auto expected_column = ColumnHelper::create_nullable_column<DataTypeInt64>(
            {1, 3, 5, 7, 9}, {true, true, false, true, false});

    EXPECT_TRUE(ColumnHelper::column_equal(std::move(dst_column), expected_column));
}

TEST(ColumnNullableTest, ScalaTypeNullInt32Testerase) {
    auto datetype_int32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = datetype_int32->create_column();
    auto column_res = datetype_int32->create_column();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column_res->insert_range_from(*column, 0, data.size());
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 3);

    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i + 2));
    }
}

TEST(ColumnNullableTest, ScalaTypeNullInt32Test2erase) {
    auto datetype_int32 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto column = datetype_int32->create_column();
    auto column_res = datetype_int32->create_column();
    std::vector<int32_t> data = {1, 2, 3, 4, 5};
    std::vector<int32_t> res = {1, 2, 5};
    for (auto d : data) {
        column->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    for (auto d : res) {
        column_res->insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    column->erase(2, 2);
    EXPECT_EQ(column->size(), 3);

    for (int i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data_at(i), column_res->get_data_at(i));
    }
}

TEST(ColumnNullableTest, ScalaTypeNullStringTesterase) {
    auto datetype_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column = datetype_string->create_column();
    auto column_res = datetype_string->create_column();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    column->insert_default();
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->insert_default();
    column_res->insert_range_from(*column, 0, data.size() + 2);
    column->erase(0, 2);
    EXPECT_EQ(column->size(), 5);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << std::endl;
        EXPECT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i + 2).to_string());
    }
}

TEST(ColumnNullableTest, ScalaTypeNullStringTest2erase) {
    auto datetype_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column = datetype_string->create_column();
    auto column_res = datetype_string->create_column();
    std::vector<StringRef> data = {StringRef("asd"), StringRef("1234567"), StringRef("3"),
                                   StringRef("4"), StringRef("5")};
    std::vector<StringRef> res = {StringRef("asd"), StringRef("4"), StringRef("5")};
    column->insert_default();
    for (auto d : data) {
        column->insert_data(d.data, d.size);
    }
    column->insert_default();

    column_res->insert_default();
    for (auto d : res) {
        column_res->insert_data(d.data, d.size);
    }
    column_res->insert_default();

    column->erase(2, 2);
    EXPECT_EQ(column->size(), 5);
    for (int i = 0; i < column->size(); ++i) {
        std::cout << column->get_data_at(i).to_string() << " , "
                  << column_res->get_data_at(i).to_string() << std::endl;
        // EXPECT_EQ(column->get_data_at(i).to_string(), column_res->get_data_at(i).to_string());
    }
}

} // namespace doris::vectorized