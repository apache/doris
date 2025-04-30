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

#include "exec/olap_common.h"
#include "runtime/define_primitive_type.h"

namespace doris {

struct ColumnValueRangeTest : public testing::Test {
    ColumnValueRangeTest() = default;
    ~ColumnValueRangeTest() override = default;

    void SetUp() override {}
};

TEST_F(ColumnValueRangeTest, test_column_value_range_INVALID_TYPE) {
    ColumnValueRange<TYPE_BIGINT> range;

    EXPECT_EQ(range.type(), INVALID_TYPE);
    EXPECT_EQ(range.get_fixed_value_size(), 0);
    EXPECT_TRUE(range.is_empty_value_range());
    {
        auto status = range.add_fixed_value(32);
        EXPECT_FALSE(status.ok());
        std::cout << status.msg() << std::endl;
    }

    {
        auto status = range.add_range(FILTER_LARGER, 32);
        EXPECT_FALSE(status.ok());
        std::cout << status.msg() << std::endl;
    }
}

TEST_F(ColumnValueRangeTest, test_column_value_range_create) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    EXPECT_EQ(range.type(), TYPE_INT);
    EXPECT_EQ(range.column_name(), "test_col");
    EXPECT_EQ(range.get_range_min_value(), 0);
    EXPECT_EQ(range.get_range_max_value(), 100);
    EXPECT_TRUE(range.is_nullable_col());
    EXPECT_FALSE(range.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_add_fixed_value) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    auto status = range.add_fixed_value(50);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(range.get_fixed_value_size(), 1);
    EXPECT_TRUE(range.is_fixed_value_range());
    EXPECT_FALSE(range.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_add_range) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    auto status = range.add_range(FILTER_LARGER, 10);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(range.get_range_min_value(), 10);
    EXPECT_EQ(range.get_range_max_value(), 100);

    status = range.add_range(FILTER_LESS, 90);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(range.get_range_min_value(), 10);
    EXPECT_EQ(range.get_range_max_value(), 90);
}

TEST_F(ColumnValueRangeTest, test_column_value_range_intersection_contain_null) {
    ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
    ColumnValueRange<TYPE_INT> range2("col2", 30, 100, true);

    range1.intersection(range2);

    EXPECT_TRUE(!range1.is_fixed_value_range());
    EXPECT_TRUE(!range1.is_scope_value_range());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_intersection_not_contain_null) {
    ColumnValueRange<TYPE_INT> range1("col1", 0, 50, false);
    ColumnValueRange<TYPE_INT> range2("col2", 30, 100, false);

    range1.intersection(range2);

    EXPECT_EQ(range1.get_range_min_value(), 30);
    EXPECT_EQ(range1.get_range_max_value(), 50);
    EXPECT_FALSE(range1.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_empty) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    range.set_empty_value_range();
    EXPECT_TRUE(range.is_empty_value_range());
    EXPECT_EQ(range.get_fixed_value_size(), 0);
    EXPECT_FALSE(range.contain_null());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_remove_fixed_value) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    EXPECT_TRUE(range.add_fixed_value(50));
    EXPECT_EQ(range.get_fixed_value_size(), 1);

    range.remove_fixed_value(50);
    EXPECT_EQ(range.get_fixed_value_size(), 0);
    EXPECT_FALSE(range.is_fixed_value_range());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_convert_to_range_value) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    auto status1 = range.add_fixed_value(10);
    EXPECT_TRUE(status1.ok());
    auto status2 = range.add_fixed_value(20);
    EXPECT_TRUE(status2.ok());
    EXPECT_TRUE(range.is_fixed_value_range());

    range.convert_to_range_value();
    EXPECT_FALSE(range.is_fixed_value_range());
    EXPECT_EQ(range.get_range_min_value(), 10);
    EXPECT_EQ(range.get_range_max_value(), 20);
}

TEST_F(ColumnValueRangeTest, test_column_value_range_set_whole_value_range) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    range.set_whole_value_range();
    EXPECT_TRUE(range.is_whole_value_range());
    EXPECT_EQ(range.get_range_min_value(), type_limit<int>::min());
    EXPECT_EQ(range.get_range_max_value(), type_limit<int>::max());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_set_contain_null) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    range.set_contain_null(true);
    EXPECT_TRUE(range.contain_null());

    range.set_contain_null(false);
    EXPECT_FALSE(range.contain_null());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_is_in_range) {
    ColumnValueRange<TYPE_INT> range("test_col", 10, 20, true);

    EXPECT_TRUE(range.is_in_range(15));
    EXPECT_FALSE(range.is_in_range(25));
}

TEST_F(ColumnValueRangeTest, test_column_value_range_has_intersection) {
    ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
    ColumnValueRange<TYPE_INT> range2("col2", 30, 100, true);

    EXPECT_TRUE(range1.has_intersection(range2));

    ColumnValueRange<TYPE_INT> range3("col3", 60, 100, true);
    EXPECT_FALSE(range1.has_intersection(range3));
}

TEST_F(ColumnValueRangeTest, test_column_value_range_is_whole_value_range) {
    ColumnValueRange<TYPE_INT> range("test_col", type_limit<int>::min(), type_limit<int>::max(),
                                     true);

    EXPECT_TRUE(range.is_whole_value_range());
}

TEST_F(ColumnValueRangeTest, test_column_value_range_attach_profile_counter) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    auto filtered_rows_counter = std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    auto input_rows_counter = std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);

    range.attach_profile_counter(1, filtered_rows_counter, input_rows_counter);

    EXPECT_EQ(range.is_begin_include(), true);
    EXPECT_EQ(range.is_end_include(), true);
}

} // namespace doris