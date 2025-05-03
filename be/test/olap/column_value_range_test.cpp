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

TEST_F(ColumnValueRangeTest, test_column_value_range_is_fixed_value_convertible) {
    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        EXPECT_TRUE(range.is_fixed_value_convertible());
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        EXPECT_TRUE(range.add_fixed_value(50));
        EXPECT_FALSE(range.is_fixed_value_convertible());
    }

    {
        auto range = ColumnValueRange<TYPE_BOOLEAN>::create_empty_column_value_range(false, 0, 0);
        EXPECT_FALSE(range.is_fixed_value_convertible());
    }
}

TEST_F(ColumnValueRangeTest, test_column_value_range_is_range_value_convertible) {
    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        EXPECT_FALSE(range.is_range_value_convertible());
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        EXPECT_TRUE(range.add_fixed_value(50));
        EXPECT_TRUE(range.is_range_value_convertible());
    }

    {
        auto range = ColumnValueRange<TYPE_BOOLEAN>::create_empty_column_value_range(false, 0, 0);
        EXPECT_TRUE(range.add_fixed_value(50));
        EXPECT_FALSE(range.is_range_value_convertible());
    }
}

TEST_F(ColumnValueRangeTest, test_convert_to_range_value) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    EXPECT_TRUE(range.add_fixed_value(10));
    EXPECT_TRUE(range.add_fixed_value(20));
    EXPECT_TRUE(range.add_fixed_value(30));

    EXPECT_TRUE(range.is_fixed_value_range());
    range.convert_to_range_value();
    EXPECT_FALSE(range.is_fixed_value_range());
    EXPECT_EQ(range.get_range_min_value(), 10);
    EXPECT_EQ(range.get_range_max_value(), 30);
}

TEST_F(ColumnValueRangeTest, test_convert_to_avg_range_value) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    std::vector<OlapTuple> begin_scan_keys;
    std::vector<OlapTuple> end_scan_keys;
    bool begin_include = false;
    bool end_include = false;

    bool result = range.convert_to_avg_range_value(begin_scan_keys, end_scan_keys, begin_include,
                                                   end_include, 5);

    EXPECT_TRUE(result);
    EXPECT_EQ(begin_scan_keys.size(), 6);
    EXPECT_EQ(end_scan_keys.size(), 6);
    EXPECT_FALSE(begin_include);
    EXPECT_FALSE(end_include);
}

TEST_F(ColumnValueRangeTest, test_convert_to_close_range) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    std::vector<OlapTuple> begin_scan_keys;
    std::vector<OlapTuple> end_scan_keys;
    bool begin_include = false;
    bool end_include = false;

    bool result = range.convert_to_close_range(begin_scan_keys, end_scan_keys, begin_include,
                                               end_include);

    EXPECT_FALSE(result);
    EXPECT_TRUE(begin_scan_keys.empty());
    EXPECT_TRUE(end_scan_keys.empty());
    EXPECT_TRUE(begin_include);
    EXPECT_TRUE(end_include);
}

TEST_F(ColumnValueRangeTest, test_column_value_range_constructor_variants) {
    ColumnValueRange<TYPE_INT> range1;
    EXPECT_EQ(range1.type(), INVALID_TYPE);

    ColumnValueRange<TYPE_INT> range2("test_col");
    EXPECT_EQ(range2.type(), TYPE_INT);
    EXPECT_EQ(range2.column_name(), "test_col");
    EXPECT_TRUE(range2.is_nullable_col());

    ColumnValueRange<TYPE_DECIMAL32> range3("decimal_col", 10, 2);
    EXPECT_EQ(range3.type(), TYPE_DECIMAL32);
    EXPECT_EQ(range3.column_name(), "decimal_col");
    EXPECT_EQ(range3.precision(), 10);
    EXPECT_EQ(range3.scale(), 2);

    ColumnValueRange<TYPE_DECIMAL32> range4("decimal_col", false, 10, 2);
    EXPECT_EQ(range4.type(), TYPE_DECIMAL32);
    EXPECT_FALSE(range4.is_nullable_col());
    EXPECT_EQ(range4.precision(), 10);
    EXPECT_EQ(range4.scale(), 2);
}

TEST_F(ColumnValueRangeTest, test_column_value_range_to_olap_filter) {
    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        EXPECT_TRUE(range.add_fixed_value(10));
        EXPECT_TRUE(range.add_fixed_value(20));

        std::vector<FilterOlapParam<TCondition>> filters;
        range.to_olap_filter(filters);

        EXPECT_EQ(filters.size(), 1);
        EXPECT_EQ(filters[0].filter.condition_op, "*=");
        EXPECT_EQ(filters[0].filter.condition_values.size(), 2);
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 10, 90, true);

        std::vector<FilterOlapParam<TCondition>> filters;
        range.to_olap_filter(filters);

        EXPECT_EQ(filters.size(), 2);
        EXPECT_EQ(filters[0].filter.condition_op, ">=");
        EXPECT_EQ(filters[1].filter.condition_op, "<=");
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", type_limit<int>::min(), type_limit<int>::max(),
                                         true);
        range.set_contain_null(false);

        std::vector<FilterOlapParam<TCondition>> filters;
        range.to_olap_filter(filters);

        EXPECT_EQ(filters.size(), 1);
        EXPECT_EQ(filters[0].filter.condition_op, "is");
        EXPECT_EQ(filters[0].filter.condition_values[0], "not null");
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        range.set_empty_value_range();
        range.set_contain_null(true);

        std::vector<FilterOlapParam<TCondition>> filters;
        range.to_olap_filter(filters);

        EXPECT_EQ(filters.size(), 1);
        EXPECT_EQ(filters[0].filter.condition_op, "is");
        EXPECT_EQ(filters[0].filter.condition_values[0], "null");
    }
}

TEST_F(ColumnValueRangeTest, test_to_in_condition) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
    EXPECT_TRUE(range.add_fixed_value(10));
    EXPECT_TRUE(range.add_fixed_value(20));

    std::vector<FilterOlapParam<TCondition>> filters;

    range.to_in_condition(filters, true);
    EXPECT_EQ(filters.size(), 1);
    EXPECT_EQ(filters[0].filter.condition_op, "*=");
    EXPECT_EQ(filters[0].filter.condition_values.size(), 2);

    filters.clear();

    range.to_in_condition(filters, false);
    EXPECT_EQ(filters.size(), 1);
    EXPECT_EQ(filters[0].filter.condition_op, "!*=");
    EXPECT_EQ(filters[0].filter.condition_values.size(), 2);
}

TEST_F(ColumnValueRangeTest, test_static_methods) {
    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        int value = 50;
        ColumnValueRange<TYPE_INT>::add_fixed_value_range(range, &value);
        EXPECT_EQ(range.get_fixed_value_size(), 1);
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        int value = 50;
        EXPECT_TRUE(range.add_fixed_value(50));
        EXPECT_EQ(range.get_fixed_value_size(), 1);

        ColumnValueRange<TYPE_INT>::remove_fixed_value_range(range, &value);
        EXPECT_EQ(range.get_fixed_value_size(), 0);
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        int value = 10;
        ColumnValueRange<TYPE_INT>::add_value_range(range, FILTER_LARGER, &value);
        EXPECT_EQ(range.get_range_min_value(), 10);
    }

    {
        auto range = ColumnValueRange<TYPE_INT>::create_empty_column_value_range(true, 10, 2);
        EXPECT_TRUE(range.is_empty_value_range());
        EXPECT_TRUE(range.is_nullable_col());
        EXPECT_EQ(range.precision(), 10);
        EXPECT_EQ(range.scale(), 2);
    }

    {
        auto range = ColumnValueRange<TYPE_INT>::create_empty_column_value_range("test_col", true,
                                                                                 10, 2);
        EXPECT_TRUE(range.is_empty_value_range());
        EXPECT_TRUE(range.is_nullable_col());
        EXPECT_EQ(range.column_name(), "test_col");
        EXPECT_EQ(range.precision(), 10);
        EXPECT_EQ(range.scale(), 2);
    }
}

TEST_F(ColumnValueRangeTest, test_olap_scan_keys_integration) {
    OlapScanKeys scan_keys;
    ColumnValueRange<TYPE_INT> range("test_col", 10, 20, true);

    bool exact_value = true;
    bool eos = false;
    bool should_break = false;

    auto status = scan_keys.extend_scan_key(range, 5, &exact_value, &eos, &should_break);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_keys.has_range_value());
    EXPECT_EQ(scan_keys.size(), 5);

    ColumnValueRange<TYPE_INT> empty_range("test_col", 0, 0, true);
    empty_range.set_empty_value_range();

    OlapScanKeys empty_scan_keys;
    status = empty_scan_keys.extend_scan_key(empty_range, 5, &exact_value, &eos, &should_break);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(empty_scan_keys.size(), 0);

    ColumnValueRange<TYPE_INT> fixed_range("test_col", 0, 100, true);
    EXPECT_TRUE(fixed_range.add_fixed_value(30));
    EXPECT_TRUE(fixed_range.add_fixed_value(40));

    OlapScanKeys fixed_scan_keys;
    status = fixed_scan_keys.extend_scan_key(fixed_range, 5, &exact_value, &eos, &should_break);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(fixed_scan_keys.has_range_value());
    EXPECT_EQ(fixed_scan_keys.size(), 2);
}

TEST_F(ColumnValueRangeTest, test_complex_operations) {
    ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);

    EXPECT_TRUE(range.add_fixed_value(50));
    EXPECT_TRUE(range.add_range(FILTER_LARGER, 30));
    EXPECT_FALSE(range.is_empty_value_range());

    range.set_whole_value_range();

    EXPECT_TRUE(range.add_range(FILTER_LARGER, 30));
    EXPECT_TRUE(range.add_range(FILTER_LESS, 70));
    EXPECT_TRUE(range.add_fixed_value(50));
    EXPECT_EQ(range.get_fixed_value_size(), 1);

    EXPECT_TRUE(range.add_fixed_value(80));
    EXPECT_FALSE(range.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, test_cast_to_string) {
    int32_t int_val = 123;
    std::string int_str = cast_to_string<TYPE_INT, int32_t>(int_val, 0);
    EXPECT_EQ(int_str, "123");

    int8_t tiny_val = 8;
    std::string tiny_str = cast_to_string<TYPE_TINYINT, int8_t>(tiny_val, 0);
    EXPECT_EQ(tiny_str, "8");

    uint32_t ipv4_val = 0x7f000001;
    std::string ipv4_str = cast_to_string<TYPE_IPV4, uint32_t>(ipv4_val, 0);
    EXPECT_EQ(ipv4_str, "127.0.0.1");
}

TEST_F(ColumnValueRangeTest, test_boundary_conditions) {
    ColumnValueRange<TYPE_INT> range("test_col", type_limit<int>::min(), type_limit<int>::max(),
                                     true);

    EXPECT_TRUE(range.add_range(FILTER_LARGER, type_limit<int>::min()));
    EXPECT_EQ(range.get_range_min_value(), type_limit<int>::min());
    EXPECT_EQ(range.get_range_max_value(), type_limit<int>::max());

    range.set_whole_value_range();

    EXPECT_TRUE(range.add_range(FILTER_LESS, type_limit<int>::max()));
    EXPECT_EQ(range.get_range_min_value(), type_limit<int>::min());
    EXPECT_EQ(range.get_range_max_value(), type_limit<int>::max());

    range.set_whole_value_range();
    EXPECT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, 50));
    EXPECT_TRUE(range.add_range(FILTER_LESS_OR_EQUAL, 50));
    EXPECT_TRUE(range.is_fixed_value_range());
    EXPECT_EQ(range.get_fixed_value_size(), 1);
}

TEST_F(ColumnValueRangeTest, test_various_range_operations) {
    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        EXPECT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, 10));
        EXPECT_EQ(range.get_range_min_value(), 10);
        EXPECT_EQ(range.get_range_low_op(), FILTER_LARGER_OR_EQUAL);

        EXPECT_TRUE(range.add_range(FILTER_LESS_OR_EQUAL, 90));
        EXPECT_EQ(range.get_range_max_value(), 90);
        EXPECT_EQ(range.get_range_high_op(), FILTER_LESS_OR_EQUAL);
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 0, 100, true);
        EXPECT_TRUE(range.add_fixed_value(10));
        EXPECT_TRUE(range.add_fixed_value(20));
        EXPECT_TRUE(range.add_fixed_value(30));

        EXPECT_TRUE(range.add_range(FILTER_LARGER, 15));
        EXPECT_TRUE(range.is_fixed_value_range());
        EXPECT_FALSE(range.is_empty_value_range());

        auto& fixed_set = range.get_fixed_value_set();
        EXPECT_EQ(fixed_set.size(), 2);
        EXPECT_TRUE(fixed_set.find(20) != fixed_set.end());
        EXPECT_TRUE(fixed_set.find(30) != fixed_set.end());
        EXPECT_TRUE(fixed_set.find(10) == fixed_set.end());
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 10, 50, true);
        EXPECT_FALSE(range.is_low_value_mininum());
        EXPECT_FALSE(range.is_high_value_maximum());
        EXPECT_FALSE(range.is_low_value_maximum());
        EXPECT_FALSE(range.is_high_value_mininum());

        range.set_whole_value_range();
        EXPECT_TRUE(range.is_low_value_mininum());
        EXPECT_TRUE(range.is_high_value_maximum());

        range.set_empty_value_range();
        EXPECT_TRUE(range.is_low_value_maximum());
        EXPECT_TRUE(range.is_high_value_mininum());
    }

    {
        ColumnValueRange<TYPE_INT> range("test_col", 10, 50, true);
        EXPECT_EQ(*range.get_range_min_value_ptr(), 10);
        EXPECT_EQ(*range.get_range_max_value_ptr(), 50);
    }
}

TEST_F(ColumnValueRangeTest, test_olap_scan_keys_methods) {
    OlapScanKeys scan_keys;

    scan_keys.set_begin_include(false);
    EXPECT_FALSE(scan_keys.begin_include());

    scan_keys.set_end_include(false);
    EXPECT_FALSE(scan_keys.end_include());

    scan_keys.set_is_convertible(false);

    ColumnValueRange<TYPE_INT> range("test_col", 10, 20, true);
    bool exact_value = true;
    bool eos = false;
    bool should_break = false;

    auto status = scan_keys.extend_scan_key(range, 5, &exact_value, &eos, &should_break);
    EXPECT_TRUE(status.ok());

    std::string debug_str = scan_keys.debug_string();
    EXPECT_FALSE(debug_str.empty());

    scan_keys.clear();
    EXPECT_FALSE(scan_keys.has_range_value());
    EXPECT_EQ(scan_keys.size(), 0);

    OlapTuple test_tuple;
    test_tuple.add_value("10");
    std::string print_key = OlapScanKeys::to_print_key(test_tuple);
    EXPECT_FALSE(print_key.empty());
}

TEST_F(ColumnValueRangeTest, test_intersection_edge_cases) {
    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 100, true);
        ColumnValueRange<TYPE_BIGINT> range2("col2", static_cast<int64_t>(0),
                                             static_cast<int64_t>(100), true);

        EXPECT_NE(range1.type(), range2.type());
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 100, true);
        ColumnValueRange<TYPE_INT> range2("col2", 0, 0, true);
        range2.set_empty_value_range();

        range1.intersection(range2);
        EXPECT_TRUE(range1.is_empty_value_range());
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 100, true);
        EXPECT_TRUE(range1.add_fixed_value(10));
        EXPECT_TRUE(range1.add_fixed_value(20));
        EXPECT_TRUE(range1.add_fixed_value(30));

        ColumnValueRange<TYPE_INT> range2("col2", 0, 100, true);
        EXPECT_TRUE(range2.add_fixed_value(20));
        EXPECT_TRUE(range2.add_fixed_value(30));
        EXPECT_TRUE(range2.add_fixed_value(40));

        range1.intersection(range2);
        EXPECT_EQ(range1.get_fixed_value_size(), 2);
        auto& fixed_set = range1.get_fixed_value_set();
        EXPECT_TRUE(fixed_set.find(20) != fixed_set.end());
        EXPECT_TRUE(fixed_set.find(30) != fixed_set.end());
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 100, true);
        EXPECT_TRUE(range1.add_fixed_value(10));
        EXPECT_TRUE(range1.add_fixed_value(20));

        ColumnValueRange<TYPE_INT> range2("col2", 15, 25, true);

        range1.intersection(range2);
        EXPECT_EQ(range1.get_fixed_value_size(), 1);
        auto& fixed_set = range1.get_fixed_value_set();
        EXPECT_TRUE(fixed_set.find(20) != fixed_set.end());
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 15, 25, true);

        ColumnValueRange<TYPE_INT> range2("col2", 0, 100, true);
        EXPECT_TRUE(range2.add_fixed_value(10));
        EXPECT_TRUE(range2.add_fixed_value(20));

        range1.intersection(range2);
        EXPECT_EQ(range1.get_fixed_value_size(), 1);
        auto& fixed_set = range1.get_fixed_value_set();
        EXPECT_TRUE(fixed_set.find(20) != fixed_set.end());
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 100, true);
        range1.set_contain_null(true);

        ColumnValueRange<TYPE_INT> range2("col2", 0, 100, true);
        range2.set_contain_null(true);

        range1.intersection(range2);
        EXPECT_TRUE(range1.contain_null());
    }
}

TEST_F(ColumnValueRangeTest, test_is_scope_value_range) {
    {
        ColumnValueRange<TYPE_INT> range1;
        EXPECT_FALSE(range1.is_scope_value_range());

        ColumnValueRange<TYPE_INT> range2("test_col", 10, 50, true);
        EXPECT_TRUE(range2.is_scope_value_range());

        ColumnValueRange<TYPE_INT> range3("test_col", 0, 0, true);
        range3.set_empty_value_range();
        EXPECT_FALSE(range3.is_scope_value_range());

        ColumnValueRange<TYPE_INT> range4("test_col", 0, 100, true);
        EXPECT_TRUE(range4.add_fixed_value(50));
        EXPECT_FALSE(range4.is_scope_value_range());
    }
}

TEST_F(ColumnValueRangeTest, test_olap_scan_keys_get_key_range) {
    OlapScanKeys scan_keys;
    ColumnValueRange<TYPE_INT> range("test_col", 10, 20, true);

    bool exact_value = true;
    bool eos = false;
    bool should_break = false;

    auto status = scan_keys.extend_scan_key(range, 5, &exact_value, &eos, &should_break);
    EXPECT_TRUE(status.ok());

    std::vector<std::unique_ptr<OlapScanRange>> key_range;
    status = scan_keys.get_key_range(&key_range);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(key_range.empty());
}

} // namespace doris