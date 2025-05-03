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

class CastToStringTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CastToStringTest, test_cast_to_string_various_types) {
    {
        int8_t value = 127;
        std::string result = cast_to_string<TYPE_TINYINT, int8_t>(value, 0);
        EXPECT_EQ("127", result);
    }

    {
        int16_t value = 32767;
        std::string result = cast_to_string<TYPE_SMALLINT, int16_t>(value, 0);
        EXPECT_EQ("32767", result);
    }

    {
        int64_t value = 9223372036854775807;
        std::string result = cast_to_string<TYPE_BIGINT, int64_t>(value, 0);
        EXPECT_EQ("9223372036854775807", result);
    }

    {
        __int128_t value = 1;
        value = value << 100;
        std::string result = cast_to_string<TYPE_LARGEINT, __int128_t>(value, 0);
        EXPECT_FALSE(result.empty());
    }

    {
        bool value = true;
        std::string result = cast_to_string<TYPE_BOOLEAN, bool>(value, 0);
        EXPECT_EQ("1", result);
    }

    {
        double value = 123.456;
        std::string result = cast_to_string<TYPE_DOUBLE, double>(value, 0);
        EXPECT_EQ("123.456", result);
    }

    {
        uint32_t value = 0x01020304;
        std::string result = cast_to_string<TYPE_IPV4, uint32_t>(value, 0);
        EXPECT_EQ("1.2.3.4", result);
    }
}

TEST_F(ColumnValueRangeTest, test_olap_scan_keys_get_key_range_with_fixed_value) {
    bool eos = false;
    bool should_break = false;

    OlapScanKeys scan_keys;
    ColumnValueRange<TYPE_INT> range("test_column");

    auto status = range.add_fixed_value(5);
    EXPECT_TRUE(status.ok());
    status = range.add_fixed_value(10);
    EXPECT_TRUE(status.ok());

    EXPECT_TRUE(range.is_fixed_value_range());
    EXPECT_EQ(2, range.get_fixed_value_size());
    const auto& fixed_set = range.get_fixed_value_set();
    EXPECT_EQ(2, fixed_set.size());

    EXPECT_FALSE(range.is_scope_value_range());
    EXPECT_FALSE(range.is_empty_value_range());
    EXPECT_EQ(TYPE_INT, range.type());
    EXPECT_TRUE(range.is_range_value_convertible());

    EXPECT_EQ("test_column", range.column_name());
    EXPECT_TRUE(range.is_nullable_col());
    EXPECT_FALSE(range.contain_null());

    range.remove_fixed_value(5);
    EXPECT_EQ(1, range.get_fixed_value_size());

    range.convert_to_range_value();
    EXPECT_FALSE(range.is_fixed_value_range());

    EXPECT_EQ(10, range.get_range_min_value());
    EXPECT_EQ(10, range.get_range_max_value());

    ColumnValueRange<TYPE_INT> range2("test_column2");

    status = range2.add_range(FILTER_LARGER, 5);
    EXPECT_TRUE(status.ok());
    status = range2.add_range(FILTER_LESS, 15);
    EXPECT_TRUE(status.ok());

    EXPECT_FALSE(range2.is_begin_include());
    EXPECT_FALSE(range2.is_end_include());

    EXPECT_EQ(5, *range2.get_range_min_value_ptr());
    EXPECT_EQ(15, *range2.get_range_max_value_ptr());

    EXPECT_EQ(FILTER_LARGER, range2.get_range_low_op());
    EXPECT_EQ(FILTER_LESS, range2.get_range_high_op());

    bool exact_value = false;
    status = scan_keys.extend_scan_key(range2, 5, &exact_value, &eos, &should_break);
    EXPECT_TRUE(status.ok());

    std::vector<std::unique_ptr<OlapScanRange>> key_range;
    status = scan_keys.get_key_range(&key_range);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(key_range.empty());

    EXPECT_TRUE(scan_keys.has_range_value());
    EXPECT_GT(scan_keys.size(), 0);

    OlapTuple tuple;
    tuple.add_value("10");
    std::string key_str = OlapScanKeys::to_print_key(tuple);
    EXPECT_FALSE(key_str.empty());

    scan_keys.clear();
    EXPECT_FALSE(scan_keys.has_range_value());
    EXPECT_EQ(0, scan_keys.size());

    std::string debug_str = scan_keys.debug_string();
    EXPECT_TRUE(debug_str.find("ScanKeys:") != std::string::npos);
}

TEST_F(ColumnValueRangeTest, test_column_value_range_various_types) {
    {
        ColumnValueRange<TYPE_BOOLEAN> range("bool_col", true, 0, 0);
        bool value = true;
        auto status = range.add_fixed_value(value);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(range.is_fixed_value_range());
        EXPECT_EQ(1, range.get_fixed_value_size());

        range.set_contain_null(true);
        EXPECT_TRUE(range.contain_null());

        std::vector<FilterOlapParam<TCondition>> filters;
        range.to_olap_filter(filters);
        EXPECT_FALSE(filters.empty());
    }

    {
        ColumnValueRange<TYPE_BIGINT> range("bigint_col");
        int64_t value = 9223372036854775807;
        auto status = range.add_fixed_value(value);
        EXPECT_TRUE(status.ok());

        range.set_empty_value_range();
        EXPECT_TRUE(range.is_empty_value_range());

        range.set_whole_value_range();
        EXPECT_TRUE(range.is_whole_value_range());
    }

    {
        ColumnValueRange<TYPE_DECIMAL32> range("decimal32_col", true, 10, 2);
        vectorized::Decimal<int32_t> value(1234);
        auto status = range.add_fixed_value(value);
        EXPECT_TRUE(status.ok());

        std::vector<OlapTuple> begin_keys;
        std::vector<OlapTuple> end_keys;
        bool begin_include = false;
        bool end_include = false;
        range.convert_to_close_range(begin_keys, end_keys, begin_include, end_include);
        EXPECT_TRUE(begin_include);
        EXPECT_TRUE(end_include);

        status = range.add_range(FILTER_LARGER, vectorized::Decimal<int32_t>(100));
        EXPECT_TRUE(status.ok());
        status = range.add_range(FILTER_LESS, vectorized::Decimal<int32_t>(2000));
        EXPECT_TRUE(status.ok());

        begin_keys.clear();
        end_keys.clear();
    }
}

TEST_F(ColumnValueRangeTest, test_additional_methods) {
    ColumnValueRange<TYPE_INT> range("int_col");
    EXPECT_TRUE(range.is_fixed_value_convertible());
    EXPECT_FALSE(range.is_reject_split_type());

    std::shared_ptr<RuntimeProfile::Counter> filtered_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    std::shared_ptr<RuntimeProfile::Counter> input_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    range.attach_profile_counter(1, filtered_counter, input_counter);

    EXPECT_EQ(-1, range.precision());
    EXPECT_EQ(-1, range.scale());

    auto empty_range = ColumnValueRange<TYPE_INT>::create_empty_column_value_range(true, 0, 0);
    EXPECT_TRUE(empty_range.is_empty_value_range());

    auto named_empty_range =
            ColumnValueRange<TYPE_INT>::create_empty_column_value_range("test", true, 0, 0);
    EXPECT_TRUE(named_empty_range.is_empty_value_range());
    EXPECT_EQ("test", named_empty_range.column_name());

    ColumnValueRange<TYPE_INT> range2("test2");
    EXPECT_TRUE(range2.is_low_value_mininum());
    EXPECT_FALSE(range2.is_low_value_maximum());
    EXPECT_TRUE(range2.is_high_value_maximum());
    EXPECT_FALSE(range2.is_high_value_mininum());

    int value = 42;
    ColumnValueRange<TYPE_INT>::add_fixed_value_range(range2, &value);
    EXPECT_TRUE(range2.is_fixed_value_range());
    EXPECT_EQ(1, range2.get_fixed_value_size());

    ColumnValueRange<TYPE_INT>::remove_fixed_value_range(range2, &value);
    EXPECT_EQ(0, range2.get_fixed_value_size());

    ColumnValueRange<TYPE_INT>::add_value_range(range2, FILTER_LARGER, &value);
}

TEST_F(ColumnValueRangeTest, test_extend_scan_key_various_types) {
    OlapScanKeys scan_keys;
    bool exact_value = false;
    bool eos = false;
    bool should_break = false;

    {
        ColumnValueRange<TYPE_TINYINT> range("tinyint_col");
        int8_t value = 100;
        auto status = range.add_fixed_value(value);
        EXPECT_TRUE(status.ok());

        status = scan_keys.extend_scan_key(range, 5, &exact_value, &eos, &should_break);
        EXPECT_TRUE(status.ok());

        scan_keys.clear();
    }

    scan_keys.set_begin_include(false);
    EXPECT_FALSE(scan_keys.begin_include());

    scan_keys.set_end_include(false);
    EXPECT_FALSE(scan_keys.end_include());

    scan_keys.set_is_convertible(false);
}

TEST_F(ColumnValueRangeTest, test_has_intersection_complete_coverage) {
    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
        ColumnValueRange<TYPE_INT> range2("col2", 30, 100, true);
        EXPECT_TRUE(range1.has_intersection(range2));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
        ColumnValueRange<TYPE_INT> range2("col2", 60, 100, true);
        EXPECT_FALSE(range1.has_intersection(range2));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 100, true);
        ColumnValueRange<TYPE_INT> range2("col2", 20, 50, true);
        EXPECT_TRUE(range1.has_intersection(range2));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
        ColumnValueRange<TYPE_INT> range2("col2", 50, 100, true);
        EXPECT_TRUE(range1.has_intersection(range2));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
        ColumnValueRange<TYPE_INT> range2("col2");
        EXPECT_TRUE(range2.add_fixed_value(25));
        EXPECT_TRUE(range1.has_intersection(range2));

        ColumnValueRange<TYPE_INT> range3("col3");
        EXPECT_TRUE(range3.add_fixed_value(75));
        EXPECT_FALSE(range1.has_intersection(range3));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1");
        EXPECT_TRUE(range1.add_fixed_value(10));
        EXPECT_TRUE(range1.add_fixed_value(20));
        EXPECT_TRUE(range1.add_fixed_value(30));

        ColumnValueRange<TYPE_INT> range2("col2");
        EXPECT_TRUE(range2.add_fixed_value(20));
        EXPECT_TRUE(range2.add_fixed_value(40));
        EXPECT_TRUE(range1.has_intersection(range2));

        ColumnValueRange<TYPE_INT> range3("col3");
        EXPECT_TRUE(range3.add_fixed_value(50));
        EXPECT_TRUE(range3.add_fixed_value(60));
        EXPECT_FALSE(range1.has_intersection(range3));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
        ColumnValueRange<TYPE_INT> range2("col2");
        range2.set_empty_value_range();
        EXPECT_FALSE(range1.has_intersection(range2));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1", 0, 50, true);
        range1.set_contain_null(true);

        ColumnValueRange<TYPE_INT> range2("col2", 60, 100, true);
        range2.set_contain_null(true);

        /// TODO: maybe should be true, but currently it is false
        EXPECT_FALSE(range1.has_intersection(range2));
    }

    {
        ColumnValueRange<TYPE_INT> range1("col1");
        range1.set_whole_value_range();

        ColumnValueRange<TYPE_INT> range2("col2", 10, 20, true);
        EXPECT_TRUE(range1.has_intersection(range2));

        ColumnValueRange<TYPE_INT> range3("col3");
        range3.set_empty_value_range();
        EXPECT_FALSE(range1.has_intersection(range3));
    }
}

TEST_F(ColumnValueRangeTest, test_extend_scan_key_comprehensive) {
    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_INT> range("int_col", 10, 50, true);

        bool exact_value = false;
        bool eos = false;
        bool should_break = false;

        auto status = scan_keys.extend_scan_key(range, 5, &exact_value, &eos, &should_break);
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(exact_value);
        EXPECT_TRUE(scan_keys.has_range_value());
    }

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_INT> empty_range("empty_col");
        empty_range.set_empty_value_range();

        bool exact_value = false;
        bool eos = false;
        bool should_break = false;

        auto status = scan_keys.extend_scan_key(empty_range, 5, &exact_value, &eos, &should_break);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(scan_keys.size(), 0);
    }

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_INT> fixed_range("fixed_col");
        EXPECT_TRUE(fixed_range.add_fixed_value(10));
        EXPECT_TRUE(fixed_range.add_fixed_value(20));

        bool exact_value = false;
        bool eos = false;
        bool should_break = false;

        auto status = scan_keys.extend_scan_key(fixed_range, 1, &exact_value, &eos, &should_break);
        EXPECT_TRUE(status.ok());
    }

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_INT> single_range("single_col", 10, 10, true);

        bool exact_value = false;
        bool eos = false;
        bool should_break = false;

        auto status = scan_keys.extend_scan_key(single_range, 5, &exact_value, &eos, &should_break);
        EXPECT_TRUE(status.ok());
    }

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_INT> range("whole_col");
        range.set_whole_value_range();

        bool exact_value = false;
        bool eos = false;
        bool should_break = false;

        auto status = scan_keys.extend_scan_key(range, 5, &exact_value, &eos, &should_break);
        EXPECT_TRUE(status.ok());
    }

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_INT> range("max_keys_col", 1, 100, true);

        bool exact_value = false;
        bool eos = false;
        bool should_break = false;

        auto status = scan_keys.extend_scan_key(range, 10, &exact_value, &eos, &should_break);
        EXPECT_TRUE(status.ok());
    }
}

} // namespace doris