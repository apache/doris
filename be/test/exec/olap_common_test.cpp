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
#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <vector>
#define protected public
#define private public

#include "exec/olap_common.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "util/cpu_info.h"
#include "util/runtime_profile.h"

namespace doris {

void construct_scan_range(TPaloScanRange* doris_scan_range) {
    TNetworkAddress host;
    host.__set_hostname("jx-ps-dise174.jx");
    host.__set_port(8010);
    doris_scan_range->hosts.push_back(host);
    doris_scan_range->__set_schema_hash("216424022");
    doris_scan_range->__set_version("0");
    // Useless but it is required in TPaloScanRange
    doris_scan_range->__set_version_hash("0");
    //    doris_scan_range->engine_table_name.push_back("ShowQStats");
    doris_scan_range->__set_db_name("olap");
    TKeyRange key_range;
    key_range.__set_column_type(to_thrift(TYPE_INT));
    key_range.__set_begin_key(-1000);
    key_range.__set_end_key(1000);
    key_range.__set_column_name("partition_column");
    doris_scan_range->partition_column_ranges.push_back(key_range);
    doris_scan_range->__isset.partition_column_ranges = true;
}

class ColumnValueRangeTest : public ::testing::Test {
public:
    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(ColumnValueRangeTest, ExceptionCase) {
    ColumnValueRange<TYPE_INT> range1;
    EXPECT_FALSE(range1.add_fixed_value(10).ok());
    EXPECT_FALSE(range1.add_range(FILTER_LESS_OR_EQUAL, 10).ok());
}

TEST_F(ColumnValueRangeTest, NormalCase) {
    ColumnValueRange<TYPE_INT> range1("col");

    EXPECT_TRUE(range1.add_fixed_value(10).ok());
    EXPECT_TRUE(range1.add_fixed_value(20).ok());
    EXPECT_TRUE(range1.add_fixed_value(30).ok());

    EXPECT_TRUE(range1.is_fixed_value_range());

    EXPECT_TRUE(range1.add_range(FILTER_LESS, 30).ok());
    EXPECT_FALSE(range1.is_empty_value_range());

    ColumnValueRange<TYPE_INT> range2("col");
    EXPECT_TRUE(range2.add_fixed_value(30).ok());
    EXPECT_FALSE(range1.has_intersection(range2));

    EXPECT_TRUE(range2.add_fixed_value(20).ok());
    EXPECT_TRUE(range1.has_intersection(range2));

    EXPECT_TRUE(range2.is_fixed_value_range());
    EXPECT_TRUE(range2.add_range(FILTER_LARGER, 50).ok());
    EXPECT_FALSE(range2.is_fixed_value_range());

    EXPECT_TRUE(range2.is_empty_value_range());
    EXPECT_FALSE(range1.has_intersection(range2));
}

TEST_F(ColumnValueRangeTest, FixedAddRangeTest) {
    ColumnValueRange<TYPE_INT> range1("col");

    for (int i = 0; i < 100; i += 10) {
        EXPECT_TRUE(range1.add_fixed_value(i).ok());
    }

    EXPECT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 10).ok());
    std::set<int32_t> res_set = range1.get_fixed_value_set();
    EXPECT_EQ(res_set.count(0), 0);

    for (int i = 10; i < 100; i += 10) {
        EXPECT_EQ(res_set.count(i), 1);
    }

    EXPECT_TRUE(range1.add_range(FILTER_LARGER, 20).ok());
    res_set = range1.get_fixed_value_set();
    EXPECT_EQ(res_set.count(10), 0);
    EXPECT_EQ(res_set.count(20), 0);

    for (int i = 30; i < 100; i += 10) {
        EXPECT_EQ(res_set.count(i), 1);
    }

    EXPECT_TRUE(range1.add_range(FILTER_LESS, 90).ok());
    res_set = range1.get_fixed_value_set();
    EXPECT_EQ(res_set.count(90), 0);

    for (int i = 30; i < 90; i += 10) {
        EXPECT_EQ(res_set.count(i), 1);
    }

    EXPECT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 70).ok());
    res_set = range1.get_fixed_value_set();
    EXPECT_EQ(res_set.count(80), 0);

    for (int i = 30; i < 80; i += 10) {
        EXPECT_EQ(res_set.count(i), 1);
    }

    EXPECT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 30).ok());
    res_set = range1.get_fixed_value_set();
    EXPECT_EQ(res_set.count(30), 1);

    for (int i = 40; i < 80; i += 10) {
        EXPECT_EQ(res_set.count(i), 0);
    }

    EXPECT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 30).ok());
    res_set = range1.get_fixed_value_set();
    EXPECT_EQ(res_set.count(30), 1);

    EXPECT_TRUE(range1.add_range(FILTER_LARGER, 30).ok());
    res_set = range1.get_fixed_value_set();
    EXPECT_EQ(res_set.count(30), 0);
}

TEST_F(ColumnValueRangeTest, ContainsNullTest) {
    ColumnValueRange<TYPE_INT> range1("col");

    // test fixed value range intersection with null and no null range
    for (int i = 0; i < 100; i += 10) {
        EXPECT_TRUE(range1.add_fixed_value(i).ok());
    }

    auto null_range = ColumnValueRange<TYPE_INT>::create_empty_column_value_range();
    null_range.set_contain_null(true);
    EXPECT_TRUE(!null_range.is_empty_value_range());
    null_range.intersection(range1);
    EXPECT_TRUE(null_range.is_empty_value_range());

    auto no_null_range = ColumnValueRange<TYPE_INT>::create_empty_column_value_range();
    no_null_range.set_contain_null(false);
    no_null_range.intersection(range1);
    EXPECT_EQ(no_null_range._fixed_values, range1._fixed_values);
    EXPECT_EQ(no_null_range._contain_null, range1._contain_null);

    // test scoped value range intersection with null and no null range
    range1.set_whole_value_range();
    range1.add_range(FILTER_LESS_OR_EQUAL, 80);
    range1.add_range(FILTER_LARGER, 50);

    null_range = ColumnValueRange<TYPE_INT>::create_empty_column_value_range();
    null_range.set_contain_null(true);
    EXPECT_TRUE(!null_range.is_empty_value_range());
    null_range.intersection(range1);
    EXPECT_TRUE(null_range.is_empty_value_range());

    no_null_range = ColumnValueRange<TYPE_INT>::create_empty_column_value_range();
    no_null_range.set_contain_null(false);
    no_null_range.intersection(range1);
    EXPECT_TRUE(no_null_range._fixed_values.empty());
    EXPECT_EQ(no_null_range._low_value, range1._low_value);
    EXPECT_EQ(no_null_range._high_value, range1._high_value);
    EXPECT_EQ(no_null_range._contain_null, range1._contain_null);
}

TEST_F(ColumnValueRangeTest, RangeAddRangeTest) {
    ColumnValueRange<TYPE_INT> range1("col");

    EXPECT_EQ(range1.get_range_min_value(), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(range1.get_range_max_value(), std::numeric_limits<int32_t>::max());

    EXPECT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
    EXPECT_EQ(range1.get_range_min_value(), 20);

    EXPECT_TRUE(range1.add_range(FILTER_LARGER, 30).ok());
    EXPECT_EQ(range1.get_range_min_value(), 30);

    EXPECT_TRUE(range1.add_range(FILTER_LESS, 100).ok());
    EXPECT_EQ(range1.get_range_max_value(), 100);

    EXPECT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 90).ok());
    EXPECT_EQ(range1.get_range_max_value(), 90);

    EXPECT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 31).ok());
    EXPECT_EQ(range1.get_range_max_value(), 31);

    EXPECT_TRUE(range1.add_range(FILTER_LESS, 31).ok());
    EXPECT_FALSE(range1.is_empty_value_range());

    EXPECT_TRUE(range1.add_range(FILTER_LESS, 30).ok());
    EXPECT_TRUE(range1.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, RangeIntersectionTest) {
    ColumnValueRange<TYPE_INT> range1("col");
    EXPECT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());

    ColumnValueRange<TYPE_INT> range2("col");
    EXPECT_TRUE(range2.add_range(FILTER_LESS, 100).ok());

    EXPECT_TRUE(range1.has_intersection(range2));

    // test intersection operation
    auto intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_EQ(intersection_range._low_value, 20);
    EXPECT_EQ(intersection_range._low_op, FILTER_LARGER_OR_EQUAL);
    EXPECT_EQ(intersection_range._high_value, 100);
    EXPECT_EQ(intersection_range._high_op, FILTER_LESS);

    EXPECT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 80).ok());
    EXPECT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
    EXPECT_TRUE(range1.has_intersection(range2));

    intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_EQ(intersection_range._low_value, 40);
    EXPECT_EQ(intersection_range._low_op, FILTER_LARGER);
    EXPECT_EQ(intersection_range._high_value, 80);
    EXPECT_EQ(intersection_range._high_op, FILTER_LESS_OR_EQUAL);

    EXPECT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 40).ok());
    EXPECT_FALSE(range1.has_intersection(range2));

    intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_TRUE(intersection_range.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, FixedValueIntersectionTest) {
    ColumnValueRange<TYPE_INT> range1("col");

    for (int i = 0; i < 100; i += 10) {
        EXPECT_TRUE(range1.add_fixed_value(i).ok());
    }

    ColumnValueRange<TYPE_INT> range2("col");

    for (int i = 50; i < 200; i += 10) {
        EXPECT_TRUE(range2.add_fixed_value(i).ok());
    }

    EXPECT_TRUE(range1.has_intersection(range2));
    // test intersection operation
    auto intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_EQ(intersection_range._fixed_values.size(), 5);
    EXPECT_TRUE(intersection_range._fixed_values.count(50) == 1);
    EXPECT_TRUE(intersection_range._fixed_values.count(90) == 1);

    EXPECT_TRUE(range2.add_range(FILTER_LESS_OR_EQUAL, 70).ok());
    EXPECT_TRUE(range1.has_intersection(range2));
    intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_EQ(intersection_range._fixed_values.size(), 3);
    EXPECT_TRUE(intersection_range._fixed_values.count(50) == 1);
    EXPECT_TRUE(intersection_range._fixed_values.count(70) == 1);

    EXPECT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 50).ok());
    EXPECT_TRUE(range1.has_intersection(range2));
    intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_EQ(intersection_range._fixed_values.size(), 3);
    EXPECT_TRUE(intersection_range._fixed_values.count(50) == 1);
    EXPECT_TRUE(intersection_range._fixed_values.count(70) == 1);

    EXPECT_TRUE(range2.add_range(FILTER_LESS, 60).ok());
    EXPECT_TRUE(range1.has_intersection(range2));
    intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_EQ(intersection_range._fixed_values.size(), 1);
    EXPECT_TRUE(intersection_range._fixed_values.count(50) == 1);

    EXPECT_TRUE(range1.add_range(FILTER_LARGER, 50).ok());
    EXPECT_FALSE(range1.has_intersection(range2));
    intersection_range = range1;
    intersection_range.intersection(range2);
    EXPECT_TRUE(intersection_range.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, FixedAndRangeIntersectionTest) {
    for (int type = TYPE_TINYINT; type <= TYPE_BIGINT; type++) {
        switch (type) {
        case TYPE_TINYINT: {
            ColumnValueRange<TYPE_TINYINT> range1("col");
            ColumnValueRange<TYPE_TINYINT> range2("col");

            for (int i = 0; i < 100; i += 10) {
                EXPECT_TRUE(range1.add_fixed_value(i).ok());
            }

            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            EXPECT_FALSE(range1.has_intersection(range2));
        }

        case TYPE_SMALLINT: {
            ColumnValueRange<TYPE_SMALLINT> range1("col");
            ColumnValueRange<TYPE_SMALLINT> range2("col");

            for (int i = 0; i < 100; i += 10) {
                EXPECT_TRUE(range1.add_fixed_value(i).ok());
            }

            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            EXPECT_FALSE(range1.has_intersection(range2));
        }

        case TYPE_INT: {
            ColumnValueRange<TYPE_INT> range1("col");
            ColumnValueRange<TYPE_INT> range2("col");

            for (int i = 0; i < 100; i += 10) {
                EXPECT_TRUE(range1.add_fixed_value(i).ok());
            }

            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            EXPECT_FALSE(range1.has_intersection(range2));
        }

        case TYPE_BIGINT: {
            ColumnValueRange<TYPE_BIGINT> range1("col");
            ColumnValueRange<TYPE_BIGINT> range2("col");

            for (int i = 0; i < 100; i += 10) {
                EXPECT_TRUE(range1.add_fixed_value(i).ok());
            }

            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            EXPECT_TRUE(range1.has_intersection(range2));
            EXPECT_TRUE(range2.has_intersection(range1));

            EXPECT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            EXPECT_FALSE(range1.has_intersection(range2));

            range2.set_whole_value_range();
            EXPECT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            EXPECT_FALSE(range1.has_intersection(range2));
        }

        default:
            break;
        }
    }
}

class OlapScanKeysTest : public ::testing::Test {
public:
    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(OlapScanKeysTest, ExtendFixedTest) {
    OlapScanKeys scan_keys;

    ColumnValueRange<TYPE_INT> range1("col");

    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(range1.add_fixed_value(i).ok());
    }

    bool exact_range = true;
    int max_key_range = 1024;
    bool eos = false;
    scan_keys.extend_scan_key(range1, max_key_range, &exact_range, &eos);
    EXPECT_EQ(exact_range, true);

    std::vector<std::unique_ptr<OlapScanRange>> key_range;
    scan_keys.get_key_range(&key_range);

    EXPECT_EQ(key_range.size(), 3);

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "0");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->end_scan_range), "0");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->begin_scan_range), "1");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->end_scan_range), "1");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->begin_scan_range), "2");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->end_scan_range), "2");

    ColumnValueRange<TYPE_INT> range2("col");

    for (int i = 0; i < 2; ++i) {
        EXPECT_TRUE(range2.add_fixed_value(i).ok());
    }

    exact_range = true;
    scan_keys.extend_scan_key(range2, max_key_range, &exact_range, &eos);
    EXPECT_EQ(exact_range, true);

    scan_keys.get_key_range(&key_range);

    EXPECT_EQ(key_range.size(), 6);

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "0,0");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->end_scan_range), "0,0");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->begin_scan_range), "1,0");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->end_scan_range), "1,0");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->begin_scan_range), "2,0");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->end_scan_range), "2,0");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[3]->begin_scan_range), "0,1");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[3]->end_scan_range), "0,1");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[4]->begin_scan_range), "1,1");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[4]->end_scan_range), "1,1");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[5]->begin_scan_range), "2,1");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[5]->end_scan_range), "2,1");

    range2.set_whole_value_range();
    EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());

    exact_range = true;
    scan_keys.extend_scan_key(range2, max_key_range, &exact_range, &eos);
    EXPECT_EQ(exact_range, true);

    scan_keys.get_key_range(&key_range);
    EXPECT_EQ(key_range.size(), 6);

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "0,0,100");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->end_scan_range), "0,0,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->begin_scan_range), "1,0,100");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->end_scan_range), "1,0,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->begin_scan_range), "2,0,100");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->end_scan_range), "2,0,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[3]->begin_scan_range), "0,1,100");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[3]->end_scan_range), "0,1,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[4]->begin_scan_range), "1,1,100");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[4]->end_scan_range), "1,1,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[5]->begin_scan_range), "2,1,100");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[5]->end_scan_range), "2,1,2147483647");
}

TEST_F(OlapScanKeysTest, ExtendFixedAndRangeTest) {
    OlapScanKeys scan_keys;

    ColumnValueRange<TYPE_INT> range1("col");

    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(range1.add_fixed_value(i).ok());
    }

    bool exact_range = true;
    int max_scan_key_num = 1024;
    bool eos = false;
    scan_keys.extend_scan_key(range1, max_scan_key_num, &exact_range, &eos);
    EXPECT_EQ(exact_range, true);

    ColumnValueRange<TYPE_INT> range2("col");
    EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());

    exact_range = true;
    scan_keys.extend_scan_key(range2, max_scan_key_num, &exact_range, &eos);
    EXPECT_EQ(exact_range, true);

    std::vector<std::unique_ptr<OlapScanRange>> key_range;

    scan_keys.get_key_range(&key_range);

    EXPECT_EQ(key_range.size(), 3);

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "0,20");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->end_scan_range), "0,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->begin_scan_range), "1,20");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->end_scan_range), "1,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->begin_scan_range), "2,20");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->end_scan_range), "2,2147483647");

    EXPECT_TRUE(range2.add_range(FILTER_LESS, 100).ok());

    exact_range = true;
    scan_keys.extend_scan_key(range2, max_scan_key_num, &exact_range, &eos);
    EXPECT_EQ(exact_range, true);

    scan_keys.get_key_range(&key_range);

    EXPECT_EQ(key_range.size(), 3);

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "0,20");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->end_scan_range), "0,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->begin_scan_range), "1,20");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->end_scan_range), "1,2147483647");

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->begin_scan_range), "2,20");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[2]->end_scan_range), "2,2147483647");
}

TEST_F(OlapScanKeysTest, ExtendRangeTest) {
    OlapScanKeys scan_keys;
    config::doris_max_scan_key_num = 1;

    ColumnValueRange<TYPE_BIGINT> range2("col");
    EXPECT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
    EXPECT_TRUE(range2.add_range(FILTER_LESS_OR_EQUAL, 100).ok());

    bool exact_range = true;
    bool eos = false;
    EXPECT_TRUE(scan_keys.extend_scan_key(range2, 1024, &exact_range, &eos).ok());
    EXPECT_EQ(exact_range, true);

    std::vector<std::unique_ptr<OlapScanRange>> key_range;

    scan_keys.get_key_range(&key_range);

    EXPECT_EQ(key_range.size(), 81);

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "20");
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[80]->end_scan_range), "100");

    EXPECT_TRUE(range2.add_range(FILTER_LESS, 50).ok());

    exact_range = true;
    EXPECT_TRUE(scan_keys.extend_scan_key(range2, 1024, &exact_range, &eos).ok());
    EXPECT_EQ(exact_range, true);

    scan_keys.get_key_range(&key_range);

    EXPECT_EQ(key_range.size(), 81);

    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "20,20");
    EXPECT_TRUE(key_range[0]->begin_include);
    EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->end_scan_range), "20,50");
    EXPECT_FALSE(key_range[0]->end_include);
}

TEST_F(OlapScanKeysTest, EachtypeTest) {
    std::vector<std::unique_ptr<OlapScanRange>> key_range;

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_TINYINT> range("col");
        bool exact_range = true;
        bool eos = false;
        EXPECT_TRUE(scan_keys.extend_scan_key(range, 1024, &exact_range, &eos).ok());
        EXPECT_EQ(exact_range, true);
        scan_keys.get_key_range(&key_range);
        // contain null, [-128, 127]
        EXPECT_EQ(key_range.size(), 257);
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->begin_scan_range), "-128");
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[256]->end_scan_range), "127");

        EXPECT_TRUE(range.add_range(FILTER_LESS, 50).ok());
        scan_keys.clear();
        exact_range = true;
        EXPECT_TRUE(scan_keys.extend_scan_key(range, 1024, &exact_range, &eos).ok());
        EXPECT_EQ(exact_range, true);
        scan_keys.get_key_range(&key_range);

        EXPECT_EQ(key_range.size(), 178);
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "-128");
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[177]->end_scan_range), "49");
    }

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<TYPE_SMALLINT> range("col");
        bool exact_range = true;
        int max_scan_key = 48;
        bool eos = false;
        EXPECT_TRUE(scan_keys.extend_scan_key(range, max_scan_key, &exact_range, &eos).ok());
        EXPECT_EQ(exact_range, true);
        scan_keys.get_key_range(&key_range);
        EXPECT_EQ(key_range.size(), 49);
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[1]->begin_scan_range), "-32768");
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[max_scan_key]->end_scan_range), "32767");

        EXPECT_TRUE(range.add_range(FILTER_LARGER, 0).ok());
        scan_keys.clear();
        exact_range = true;
        EXPECT_TRUE(scan_keys.extend_scan_key(range, max_scan_key, &exact_range, &eos).ok());
        EXPECT_EQ(exact_range, true);
        scan_keys.get_key_range(&key_range);

        EXPECT_EQ(key_range.size(), max_scan_key);
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "1");
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[max_scan_key - 1]->end_scan_range), "32767");

        EXPECT_TRUE(range.add_range(FILTER_LESS, 32766).ok());
        scan_keys.clear();
        exact_range = true;
        EXPECT_TRUE(scan_keys.extend_scan_key(range, max_scan_key, &exact_range, &eos).ok());
        EXPECT_EQ(exact_range, true);
        scan_keys.get_key_range(&key_range);

        EXPECT_EQ(key_range.size(), max_scan_key);
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[0]->begin_scan_range), "2");
        EXPECT_EQ(OlapScanKeys::to_print_key(key_range[max_scan_key - 1]->end_scan_range), "32765");
    }
}

TEST_F(OlapScanKeysTest, ToOlapFilterTest) {
    ColumnValueRange<TYPE_INT> range("col");

    std::vector<TCondition> filters;
    range.to_olap_filter(filters);
    EXPECT_TRUE(filters.empty());

    range.set_contain_null(true);
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, "is");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "null");

    range.set_contain_null(false);
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, "is");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "not null");

    EXPECT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, ">=");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "20");

    EXPECT_TRUE(range.add_range(FILTER_LESS, 100).ok());
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, ">=");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "20");

    EXPECT_EQ(std::next(filters.begin(), 1)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 1)->condition_op, "<<");
    EXPECT_EQ(std::next(filters.begin(), 1)->condition_values[0], "100");

    range.set_whole_value_range();
    EXPECT_TRUE(range.add_range(FILTER_LESS_OR_EQUAL, 100).ok());
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, "<=");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "100");

    EXPECT_TRUE(range.add_range(FILTER_LARGER, 20).ok());
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, ">>");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "20");
    EXPECT_EQ(std::next(filters.begin(), 1)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 1)->condition_op, "<=");
    EXPECT_EQ(std::next(filters.begin(), 1)->condition_values[0], "100");

    range.set_whole_value_range();
    EXPECT_TRUE(range.add_fixed_value(30).ok());
    EXPECT_TRUE(range.add_fixed_value(40).ok());
    EXPECT_TRUE(range.add_fixed_value(50).ok());
    EXPECT_TRUE(range.add_fixed_value(20).ok());
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, "*=");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "20");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[1], "30");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[2], "40");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[3], "50");

    filters.clear();
    range.to_in_condition(filters, false);
    EXPECT_EQ(std::next(filters.begin(), 0)->column_name, "col");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_op, "!*=");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "20");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[1], "30");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[2], "40");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[3], "50");

    EXPECT_TRUE(range.add_range(FILTER_LARGER, 20).ok());
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "30");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[1], "40");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[2], "50");

    EXPECT_TRUE(range.add_range(FILTER_LESS_OR_EQUAL, 40).ok());
    filters.clear();
    range.to_olap_filter(filters);
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[0], "30");
    EXPECT_EQ(std::next(filters.begin(), 0)->condition_values[1], "40");
}

} // namespace doris
