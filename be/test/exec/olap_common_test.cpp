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

#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <vector>

#define protected public
#define private public
#include <gtest/gtest.h>

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
    doris_scan_range->__set_version_hash("3997217299075720338");
    doris_scan_range->engine_table_name.push_back("ShowQStats");
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
    virtual void SetUp() {

    }

    virtual void TearDown() {

    }

};

TEST_F(ColumnValueRangeTest, ExceptionCase) {
    ColumnValueRange<int32_t> range1;
    ASSERT_FALSE(range1.add_fixed_value(10).ok());
    ASSERT_FALSE(range1.add_range(FILTER_LESS_OR_EQUAL, 10).ok());
}

TEST_F(ColumnValueRangeTest, NormalCase) {
    ColumnValueRange<int32_t> range1("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    ASSERT_TRUE(range1.add_fixed_value(10).ok());
    ASSERT_TRUE(range1.add_fixed_value(20).ok());
    ASSERT_TRUE(range1.add_fixed_value(30).ok());

    ASSERT_TRUE(range1.is_fixed_value_range());

    ASSERT_TRUE(range1.add_range(FILTER_LESS, 30).ok());
    ASSERT_FALSE(range1.is_empty_value_range());

    ColumnValueRange<int32_t> range2("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    ASSERT_TRUE(range2.add_fixed_value(30).ok());
    ASSERT_FALSE(range1.has_intersection(range2));

    ASSERT_TRUE(range2.add_fixed_value(20).ok());
    ASSERT_TRUE(range1.has_intersection(range2));

    ASSERT_TRUE(range2.is_fixed_value_range());
    ASSERT_TRUE(range2.add_range(FILTER_LARGER, 50).ok());
    ASSERT_FALSE(range2.is_fixed_value_range());

    ASSERT_TRUE(range2.is_empty_value_range());
    ASSERT_FALSE(range1.has_intersection(range2));
}

TEST_F(ColumnValueRangeTest, FixedAddRangeTest) {
    ColumnValueRange<int32_t> range1("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    for (int i = 0; i < 100; i += 10) {
        ASSERT_TRUE(range1.add_fixed_value(i).ok());
    }

    ASSERT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 10).ok());
    std::set<int32_t> res_set = range1.get_fixed_value_set();
    ASSERT_EQ(res_set.count(0), 0);

    for (int i = 10; i < 100; i += 10) {
        ASSERT_EQ(res_set.count(i), 1);
    }

    ASSERT_TRUE(range1.add_range(FILTER_LARGER, 20).ok());
    res_set = range1.get_fixed_value_set();
    ASSERT_EQ(res_set.count(10), 0);
    ASSERT_EQ(res_set.count(20), 0);

    for (int i = 30; i < 100; i += 10) {
        ASSERT_EQ(res_set.count(i), 1);
    }

    ASSERT_TRUE(range1.add_range(FILTER_LESS, 90).ok());
    res_set = range1.get_fixed_value_set();
    ASSERT_EQ(res_set.count(90), 0);

    for (int i = 30; i < 90; i += 10) {
        ASSERT_EQ(res_set.count(i), 1);
    }

    ASSERT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 70).ok());
    res_set = range1.get_fixed_value_set();
    ASSERT_EQ(res_set.count(80), 0);

    for (int i = 30; i < 80; i += 10) {
        ASSERT_EQ(res_set.count(i), 1);
    }

    ASSERT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 30).ok());
    res_set = range1.get_fixed_value_set();
    ASSERT_EQ(res_set.count(30), 1);

    for (int i = 40; i < 80; i += 10) {
        ASSERT_EQ(res_set.count(i), 0);
    }

    ASSERT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 30).ok());
    res_set = range1.get_fixed_value_set();
    ASSERT_EQ(res_set.count(30), 1);

    ASSERT_TRUE(range1.add_range(FILTER_LARGER, 30).ok());
    res_set = range1.get_fixed_value_set();
    ASSERT_EQ(res_set.count(30), 0);

}

TEST_F(ColumnValueRangeTest, RangeAddRangeTest) {
    ColumnValueRange<int32_t> range1("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    ASSERT_EQ(range1.get_range_min_value(), std::numeric_limits<int32_t>::min());
    ASSERT_EQ(range1.get_range_max_value(), std::numeric_limits<int32_t>::max());

    ASSERT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
    ASSERT_EQ(range1.get_range_min_value(), 20);

    ASSERT_TRUE(range1.add_range(FILTER_LARGER, 30).ok());
    ASSERT_EQ(range1.get_range_min_value(), 30);

    ASSERT_TRUE(range1.add_range(FILTER_LESS, 100).ok());
    ASSERT_EQ(range1.get_range_max_value(), 100);

    ASSERT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 90).ok());
    ASSERT_EQ(range1.get_range_max_value(), 90);

    ASSERT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 31).ok());
    ASSERT_EQ(range1.get_range_max_value(), 31);

    ASSERT_TRUE(range1.add_range(FILTER_LESS, 31).ok());
    ASSERT_FALSE(range1.is_empty_value_range());

    ASSERT_TRUE(range1.add_range(FILTER_LESS, 30).ok());
    ASSERT_TRUE(range1.is_empty_value_range());
}

TEST_F(ColumnValueRangeTest, RangeIntersectionTest) {
    ColumnValueRange<int32_t> range1("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    ASSERT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());

    ColumnValueRange<int32_t> range2("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    ASSERT_TRUE(range2.add_range(FILTER_LESS, 100).ok());

    ASSERT_TRUE(range1.has_intersection(range2));

    ASSERT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 80).ok());
    ASSERT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());

    ASSERT_TRUE(range1.has_intersection(range2));

    ASSERT_TRUE(range1.add_range(FILTER_LESS_OR_EQUAL, 40).ok());
    ASSERT_FALSE(range1.has_intersection(range2));

}

TEST_F(ColumnValueRangeTest, FixedValueIntersectionTest) {
    ColumnValueRange<int32_t> range1("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    for (int i = 0; i < 100; i += 10) {
        ASSERT_TRUE(range1.add_fixed_value(i).ok());
    }

    ColumnValueRange<int32_t> range2("col", TYPE_INT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    for (int i = 50; i < 200; i += 10) {
        ASSERT_TRUE(range2.add_fixed_value(i).ok());
    }

    ASSERT_TRUE(range1.has_intersection(range2));

    ASSERT_TRUE(range2.add_range(FILTER_LESS_OR_EQUAL, 70).ok());
    ASSERT_TRUE(range1.has_intersection(range2));

    ASSERT_TRUE(range1.add_range(FILTER_LARGER_OR_EQUAL, 50).ok());
    ASSERT_TRUE(range1.has_intersection(range2));

    ASSERT_TRUE(range2.add_range(FILTER_LESS, 60).ok());
    ASSERT_TRUE(range1.has_intersection(range2));

    ASSERT_TRUE(range1.add_range(FILTER_LARGER, 50).ok());
    ASSERT_FALSE(range1.has_intersection(range2));
}

TEST_F(ColumnValueRangeTest, FixedAndRangeIntersectionTest) {
    for (int type = TYPE_TINYINT; type <= TYPE_BIGINT; type++) {
        switch (type) {
        case TYPE_TINYINT: {
            ColumnValueRange<int8_t> range1("col", TYPE_TINYINT,
                                            std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max());
            ColumnValueRange<int8_t> range2("col", TYPE_TINYINT,
                                            std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max());

            for (int i = 0; i < 100; i += 10) {
                ASSERT_TRUE(range1.add_fixed_value(i).ok());
            }

            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            ASSERT_FALSE(range1.has_intersection(range2));
        }

        case TYPE_SMALLINT: {
            ColumnValueRange<int16_t> range1("col", TYPE_SMALLINT,
                                             std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
            ColumnValueRange<int16_t> range2("col", TYPE_SMALLINT,
                                             std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());

            for (int i = 0; i < 100; i += 10) {
                ASSERT_TRUE(range1.add_fixed_value(i).ok());
            }

            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            ASSERT_FALSE(range1.has_intersection(range2));
        }

        case TYPE_INT: {
            ColumnValueRange<int32_t> range1("col", TYPE_INT,
                                             std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
            ColumnValueRange<int32_t> range2("col", TYPE_INT,
                                             std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

            for (int i = 0; i < 100; i += 10) {
                ASSERT_TRUE(range1.add_fixed_value(i).ok());
            }

            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            ASSERT_FALSE(range1.has_intersection(range2));
        }

        case TYPE_BIGINT: {
            ColumnValueRange<int64_t> range1("col", TYPE_BIGINT,
                                             std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
            ColumnValueRange<int64_t> range2("col", TYPE_BIGINT,
                                             std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());

            for (int i = 0; i < 100; i += 10) {
                ASSERT_TRUE(range1.add_fixed_value(i).ok());
            }

            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LESS, 50).ok());
            ASSERT_TRUE(range1.has_intersection(range2));
            ASSERT_TRUE(range2.has_intersection(range1));

            ASSERT_TRUE(range2.add_range(FILTER_LARGER, 40).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());
            ASSERT_FALSE(range1.has_intersection(range2));

            range2.clear();
            ASSERT_TRUE(range2.add_range(FILTER_LESS, 0).ok());
            ASSERT_FALSE(range1.has_intersection(range2));
        }

        default:
            break;
        }

    }
}

class OlapScanKeysTest : public ::testing::Test {
public:
    virtual void SetUp() {

    }

    virtual void TearDown() {

    }

};

TEST_F(OlapScanKeysTest, ExtendFixedTest) {
    OlapScanKeys scan_keys;

    ColumnValueRange<int32_t> range1("col", TYPE_BIGINT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(range1.add_fixed_value(i).ok());
    }

    scan_keys.extend_scan_key(range1);

    std::vector<OlapScanRange > key_range;
    scan_keys.get_key_range(&key_range);

    ASSERT_EQ(key_range.size(), 3);

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "0");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].begin_scan_range), "1");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].end_scan_range), "1");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].begin_scan_range), "2");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].end_scan_range), "2");

    ColumnValueRange<int32_t> range2("col", TYPE_BIGINT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(range2.add_fixed_value(i).ok());
    }

    scan_keys.extend_scan_key(range2);

    scan_keys.get_key_range(&key_range);

    ASSERT_EQ(key_range.size(), 6);

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0,0");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "0,0");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].begin_scan_range), "1,0");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].end_scan_range), "1,0");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].begin_scan_range), "2,0");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].end_scan_range), "2,0");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[3].begin_scan_range), "0,1");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[3].end_scan_range), "0,1");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[4].begin_scan_range), "1,1");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[4].end_scan_range), "1,1");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[5].begin_scan_range), "2,1");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[5].end_scan_range), "2,1");

    range2.clear();
    ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 100).ok());

    scan_keys.extend_scan_key(range2);

    scan_keys.get_key_range(&key_range);

    ASSERT_EQ(key_range.size(), 6);

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0,0,100");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "0,0,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].begin_scan_range), "1,0,100");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].end_scan_range), "1,0,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].begin_scan_range), "2,0,100");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].end_scan_range), "2,0,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[3].begin_scan_range), "0,1,100");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[3].end_scan_range), "0,1,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[4].begin_scan_range), "1,1,100");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[4].end_scan_range), "1,1,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[5].begin_scan_range), "2,1,100");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[5].end_scan_range), "2,1,2147483647");
}

TEST_F(OlapScanKeysTest, ExtendFixedAndRangeTest) {
    OlapScanKeys scan_keys;

    ColumnValueRange<int32_t> range1("col", TYPE_BIGINT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(range1.add_fixed_value(i).ok());
    }

    scan_keys.extend_scan_key(range1);

    ColumnValueRange<int32_t> range2("col", TYPE_BIGINT,
                                     std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());

    scan_keys.extend_scan_key(range2);

    std::vector<OlapScanRange > key_range;

    scan_keys.get_key_range(&key_range);

    ASSERT_EQ(key_range.size(), 3);

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0,20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "0,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].begin_scan_range), "1,20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].end_scan_range), "1,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].begin_scan_range), "2,20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].end_scan_range), "2,2147483647");

    ASSERT_TRUE(range2.add_range(FILTER_LESS, 100).ok());

    scan_keys.extend_scan_key(range2);

    scan_keys.get_key_range(&key_range);

    ASSERT_EQ(key_range.size(), 3);

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0,20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "0,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].begin_scan_range), "1,20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[1].end_scan_range), "1,2147483647");

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].begin_scan_range), "2,20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[2].end_scan_range), "2,2147483647");
}

TEST_F(OlapScanKeysTest, ExtendRangeTest) {
    OlapScanKeys scan_keys;
    config::doris_max_scan_key_num = 1;

    ColumnValueRange<int64_t> range2("col", TYPE_BIGINT,
                                     std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
    ASSERT_TRUE(range2.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
    ASSERT_TRUE(range2.add_range(FILTER_LESS, 100).ok());

    ASSERT_TRUE(scan_keys.extend_scan_key(range2).ok());

    std::vector<OlapScanRange > key_range;

    scan_keys.get_key_range(&key_range);

    ASSERT_EQ(key_range.size(), 1);

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "100");

    ASSERT_TRUE(range2.add_range(FILTER_LESS, 50).ok());

    ASSERT_TRUE(scan_keys.extend_scan_key(range2).ok());

    scan_keys.get_key_range(&key_range);

    ASSERT_EQ(key_range.size(), 1);

    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "20");
    ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "100");
}

TEST_F(OlapScanKeysTest, EachtypeTest) {
    std::vector<OlapScanRange > key_range;

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<int32_t> range("col", TYPE_TINYINT,
                                        std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max());
        ASSERT_TRUE(scan_keys.extend_scan_key(range).ok());
        scan_keys.get_key_range(&key_range);
        ASSERT_EQ(key_range.size(), 1);
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "-128");
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "127");

        ASSERT_TRUE(range.add_range(FILTER_LESS, 50).ok());
        scan_keys.clear();
        ASSERT_TRUE(scan_keys.extend_scan_key(range).ok());
        scan_keys.get_key_range(&key_range);

        ASSERT_EQ(key_range.size(), 1);
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "-128");
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "50");
    }

    {
        OlapScanKeys scan_keys;
        ColumnValueRange<int32_t> range("col", TYPE_SMALLINT,
                                        std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
        ASSERT_TRUE(scan_keys.extend_scan_key(range).ok());
        scan_keys.get_key_range(&key_range);
        ASSERT_EQ(key_range.size(), 1);
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "-32768");
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "32767");

        ASSERT_TRUE(range.add_range(FILTER_LARGER, 0).ok());
        scan_keys.clear();
        ASSERT_TRUE(scan_keys.extend_scan_key(range).ok());
        scan_keys.get_key_range(&key_range);

        ASSERT_EQ(key_range.size(), 1);
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0");
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "32767");

        ASSERT_TRUE(range.add_range(FILTER_LESS, 32768).ok());
        scan_keys.clear();
        ASSERT_TRUE(scan_keys.extend_scan_key(range).ok());
        scan_keys.get_key_range(&key_range);

        ASSERT_EQ(key_range.size(), 1);
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0");
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "32767");

        ASSERT_TRUE(range.add_range(FILTER_LESS, 32766).ok());
        scan_keys.clear();
        ASSERT_TRUE(scan_keys.extend_scan_key(range).ok());
        scan_keys.get_key_range(&key_range);

        ASSERT_EQ(key_range.size(), 1);
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].begin_scan_range), "0");
        ASSERT_EQ(OlapScanKeys::to_print_key(key_range[0].end_scan_range), "32766");
    }
}

class DorisScanRangeTest : public ::testing::Test {
public:
    void SetUp() {

    }

    void TearDown() {

    }
};

TEST_F(DorisScanRangeTest, ToOlapFilterTest) {
    ColumnValueRange<int32_t> range("col", TYPE_INT,
                                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    ASSERT_EQ(range.to_olap_filter(), std::string(""));

    ASSERT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col>=20"));

    ASSERT_TRUE(range.add_range(FILTER_LESS, 100).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col>=20,col<<100"));

    range.clear();
    ASSERT_TRUE(range.add_range(FILTER_LESS_OR_EQUAL, 100).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col<=100"));

    ASSERT_TRUE(range.add_range(FILTER_LARGER, 20).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col>>20,col<=100"));

    range.clear();
    ASSERT_TRUE(range.add_fixed_value(30).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col*=30"));

    ASSERT_TRUE(range.add_fixed_value(40).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col*=30|40"));

    ASSERT_TRUE(range.add_fixed_value(50).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col*=30|40|50"));

    ASSERT_TRUE(range.add_fixed_value(20).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col*=20|30|40|50"));

    ASSERT_TRUE(range.add_range(FILTER_LARGER, 20).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col*=30|40|50"));

    ASSERT_TRUE(range.add_range(FILTER_LESS_OR_EQUAL, 40).ok());
    ASSERT_EQ(range.to_olap_filter(), std::string("col*=30|40"));
}

TEST_F(DorisScanRangeTest, RangeIntersectionTest) {
    TPaloScanRange doris_scan_range;
    construct_scan_range(&doris_scan_range);

    DorisScanRange scan_range(doris_scan_range);

    ASSERT_TRUE(scan_range.init().ok());

    ColumnValueRange<int32_t> range("col", TYPE_INT,
                                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    ColumnValueRangeType range_type = range;

    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));

    ASSERT_EQ(-1, scan_range.has_intersection("nopartition_column", range_type));

    ASSERT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, 20).ok());
    ASSERT_TRUE(range.add_range(FILTER_LESS, 100).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));

    range.clear();
    ASSERT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, -5000).ok());
    ASSERT_TRUE(range.add_range(FILTER_LESS, 100).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));

    range.clear();
    ASSERT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, -5000).ok());
    ASSERT_TRUE(range.add_range(FILTER_LESS, -999).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));

    range.clear();
    ASSERT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, 999).ok());
    ASSERT_TRUE(range.add_range(FILTER_LESS, 5000).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));

    range.clear();
    ASSERT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, -5000).ok());
    ASSERT_TRUE(range.add_range(FILTER_LESS, -1000).ok());
    range_type = range;
    ASSERT_EQ(0, scan_range.has_intersection("partition_column", range_type));

    range.clear();
    ASSERT_TRUE(range.add_range(FILTER_LARGER_OR_EQUAL, 1000).ok());
    ASSERT_TRUE(range.add_range(FILTER_LESS, 5000).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));
}

TEST_F(DorisScanRangeTest, FixedIntersectionTest) {
    TPaloScanRange doris_scan_range;
    construct_scan_range(&doris_scan_range);

    DorisScanRange scan_range(doris_scan_range);

    ASSERT_TRUE(scan_range.init().ok());

    ColumnValueRange<int32_t> range("col", TYPE_INT,
                                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    ColumnValueRangeType range_type = range;

    ASSERT_TRUE(range.add_fixed_value(-1001).ok());
    range_type = range;
    ASSERT_EQ(0, scan_range.has_intersection("partition_column", range_type));

    ASSERT_TRUE(range.add_fixed_value(1000).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));

    ASSERT_TRUE(range.add_fixed_value(-1000).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));

    range.clear();
    ASSERT_TRUE(range.add_fixed_value(999).ok());
    range_type = range;
    ASSERT_EQ(1, scan_range.has_intersection("partition_column", range_type));
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::Init();
    return RUN_ALL_TESTS();
}
