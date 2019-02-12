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

#include "olap/row_cursor.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"
#include "util/logging.h"

namespace doris {

void set_tablet_schema_for_init(std::vector<FieldInfo>* tablet_schema) {
    FieldInfo k1;
    k1.name = "k1";
    k1.type = OLAP_FIELD_TYPE_TINYINT;
    k1.length = 1;
    k1.is_key = true;
    k1.index_length = 1;
    k1.is_allow_null = true;
    tablet_schema->push_back(k1);

    FieldInfo k2;
    k2.name = "k2";
    k2.type = OLAP_FIELD_TYPE_SMALLINT;
    k2.length = 2;
    k2.default_value = "0";
    k2.is_key = true;
    k2.index_length = 2;
    k2.is_allow_null = true;
    tablet_schema->push_back(k2);

    FieldInfo k3;
    k3.name = "k3";
    k3.type = OLAP_FIELD_TYPE_INT;
    k3.length = 4;
    k3.is_key = true;
    k3.index_length = 4;
    k3.is_allow_null = true;
    tablet_schema->push_back(k3);

    FieldInfo k4;
    k4.name = "k4";
    k4.type = OLAP_FIELD_TYPE_DATE;
    k4.length = 3;
    k4.is_key = true;
    k4.index_length = 3;
    k4.is_allow_null = true;
    tablet_schema->push_back(k4);

    FieldInfo k5;
    k5.name = "k5";
    k5.type = OLAP_FIELD_TYPE_DATETIME;
    k5.length = 8;
    k5.is_key = true;
    k5.index_length = 8;
    k5.is_allow_null = true;
    tablet_schema->push_back(k5);

    FieldInfo k6;
    k6.name = "k6";
    k6.type = OLAP_FIELD_TYPE_DECIMAL;
    k6.length = 12;
    k6.precision = 6;
    k6.frac = 3;
    k6.is_key = true;
    k6.index_length = 12;
    k6.is_allow_null = true;
    tablet_schema->push_back(k6);

    FieldInfo k7;
    k7.name = "k7";
    k7.type = OLAP_FIELD_TYPE_CHAR;
    k7.length = 4;
    k7.default_value = "char";
    k7.is_key = true;
    k7.index_length = 4;
    k7.is_allow_null = true;
    tablet_schema->push_back(k7);

    FieldInfo v1;
    v1.name = "v1";
    v1.type = OLAP_FIELD_TYPE_BIGINT;
    v1.length = 8;
    v1.aggregation = OLAP_FIELD_AGGREGATION_SUM;
    v1.is_key = false;
    v1.is_allow_null = true;
    tablet_schema->push_back(v1);

    FieldInfo v2;
    v2.name = "v2";
    v2.type = OLAP_FIELD_TYPE_VARCHAR;
    v2.length = 16 + OLAP_STRING_MAX_BYTES;
    v2.aggregation = OLAP_FIELD_AGGREGATION_REPLACE;
    v2.is_key = false;
    v2.is_allow_null = true;
    tablet_schema->push_back(v2);

    FieldInfo v3;
    v3.name = "v3";
    v3.type = OLAP_FIELD_TYPE_LARGEINT;
    v3.length = 16;
    v3.aggregation = OLAP_FIELD_AGGREGATION_MAX;
    v3.is_key = false;
    v3.is_allow_null = true;
    tablet_schema->push_back(v3);

    FieldInfo v4;
    v4.name = "v4";
    v4.type = OLAP_FIELD_TYPE_DECIMAL;
    v4.length = 12;
    v4.aggregation = OLAP_FIELD_AGGREGATION_MIN;
    v4.is_key = false;
    v4.is_allow_null = true;
    tablet_schema->push_back(v4);

    FieldInfo v5;
    v5.name = "v5";
    v5.type = OLAP_FIELD_TYPE_HLL;
    v5.length = HLL_COLUMN_DEFAULT_LEN;
    v5.aggregation = OLAP_FIELD_AGGREGATION_HLL_UNION;
    v5.is_key = false;
    v5.is_allow_null = true;
    tablet_schema->push_back(v5);
}

void set_tablet_schema_for_scan_key(std::vector<FieldInfo>* tablet_schema) {
    FieldInfo k1;
    k1.name = "k1";
    k1.type = OLAP_FIELD_TYPE_CHAR;
    k1.length = 4;
    k1.index_length = 4;
    k1.default_value = "char";
    k1.is_key = true;
    k1.is_allow_null = true;
    tablet_schema->push_back(k1);

    FieldInfo k2;
    k2.name = "k2";
    k2.type = OLAP_FIELD_TYPE_VARCHAR;
    k2.length = 16 + OLAP_STRING_MAX_BYTES;
    k2.index_length = 20;
    k2.is_key = true;
    k2.is_allow_null = true;
    tablet_schema->push_back(k2);

    FieldInfo v1;
    v1.name = "v1";
    v1.type = OLAP_FIELD_TYPE_LARGEINT;
    v1.length = 16;
    v1.aggregation = OLAP_FIELD_AGGREGATION_MAX;
    v1.is_key = false;
    v1.is_allow_null = true;
    tablet_schema->push_back(v1);

    FieldInfo v2;
    v2.name = "v2";
    v2.type = OLAP_FIELD_TYPE_DECIMAL;
    v2.length = 12;
    v2.aggregation = OLAP_FIELD_AGGREGATION_MIN;
    v2.is_key = false;
    v2.is_allow_null = true;
    tablet_schema->push_back(v2);
}

void set_tablet_schema_for_cmp_and_aggregate(std::vector<FieldInfo>* tablet_schema) {
    FieldInfo k1;
    k1.name = "k1";
    k1.type = OLAP_FIELD_TYPE_CHAR;
    k1.length = 4;
    k1.default_value = "char";
    k1.is_key = true;
    k1.index_length = 4;
    k1.is_allow_null = true;
    tablet_schema->push_back(k1);

    FieldInfo k2;
    k2.name = "k2";
    k2.type = OLAP_FIELD_TYPE_INT;
    k2.length = 4;
    k2.is_key = true;
    k2.index_length = 4;
    k2.is_allow_null = true;
    tablet_schema->push_back(k2);

    FieldInfo v1;
    v1.name = "v1";
    v1.type = OLAP_FIELD_TYPE_LARGEINT;
    v1.length = 16;
    v1.aggregation = OLAP_FIELD_AGGREGATION_SUM;
    v1.is_key = false;
    v1.is_allow_null = true;
    tablet_schema->push_back(v1);

    FieldInfo v2;
    v2.name = "v2";
    v2.type = OLAP_FIELD_TYPE_DOUBLE;
    v2.length = 8;
    v2.aggregation = OLAP_FIELD_AGGREGATION_MIN;
    v2.is_key = false;
    v2.is_allow_null = true;
    tablet_schema->push_back(v2);

    FieldInfo v3;
    v3.name = "v3";
    v3.type = OLAP_FIELD_TYPE_DECIMAL;
    v3.length = 12;
    v3.aggregation = OLAP_FIELD_AGGREGATION_MAX;
    v3.is_key = false;
    v3.is_allow_null = true;
    tablet_schema->push_back(v3);

    FieldInfo v4;
    v4.name = "v4";
    v4.type = OLAP_FIELD_TYPE_VARCHAR;
    v4.length = 16 + OLAP_STRING_MAX_BYTES;
    v4.aggregation = OLAP_FIELD_AGGREGATION_REPLACE;
    v4.is_key = false;
    v4.is_allow_null = true;
    tablet_schema->push_back(v4);
}

class TestRowCursor : public testing::Test {
public:
    TestRowCursor() {
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    virtual void SetUp() {}

    virtual void TearDown() {}

    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
};

TEST_F(TestRowCursor, InitRowCursor) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_init(&tablet_schema);
    RowCursor row;
    OLAPStatus res = row.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 126);
    ASSERT_EQ(row.get_variable_len(), 16413);
}

TEST_F(TestRowCursor, InitRowCursorWithColumnCount) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_init(&tablet_schema);
    RowCursor row;
    OLAPStatus res = row.init(tablet_schema, 5);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 23);
    ASSERT_EQ(row.get_variable_len(), 0);
    row.allocate_memory_for_string_type(tablet_schema);
    ASSERT_EQ(row.get_variable_len(), 0);
}

TEST_F(TestRowCursor, InitRowCursorWithColIds) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_init(&tablet_schema);

    std::vector<uint32_t> col_ids;
    for (size_t i = 0; i < tablet_schema.size() / 2; ++i) {
        col_ids.push_back(i * 2);
    }

    RowCursor row;
    OLAPStatus res = row.init(tablet_schema, col_ids);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 63);
    ASSERT_EQ(row.get_variable_len(), 20);
}

TEST_F(TestRowCursor, InitRowCursorWithScanKey) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_scan_key(&tablet_schema);

    std::vector<std::string> scan_keys;
    scan_keys.push_back("char_exceed_length");
    scan_keys.push_back("varchar_exceed_length");

    RowCursor row;
    OLAPStatus res = row.init_scan_key(tablet_schema, scan_keys);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 34);
    ASSERT_EQ(row.get_variable_len(), 39);

    OlapTuple tuple1(scan_keys);
    res = row.from_tuple(tuple1);
    ASSERT_EQ(res, OLAP_SUCCESS);

    OlapTuple tuple2 = row.to_tuple();
    ASSERT_TRUE(strncmp(tuple2.get_value(0).c_str(), "0&char_exceed_length", 20));
    ASSERT_TRUE(strncmp(tuple2.get_value(1).c_str(), "0&varchar_exceed_length", 23)); 
}

TEST_F(TestRowCursor, SetMinAndMaxKey) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_init(&tablet_schema);

    RowCursor min_row;
    OLAPStatus res = min_row.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(min_row.get_fixed_len(), 126);

    res = min_row.build_min_key();
    ASSERT_EQ(res, OLAP_SUCCESS);
    for (size_t i = 0; i < tablet_schema.size(); ++i) {
        ASSERT_TRUE(min_row.is_min(i));
    }

    RowCursor max_row;
    res = max_row.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(max_row.get_fixed_len(), 126);
}

TEST_F(TestRowCursor, EqualAndCompare) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_cmp_and_aggregate(&tablet_schema);

    RowCursor left;
    OLAPStatus res = left.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(left.get_fixed_len(), 78);
    ASSERT_EQ(left.get_variable_len(), 20);

    Slice l_char("well");
    int32_t l_int = 10;
    left.set_field_content(0, reinterpret_cast<char*>(&l_char), _mem_pool.get());
    left.set_field_content(1, reinterpret_cast<char*>(&l_int), _mem_pool.get());

    // right row only has k2 in int type
    std::vector<uint32_t> col_ids;
    col_ids.push_back(1);

    RowCursor right_eq;
    res = right_eq.init(tablet_schema, col_ids);
    int32_t r_int_eq = 10;
    right_eq.set_field_content(1, reinterpret_cast<char*>(&r_int_eq), _mem_pool.get());
    ASSERT_TRUE(left.equal(right_eq));
    ASSERT_EQ(left.cmp(right_eq), 0);

    RowCursor right_lt;
    res = right_lt.init(tablet_schema, col_ids);
    int32_t r_int_lt = 11;
    right_lt.set_field_content(1, reinterpret_cast<char*>(&r_int_lt), _mem_pool.get());
    ASSERT_FALSE(left.equal(right_lt));
    ASSERT_LT(left.cmp(right_lt), 0);

    RowCursor right_gt;
    res = right_gt.init(tablet_schema, col_ids);
    int32_t r_int_gt = 9;
    right_gt.set_field_content(1, reinterpret_cast<char*>(&r_int_gt), _mem_pool.get());
    ASSERT_FALSE(left.equal(right_gt));
    ASSERT_GT(left.cmp(right_gt), 0);
}

TEST_F(TestRowCursor, IndexCmp) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_cmp_and_aggregate(&tablet_schema);

    RowCursor left;
    OLAPStatus res = left.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(left.get_fixed_len(), 78);
    ASSERT_EQ(left.get_variable_len(), 20);

    Slice l_char("well");
    int32_t l_int = 10;
    left.set_field_content(0, reinterpret_cast<char*>(&l_char), _mem_pool.get());
    left.set_field_content(1, reinterpret_cast<char*>(&l_int), _mem_pool.get());

    RowCursor right_eq;
    res = right_eq.init(tablet_schema);
    Slice r_char_eq("well");
    int32_t r_int_eq = 10;
    right_eq.set_field_content(0, reinterpret_cast<char*>(&r_char_eq), _mem_pool.get());
    right_eq.set_field_content(1, reinterpret_cast<char*>(&r_int_eq), _mem_pool.get());

    ASSERT_EQ(left.index_cmp(right_eq), 0);

    RowCursor right_lt;
    res = right_lt.init(tablet_schema);
    Slice r_char_lt("well");
    int32_t r_int_lt = 11;
    right_lt.set_field_content(0, reinterpret_cast<char*>(&r_char_lt), _mem_pool.get());
    right_lt.set_field_content(1, reinterpret_cast<char*>(&r_int_lt), _mem_pool.get());
    ASSERT_LT(left.index_cmp(right_lt), 0);

    RowCursor right_gt;
    res = right_gt.init(tablet_schema);
    Slice r_char_gt("good");
    int32_t r_int_gt = 10;
    right_gt.set_field_content(0, reinterpret_cast<char*>(&r_char_gt), _mem_pool.get());
    right_gt.set_field_content(1, reinterpret_cast<char*>(&r_int_gt), _mem_pool.get());
    ASSERT_GT(left.index_cmp(right_gt), 0);
}

TEST_F(TestRowCursor, FullKeyCmp) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_cmp_and_aggregate(&tablet_schema);

    RowCursor left;
    OLAPStatus res = left.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(left.get_fixed_len(), 78);
    ASSERT_EQ(left.get_variable_len(), 20);

    Slice l_char("well");
    int32_t l_int = 10;
    left.set_field_content(0, reinterpret_cast<char*>(&l_char), _mem_pool.get());
    left.set_field_content(1, reinterpret_cast<char*>(&l_int), _mem_pool.get());

    RowCursor right_eq;
    res = right_eq.init(tablet_schema);
    Slice r_char_eq("well");
    int32_t r_int_eq = 10;
    right_eq.set_field_content(0, reinterpret_cast<char*>(&r_char_eq), _mem_pool.get());
    right_eq.set_field_content(1, reinterpret_cast<char*>(&r_int_eq), _mem_pool.get());
    ASSERT_EQ(left.full_key_cmp(right_eq), 0);

    RowCursor right_lt;
    res = right_lt.init(tablet_schema);
    Slice r_char_lt("well");
    int32_t r_int_lt = 11;
    right_lt.set_field_content(0, reinterpret_cast<char*>(&r_char_lt), _mem_pool.get());
    right_lt.set_field_content(1, reinterpret_cast<char*>(&r_int_lt), _mem_pool.get());
    ASSERT_LT(left.full_key_cmp(right_lt), 0);

    RowCursor right_gt;
    res = right_gt.init(tablet_schema);
    Slice r_char_gt("good");
    int32_t r_int_gt = 10;
    right_gt.set_field_content(0, reinterpret_cast<char*>(&r_char_gt), _mem_pool.get());
    right_gt.set_field_content(1, reinterpret_cast<char*>(&r_int_gt), _mem_pool.get());
    ASSERT_GT(left.full_key_cmp(right_gt), 0);
}

TEST_F(TestRowCursor, AggregateWithoutNull) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_cmp_and_aggregate(&tablet_schema);

    RowCursor row;
    OLAPStatus res = row.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 78);
    ASSERT_EQ(row.get_variable_len(), 20);
    row.allocate_memory_for_string_type(tablet_schema);

    RowCursor left;
    res = left.init(tablet_schema);

    Slice l_char("well");
    int32_t l_int = 10;
    int128_t l_largeint = (int128_t)(1) << 100;
    double l_double = 8.8;
    decimal12_t l_decimal(11, 22);
    Slice l_varchar("beijing");
    left.set_field_content(0, reinterpret_cast<char*>(&l_char), _mem_pool.get());
    left.set_field_content(1, reinterpret_cast<char*>(&l_int), _mem_pool.get());
    left.set_field_content(2, reinterpret_cast<char*>(&l_largeint), _mem_pool.get());
    left.set_field_content(3, reinterpret_cast<char*>(&l_double), _mem_pool.get());
    left.set_field_content(4, reinterpret_cast<char*>(&l_decimal), _mem_pool.get());
    left.set_field_content(5, reinterpret_cast<char*>(&l_varchar), _mem_pool.get());

    res = row.agg_init(left);
    ASSERT_EQ(res, OLAP_SUCCESS);

    RowCursor right;
    res = right.init(tablet_schema);
    Slice r_char("well");
    int32_t r_int = 10;
    int128_t r_largeint = (int128_t)(1) << 100;
    double r_double = 5.5;
    decimal12_t r_decimal(22, 22);
    Slice r_varchar("shenzhen");
    right.set_field_content(0, reinterpret_cast<char*>(&r_char), _mem_pool.get());
    right.set_field_content(1, reinterpret_cast<char*>(&r_int), _mem_pool.get());
    right.set_field_content(2, reinterpret_cast<char*>(&r_largeint), _mem_pool.get());
    right.set_field_content(3, reinterpret_cast<char*>(&r_double), _mem_pool.get());
    right.set_field_content(4, reinterpret_cast<char*>(&r_decimal), _mem_pool.get());
    right.set_field_content(5, reinterpret_cast<char*>(&r_varchar), _mem_pool.get());

    row.aggregate(right);

    int128_t agg_value = *reinterpret_cast<int128_t*>(row.get_field_content_ptr(2));
    ASSERT_TRUE(agg_value == ((int128_t)(1) << 101));

    double agg_double = *reinterpret_cast<double*>(row.get_field_content_ptr(3));
    ASSERT_TRUE(agg_double == r_double);

    decimal12_t agg_decimal = *reinterpret_cast<decimal12_t*>(row.get_field_content_ptr(4));
    ASSERT_TRUE(agg_decimal == r_decimal);

    Slice* agg_varchar = reinterpret_cast<Slice*>(row.get_field_content_ptr(5));
    ASSERT_EQ(agg_varchar->compare(r_varchar), 0);
}

TEST_F(TestRowCursor, AggregateWithNull) {
    std::vector<FieldInfo> tablet_schema;
    set_tablet_schema_for_cmp_and_aggregate(&tablet_schema);

    RowCursor row;
    OLAPStatus res = row.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 78);
    ASSERT_EQ(row.get_variable_len(), 20);
    row.allocate_memory_for_string_type(tablet_schema);

    RowCursor left;
    res = left.init(tablet_schema);

    Slice l_char("well");
    int32_t l_int = 10;
    int128_t l_largeint = (int128_t)(1) << 100;
    Slice l_varchar("beijing");
    left.set_field_content(0, reinterpret_cast<char*>(&l_char), _mem_pool.get());
    left.set_field_content(1, reinterpret_cast<char*>(&l_int), _mem_pool.get());
    left.set_field_content(2, reinterpret_cast<char*>(&l_largeint), _mem_pool.get());
    left.set_null(3);
    left.set_null(4);
    left.set_field_content(5, reinterpret_cast<char*>(&l_varchar), _mem_pool.get());

    res = row.agg_init(left);
    ASSERT_EQ(res, OLAP_SUCCESS);

    RowCursor right;
    res = right.init(tablet_schema);
    Slice r_char("well");
    int32_t r_int = 10;
    int128_t r_largeint = (int128_t)(1) << 100;
    double r_double = 5.5;
    decimal12_t r_decimal(22, 22);
    right.set_field_content(0, reinterpret_cast<char*>(&r_char), _mem_pool.get());
    right.set_field_content(1, reinterpret_cast<char*>(&r_int), _mem_pool.get());
    right.set_field_content(2, reinterpret_cast<char*>(&r_largeint), _mem_pool.get());
    right.set_field_content(3, reinterpret_cast<char*>(&r_double), _mem_pool.get());
    right.set_field_content(4, reinterpret_cast<char*>(&r_decimal), _mem_pool.get());
    right.set_null(5);

    row.aggregate(right);

    int128_t agg_value = *reinterpret_cast<int128_t*>(row.get_field_content_ptr(2));
    ASSERT_TRUE(agg_value == ((int128_t)(1) << 101));

    bool is_null_double = left.is_null(3);
    ASSERT_TRUE(is_null_double);

    decimal12_t agg_decimal = *reinterpret_cast<decimal12_t*>(row.get_field_content_ptr(4));
    ASSERT_TRUE(agg_decimal == r_decimal);

    bool is_null_varchar = row.is_null(5);
    ASSERT_TRUE(is_null_varchar);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    return ret;
}
