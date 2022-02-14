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

#include "olap/row_cursor.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "olap/row.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/logging.h"

namespace doris {

void set_tablet_schema_for_init(TabletSchema* tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("column_1");
    column_1->set_type("TINYINT");
    column_1->set_is_key(true);
    column_1->set_is_nullable(true);
    column_1->set_length(1);
    column_1->set_index_length(1);

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("column_2");
    column_2->set_type("SMALLINT");
    column_2->set_is_key(true);
    column_2->set_is_nullable(true);
    column_2->set_length(2);
    column_2->set_index_length(2);
    column_2->set_default_value("0");

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("column_3");
    column_3->set_type("INT");
    column_3->set_is_key(true);
    column_3->set_is_nullable(true);
    column_3->set_length(4);
    column_3->set_index_length(4);

    ColumnPB* column_4 = tablet_schema_pb.add_column();
    column_4->set_unique_id(4);
    column_4->set_name("column_4");
    column_4->set_type("DATE");
    column_4->set_is_key(true);
    column_4->set_is_nullable(true);
    column_4->set_length(3);
    column_4->set_index_length(3);

    ColumnPB* column_5 = tablet_schema_pb.add_column();
    column_5->set_unique_id(5);
    column_5->set_name("column_5");
    column_5->set_type("DATETIME");
    column_5->set_is_key(true);
    column_5->set_is_nullable(true);
    column_5->set_length(8);
    column_5->set_index_length(8);

    ColumnPB* column_6 = tablet_schema_pb.add_column();
    column_6->set_unique_id(6);
    column_6->set_name("column_6");
    column_6->set_type("DECIMAL");
    column_6->set_is_key(true);
    column_6->set_is_nullable(true);
    column_6->set_length(12);
    column_6->set_index_length(12);
    column_6->set_frac(3);
    column_6->set_precision(6);

    ColumnPB* column_7 = tablet_schema_pb.add_column();
    column_7->set_unique_id(7);
    column_7->set_name("column_7");
    column_7->set_type("CHAR");
    column_7->set_is_key(true);
    column_7->set_is_nullable(true);
    column_7->set_length(4);
    column_7->set_index_length(4);
    column_7->set_default_value("char");

    ColumnPB* column_8 = tablet_schema_pb.add_column();
    column_8->set_unique_id(8);
    column_8->set_name("column_8");
    column_8->set_type("BIGINT");
    column_8->set_is_nullable(true);
    column_8->set_length(8);
    column_8->set_aggregation("SUM");
    column_8->set_is_key(false);

    ColumnPB* column_9 = tablet_schema_pb.add_column();
    column_9->set_unique_id(9);
    column_9->set_name("column_9");
    column_9->set_type("VARCHAR");
    column_9->set_is_nullable(true);
    column_9->set_length(16 + OLAP_VARCHAR_MAX_BYTES);
    column_9->set_aggregation("REPLACE");
    column_9->set_is_key(false);

    ColumnPB* column_10 = tablet_schema_pb.add_column();
    column_10->set_unique_id(10);
    column_10->set_name("column_10");
    column_10->set_type("LARGEINT");
    column_10->set_is_nullable(true);
    column_10->set_length(16);
    column_10->set_aggregation("MAX");
    column_10->set_is_key(false);

    ColumnPB* column_11 = tablet_schema_pb.add_column();
    column_11->set_unique_id(11);
    column_11->set_name("column_11");
    column_11->set_type("DECIMAL");
    column_11->set_is_nullable(true);
    column_11->set_length(12);
    column_11->set_aggregation("MIN");
    column_11->set_is_key(false);

    ColumnPB* column_12 = tablet_schema_pb.add_column();
    column_12->set_unique_id(12);
    column_12->set_name("column_12");
    column_12->set_type("HLL");
    column_12->set_is_nullable(true);
    column_12->set_length(HLL_COLUMN_DEFAULT_LEN);
    column_12->set_aggregation("HLL_UNION");
    column_12->set_is_key(false);

    tablet_schema->init_from_pb(tablet_schema_pb);
}

void set_tablet_schema_for_scan_key(TabletSchema* tablet_schema) {
    TabletSchemaPB tablet_schema_pb;

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("column_1");
    column_1->set_type("CHAR");
    column_1->set_is_key(true);
    column_1->set_is_nullable(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_default_value("char");

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("column_2");
    column_2->set_type("VARCHAR");
    column_2->set_is_key(true);
    column_2->set_is_nullable(true);
    column_2->set_length(16 + OLAP_VARCHAR_MAX_BYTES);
    column_2->set_index_length(20);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("column_3");
    column_3->set_type("LARGEINT");
    column_3->set_is_nullable(true);
    column_3->set_length(16);
    column_3->set_aggregation("MAX");
    column_3->set_is_key(false);

    ColumnPB* column_4 = tablet_schema_pb.add_column();
    column_4->set_unique_id(9);
    column_4->set_name("column_4");
    column_4->set_type("DECIMAL");
    column_4->set_is_nullable(true);
    column_4->set_length(12);
    column_4->set_aggregation("MIN");
    column_4->set_is_key(false);

    tablet_schema->init_from_pb(tablet_schema_pb);
}

void set_tablet_schema_for_cmp_and_aggregate(TabletSchema* tablet_schema) {
    TabletSchemaPB tablet_schema_pb;

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("column_1");
    column_1->set_type("CHAR");
    column_1->set_is_key(true);
    column_1->set_is_nullable(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_default_value("char");

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("column_2");
    column_2->set_type("INT");
    column_2->set_is_key(true);
    column_2->set_is_nullable(true);
    column_2->set_length(4);
    column_2->set_index_length(4);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("column_3");
    column_3->set_type("LARGEINT");
    column_3->set_is_nullable(true);
    column_3->set_length(16);
    column_3->set_aggregation("SUM");
    column_3->set_is_key(false);

    ColumnPB* column_4 = tablet_schema_pb.add_column();
    column_4->set_unique_id(9);
    column_4->set_name("column_4");
    column_4->set_type("DOUBLE");
    column_4->set_is_nullable(true);
    column_4->set_length(8);
    column_4->set_aggregation("MIN");
    column_4->set_is_key(false);

    ColumnPB* column_5 = tablet_schema_pb.add_column();
    column_5->set_unique_id(3);
    column_5->set_name("column_5");
    column_5->set_type("DECIMAL");
    column_5->set_is_nullable(true);
    column_5->set_length(12);
    column_5->set_aggregation("MAX");
    column_5->set_is_key(false);

    ColumnPB* column_6 = tablet_schema_pb.add_column();
    column_6->set_unique_id(9);
    column_6->set_name("column_6");
    column_6->set_type("VARCHAR");
    column_6->set_is_nullable(true);
    column_6->set_length(16 + OLAP_VARCHAR_MAX_BYTES);
    column_6->set_aggregation("REPLACE");
    column_6->set_is_key(false);

    tablet_schema->init_from_pb(tablet_schema_pb);
}

class TestRowCursor : public testing::Test {
public:
    TestRowCursor() {
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    virtual void SetUp() {}

    virtual void TearDown() {}

    std::shared_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
};

TEST_F(TestRowCursor, InitRowCursor) {
    TabletSchema tablet_schema;
    set_tablet_schema_for_init(&tablet_schema);
    RowCursor row;
    OLAPStatus res = row.init(tablet_schema);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 126);
    ASSERT_EQ(row.get_variable_len(), 20);
}

TEST_F(TestRowCursor, InitRowCursorWithColumnCount) {
    TabletSchema tablet_schema;
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
    TabletSchema tablet_schema;
    set_tablet_schema_for_init(&tablet_schema);

    std::vector<uint32_t> col_ids;
    for (size_t i = 0; i < tablet_schema.num_columns() / 2; ++i) {
        col_ids.push_back(i * 2);
    }

    RowCursor row;
    OLAPStatus res = row.init(tablet_schema, col_ids);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(row.get_fixed_len(), 63);
    ASSERT_EQ(row.get_variable_len(), 20);
}

TEST_F(TestRowCursor, InitRowCursorWithScanKey) {
    TabletSchema tablet_schema;
    set_tablet_schema_for_scan_key(&tablet_schema);

    std::vector<std::string> scan_keys;
    scan_keys.push_back("char_exceed_length");
    scan_keys.push_back("varchar_exceed_length");

    std::vector<uint32_t> columns {0, 1};
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(tablet_schema.columns(), columns);

    RowCursor row;
    OLAPStatus res = row.init_scan_key(tablet_schema, scan_keys, schema);
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

TEST_F(TestRowCursor, EqualAndCompare) {
    TabletSchema tablet_schema;
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
    col_ids.push_back(0);

    RowCursor right_eq;
    res = right_eq.init(tablet_schema, col_ids);
    Slice r_char_eq = ("well");
    right_eq.set_field_content(0, reinterpret_cast<char*>(&r_char_eq), _mem_pool.get());
    ASSERT_EQ(compare_row_key(left, right_eq), 0);

    RowCursor right_lt;
    res = right_lt.init(tablet_schema, col_ids);
    Slice r_char_lt = ("welm");
    right_lt.set_field_content(0, reinterpret_cast<char*>(&r_char_lt), _mem_pool.get());
    ASSERT_LT(compare_row_key(left, right_lt), 0);

    RowCursor right_gt;
    res = right_gt.init(tablet_schema, col_ids);
    Slice r_char_gt = ("welk");
    right_gt.set_field_content(0, reinterpret_cast<char*>(&r_char_gt), _mem_pool.get());
    ASSERT_GT(compare_row_key(left, right_gt), 0);
}

TEST_F(TestRowCursor, IndexCmp) {
    TabletSchema tablet_schema;
    set_tablet_schema_for_cmp_and_aggregate(&tablet_schema);

    RowCursor left;
    OLAPStatus res = left.init(tablet_schema, 2);
    ASSERT_EQ(res, OLAP_SUCCESS);
    ASSERT_EQ(left.get_fixed_len(), 22);
    ASSERT_EQ(left.get_variable_len(), 4);

    Slice l_char("well");
    int32_t l_int = 10;
    left.set_field_content(0, reinterpret_cast<char*>(&l_char), _mem_pool.get());
    left.set_field_content(1, reinterpret_cast<char*>(&l_int), _mem_pool.get());

    RowCursor right_eq;
    res = right_eq.init(tablet_schema, 2);
    Slice r_char_eq("well");
    int32_t r_int_eq = 10;
    right_eq.set_field_content(0, reinterpret_cast<char*>(&r_char_eq), _mem_pool.get());
    right_eq.set_field_content(1, reinterpret_cast<char*>(&r_int_eq), _mem_pool.get());

    ASSERT_EQ(index_compare_row(left, right_eq), 0);

    RowCursor right_lt;
    res = right_lt.init(tablet_schema, 2);
    Slice r_char_lt("well");
    int32_t r_int_lt = 11;
    right_lt.set_field_content(0, reinterpret_cast<char*>(&r_char_lt), _mem_pool.get());
    right_lt.set_field_content(1, reinterpret_cast<char*>(&r_int_lt), _mem_pool.get());
    ASSERT_LT(index_compare_row(left, right_lt), 0);

    RowCursor right_gt;
    res = right_gt.init(tablet_schema, 2);
    Slice r_char_gt("good");
    int32_t r_int_gt = 10;
    right_gt.set_field_content(0, reinterpret_cast<char*>(&r_char_gt), _mem_pool.get());
    right_gt.set_field_content(1, reinterpret_cast<char*>(&r_int_gt), _mem_pool.get());
    ASSERT_GT(index_compare_row(left, right_gt), 0);
}

TEST_F(TestRowCursor, FullKeyCmp) {
    TabletSchema tablet_schema;
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
    ASSERT_EQ(compare_row(left, right_eq), 0);

    RowCursor right_lt;
    res = right_lt.init(tablet_schema);
    Slice r_char_lt("well");
    int32_t r_int_lt = 11;
    right_lt.set_field_content(0, reinterpret_cast<char*>(&r_char_lt), _mem_pool.get());
    right_lt.set_field_content(1, reinterpret_cast<char*>(&r_int_lt), _mem_pool.get());
    ASSERT_LT(compare_row(left, right_lt), 0);

    RowCursor right_gt;
    res = right_gt.init(tablet_schema);
    Slice r_char_gt("good");
    int32_t r_int_gt = 10;
    right_gt.set_field_content(0, reinterpret_cast<char*>(&r_char_gt), _mem_pool.get());
    right_gt.set_field_content(1, reinterpret_cast<char*>(&r_int_gt), _mem_pool.get());
    ASSERT_GT(compare_row(left, right_gt), 0);
}

TEST_F(TestRowCursor, AggregateWithoutNull) {
    TabletSchema tablet_schema;
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
    decimal12_t l_decimal = {11, 22};
    Slice l_varchar("beijing");
    left.set_field_content(0, reinterpret_cast<char*>(&l_char), _mem_pool.get());
    left.set_field_content(1, reinterpret_cast<char*>(&l_int), _mem_pool.get());
    left.set_field_content(2, reinterpret_cast<char*>(&l_largeint), _mem_pool.get());
    left.set_field_content(3, reinterpret_cast<char*>(&l_double), _mem_pool.get());
    left.set_field_content(4, reinterpret_cast<char*>(&l_decimal), _mem_pool.get());
    left.set_field_content(5, reinterpret_cast<char*>(&l_varchar), _mem_pool.get());

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    ObjectPool agg_object_pool;
    init_row_with_others(&row, left, mem_pool.get(), &agg_object_pool);

    RowCursor right;
    res = right.init(tablet_schema);
    Slice r_char("well");
    int32_t r_int = 10;
    int128_t r_largeint = (int128_t)(1) << 100;
    double r_double = 5.5;
    decimal12_t r_decimal = {22, 22};
    Slice r_varchar("shenzhen");
    right.set_field_content(0, reinterpret_cast<char*>(&r_char), _mem_pool.get());
    right.set_field_content(1, reinterpret_cast<char*>(&r_int), _mem_pool.get());
    right.set_field_content(2, reinterpret_cast<char*>(&r_largeint), _mem_pool.get());
    right.set_field_content(3, reinterpret_cast<char*>(&r_double), _mem_pool.get());
    right.set_field_content(4, reinterpret_cast<char*>(&r_decimal), _mem_pool.get());
    right.set_field_content(5, reinterpret_cast<char*>(&r_varchar), _mem_pool.get());

    agg_update_row(&row, right, nullptr);

    int128_t agg_value = 0;
    memcpy(&agg_value, row.cell_ptr(2), 16);
    ASSERT_TRUE(agg_value == ((int128_t)(1) << 101));

    double agg_double = *reinterpret_cast<double*>(row.cell_ptr(3));
    ASSERT_TRUE(agg_double == r_double);

    decimal12_t agg_decimal = *reinterpret_cast<decimal12_t*>(row.cell_ptr(4));
    ASSERT_TRUE(agg_decimal == r_decimal);

    Slice* agg_varchar = reinterpret_cast<Slice*>(row.cell_ptr(5));
    ASSERT_EQ(agg_varchar->compare(r_varchar), 0);
}

TEST_F(TestRowCursor, AggregateWithNull) {
    TabletSchema tablet_schema;
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

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    ObjectPool agg_object_pool;
    init_row_with_others(&row, left, mem_pool.get(), &agg_object_pool);

    RowCursor right;
    res = right.init(tablet_schema);
    Slice r_char("well");
    int32_t r_int = 10;
    int128_t r_largeint = (int128_t)(1) << 100;
    double r_double = 5.5;
    decimal12_t r_decimal = {22, 22};
    right.set_field_content(0, reinterpret_cast<char*>(&r_char), _mem_pool.get());
    right.set_field_content(1, reinterpret_cast<char*>(&r_int), _mem_pool.get());
    right.set_field_content(2, reinterpret_cast<char*>(&r_largeint), _mem_pool.get());
    right.set_field_content(3, reinterpret_cast<char*>(&r_double), _mem_pool.get());
    right.set_field_content(4, reinterpret_cast<char*>(&r_decimal), _mem_pool.get());
    right.set_null(5);

    agg_update_row(&row, right, nullptr);

    int128_t agg_value = 0;
    memcpy(&agg_value, row.cell_ptr(2), 16);
    ASSERT_TRUE(agg_value == ((int128_t)(1) << 101));

    bool is_null_double = left.is_null(3);
    ASSERT_TRUE(is_null_double);

    decimal12_t agg_decimal = *reinterpret_cast<decimal12_t*>(row.cell_ptr(4));
    ASSERT_TRUE(agg_decimal == r_decimal);

    bool is_null_varchar = row.is_null(5);
    ASSERT_TRUE(is_null_varchar);
}

} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
