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
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "util/types.h"

namespace doris {

void set_tablet_schema_for_init(TabletSchemaSPtr tablet_schema) {
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
    column_6->set_unique_id(5);
    column_6->set_name("column_6");
    column_6->set_type("DATEV2");
    column_6->set_is_key(true);
    column_6->set_is_nullable(true);
    column_6->set_length(4);
    column_6->set_index_length(4);

    ColumnPB* column_7 = tablet_schema_pb.add_column();
    column_7->set_unique_id(6);
    column_7->set_name("column_7");
    column_7->set_type("DECIMAL");
    column_7->set_is_key(true);
    column_7->set_is_nullable(true);
    column_7->set_length(12);
    column_7->set_index_length(12);
    column_7->set_frac(3);
    column_7->set_precision(6);

    ColumnPB* column_8 = tablet_schema_pb.add_column();
    column_8->set_unique_id(7);
    column_8->set_name("column_8");
    column_8->set_type("CHAR");
    column_8->set_is_key(true);
    column_8->set_is_nullable(true);
    column_8->set_length(4);
    column_8->set_index_length(4);
    column_8->set_default_value("char");

    ColumnPB* column_9 = tablet_schema_pb.add_column();
    column_9->set_unique_id(8);
    column_9->set_name("column_9");
    column_9->set_type("BIGINT");
    column_9->set_is_nullable(true);
    column_9->set_length(8);
    column_9->set_aggregation("SUM");
    column_9->set_is_key(false);

    ColumnPB* column_10 = tablet_schema_pb.add_column();
    column_10->set_unique_id(9);
    column_10->set_name("column_10");
    column_10->set_type("VARCHAR");
    column_10->set_is_nullable(true);
    column_10->set_length(16 + OLAP_VARCHAR_MAX_BYTES);
    column_10->set_aggregation("REPLACE");
    column_10->set_is_key(false);

    ColumnPB* column_11 = tablet_schema_pb.add_column();
    column_11->set_unique_id(10);
    column_11->set_name("column_11");
    column_11->set_type("LARGEINT");
    column_11->set_is_nullable(true);
    column_11->set_length(16);
    column_11->set_aggregation("MAX");
    column_11->set_is_key(false);

    ColumnPB* column_12 = tablet_schema_pb.add_column();
    column_12->set_unique_id(11);
    column_12->set_name("column_12");
    column_12->set_type("DECIMAL");
    column_12->set_is_nullable(true);
    column_12->set_length(12);
    column_12->set_aggregation("MIN");
    column_12->set_is_key(false);

    ColumnPB* column_13 = tablet_schema_pb.add_column();
    column_13->set_unique_id(12);
    column_13->set_name("column_13");
    column_13->set_type("HLL");
    column_13->set_is_nullable(true);
    column_13->set_length(HLL_COLUMN_DEFAULT_LEN);
    column_13->set_aggregation("HLL_UNION");
    column_13->set_is_key(false);

    tablet_schema->init_from_pb(tablet_schema_pb);
}

void set_tablet_schema_for_scan_key(TabletSchemaSPtr tablet_schema) {
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

void set_tablet_schema_for_cmp_and_aggregate(TabletSchemaSPtr tablet_schema) {
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
    TestRowCursor() { _arena.reset(new vectorized::Arena()); }

    virtual void SetUp() {}

    virtual void TearDown() {}

    std::unique_ptr<vectorized::Arena> _arena;
};

TEST_F(TestRowCursor, InitRowCursor) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    set_tablet_schema_for_init(tablet_schema);
    RowCursor row;
    Status res = row.init(tablet_schema);
    EXPECT_EQ(res, Status::OK());
    EXPECT_EQ(row.get_fixed_len(), 131);
    EXPECT_EQ(row.get_variable_len(), 20);
}

TEST_F(TestRowCursor, InitRowCursorWithColumnCount) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    set_tablet_schema_for_init(tablet_schema);
    RowCursor row;
    Status res = row.init(tablet_schema, 5);
    EXPECT_EQ(res, Status::OK());
    EXPECT_EQ(row.get_fixed_len(), 23);
    EXPECT_EQ(row.get_variable_len(), 0);
}

TEST_F(TestRowCursor, InitRowCursorWithColIds) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    set_tablet_schema_for_init(tablet_schema);

    std::vector<uint32_t> col_ids;
    for (size_t i = 0; i < tablet_schema->num_columns() / 2; ++i) {
        col_ids.push_back(i * 2);
    }

    RowCursor row;
    Status res = row.init(tablet_schema, col_ids);
    EXPECT_EQ(res, Status::OK());
    EXPECT_EQ(row.get_fixed_len(), 55);
    EXPECT_EQ(row.get_variable_len(), 0);
}

TEST_F(TestRowCursor, InitRowCursorWithScanKey) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    set_tablet_schema_for_scan_key(tablet_schema);

    std::vector<std::string> scan_keys;
    scan_keys.push_back("char_exceed_length");
    scan_keys.push_back("varchar_exceed_length");

    std::vector<uint32_t> columns {0, 1};
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(tablet_schema->columns(), columns);

    RowCursor row;
    Status res = row.init_scan_key(tablet_schema, scan_keys, schema);
    EXPECT_EQ(res, Status::OK());
    EXPECT_EQ(row.get_fixed_len(), 34);
    EXPECT_EQ(row.get_variable_len(), 39);

    OlapTuple tuple1(scan_keys);
    res = row.from_tuple(tuple1);
    EXPECT_EQ(res, Status::OK());

    OlapTuple tuple2 = row.to_tuple();
    EXPECT_TRUE(strncmp(tuple2.get_value(0).c_str(), "0&char_exceed_length", 20));
    EXPECT_TRUE(strncmp(tuple2.get_value(1).c_str(), "0&varchar_exceed_length", 23));
}

} // namespace doris
