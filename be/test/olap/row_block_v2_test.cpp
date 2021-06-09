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

#include "olap/row_block2.h"

namespace doris {

class TestRowBlockV2 : public testing::Test {
public:
    TestRowBlockV2() {}
    void SetUp() {}
    void TearDown() {}
};

void init_tablet_schema(TabletSchema* tablet_schema, bool is_nullable) {
    TabletSchemaPB tablet_schema_pb;
    {
        // k1: bigint
        {
            ColumnPB* column_1 = tablet_schema_pb.add_column();
            column_1->set_unique_id(1);
            column_1->set_name("k1");
            column_1->set_type("BIGINT");
            column_1->set_is_key(true);
            column_1->set_length(8);
            column_1->set_is_nullable(is_nullable);
            column_1->set_aggregation("NONE");
        }
        // k2: char
        {
            ColumnPB* column_2 = tablet_schema_pb.add_column();
            column_2->set_unique_id(2);
            column_2->set_name("k2");
            column_2->set_type("CHAR");
            column_2->set_is_key(true);
            column_2->set_length(10);
            column_2->set_is_nullable(is_nullable);
            column_2->set_aggregation("NONE");
        }
        // k3: varchar
        {
            ColumnPB* column_3 = tablet_schema_pb.add_column();
            column_3->set_unique_id(3);
            column_3->set_name("k3");
            column_3->set_type("VARCHAR");
            column_3->set_is_key(true);
            column_3->set_length(20);
            column_3->set_is_nullable(is_nullable);
            column_3->set_aggregation("NONE");
        }
        // v1: int
        {
            ColumnPB* column_4 = tablet_schema_pb.add_column();
            column_4->set_unique_id(3);
            column_4->set_name("v1");
            column_4->set_type("INT");
            column_4->set_is_key(false);
            column_4->set_length(4);
            column_4->set_is_nullable(false);
            column_4->set_aggregation("SUM");
        }
    }
    tablet_schema->init_from_pb(tablet_schema_pb);
}

TEST_F(TestRowBlockV2, test_convert) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema, true);
    Schema schema(tablet_schema);
    RowBlockV2 input_block(schema, 1024);
    RowBlock output_block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    output_block.init(block_info);
    auto tracker = std::make_shared<MemTracker>();
    MemPool pool(tracker.get());
    for (int i = 0; i < input_block.capacity(); ++i) {
        RowBlockRow row = input_block.row(i);

        // column_1
        row.set_is_null(0, false);
        uint8_t* cell0 = row.mutable_cell_ptr(0);
        (*(uint64_t*)cell0) = i;

        // column_2
        uint8_t* buf = pool.allocate(10);
        memset(buf, 'a' + (i % 10), 10);
        Slice str1(buf, 10);
        row.set_is_null(1, false);
        uint8_t* cell1 = row.mutable_cell_ptr(1);
        (*(Slice*)cell1) = str1;

        // column_3
        uint8_t* buf2 = pool.allocate(10);
        memset(buf2, 'A' + (i % 10), 10);
        Slice str2(buf2, 10);
        row.set_is_null(2, false);
        uint8_t* cell3 = row.mutable_cell_ptr(2);
        (*(Slice*)cell3) = str2;

        // column_4
        row.set_is_null(3, false);
        uint8_t* cell4 = row.mutable_cell_ptr(3);
        (*(uint32_t*)cell4) = 10 * i;
    }

    input_block.set_selected_size(5);
    uint16_t* select_vector = input_block.selection_vector();
    for (int i = 0; i < input_block.selected_size(); ++i) {
        // 10, 20, 30, 40, 50
        select_vector[i] = (i + 1) * 10;
    }

    RowCursor helper;
    helper.init(tablet_schema);
    auto st = input_block.convert_to_row_block(&helper, &output_block);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(5, output_block.limit());
    for (int i = 0; i < 5; ++i) {
        char* field1 = output_block.field_ptr(i, 0);
        char* field2 = output_block.field_ptr(i, 1);
        char* field3 = output_block.field_ptr(i, 2);
        char* field4 = output_block.field_ptr(i, 3);
        // test null bit
        ASSERT_FALSE(*reinterpret_cast<bool*>(field1));
        ASSERT_FALSE(*reinterpret_cast<bool*>(field2));
        ASSERT_FALSE(*reinterpret_cast<bool*>(field3));
        ASSERT_FALSE(*reinterpret_cast<bool*>(field4));

        uint64_t k1 = *reinterpret_cast<uint64_t*>(field1 + 1);
        ASSERT_EQ((i + 1) * 10, k1);

        Slice k2 = *reinterpret_cast<Slice*>(field2 + 1);
        char buf[10];
        memset(buf, 'a' + ((i + 1) * 10) % 10, 10);
        Slice k2_v(buf, 10);
        ASSERT_EQ(k2_v, k2);

        Slice k3 = *reinterpret_cast<Slice*>(field3 + 1);
        char buf2[10];
        memset(buf2, 'A' + ((i + 1) * 10) % 10, 10);
        Slice k3_v(buf2, 10);
        ASSERT_EQ(k3_v, k3);

        uint32_t v1 = *reinterpret_cast<uint32_t*>(field4 + 1);
        ASSERT_EQ((i + 1) * 10 * 10, v1);
    }
}

} // namespace doris

// @brief Test Stub
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
