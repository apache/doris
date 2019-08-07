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
#include <sstream>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "olap/row_block.h"
#include "olap/tablet.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/logging.h"

using std::vector;
using std::string;
using std::endl;
using std::stringstream;

namespace doris {

class TestRowBlock : public testing::Test {
public:
    TestRowBlock() {}
    void SetUp() {
    }
    void TearDown() {
    }
    void SetRows(RowBlock& row_block) {
    }
};

void init_tablet_schema(TabletSchema* tablet_schema) {
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
            column_3->set_aggregation("NONE");
        }
    }
    tablet_schema->init_from_pb(tablet_schema_pb);
}

TEST_F(TestRowBlock, init) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    {
        // has nullbyte
        RowBlock block(&tablet_schema);
        RowBlockInfo block_info;
        block_info.row_num = 1024;
        block_info.null_supported = true;
        auto res = block.init(block_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(9 + 17 + 17, block._mem_row_bytes);
    }
    {
        // has nullbyte
        RowBlock block(&tablet_schema);
        RowBlockInfo block_info;
        block_info.row_num = 1024;
        block_info.null_supported = false;
        auto res = block.init(block_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(9 + 17 + 17, block._mem_row_bytes);
    }
    {
        RowBlock block(&tablet_schema);
        RowBlockInfo block_info;
        block_info.row_num = 1024;
        block_info.null_supported = true;
        block_info.column_ids.push_back(1);
        auto res = block.init(block_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        // null + sizeof(Slice)
        ASSERT_EQ(17, block._mem_row_bytes);
        ASSERT_EQ(std::numeric_limits<size_t>::max(), block._field_offset_in_memory[0]);
        ASSERT_EQ(0, block._field_offset_in_memory[1]);
        ASSERT_EQ(std::numeric_limits<size_t>::max(), block._field_offset_in_memory[2]);
    }
}

TEST_F(TestRowBlock, write_and_read) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    auto res = block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    RowCursor row;
    row.init(tablet_schema);
    for (int i = 0; i < 5; ++i) {
        block.get_row(i, &row);

        // bigint
        {
            int64_t val = i; 
            row.set_not_null(0);
            row.set_field_content(0, (const char*)&val, block.mem_pool());
        }
        // char
        {
            char buf[10];
            memset(buf, 'a' + i, 10);
            Slice val(buf, 10);
            row.set_not_null(1);
            row.set_field_content(1, (const char*)&val, block.mem_pool());
        }
        // varchar
        {
            char buf[10];
            memset(buf, '0' + i, 10);
            Slice val(buf, 10);
            row.set_not_null(2);
            row.set_field_content(2, (const char*)&val, block.mem_pool());
        }
    }
    block.finalize(5);
    ASSERT_EQ(5, block.row_num());
}

TEST_F(TestRowBlock, write_and_read_without_nullbyte) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = false;
    auto res = block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    RowCursor row;
    row.init(tablet_schema);
    for (int i = 0; i < 5; ++i) {
        block.get_row(i, &row);

        // bigint
        {
            int64_t val = i; 
            row.set_not_null(0);
            row.set_field_content(0, (const char*)&val, block.mem_pool());
        }
        // char
        {
            char buf[10];
            memset(buf, 'a' + i, 10);
            Slice val(buf, 10);
            row.set_not_null(1);
            row.set_field_content(1, (const char*)&val, block.mem_pool());
        }
        // varchar
        {
            char buf[10];
            memset(buf, '0' + i, 10);
            Slice val(buf, 10);
            row.set_not_null(2);
            row.set_field_content(2, (const char*)&val, block.mem_pool());
        }
    }
    block.finalize(5);
    ASSERT_EQ(5, block.row_num());
}

TEST_F(TestRowBlock, compress_failed) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    auto res = block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    RowCursor row;
    row.init(tablet_schema);
    for (int i = 0; i < 5; ++i) {
        block.get_row(i, &row);

        // bigint
        {
            int64_t val = i; 
            row.set_field_content(0, (const char*)&val, block.mem_pool());
        }
        // char
        {
            char buf[10];
            memset(buf, 'a' + i, 10);
            Slice val(buf, 10);
            row.set_field_content(1, (const char*)&val, block.mem_pool());
        }
        // varchar
        {
            char buf[10];
            memset(buf, '0' + i, 10);
            Slice val(buf, 10);
            row.set_field_content(2, (const char*)&val, block.mem_pool());
        }
    }
    block.finalize(5);
    ASSERT_EQ(5, block.row_num());
}

TEST_F(TestRowBlock, decompress_failed) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    auto res = block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    RowCursor row;
    row.init(tablet_schema);
    for (int i = 0; i < 5; ++i) {
        block.get_row(i, &row);

        // bigint
        {
            int64_t val = i; 
            row.set_field_content(0, (const char*)&val, block.mem_pool());
        }
        // char
        {
            char buf[10];
            memset(buf, 'a' + i, 10);
            Slice val(buf, 10);
            row.set_field_content(1, (const char*)&val, block.mem_pool());
        }
        // varchar
        {
            char buf[10];
            memset(buf, '0' + i, 10);
            Slice val(buf, 10);
            row.set_field_content(2, (const char*)&val, block.mem_pool());
        }
    }
    block.finalize(5);
    ASSERT_EQ(5, block.row_num());
}

TEST_F(TestRowBlock, find_row) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    auto res = block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    RowCursor row;
    row.init(tablet_schema);
    for (int i = 0; i < 5; ++i) {
        block.get_row(i, &row);

        // bigint
        {
            int64_t val = i; 
            row.set_not_null(0);
            row.set_field_content(0, (const char*)&val, block.mem_pool());
        }
        // char
        {
            char buf[10];
            memset(buf, 'a' + i, 10);
            Slice val(buf, 10);
            row.set_not_null(1);
            row.set_field_content(1, (const char*)&val, block.mem_pool());
        }
        // varchar
        {
            char buf[10];
            memset(buf, '0' + i, 10);
            Slice val(buf, 10);
            row.set_not_null(2);
            row.set_field_content(2, (const char*)&val, block.mem_pool());
        }
    }
    block.finalize(5);
    ASSERT_EQ(5, block.row_num());
    
    {
        RowCursor find_row;
        find_row.init(tablet_schema);
        for (int i = 0; i < 5; ++i) {
            // bigint
            {
                int64_t val = i; 
                find_row.set_not_null(0);
                find_row.set_field_content(0, (const char*)&val, block.mem_pool());
            }
            // char
            {
                char buf[10];
                memset(buf, 'a' + i, 10);
                Slice val(buf, 10);
                find_row.set_not_null(1);
                find_row.set_field_content(1, (const char*)&val, block.mem_pool());
            }
            // varchar
            {
                char buf[10];
                memset(buf, '0' + i, 10);
                Slice val(buf, 10);
                find_row.set_not_null(2);
                find_row.set_field_content(2, (const char*)&val, block.mem_pool());
            }
            uint32_t row_index;
            res = block.find_row(find_row, false, &row_index);
            ASSERT_EQ(OLAP_SUCCESS, res);
            ASSERT_EQ(i, row_index);

            res = block.find_row(find_row, true, &row_index);
            ASSERT_EQ(OLAP_SUCCESS, res);
            ASSERT_EQ(i + 1, row_index);
        }
        {
            // bigint
            {
                int64_t val = 1; 
                find_row.set_field_content(0, (const char*)&val, block.mem_pool());
            }
            // char
            {
                char buf[10];
                memset(buf, 'c', 9);
                Slice val(buf, 9);
                find_row.set_field_content(1, (const char*)&val, block.mem_pool());
            }
            // varchar
            {
                char buf[10];
                memset(buf, '0', 10);
                Slice val(buf, 10);
                find_row.set_field_content(2, (const char*)&val, block.mem_pool());
            }
            uint32_t row_index;
            res = block.find_row(find_row, true, &row_index);
            ASSERT_EQ(OLAP_SUCCESS, res);
            ASSERT_EQ(2, row_index);
        }
        {
            // bigint
            {
                int64_t val = -1;
                find_row.set_field_content(0, (const char*)&val, block.mem_pool());
            }
            // char
            {
                char buf[10];
                memset(buf, 'c', 9);
                Slice val(buf, 9);
                find_row.set_field_content(1, (const char*)&val, block.mem_pool());
            }
            // varchar
            {
                char buf[10];
                memset(buf, '0', 10);
                Slice val(buf, 10);
                find_row.set_field_content(2, (const char*)&val, block.mem_pool());
            }
            uint32_t row_index;
            res = block.find_row(find_row, true, &row_index);
            ASSERT_EQ(OLAP_SUCCESS, res);
            ASSERT_EQ(0, row_index);
        }
    }
}

TEST_F(TestRowBlock, clear) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    auto res = block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    block.finalize(5);
    ASSERT_EQ(5, block.row_num());
    ASSERT_EQ(1024, block.capacity());
    block.clear();
    ASSERT_EQ(1024, block.row_num());
}

TEST_F(TestRowBlock, pos_limit) {
    TabletSchema tablet_schema;
    init_tablet_schema(&tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    auto res = block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // assert init value
    ASSERT_EQ(0, block.pos());
    ASSERT_EQ(0, block.limit());
    ASSERT_FALSE(block.has_remaining());
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, block.block_status());

    block.set_limit(100);
    ASSERT_EQ(100, block.limit());
    ASSERT_TRUE(block.has_remaining());
    ASSERT_EQ(100, block.remaining());

    block.set_pos(2);
    ASSERT_TRUE(block.has_remaining());
    ASSERT_EQ(98, block.remaining());

    block.pos_inc();
    ASSERT_TRUE(block.has_remaining());
    ASSERT_EQ(97, block.remaining());

    block.set_block_status(DEL_SATISFIED);
    ASSERT_EQ(DEL_SATISFIED, block.block_status());
}
}

// @brief Test Stub
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

