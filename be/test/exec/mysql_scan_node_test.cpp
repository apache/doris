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

#include "exec/mysql_scan_node.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "exec/text_converter.inline.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "schema_scan_node.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

using std::vector;

namespace doris {

// mock
class MysqlScanNodeTest : public testing::Test {
public:
    MysqlScanNodeTest() : _runtim_state("test") {
        TDescriptorTable t_desc_table;

        // table descriptors
        TTableDescriptor t_table_desc;

        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::MYSQL_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_table_desc.mysqlTable.tableName = "table";
        t_table_desc.mysqlTable.mysqlHost = "host";
        t_table_desc.mysqlTable.mysqlPort = "port";
        t_table_desc.mysqlTable.mysqlUser = "user";
        t_table_desc.mysqlTable.mysqlPasswd = "passwd";
        t_table_desc.tableName = "table";
        t_table_desc.dbName = "db";
        t_table_desc.__isset.mysqlTable = true;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // dummy
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(to_thrift(TYPE_INT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(false);
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int);
        }
        // id
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(to_thrift(TYPE_INT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int);
        }
        ++i;
        // model
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(to_thrift(TYPE_STRING));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(0);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;
        // price
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(to_thrift(TYPE_STRING));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }
        ++i;
        // grade
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(to_thrift(TYPE_STRING));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(2);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(StringValue);
        }

        t_desc_table.__isset.slotDescriptors = true;
        // TTupleDescriptor
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

        _runtim_state.set_desc_tbl(_desc_tbl);

        // Node Id
        _tnode.node_id = 0;
        _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
        _tnode.num_children = 0;
        _tnode.limit = -1;
        _tnode.row_tuples.push_back(0);
        _tnode.nullable_tuples.push_back(false);
        _tnode.mysql_scan_node.tuple_id = 0;
        _tnode.mysql_scan_node.table_name = "dim_lbs_device";
        _tnode.mysql_scan_node.columns.push_back("*");
        _tnode.mysql_scan_node.filters.push_back("id = 1");
        _tnode.__isset.mysql_scan_node = true;
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtim_state;
};

TEST_F(MysqlScanNodeTest, normal_use) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtim_state);
    ASSERT_TRUE(status.ok());
    std::vector<TScanRangeParams> scan_ranges;
    status = scan_node.set_scan_ranges(scan_ranges);
    ASSERT_TRUE(status.ok());
    std::stringstream out;
    scan_node.debug_string(1, &out);
    LOG(WARNING) << out.str();

    status = scan_node.open(&_runtim_state);
    ASSERT_TRUE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, 100);
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(&_runtim_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());

        if (!eos) {
            for (int i = 0; i < row_batch.num_rows(); ++i) {
                TupleRow* row = row_batch.get_row(i);
                LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);
            }
        }
    }

    status = scan_node.close(&_runtim_state);
    ASSERT_TRUE(status.ok());
}
TEST_F(MysqlScanNodeTest, Prepare_fail_1) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node._tuple_id = 1;
    Status status = scan_node.prepare(&_runtim_state);
    ASSERT_FALSE(status.ok());
}
TEST_F(MysqlScanNodeTest, Prepare_fail_2) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    TableDescriptor* old = _desc_tbl->_tuple_desc_map[(TupleId)0]->_table_desc;
    _desc_tbl->_tuple_desc_map[(TupleId)0]->_table_desc = nullptr;
    Status status = scan_node.prepare(&_runtim_state);
    ASSERT_FALSE(status.ok());
    _desc_tbl->_tuple_desc_map[(TupleId)0]->_table_desc = old;
}
TEST_F(MysqlScanNodeTest, open_fail_1) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtim_state);
    ASSERT_TRUE(status.ok());
    scan_node._table_name = "no_such_table";
    status = scan_node.open(&_runtim_state);
    ASSERT_FALSE(status.ok());
}
TEST_F(MysqlScanNodeTest, open_fail_3) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtim_state);
    ASSERT_TRUE(status.ok());
    scan_node._columns.clear();
    scan_node._columns.push_back("id");
    status = scan_node.open(&_runtim_state);
    ASSERT_FALSE(status.ok());
}
TEST_F(MysqlScanNodeTest, open_fail_2) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtim_state);
    ASSERT_TRUE(status.ok());
    scan_node._my_param.host = "";
    status = scan_node.open(&_runtim_state);
    ASSERT_FALSE(status.ok());
}
TEST_F(MysqlScanNodeTest, invalid_input) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(nullptr);
    ASSERT_FALSE(status.ok());
    status = scan_node.prepare(&_runtim_state);
    ASSERT_TRUE(status.ok());
    status = scan_node.prepare(&_runtim_state);
    ASSERT_TRUE(status.ok());
    status = scan_node.open(nullptr);
    ASSERT_FALSE(status.ok());
    status = scan_node.open(&_runtim_state);
    ASSERT_TRUE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, 100);
    bool eos = false;
    status = scan_node.get_next(nullptr, &row_batch, &eos);
    ASSERT_FALSE(status.ok());

    while (!eos) {
        status = scan_node.get_next(&_runtim_state, &row_batch, &eos);
        ASSERT_TRUE(status.ok());

        for (int i = 0; i < row_batch.num_rows(); ++i) {
            TupleRow* row = row_batch.get_row(i);
            LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);
        }
    }
}
TEST_F(MysqlScanNodeTest, no_init) {
    MysqlScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.open(&_runtim_state);
    ASSERT_FALSE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, 100);
    bool eos = false;
    status = scan_node.get_next(&_runtim_state, &row_batch, &eos);
    ASSERT_FALSE(status.ok());
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
