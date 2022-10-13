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

#include "exec/schema_scan_node.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "exec/text_converter.hpp"
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

namespace doris {

// mock
class SchemaScanNodeTest : public testing::Test {
public:
    SchemaScanNodeTest() : runtime_state("test") {
        TDescriptorTable t_desc_table;

        // table descriptors
        TTableDescriptor t_table_desc;

        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::SCHEMA_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_table_desc.schemaTable.tableType = TSchemaTableType::SCH_AUTHORS;
        t_table_desc.tableName = "test_table";
        t_table_desc.dbName = "test_db";
        t_table_desc.__isset.schemaTable = true;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 0;

        for (int i = 0; i < 3; ++i) {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(to_thrift(TYPE_STRING));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
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
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

        runtime_state.set_desc_tbl(_desc_tbl);

        // Node Id
        _tnode.node_id = 0;
        _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
        _tnode.num_children = 0;
        _tnode.limit = -1;
        _tnode.row_tuples.push_back(0);
        _tnode.nullable_tuples.push_back(false);
        _tnode.schema_scan_node.table_name = "test_table";
        _tnode.schema_scan_node.tuple_id = 0;
        _tnode.__isset.schema_scan_node = true;
    }

    virtual ~SchemaScanNodeTest() {}

    virtual void SetUp() {}
    virtual void TearDown() {}

private:
    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState runtime_state;
};

TEST_F(SchemaScanNodeTest, normal_use) {
    SchemaScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&runtime_state);
    EXPECT_TRUE(status.ok());
    status = scan_node.prepare(&runtime_state);
    EXPECT_TRUE(status.ok());
    std::vector<TScanRangeParams> scan_ranges;
    status = scan_node.set_scan_ranges(scan_ranges);
    EXPECT_TRUE(status.ok());
    std::stringstream out;
    scan_node.debug_string(1, &out);
    LOG(WARNING) << out.str();

    status = scan_node.open(&runtime_state);
    EXPECT_TRUE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, 100);
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(&runtime_state, &row_batch, &eos);
        EXPECT_TRUE(status.ok());

        if (!eos) {
            for (int i = 0; i < row_batch.num_rows(); ++i) {
                TupleRow* row = row_batch.get_row(i);
                LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);
            }
        }
    }

    status = scan_node.close(&runtime_state);
    EXPECT_TRUE(status.ok());
}
TEST_F(SchemaScanNodeTest, Prepare_fail_1) {
    SchemaScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    TableDescriptor* old = _desc_tbl->_tuple_desc_map[(TupleId)0]->_table_desc;
    _desc_tbl->_tuple_desc_map[(TupleId)0]->_table_desc = nullptr;
    Status status = scan_node.prepare(&runtime_state);
    EXPECT_FALSE(status.ok());
    _desc_tbl->_tuple_desc_map[(TupleId)0]->_table_desc = old;
}
TEST_F(SchemaScanNodeTest, Prepare_fail_2) {
    SchemaScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node._tuple_id = 1;
    Status status = scan_node.prepare(&runtime_state);
    EXPECT_FALSE(status.ok());
}
TEST_F(SchemaScanNodeTest, dummy) {
    SchemaTableDescriptor* t_desc =
            (SchemaTableDescriptor*)_desc_tbl->_tuple_desc_map[(TupleId)0]->_table_desc;
    t_desc->_schema_table_type = TSchemaTableType::SCH_EVENTS;
    SchemaScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&runtime_state);
    EXPECT_TRUE(status.ok());
    status = scan_node.prepare(&runtime_state);
    EXPECT_TRUE(status.ok());
    std::vector<TScanRangeParams> scan_ranges;
    status = scan_node.set_scan_ranges(scan_ranges);
    EXPECT_TRUE(status.ok());
    std::stringstream out;
    scan_node.debug_string(1, &out);
    LOG(WARNING) << out.str();

    status = scan_node.open(&runtime_state);
    EXPECT_TRUE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, 100);
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(&runtime_state, &row_batch, &eos);
        EXPECT_TRUE(status.ok());

        if (!eos) {
            for (int i = 0; i < row_batch.num_rows(); ++i) {
                TupleRow* row = row_batch.get_row(i);
                LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);
            }
        }
    }

    status = scan_node.close(&runtime_state);
    EXPECT_TRUE(status.ok());
    t_desc->_schema_table_type = TSchemaTableType::SCH_AUTHORS;
}
TEST_F(SchemaScanNodeTest, get_dest_desc_fail) {
    SchemaScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node._tuple_id = 1;
    Status status = scan_node.prepare(&runtime_state);
    EXPECT_FALSE(status.ok());
}
TEST_F(SchemaScanNodeTest, invalid_param) {
    SchemaScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(nullptr);
    EXPECT_FALSE(status.ok());
    status = scan_node.prepare(&runtime_state);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(nullptr);
    EXPECT_FALSE(status.ok());
    status = scan_node.open(&runtime_state);
    EXPECT_TRUE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, 100);
    bool eos;
    status = scan_node.get_next(nullptr, &row_batch, &eos);
    EXPECT_FALSE(status.ok());
}

TEST_F(SchemaScanNodeTest, no_init) {
    SchemaScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    //Status status = scan_node.prepare(&runtime_state);
    //EXPECT_TRUE(status.ok());
    Status status = scan_node.open(&runtime_state);
    EXPECT_FALSE(status.ok());
    RowBatch row_batch(scan_node._row_descriptor, 100);
    bool eos;
    status = scan_node.get_next(&runtime_state, &row_batch, &eos);
    EXPECT_FALSE(status.ok());
}

} // namespace doris
