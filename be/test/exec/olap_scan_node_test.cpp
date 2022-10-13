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

#include "exec/olap_scan_node.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <vector>

#include "exec/text_converter.hpp"
#include "exprs/binary_predicate.h"
#include "exprs/in_predicate.h"
#include "exprs/int_literal.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {

class OlapScanNodeTest : public ::testing::Test {
public:
    OlapScanNodeTest() : _runtime_stat("test") {}

    virtual void SetUp() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.disable_codegen = true;
        ExecEnv* exec_env = new ExecEnv();
        _runtime_stat.init(fragment_id, query_options, "test", exec_env);

        TDescriptorTable t_desc_table;

        // table descriptors
        TTableDescriptor t_table_desc;

        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::OLAP_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_table_desc.olapTable.tableName = "";
        t_table_desc.tableName = "";
        t_table_desc.dbName = "";
        t_table_desc.__isset.mysqlTable = true;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;
        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // UserId
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(to_thrift(TYPE_INT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("UserId");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;
        // UrlId
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(to_thrift(TYPE_BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("UrlId");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
        }
        ++i;
        // Date
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(to_thrift(TYPE_INT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("Date");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int32_t);
        }
        ++i;
        // PageViews
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_id(i);
            t_slot_desc.__set_slotType(to_thrift(TYPE_BIGINT));
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.__set_colName("PageViews");
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int64_t);
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

        _runtime_stat.set_desc_tbl(_desc_tbl);

        // Node Id
        _tnode.node_id = 0;
        _tnode.node_type = TPlanNodeType::OLAP_SCAN_NODE;
        _tnode.num_children = 0;
        _tnode.limit = 100;
        _tnode.row_tuples.push_back(0);
        _tnode.nullable_tuples.push_back(false);
        _tnode.tuple_ids.push_back(0);
        _tnode.olap_scan_node.tuple_id = 0;
        _tnode.olap_scan_node.key_column_name.push_back("UserId");
        _tnode.olap_scan_node.key_column_name.push_back("UrlId");
        _tnode.olap_scan_node.key_column_name.push_back("Date");
        _tnode.olap_scan_node.key_column_type.push_back(to_thrift(TYPE_INT));
        _tnode.olap_scan_node.key_column_type.push_back(to_thrift(TYPE_BIGINT));
        _tnode.olap_scan_node.key_column_type.push_back(to_thrift(TYPE_INT));
        _tnode.__isset.olap_scan_node = true;

        {
            TScanRangeParams param;
            TPaloScanRange doris_scan_range;
            TNetworkAddress host;
            host.__set_hostname("host");
            host.__set_port(9999);
            doris_scan_range.hosts.push_back(host);
            doris_scan_range.__set_schema_hash("462300563");
            doris_scan_range.__set_version("94");
            // Useless but it is required in TPaloScanRange
            doris_scan_range.__set_version_hash("0");
            doris_scan_range.engine_table_name.push_back("DorisTestStats");
            doris_scan_range.__set_db_name("olap");
            //TKeyRange key_range;
            //key_range.__set_column_type(to_thrift(TYPE_BIGINT));
            //key_range.__set_begin_key(-5000);
            //key_range.__set_end_key(5000);
            //key_range.__set_column_name("UrlId");
            //doris_scan_range.partition_column_ranges.push_back(key_range);
            param.scan_range.__set_doris_scan_range(doris_scan_range);
            _scan_ranges.push_back(param);
        }
    }

    virtual void TearDown() {}

private:
    TPlanNode _tnode;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_stat;
    std::vector<TScanRangeParams> _scan_ranges;
};

TEST_F(OlapScanNodeTest, NormalUse) {
    OlapScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    Status status = scan_node.prepare(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_node.set_scanRanges(_scan_ranges).ok());

    RowBatch row_batch(scan_node._row_descriptor, _runtime_stat.batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(&_runtime_stat, &row_batch, &eos);
        EXPECT_TRUE(status.ok());
        int num = std::min(row_batch.num_rows(), 10);
        EXPECT_TRUE(num > 0);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);

            if (0 == i) {
                EXPECT_EQ("[(-5000 -5000 -5000 1)]", print_row(row, scan_node._row_descriptor));
            }
        }

        eos = true;
    }

    EXPECT_TRUE(scan_node.close(&_runtime_stat).ok());
}

TEST_F(OlapScanNodeTest, PushDownBinaryPredicate) {
    OlapScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);

    TExprNode binary_node;
    binary_node.node_type = TExprNodeType::BINARY_PRED;
    binary_node.type = to_tcolumn_type_thrift(TPrimitiveType::BOOLEAN);
    binary_node.num_children = 2;
    binary_node.opcode = TExprOpcode::LT_INT_INT;
    binary_node.__isset.opcode = true;

    BinaryPredicate bin_pre(binary_node);

    TExprNode slot_node;
    slot_node.node_type = TExprNodeType::SLOT_REF;
    slot_node.type = to_tcolumn_type_thrift(TPrimitiveType::INT);
    slot_node.num_children = 0;
    slot_node.slot_ref.slot_id = 0;
    slot_node.slot_ref.tuple_id = 0;
    slot_node.__isset.slot_ref = true;

    std::vector<TTupleId> row_tuples;
    row_tuples.push_back(0);
    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    RowDescriptor row_desc(*_desc_tbl, row_tuples, nullable_tuples);

    bin_pre._children.push_back(_obj_pool.add(new SlotRef(slot_node)));
    int v = -4999;
    bin_pre._children.push_back(_obj_pool.add(new IntLiteral(TYPE_INT, &v)));

    Status status = bin_pre.prepare(&_runtime_stat, row_desc);
    EXPECT_TRUE(status.ok());

    std::list<Expr*> exprs;
    exprs.push_back(&bin_pre);

    scan_node.push_down_predicate(&_runtime_stat, &exprs);

    status = scan_node.prepare(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_node.set_scan_ranges(_scan_ranges).ok());

    RowBatch row_batch(scan_node._row_descriptor, _runtime_stat.batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(&_runtime_stat, &row_batch, &eos);
        EXPECT_TRUE(status.ok());
        int num = std::min(row_batch.num_rows(), 10);
        EXPECT_TRUE(num > 0);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);
            EXPECT_EQ("[(-5000 -5000 -5000 1)]", print_row(row, scan_node._row_descriptor));
        }

        eos = true;
    }

    EXPECT_TRUE(scan_node.close(&_runtime_stat).ok());
}

TEST_F(OlapScanNodeTest, PushDownBinaryEqualPredicate) {
    OlapScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);

    TExprNode binary_node;
    binary_node.node_type = TExprNodeType::BINARY_PRED;
    binary_node.type = to_tcolumn_type_thrift(TPrimitiveType::BOOLEAN);
    binary_node.num_children = 2;
    binary_node.opcode = TExprOpcode::EQ_INT_INT;
    binary_node.__isset.opcode = true;

    BinaryPredicate bin_pre(binary_node);

    TExprNode slot_node;
    slot_node.node_type = TExprNodeType::SLOT_REF;
    slot_node.type = to_tcolumn_type_thrift(TPrimitiveType::INT);
    slot_node.num_children = 0;
    slot_node.slot_ref.slot_id = 0;
    slot_node.slot_ref.tuple_id = 0;
    slot_node.__isset.slot_ref = true;

    std::vector<TTupleId> row_tuples;
    row_tuples.push_back(0);
    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    RowDescriptor row_desc(*_desc_tbl, row_tuples, nullable_tuples);

    bin_pre._children.push_back(_obj_pool.add(new SlotRef(slot_node)));
    int v = -5000;
    bin_pre._children.push_back(_obj_pool.add(new IntLiteral(TYPE_INT, &v)));

    Status status = bin_pre.prepare(&_runtime_stat, row_desc);
    EXPECT_TRUE(status.ok());

    std::list<Expr*> exprs;
    exprs.push_back(&bin_pre);

    scan_node.push_down_predicate(&_runtime_stat, &exprs);

    status = scan_node.prepare(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_node.set_scan_ranges(_scan_ranges).ok());

    RowBatch row_batch(scan_node._row_descriptor, _runtime_stat.batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(&_runtime_stat, &row_batch, &eos);
        EXPECT_TRUE(status.ok());
        int num = std::min(row_batch.num_rows(), 10);
        EXPECT_TRUE(num > 0);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);
            EXPECT_EQ("[(-5000 -5000 -5000 1)]", print_row(row, scan_node._row_descriptor));
        }

        eos = true;
    }

    EXPECT_TRUE(scan_node.close(&_runtime_stat).ok());
}

TEST_F(OlapScanNodeTest, PushDownInPredicateCase) {
    OlapScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);

    TExprNode in_node;
    in_node.node_type = TExprNodeType::IN_PRED;
    in_node.type = to_tcolumn_type_thrift(TPrimitiveType::BOOLEAN);
    in_node.num_children = 0;
    in_node.in_predicate.is_not_in = false;
    in_node.__isset.in_predicate = true;
    InPredicate in_pre(in_node);
    TExprNode slot_node;
    slot_node.node_type = TExprNodeType::SLOT_REF;
    slot_node.type = to_tcolumn_type_thrift(TPrimitiveType::INT);
    slot_node.num_children = 0;
    slot_node.slot_ref.slot_id = 0;
    slot_node.slot_ref.tuple_id = 0;
    slot_node.__isset.slot_ref = true;

    std::vector<TTupleId> row_tuples;
    row_tuples.push_back(0);
    std::vector<bool> nullable_tuples;
    nullable_tuples.push_back(false);
    RowDescriptor row_desc(*_desc_tbl, row_tuples, nullable_tuples);

    in_pre._children.push_back(_obj_pool.add(new SlotRef(slot_node)));

    Status status = in_pre.prepare(&_runtime_stat, row_desc);
    EXPECT_TRUE(status.ok());

    for (int i = -5000; i < -4999; ++i) {
        in_pre.insert(&i);
    }

    std::list<Expr*> exprs;
    exprs.push_back(&in_pre);

    scan_node.push_down_predicate(&_runtime_stat, &exprs);

    status = scan_node.prepare(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    status = scan_node.open(&_runtime_stat);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(scan_node.set_scan_ranges(_scan_ranges).ok());

    RowBatch row_batch(scan_node._row_descriptor, _runtime_stat.batch_size());
    bool eos = false;

    while (!eos) {
        status = scan_node.get_next(&_runtime_stat, &row_batch, &eos);
        EXPECT_TRUE(status.ok());
        int num = std::min(row_batch.num_rows(), 10);
        EXPECT_TRUE(num > 0);

        for (int i = 0; i < num; ++i) {
            TupleRow* row = row_batch.get_row(i);
            LOG(WARNING) << "input row: " << print_row(row, scan_node._row_descriptor);
            EXPECT_EQ("[(-5000 -5000 -5000 1)]", print_row(row, scan_node._row_descriptor));
        }

        eos = true;
    }

    EXPECT_TRUE(scan_node.close(&_runtime_stat).ok());
}

} // namespace doris
