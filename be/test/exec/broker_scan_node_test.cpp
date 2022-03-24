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

#include "exec/broker_scan_node.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"

namespace doris {

class BrokerScanNodeTest : public testing::Test {
public:
    BrokerScanNodeTest() : _runtime_state(TQueryGlobals()) {
        init();
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
    }
    void init();
    static void SetUpTestCase() {
        UserFunctionCache::instance()->init(
                "./be/test/runtime/test_data/user_function_cache/normal");
        CastFunctions::init();
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}

private:
    void init_desc_table();
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
    std::map<std::string, SlotDescriptor*> _slots_map;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
};

void BrokerScanNodeTest::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::MYSQL_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 1;
    // TSlotDescriptor
    // int offset = 1;
    // int i = 0;
    // k1
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 0;
        slot_desc.byteOffset = 0;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k1";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k2
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 4;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k2";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k3
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 8;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k3";
        slot_desc.slotIdx = 3;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k4(partitioned column)
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 12;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k4";
        slot_desc.slotIdx = 4;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = 16;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }

    // source tuple descriptor
    // TSlotDescriptor
    // int offset = 1;
    // int i = 0;
    // k1
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 0;
        slot_desc.byteOffset = 0;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k1";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k2
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 16;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k2";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k3
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 32;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k3";
        slot_desc.slotIdx = 3;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k4(partitioned column)
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 48;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k4";
        slot_desc.slotIdx = 4;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    {
        // TTupleDescriptor source
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 1;
        t_tuple_desc.byteSize = 64;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void BrokerScanNodeTest::init() {
    _params.column_separator = ',';
    _params.line_delimiter = '\n';

    TTypeDesc int_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        node.__set_scalar_type(scalar_type);
        int_type.types.push_back(node);
    }
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(5000);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }

    for (int i = 0; i < 4; ++i) {
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttoint";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 5 + i;
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(i + 1, expr);
        _params.src_slot_ids.push_back(5 + i);
    }
    // _params.__isset.expr_of_dest_slot = true;
    _params.__set_dest_tuple_id(0);
    _params.__set_src_tuple_id(1);

    init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.broker_scan_node.tuple_id = 0;
    _tnode.__isset.broker_scan_node = true;
}

TEST_F(BrokerScanNodeTest, normal) {
    BrokerScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node.init(_tnode);
    auto status = scan_node.prepare(&_runtime_state);
    ASSERT_TRUE(status.ok());

    // set scan range
    std::vector<TScanRangeParams> scan_ranges;

    {
        TScanRangeParams scan_range_params;

        TBrokerScanRange broker_scan_range;
        broker_scan_range.params = _params;

        TBrokerRangeDesc range;
        range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
        range.start_offset = 0;
        range.size = -1;
        range.file_type = TFileType::FILE_LOCAL;
        range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
        range.splittable = true;
        std::vector<std::string> columns_from_path{"1"};
        range.__set_columns_from_path(columns_from_path);
        range.__set_num_of_columns_from_file(3);
        broker_scan_range.ranges.push_back(range);

        scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);

        scan_ranges.push_back(scan_range_params);
    }
    {
        TScanRangeParams scan_range_params;

        TBrokerScanRange broker_scan_range;
        broker_scan_range.params = _params;

        TBrokerRangeDesc range;
        range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
        range.start_offset = 1;
        range.size = 7;
        range.file_type = TFileType::FILE_LOCAL;
        range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
        range.splittable = true;
        std::vector<std::string> columns_from_path{"2"};
        range.__set_columns_from_path(columns_from_path);
        range.__set_num_of_columns_from_file(3);
        broker_scan_range.ranges.push_back(range);

        scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);

        scan_ranges.push_back(scan_range_params);
    }

    scan_node.set_scan_ranges(scan_ranges);

    status = scan_node.open(&_runtime_state);
    ASSERT_TRUE(status.ok());

    // Get batch
    RowBatch batch(scan_node.row_desc(), _runtime_state.batch_size());

    bool eos = false;
    status = scan_node.get_next(&_runtime_state, &batch, &eos);
    ASSERT_EQ(3, batch.num_rows());
    ASSERT_FALSE(eos);

    batch.reset();
    status = scan_node.get_next(&_runtime_state, &batch, &eos);
    ASSERT_EQ(1, batch.num_rows());
    ASSERT_FALSE(eos);

    batch.reset();
    status = scan_node.get_next(&_runtime_state, &batch, &eos);
    ASSERT_EQ(0, batch.num_rows());
    ASSERT_TRUE(eos);

    scan_node.close(&_runtime_state);
    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }
}

} // namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    // init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
