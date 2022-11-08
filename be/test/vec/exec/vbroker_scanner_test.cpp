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
#include "vec/exec/vbroker_scanner.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/local_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"

namespace doris {

namespace vectorized {
class VBrokerScannerTest : public testing::Test {
public:
    VBrokerScannerTest() : _runtime_state(TQueryGlobals()) {
        init();
        _profile = _runtime_state.runtime_profile();
        _runtime_state.init_mem_trackers();

        TUniqueId unique_id;
        TQueryOptions query_options;
        query_options.__set_enable_vectorized_engine(true);
        TQueryGlobals query_globals;

        _runtime_state.init(unique_id, query_options, query_globals, nullptr);
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
    void init_params();

    TupleId _dst_tuple_id = 0;
    TupleId _src_tuple_id = 1;
    RuntimeState _runtime_state;
    RuntimeProfile* _profile;
    ObjectPool _obj_pool;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    std::vector<TNetworkAddress> _addresses;
    ScannerCounter _counter;
    std::vector<TExpr> _pre_filter;
};

void VBrokerScannerTest::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
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
        slot_desc.columnPos = 2;
        slot_desc.byteOffset = 8;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k3";
        slot_desc.slotIdx = 3;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = 12;
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
        slot_desc.nullIndicatorBit = 0;
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
        slot_desc.nullIndicatorBit = 1;
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
        slot_desc.columnPos = 2;
        slot_desc.byteOffset = 32;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 2;
        slot_desc.colName = "k3";
        slot_desc.slotIdx = 3;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    {
        // TTupleDescriptor source
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 1;
        t_tuple_desc.byteSize = 48;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void VBrokerScannerTest::init_params() {
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

    for (int i = 0; i < 3; ++i) {
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
        slot_ref.slot_ref.slot_id = 4 + i;
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(i + 1, expr);
        _params.src_slot_ids.push_back(4 + i);
    }
    // _params.__isset.expr_of_dest_slot = true;
    _params.__set_dest_tuple_id(_dst_tuple_id);
    _params.__set_src_tuple_id(_src_tuple_id);
}

void VBrokerScannerTest::init() {
    init_desc_table();
    init_params();
}

TEST_F(VBrokerScannerTest, normal) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 0;
    range.size = -1;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);
    VBrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                           &_counter);
    auto st = scanner.open();
    ASSERT_TRUE(st.ok());

    std::unique_ptr<vectorized::Block> block(new vectorized::Block());
    bool eof = false;
    st = scanner.get_next(block.get(), &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);
    auto columns = block->get_columns();
    ASSERT_EQ(columns.size(), 3);
    ASSERT_EQ(columns[0]->get_int(0), 1);
    ASSERT_EQ(columns[0]->get_int(1), 4);
    ASSERT_EQ(columns[0]->get_int(2), 8);

    ASSERT_EQ(columns[1]->get_int(0), 2);
    ASSERT_EQ(columns[1]->get_int(1), 5);
    ASSERT_EQ(columns[1]->get_int(2), 9);

    ASSERT_EQ(columns[2]->get_int(0), 3);
    ASSERT_EQ(columns[2]->get_int(1), 6);
    ASSERT_EQ(columns[2]->get_int(2), 10);
}

TEST_F(VBrokerScannerTest, normal_with_pre_filter) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 0;
    range.size = -1;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    // init pre_filter expr: k1 < '8'
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

    TExpr filter_expr;
    {
        TExprNode expr_node;
        expr_node.__set_node_type(TExprNodeType::BINARY_PRED);
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        expr_node.__set_num_children(2);
        expr_node.__isset.opcode = true;
        expr_node.__set_opcode(TExprOpcode::LT);
        expr_node.__isset.vector_opcode = true;
        expr_node.__set_vector_opcode(TExprOpcode::LT);
        expr_node.__isset.fn = true;
        expr_node.fn.name.function_name = "lt";
        expr_node.fn.binary_type = TFunctionBinaryType::BUILTIN;
        expr_node.fn.ret_type = int_type;
        expr_node.fn.has_var_args = false;
        filter_expr.nodes.push_back(expr_node);
    }
    {
        TExprNode expr_node;
        expr_node.__set_node_type(TExprNodeType::SLOT_REF);
        expr_node.type = varchar_type;
        expr_node.__set_num_children(0);
        expr_node.__isset.slot_ref = true;
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(4);
        slot_ref.__set_tuple_id(1);
        expr_node.__set_slot_ref(slot_ref);
        expr_node.__isset.output_column = true;
        expr_node.__set_output_column(0);
        filter_expr.nodes.push_back(expr_node);
    }
    {
        TExprNode expr_node;
        expr_node.__set_node_type(TExprNodeType::STRING_LITERAL);
        expr_node.type = varchar_type;
        expr_node.__set_num_children(0);
        expr_node.__isset.string_literal = true;
        TStringLiteral string_literal;
        string_literal.__set_value("8");
        expr_node.__set_string_literal(string_literal);
        filter_expr.nodes.push_back(expr_node);
    }
    _pre_filter.push_back(filter_expr);
    VBrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                           &_counter);
    auto st = scanner.open();
    ASSERT_TRUE(st.ok());

    std::unique_ptr<vectorized::Block> block(new vectorized::Block());
    bool eof = false;
    // end of file
    st = scanner.get_next(block.get(), &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);
    auto columns = block->get_columns();
    ASSERT_EQ(columns.size(), 3);

    ASSERT_EQ(columns[0]->get_int(0), 1);
    ASSERT_EQ(columns[0]->get_int(1), 4);

    ASSERT_EQ(columns[1]->get_int(0), 2);
    ASSERT_EQ(columns[1]->get_int(1), 5);

    ASSERT_EQ(columns[2]->get_int(0), 3);
    ASSERT_EQ(columns[2]->get_int(1), 6);
}

TEST_F(VBrokerScannerTest, normal2) {
    std::vector<TBrokerRangeDesc> ranges;

    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal2_1.csv";
    range.start_offset = 0;
    range.size = 7;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    range.path = "./be/test/exec/test_data/broker_scanner/normal2_2.csv";
    range.start_offset = 0;
    range.size = 4;
    ranges.push_back(range);
    VBrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                           &_counter);
    auto st = scanner.open();
    ASSERT_TRUE(st.ok());

    std::unique_ptr<vectorized::Block> block(new vectorized::Block());
    bool eof = false;
    st = scanner.get_next(block.get(), &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);
    auto columns = block->get_columns();
    ASSERT_EQ(columns.size(), 3);

    ASSERT_EQ(columns[0]->get_int(0), 1);
    ASSERT_EQ(columns[0]->get_int(1), 3);

    ASSERT_EQ(columns[1]->get_int(0), 2);
    ASSERT_EQ(columns[1]->get_int(1), 4);

    ASSERT_EQ(columns[2]->get_int(0), 3);
    ASSERT_EQ(columns[2]->get_int(1), 5);
}

TEST_F(VBrokerScannerTest, normal5) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 0;
    range.size = 0;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);
    VBrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                           &_counter);
    auto st = scanner.open();
    ASSERT_TRUE(st.ok());

    std::unique_ptr<vectorized::Block> block(new vectorized::Block());
    bool eof = false;
    // end of file
    st = scanner.get_next(block.get(), &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);
    auto columns = block->get_columns();
    ASSERT_EQ(columns.size(), 0);
}

} // namespace vectorized
} // namespace doris
