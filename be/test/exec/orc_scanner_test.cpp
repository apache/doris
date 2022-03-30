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

#include "exec/orc_scanner.h"

#include <gtest/gtest.h>
#include <runtime/descriptor_helper.h>
#include <time.h>

#include <map>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exec/broker_scan_node.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "exprs/decimalv2_operators.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"

namespace doris {

class OrcScannerTest : public testing::Test {
public:
    OrcScannerTest() : _runtime_state(TQueryGlobals()) {
        _profile = _runtime_state.runtime_profile();
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
    }

    static void SetUpTestCase() {
        UserFunctionCache::instance()->init(
                "./be/test/runtime/test_data/user_function_cache/normal");
        CastFunctions::init();
        DecimalV2Operators::init();
    }

protected:
    virtual void SetUp() {}

    virtual void TearDown() {}

private:
    RuntimeState _runtime_state;
    RuntimeProfile* _profile;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    std::vector<TNetworkAddress> _addresses;
    ScannerCounter _counter;
    std::vector<TExpr> _pre_filter;
    bool _fill_tuple;
};

TEST_F(OrcScannerTest, normal) {
    TBrokerScanRangeParams params;
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(65535);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }

    TTypeDesc int_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        node.__set_scalar_type(scalar_type);
        int_type.types.push_back(node);
    }

    TTypeDesc big_int_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::BIGINT);
        node.__set_scalar_type(scalar_type);
        big_int_type.types.push_back(node);
    }

    TTypeDesc float_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::FLOAT);
        node.__set_scalar_type(scalar_type);
        float_type.types.push_back(node);
    }

    TTypeDesc double_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::DOUBLE);
        node.__set_scalar_type(scalar_type);
        double_type.types.push_back(node);
    }

    TTypeDesc date_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::DATE);
        node.__set_scalar_type(scalar_type);
        date_type.types.push_back(node);
    }

    //col1 varchar -> bigint
    {
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = big_int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttobigint";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = big_int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_big_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 0;
        slot_ref.slot_ref.tuple_id = 0;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        params.expr_of_dest_slot.emplace(8, expr);
        params.src_slot_ids.push_back(0);
    }
    //col2, col3
    for (int i = 1; i <= 2; i++) {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = i;
        slot_ref.slot_ref.tuple_id = 0;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        params.expr_of_dest_slot.emplace(8 + i, expr);
        params.src_slot_ids.push_back(i);
    }

    //col5 varchar -> double
    {
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = double_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttodouble";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = double_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_double_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 3;
        slot_ref.slot_ref.tuple_id = 0;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        params.expr_of_dest_slot.emplace(11, expr);
        params.src_slot_ids.push_back(3);
    }

    //col6 varchar -> float
    {
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = float_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttofloat";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = float_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_float_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 4;
        slot_ref.slot_ref.tuple_id = 0;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        params.expr_of_dest_slot.emplace(12, expr);
        params.src_slot_ids.push_back(4);
    }
    //col7,col8
    for (int i = 5; i <= 6; i++) {
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
        slot_ref.slot_ref.slot_id = i;
        slot_ref.slot_ref.tuple_id = 0;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        params.expr_of_dest_slot.emplace(8 + i, expr);
        params.src_slot_ids.push_back(i);
    }

    //col9 varchar -> var
    {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 7;
        slot_ref.slot_ref.tuple_id = 0;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        params.expr_of_dest_slot.emplace(15, expr);
        params.src_slot_ids.push_back(7);
    }

    params.__set_src_tuple_id(0);
    params.__set_dest_tuple_id(1);

    //init_desc_table
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::BROKER_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder src_tuple_builder;
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col1")
                                       .column_pos(1)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col2")
                                       .column_pos(2)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col3")
                                       .column_pos(3)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col5")
                                       .column_pos(4)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col6")
                                       .column_pos(5)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col7")
                                       .column_pos(6)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col8")
                                       .column_pos(7)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col9")
                                       .column_pos(8)
                                       .build());
    src_tuple_builder.build(&dtb);

    TTupleDescriptorBuilder dest_tuple_builder;
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("col1").column_pos(1).build());
    dest_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                        .string_type(65535)
                                        .nullable(true)
                                        .column_name("col2")
                                        .column_pos(2)
                                        .build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().string_type(65535).column_name("col3").column_pos(3).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DOUBLE).column_name("col5").column_pos(4).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_FLOAT).column_name("col6").column_pos(5).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("col7").column_pos(6).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("col8").column_pos(7).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().string_type(65535).column_name("col9").column_pos(8).build());
    dest_tuple_builder.build(&dtb);
    t_desc_table = dtb.desc_tbl();

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);
    _runtime_state.set_desc_tbl(_desc_tbl);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc rangeDesc;
    rangeDesc.start_offset = 0;
    rangeDesc.size = -1;
    rangeDesc.format_type = TFileFormatType::FORMAT_ORC;
    rangeDesc.splittable = false;

    rangeDesc.path = "./be/test/exec/test_data/orc_scanner/my-file.orc";
    rangeDesc.file_type = TFileType::FILE_LOCAL;
    ranges.push_back(rangeDesc);

    ORCScanner scanner(&_runtime_state, _profile, params, ranges, _addresses, _pre_filter,
                       &_counter);
    ASSERT_TRUE(scanner.open().ok());

    auto tracker = std::make_shared<MemTracker>();
    MemPool tuple_pool(tracker.get());

    Tuple* tuple = (Tuple*)tuple_pool.allocate(_desc_tbl->get_tuple_descriptor(1)->byte_size());
    bool eof = false;

    ASSERT_TRUE(scanner.get_next(tuple, &tuple_pool, &eof, &_fill_tuple).ok());
    ASSERT_EQ(Tuple::to_string(tuple, *_desc_tbl->get_tuple_descriptor(1)),
              "(0 null doris      1.567 1.567000031471252 12345 1 doris)");
    ASSERT_TRUE(scanner.get_next(tuple, &tuple_pool, &eof, &_fill_tuple).ok());
    ASSERT_EQ(Tuple::to_string(tuple, *_desc_tbl->get_tuple_descriptor(1)),
              "(1 true doris      1.567 1.567000031471252 12345 1 doris)");
    ASSERT_FALSE(eof);
    for (int i = 2; i < 10; i++) {
        ASSERT_TRUE(scanner.get_next(tuple, &tuple_pool, &eof, &_fill_tuple).ok());
    }
    ASSERT_TRUE(scanner.get_next(tuple, &tuple_pool, &eof, &_fill_tuple).ok());
    ASSERT_TRUE(eof);
    scanner.close();
}

TEST_F(OrcScannerTest, normal2) {
    TBrokerScanRangeParams params;
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(65535);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }

    TTypeDesc int_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        node.__set_scalar_type(scalar_type);
        int_type.types.push_back(node);
    }

    {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 1;
        slot_ref.slot_ref.tuple_id = 0;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        params.expr_of_dest_slot.emplace(3, expr);
        params.src_slot_ids.push_back(1);
    }
    params.__set_src_tuple_id(0);
    params.__set_dest_tuple_id(1);

    //init_desc_table
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::BROKER_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder src_tuple_builder;
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col1")
                                       .column_pos(1)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col2")
                                       .column_pos(2)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col3")
                                       .column_pos(3)
                                       .build());
    src_tuple_builder.build(&dtb);
    TTupleDescriptorBuilder dest_tuple_builder;
    dest_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                        .string_type(65535)
                                        .column_name("value_from_col2")
                                        .column_pos(1)
                                        .build());

    dest_tuple_builder.build(&dtb);
    t_desc_table = dtb.desc_tbl();

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);
    _runtime_state.set_desc_tbl(_desc_tbl);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc rangeDesc;
    rangeDesc.start_offset = 0;
    rangeDesc.size = -1;
    rangeDesc.format_type = TFileFormatType::FORMAT_ORC;
    rangeDesc.splittable = false;

    rangeDesc.path = "./be/test/exec/test_data/orc_scanner/my-file.orc";
    rangeDesc.file_type = TFileType::FILE_LOCAL;
    ranges.push_back(rangeDesc);

    ORCScanner scanner(&_runtime_state, _profile, params, ranges, _addresses, _pre_filter,
                       &_counter);
    ASSERT_TRUE(scanner.open().ok());

    auto tracker = std::make_shared<MemTracker>();
    MemPool tuple_pool(tracker.get());

    Tuple* tuple = (Tuple*)tuple_pool.allocate(_desc_tbl->get_tuple_descriptor(1)->byte_size());
    bool eof = false;
    ASSERT_TRUE(scanner.get_next(tuple, &tuple_pool, &eof, &_fill_tuple).ok());
    ASSERT_EQ(Tuple::to_string(tuple, *_desc_tbl->get_tuple_descriptor(1)), "(null)");
    ASSERT_TRUE(scanner.get_next(tuple, &tuple_pool, &eof, &_fill_tuple).ok());
    ASSERT_EQ(Tuple::to_string(tuple, *_desc_tbl->get_tuple_descriptor(1)), "(true)");
    scanner.close();
}

TEST_F(OrcScannerTest, normal3) {
    TBrokerScanRangeParams params;
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(65535);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }

    TTypeDesc decimal_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::DECIMALV2);
        scalar_type.__set_precision(64);
        scalar_type.__set_scale(64);
        node.__set_scalar_type(scalar_type);
        decimal_type.types.push_back(node);
    }

    TTypeDesc tinyint_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::TINYINT);
        node.__set_scalar_type(scalar_type);
        tinyint_type.types.push_back(node);
    }

    TTypeDesc datetime_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::DATETIME);
        node.__set_scalar_type(scalar_type);
        datetime_type.types.push_back(node);
    }

    TTypeDesc date_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::DATE);
        node.__set_scalar_type(scalar_type);
        date_type.types.push_back(node);
    }

    {
        for (int i = 0; i < 5; ++i) {
            TExprNode cast_expr;
            cast_expr.node_type = TExprNodeType::CAST_EXPR;
            cast_expr.type = decimal_type;
            cast_expr.__set_opcode(TExprOpcode::CAST);
            cast_expr.__set_num_children(1);
            cast_expr.__set_output_scale(-1);
            cast_expr.__isset.fn = true;
            cast_expr.fn.name.function_name = "casttodecimalv2";
            cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
            cast_expr.fn.arg_types.push_back(varchar_type);
            cast_expr.fn.ret_type = decimal_type;
            cast_expr.fn.has_var_args = false;
            cast_expr.fn.__set_signature("cast_to_decimalv2_val(VARCHAR(*))");
            cast_expr.fn.__isset.scalar_fn = true;
            cast_expr.fn.scalar_fn.symbol = "doris::DecimalV2Operators::cast_to_decimalv2_val";

            TExprNode slot_ref;
            slot_ref.node_type = TExprNodeType::SLOT_REF;
            slot_ref.type = varchar_type;
            slot_ref.num_children = 0;
            slot_ref.__isset.slot_ref = true;
            slot_ref.slot_ref.slot_id = i;
            slot_ref.slot_ref.tuple_id = 0;

            TExpr expr;
            expr.nodes.push_back(cast_expr);
            expr.nodes.push_back(slot_ref);

            params.expr_of_dest_slot.emplace(9 + i, expr);
            params.src_slot_ids.push_back(i);
        }

        {
            TExprNode cast_expr;
            cast_expr.node_type = TExprNodeType::CAST_EXPR;
            cast_expr.type = tinyint_type;
            cast_expr.__set_opcode(TExprOpcode::CAST);
            cast_expr.__set_num_children(1);
            cast_expr.__set_output_scale(-1);
            cast_expr.__isset.fn = true;
            cast_expr.fn.name.function_name = "casttotinyint";
            cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
            cast_expr.fn.arg_types.push_back(varchar_type);
            cast_expr.fn.ret_type = tinyint_type;
            cast_expr.fn.has_var_args = false;
            cast_expr.fn.__set_signature("cast_to_tiny_int_val(VARCHAR(*))");
            cast_expr.fn.__isset.scalar_fn = true;
            cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_tiny_int_val";

            TExprNode slot_ref;
            slot_ref.node_type = TExprNodeType::SLOT_REF;
            slot_ref.type = varchar_type;
            slot_ref.num_children = 0;
            slot_ref.__isset.slot_ref = true;
            slot_ref.slot_ref.slot_id = 5;
            slot_ref.slot_ref.tuple_id = 0;

            TExpr expr;
            expr.nodes.push_back(cast_expr);
            expr.nodes.push_back(slot_ref);

            params.expr_of_dest_slot.emplace(14, expr);
            params.src_slot_ids.push_back(5);
        }

        {
            TExprNode cast_expr;
            cast_expr.node_type = TExprNodeType::CAST_EXPR;
            cast_expr.type = datetime_type;
            cast_expr.__set_opcode(TExprOpcode::CAST);
            cast_expr.__set_num_children(1);
            cast_expr.__set_output_scale(-1);
            cast_expr.__isset.fn = true;
            cast_expr.fn.name.function_name = "casttodatetime";
            cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
            cast_expr.fn.arg_types.push_back(varchar_type);
            cast_expr.fn.ret_type = datetime_type;
            cast_expr.fn.has_var_args = false;
            cast_expr.fn.__set_signature("cast_to_datetime_val(VARCHAR(*))");
            cast_expr.fn.__isset.scalar_fn = true;
            cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_datetime_val";

            TExprNode slot_ref;
            slot_ref.node_type = TExprNodeType::SLOT_REF;
            slot_ref.type = varchar_type;
            slot_ref.num_children = 0;
            slot_ref.__isset.slot_ref = true;
            slot_ref.slot_ref.slot_id = 6;
            slot_ref.slot_ref.tuple_id = 0;

            TExpr expr;
            expr.nodes.push_back(cast_expr);
            expr.nodes.push_back(slot_ref);

            params.expr_of_dest_slot.emplace(15, expr);
            params.src_slot_ids.push_back(6);
        }
        {
            TExprNode cast_expr;
            cast_expr.node_type = TExprNodeType::CAST_EXPR;
            cast_expr.type = date_type;
            cast_expr.__set_opcode(TExprOpcode::CAST);
            cast_expr.__set_num_children(1);
            cast_expr.__set_output_scale(-1);
            cast_expr.__isset.fn = true;
            cast_expr.fn.name.function_name = "casttodate";
            cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
            cast_expr.fn.arg_types.push_back(varchar_type);
            cast_expr.fn.ret_type = date_type;
            cast_expr.fn.has_var_args = false;
            cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
            cast_expr.fn.__isset.scalar_fn = true;
            cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_date_val";

            TExprNode slot_ref;
            slot_ref.node_type = TExprNodeType::SLOT_REF;
            slot_ref.type = varchar_type;
            slot_ref.num_children = 0;
            slot_ref.__isset.slot_ref = true;
            slot_ref.slot_ref.slot_id = 7;
            slot_ref.slot_ref.tuple_id = 0;

            TExpr expr;
            expr.nodes.push_back(cast_expr);
            expr.nodes.push_back(slot_ref);

            params.expr_of_dest_slot.emplace(16, expr);
            params.src_slot_ids.push_back(7);
        }
        {
            TExprNode cast_expr;
            cast_expr.node_type = TExprNodeType::CAST_EXPR;
            cast_expr.type = decimal_type;
            cast_expr.__set_opcode(TExprOpcode::CAST);
            cast_expr.__set_num_children(1);
            cast_expr.__set_output_scale(-1);
            cast_expr.__isset.fn = true;
            cast_expr.fn.name.function_name = "casttodecimalv2";
            cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
            cast_expr.fn.arg_types.push_back(varchar_type);
            cast_expr.fn.ret_type = decimal_type;
            cast_expr.fn.has_var_args = false;
            cast_expr.fn.__set_signature("cast_to_decimalv2_val(VARCHAR(*))");
            cast_expr.fn.__isset.scalar_fn = true;
            cast_expr.fn.scalar_fn.symbol = "doris::DecimalV2Operators::cast_to_decimalv2_val";

            TExprNode slot_ref;
            slot_ref.node_type = TExprNodeType::SLOT_REF;
            slot_ref.type = varchar_type;
            slot_ref.num_children = 0;
            slot_ref.__isset.slot_ref = true;
            slot_ref.slot_ref.slot_id = 8;
            slot_ref.slot_ref.tuple_id = 0;

            TExpr expr;
            expr.nodes.push_back(cast_expr);
            expr.nodes.push_back(slot_ref);

            params.expr_of_dest_slot.emplace(17, expr);
            params.src_slot_ids.push_back(8);
        }
    }
    params.__set_src_tuple_id(0);
    params.__set_dest_tuple_id(1);

    //init_desc_table
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::BROKER_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder src_tuple_builder;
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col1")
                                       .column_pos(1)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col2")
                                       .column_pos(2)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col3")
                                       .column_pos(3)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col4")
                                       .column_pos(4)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col5")
                                       .column_pos(5)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col6")
                                       .column_pos(6)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col7")
                                       .column_pos(7)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col8")
                                       .column_pos(8)
                                       .build());
    src_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65535)
                                       .nullable(true)
                                       .column_name("col9")
                                       .column_pos(9)
                                       .build());
    src_tuple_builder.build(&dtb);

    TTupleDescriptorBuilder dest_tuple_builder;
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(10, 9).column_name("col1").column_pos(1).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(7, 5).column_name("col2").column_pos(2).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(10, 9).column_name("col3").column_pos(3).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(10, 5).column_name("col4").column_pos(4).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(10, 5).column_name("col5").column_pos(5).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("col6").column_pos(6).build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("col7").column_pos(7).build());
    dest_tuple_builder.add_slot(TSlotDescriptorBuilder()
                                        .type(TYPE_DATE)
                                        .nullable(true)
                                        .column_name("col8")
                                        .column_pos(8)
                                        .build());
    dest_tuple_builder.add_slot(
            TSlotDescriptorBuilder().decimal_type(27, 9).column_name("col9").column_pos(9).build());

    dest_tuple_builder.build(&dtb);
    t_desc_table = dtb.desc_tbl();

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);
    _runtime_state.set_desc_tbl(_desc_tbl);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc rangeDesc;
    rangeDesc.start_offset = 0;
    rangeDesc.size = -1;
    rangeDesc.format_type = TFileFormatType::FORMAT_ORC;
    rangeDesc.splittable = false;

    rangeDesc.path = "./be/test/exec/test_data/orc_scanner/decimal_and_timestamp.orc";
    rangeDesc.file_type = TFileType::FILE_LOCAL;
    ranges.push_back(rangeDesc);

    ORCScanner scanner(&_runtime_state, _profile, params, ranges, _addresses, _pre_filter,
                       &_counter);
    ASSERT_TRUE(scanner.open().ok());

    auto tracker = std::make_shared<MemTracker>();
    MemPool tuple_pool(tracker.get());

    Tuple* tuple = (Tuple*)tuple_pool.allocate(_desc_tbl->get_tuple_descriptor(1)->byte_size());
    bool eof = false;
    ASSERT_TRUE(scanner.get_next(tuple, &tuple_pool, &eof, &_fill_tuple).ok());
    ASSERT_EQ(Tuple::to_string(tuple, *_desc_tbl->get_tuple_descriptor(1)),
              "(0.123456789 1.12 -1.12345 0.12345 0 1 2020-01-14 14:12:19 2020-02-10 "
              "-0.0014)");
    scanner.close();
}

} // end namespace doris
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
